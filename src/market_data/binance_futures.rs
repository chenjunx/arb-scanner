use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use log::{debug, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::net::connect_tcp;
use crate::market_data::orderbook::OrderBookLevels;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{
    Kline, KlineInterval, KlineProvider, MarketDataSource, RANGE_MAX_PAGES, closed_klines, closed_klines_in_range, now_ms,
};

const MAINNET_WS_HOST: &str = "fstream.binance.com";
const MAINNET_WS_PORT: u16 = 443;
const TESTNET_WS_HOST: &str = "stream.binancefuture.com";
const TESTNET_WS_PORT: u16 = 443;
const MAINNET_REST_HOST: &str = "https://fapi.binance.com";
const TESTNET_REST_HOST: &str = "https://testnet.binancefuture.com";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(20);
const PING_SEND_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// 币安 U 本位永续合约行情源：通过 combined stream WebSocket 订阅 bookTicker 和
/// 20 档深度快照,断线后自动按指数退避重连。支持通过 HTTP CONNECT 代理出网。
/// 消息格式和订阅方式与 [`super::binance::BinanceSpotSource`] 完全一致,只是 host 换成
/// 期货域名(mainnet `fstream.binance.com`,testnet `stream.binancefuture.com`)。
pub struct BinanceFuturesSource {
    venue: Venue,
    symbols: Vec<Symbol>,
    ws_host: &'static str,
    ws_port: u16,
    proxy: Option<String>,
}

impl BinanceFuturesSource {
    pub fn new(venue: Venue, symbols: Vec<Symbol>, testnet: bool, proxy: Option<String>) -> Self {
        let (ws_host, ws_port) = if testnet {
            (TESTNET_WS_HOST, TESTNET_WS_PORT)
        } else {
            (MAINNET_WS_HOST, MAINNET_WS_PORT)
        };
        Self {
            venue,
            symbols,
            ws_host,
            ws_port,
            proxy,
        }
    }

    fn binance_symbol(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }

    fn symbol_map(&self) -> HashMap<String, Symbol> {
        self.symbols
            .iter()
            .map(|s| (Self::binance_symbol(s), s.clone()))
            .collect()
    }

    fn stream_url(&self) -> String {
        let streams = self
            .symbols
            .iter()
            .flat_map(|s| {
                let symbol = Self::binance_symbol(s).to_ascii_lowercase();
                [format!("{symbol}@bookTicker"), format!("{symbol}@depth20@100ms")]
            })
            .collect::<Vec<_>>()
            .join("/");
        format!("wss://{}:{}/stream?streams={}", self.ws_host, self.ws_port, streams)
    }

    /// 建连:先拿到 TCP 流(直连或经代理隧道),再在其上做 TLS + WebSocket 握手。
    async fn connect(&self, url: &str) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        tokio::time::timeout(CONNECT_TIMEOUT, async {
            let tcp = connect_tcp(self.ws_host, self.ws_port, self.proxy.as_deref()).await?;
            let (ws, _) = tokio_tungstenite::client_async_tls(url, tcp)
                .await
                .context("websocket handshake failed")?;
            Ok(ws)
        })
        .await
        .with_context(|| format!("websocket connect timed out after {CONNECT_TIMEOUT:?}"))?
    }
}

impl MarketDataSource for BinanceFuturesSource {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let symbol_map = self.symbol_map();
            let url = self.stream_url();
            let mut backoff = MIN_BACKOFF;

            loop {
                let mut ws = match self.connect(&url).await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "binance futures ws connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };
                debug!("binance futures ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                drive_ws_stream(&mut ws, &self.venue, &symbol_map, &bus, PING_INTERVAL, IDLE_TIMEOUT).await;

                warn!(
                    "binance futures ws disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        })
    }
}

async fn drive_ws_stream<S>(
    ws: &mut S,
    venue: &Venue,
    symbol_map: &HashMap<String, Symbol>,
    bus: &TopicBus,
    ping_interval: Duration,
    idle_timeout: Duration,
) where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin,
{
    let mut ping = tokio::time::interval(ping_interval);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await;
    // 独立的 idle deadline，避免 ping 分支触发时重置超时计时器
    let idle_sleep = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle_sleep);

    loop {
        tokio::select! {
            _ = ping.tick() => {
                match tokio::time::timeout(PING_SEND_TIMEOUT, ws.send(Message::Ping(Vec::new()))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        warn!("binance futures ws ping failed for venue={venue} err={err}");
                        break;
                    }
                    Err(_) => {
                        warn!("binance futures ws ping send timed out for venue={venue}");
                        break;
                    }
                }
            }
            _ = &mut idle_sleep => {
                warn!("binance futures ws idle timeout for venue={venue}, no message in {idle_timeout:?}");
                break;
            }
            msg = ws.next() => {
                let msg = match msg {
                    Some(Ok(msg)) => msg,
                    Some(Err(err)) => {
                        warn!("binance futures ws error for venue={venue} err={err}");
                        break;
                    }
                    None => break,
                };
                if matches!(msg, Message::Close(_)) {
                    break;
                }
                idle_sleep.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
                let Message::Text(text) = msg else {
                    continue;
                };
                let Ok(stream) = serde_json::from_str::<StreamName>(&text) else {
                    continue;
                };
                if stream.stream.ends_with("@bookTicker") {
                    let Some((symbol, quote)) = parse_book_ticker(&text, symbol_map) else {
                        continue;
                    };
                    bus.publish(Topic::quote(venue.clone(), symbol), quote);
                } else if stream.stream.ends_with("@depth20@100ms") {
                    let Some((symbol, book)) = parse_partial_depth(&text, symbol_map) else {
                        continue;
                    };
                    bus.publish(Topic::order_book(venue.clone(), symbol), book);
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct BookTickerPayload {
    s: String,
    b: Decimal,
    #[serde(rename = "B")]
    bid_qty: Decimal,
    a: Decimal,
    #[serde(rename = "A")]
    ask_qty: Decimal,
}

#[derive(Debug, Deserialize)]
struct CombinedStreamEnvelope {
    data: BookTickerPayload,
}

#[derive(Debug, Deserialize)]
struct StreamName {
    stream: String,
}

#[derive(Debug, Deserialize)]
struct PartialDepthPayload {
    s: String,
    b: Vec<Vec<String>>,
    a: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct CombinedDepthEnvelope {
    data: PartialDepthPayload,
}

/// 解析一条 combined stream 消息,查表得到内部 Symbol。纯函数,不依赖网络连接,
/// 便于脱离真实 WebSocket 连接做单元测试。
fn parse_book_ticker(text: &str, symbol_map: &HashMap<String, Symbol>) -> Option<(Symbol, Quote)> {
    let envelope: CombinedStreamEnvelope = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(err) => {
            warn!("failed to parse binance futures book ticker message: {err}");
            return None;
        }
    };
    let payload = envelope.data;
    let Some(symbol) = symbol_map.get(&payload.s) else {
        warn!("binance futures book ticker for unknown symbol: {}", payload.s);
        return None;
    };

    Some((
        symbol.clone(),
        Quote {
            bid: payload.b,
            bid_size: payload.bid_qty,
            ask: payload.a,
            ask_size: payload.ask_qty,
            ts_ms: now_ms(),
        },
    ))
}

fn parse_partial_depth(text: &str, symbol_map: &HashMap<String, Symbol>) -> Option<(Symbol, OrderBookLevels)> {
    let envelope: CombinedDepthEnvelope = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(err) => {
            warn!("failed to parse binance futures partial depth message: {err}");
            return None;
        }
    };
    let payload = envelope.data;
    let symbol = symbol_map.get(&payload.s)?.clone();
    let parse_levels = |rows: Vec<Vec<String>>| {
        rows.into_iter()
            .filter_map(|row| {
                let price = row.first()?.parse::<Decimal>().ok()?;
                let qty = row.get(1)?.parse::<Decimal>().ok()?;
                (price > Decimal::ZERO && qty > Decimal::ZERO).then_some((price, qty))
            })
            .collect::<Vec<_>>()
    };
    let bids = parse_levels(payload.b);
    let asks = parse_levels(payload.a);
    if bids.is_empty() || asks.is_empty() {
        return None;
    }
    Some((symbol, OrderBookLevels { bids: Arc::new(bids), asks: Arc::new(asks), ts_ms: now_ms() }))
}

/// 币安 U 本位永续的公开 K 线 / 24h 行情 REST 客户端，不需要签名。
pub struct BinanceFuturesKlineClient {
    venue: Venue,
    host: &'static str,
    http: reqwest::Client,
}

impl BinanceFuturesKlineClient {
    pub fn new(venue: Venue, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if let Some(proxy) = proxy {
            builder = builder.proxy(reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?);
        }
        let http = builder.build().context("failed to build binance futures http client")?;
        let host = if testnet { TESTNET_REST_HOST } else { MAINNET_REST_HOST };
        Ok(Self { venue, host, http })
    }

    async fn public_get(&self, path: &str, query: &[(&str, String)]) -> anyhow::Result<String> {
        crate::ratelimit::throttle(self.host).await;
        let resp = self
            .http
            .get(format!("{}{path}", self.host))
            .query(query)
            .send()
            .await
            .with_context(|| format!("binance futures request {path} failed"))?;
        resp.text().await.context("failed to read binance futures response body")
    }
}

#[async_trait]
impl KlineProvider for BinanceFuturesKlineClient {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `GET /fapi/v1/klines` 按开盘时间升序返回，最后一根是当前未收盘的，
    /// 所以多要 1 根再剔除。
    async fn klines(&self, symbol: &Symbol, interval: KlineInterval, limit: usize) -> anyhow::Result<Vec<Kline>> {
        let query = [
            ("symbol", BinanceFuturesSource::binance_symbol(symbol)),
            ("interval", kline_interval_param(interval).to_string()),
            ("limit", (limit + 1).min(KLINES_LIMIT_MAX).to_string()),
        ];
        let text = self.public_get("/fapi/v1/klines", &query).await?;
        Ok(closed_klines(parse_klines(&text)?, interval, now_ms(), limit))
    }

    /// 同一个接口带 `startTime`/`endTime`，单次最多 1500 根，从 `start_ms` 往后翻页。
    async fn klines_range(
        &self,
        symbol: &Symbol,
        interval: KlineInterval,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<Kline>> {
        let mut klines: Vec<Kline> = Vec::new();
        let mut from = start_ms;
        for _ in 0..RANGE_MAX_PAGES {
            if from > end_ms {
                break;
            }
            let query = [
                ("symbol", BinanceFuturesSource::binance_symbol(symbol)),
                ("interval", kline_interval_param(interval).to_string()),
                ("startTime", from.to_string()),
                ("endTime", end_ms.to_string()),
                ("limit", KLINES_LIMIT_MAX.to_string()),
            ];
            let text = self.public_get("/fapi/v1/klines", &query).await?;
            let page = parse_klines(&text)?;
            let Some(last) = page.iter().map(|k| k.open_time_ms).max() else {
                break;
            };
            let full = page.len() >= KLINES_LIMIT_MAX;
            klines.extend(page);
            if !full {
                break;
            }
            from = last + interval.millis();
        }
        Ok(closed_klines_in_range(klines, interval, now_ms(), start_ms, end_ms))
    }

    async fn quote_volume_24h(&self, symbol: &Symbol) -> anyhow::Result<Decimal> {
        let query = [("symbol", BinanceFuturesSource::binance_symbol(symbol))];
        let text = self.public_get("/fapi/v1/ticker/24hr", &query).await?;
        parse_quote_volume(&text)
    }
}

#[derive(Debug, Deserialize)]
struct RestErrorResponse {
    code: i64,
    msg: String,
}

/// `GET /fapi/v1/klines` 单次最多 1500 根。
const KLINES_LIMIT_MAX: usize = 1500;

fn kline_interval_param(interval: KlineInterval) -> &'static str {
    match interval {
        KlineInterval::M5 => "5m",
        KlineInterval::H1 => "1h",
    }
}

/// 每根 K 线是数组 `[openTimeMs, "open", "high", "low", "close", "volume", closeTimeMs, ...]`。
fn parse_klines(text: &str) -> anyhow::Result<Vec<Kline>> {
    if let Ok(err) = serde_json::from_str::<RestErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let rows: Vec<Vec<serde_json::Value>> =
        serde_json::from_str(text).with_context(|| format!("failed to parse binance futures klines, raw body: {text}"))?;
    rows.into_iter()
        .map(|row| {
            let open_time_ms = row.first().and_then(|v| v.as_u64()).context("kline missing open time")?;
            let close = row
                .get(4)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .context("kline missing close price")?;
            let volume = row
                .get(5)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .context("kline missing volume")?;
            Ok(Kline { open_time_ms, close, volume })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Ticker24h {
    quote_volume: Decimal,
}

fn parse_quote_volume(text: &str) -> anyhow::Result<Decimal> {
    if let Ok(err) = serde_json::from_str::<RestErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let ticker: Ticker24h =
        serde_json::from_str(text).with_context(|| format!("failed to parse binance futures 24hr ticker, raw body: {text}"))?;
    Ok(ticker.quote_volume)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(BinanceFuturesSource::binance_symbol(&symbol), symbol);
        map
    }

    #[test]
    fn parses_combined_stream_book_ticker() {
        let symbol = Symbol::new("BNB", "USDT");
        let map = map_with(symbol.clone());
        let text = r#"{
            "stream": "bnbusdt@bookTicker",
            "data": {
                "u": 400900217,
                "s": "BNBUSDT",
                "b": "25.35190000",
                "B": "31.21000000",
                "a": "25.36520000",
                "A": "40.66000000"
            }
        }"#;

        let (parsed_symbol, quote) = parse_book_ticker(text, &map).expect("should parse");

        assert_eq!(parsed_symbol, symbol);
        assert_eq!(quote.bid, "25.35190000".parse::<Decimal>().unwrap());
        assert_eq!(quote.bid_size, "31.21000000".parse::<Decimal>().unwrap());
        assert_eq!(quote.ask, "25.36520000".parse::<Decimal>().unwrap());
        assert_eq!(quote.ask_size, "40.66000000".parse::<Decimal>().unwrap());
    }

    #[test]
    fn ignores_message_for_unmapped_symbol() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        let text = r#"{
            "stream": "ethusdt@bookTicker",
            "data": {
                "u": 1,
                "s": "ETHUSDT",
                "b": "1.0",
                "B": "1.0",
                "a": "1.1",
                "A": "1.0"
            }
        }"#;

        assert!(parse_book_ticker(text, &map).is_none());
    }

    #[test]
    fn ignores_malformed_message() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        assert!(parse_book_ticker("not json", &map).is_none());
    }

    #[test]
    fn builds_lowercase_combined_stream_url() {
        let source = BinanceFuturesSource::new(
            Venue::new("binance_futures"),
            vec![Symbol::new("BTC", "USDT"), Symbol::new("ETH", "BTC")],
            false,
            None,
        );

        assert_eq!(
            source.stream_url(),
            "wss://fstream.binance.com:443/stream?streams=btcusdt@bookTicker/btcusdt@depth20@100ms/ethbtc@bookTicker/ethbtc@depth20@100ms"
        );
    }

    #[test]
    fn parses_combined_stream_partial_depth() {
        let symbol = Symbol::new("BTC", "USDT");
        let map = map_with(symbol.clone());
        let text = r#"{
            "stream": "btcusdt@depth20@100ms",
            "data": {
                "lastUpdateId": 160,
                "E": 123456789,
                "T": 123456780,
                "s": "BTCUSDT",
                "b": [["100", "2"], ["99", "3"]],
                "a": [["101", "4"], ["102", "5"]]
            }
        }"#;
        let (parsed_symbol, book) = parse_partial_depth(text, &map).expect("should parse");
        assert_eq!(parsed_symbol, symbol);
        assert_eq!(book.bids.as_ref(), &vec![(Decimal::from(100), Decimal::from(2)), (Decimal::from(99), Decimal::from(3))]);
        assert_eq!(book.asks.as_ref(), &vec![(Decimal::from(101), Decimal::from(4)), (Decimal::from(102), Decimal::from(5))]);
    }

    #[test]
    fn parses_klines_open_time_and_close() {
        let text = r#"[
            [1791435600000,"0.004000","0.004100","0.003900","0.004036","123",1791439199999,"0.5",10,"60","0.2","0"],
            [1791439200000,"0.004036","0.004100","0.003900","0.004050","123",1791442799999,"0.5",10,"60","0.2","0"]
        ]"#;
        let klines = parse_klines(text).expect("should parse");
        assert_eq!(klines.len(), 2);
        assert_eq!(
            klines[0],
            Kline { open_time_ms: 1791435600000, close: "0.004036".parse().unwrap(), volume: Decimal::from(123) }
        );
        assert_eq!(klines[1].open_time_ms, 1791439200000);
    }

    #[test]
    fn parse_klines_surfaces_error_response() {
        let err = parse_klines(r#"{"code":-1121,"msg":"Invalid symbol."}"#).unwrap_err();
        assert!(err.to_string().contains("Invalid symbol."));
    }

    #[test]
    fn parses_24hr_quote_volume() {
        let text = r#"{"symbol":"1000PEPEUSDT","volume":"1","quoteVolume":"176638409.33"}"#;
        assert_eq!(parse_quote_volume(text).unwrap(), "176638409.33".parse::<Decimal>().unwrap());
    }

    #[test]
    fn testnet_uses_testnet_host() {
        let source =
            BinanceFuturesSource::new(Venue::new("binance_futures"), vec![Symbol::new("BTC", "USDT")], true, None);

        assert!(source.stream_url().starts_with(&format!("wss://{TESTNET_WS_HOST}")));
    }

    struct FakeWs {
        rx: tokio::sync::mpsc::UnboundedReceiver<Result<Message, tokio_tungstenite::tungstenite::Error>>,
        sent_tx: tokio::sync::mpsc::UnboundedSender<Message>,
    }

    impl Stream for FakeWs {
        type Item = Result<Message, tokio_tungstenite::tungstenite::Error>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.rx.poll_recv(cx)
        }
    }

    impl Sink<Message> for FakeWs {
        type Error = tokio_tungstenite::tungstenite::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(self: std::pin::Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            let _ = self.sent_tx.send(item);
            Ok(())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn drive_ws_stream_breaks_on_idle_timeout_despite_pings() {
        let (_in_tx, in_rx) = tokio::sync::mpsc::unbounded_channel();
        let (sent_tx, mut sent_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut ws = FakeWs { rx: in_rx, sent_tx };
        let venue = Venue::new("binance_futures");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let bus = TopicBus::new();

        tokio::time::timeout(
            Duration::from_millis(500),
            drive_ws_stream(
                &mut ws,
                &venue,
                &map,
                &bus,
                Duration::from_millis(20),
                Duration::from_millis(70),
            ),
        )
        .await
        .expect("idle timeout should break loop even while pings fire");

        let mut pings = 0;
        while let Ok(msg) = sent_rx.try_recv() {
            if matches!(msg, Message::Ping(_)) {
                pings += 1;
            }
        }
        assert!(pings >= 1, "expected at least one ping before idle timeout");
    }

    #[tokio::test]
    async fn drive_ws_stream_stays_alive_when_pong_arrives() {
        let (in_tx, in_rx) = tokio::sync::mpsc::unbounded_channel();
        let (sent_tx, mut sent_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut ws = FakeWs { rx: in_rx, sent_tx };
        let venue = Venue::new("binance_futures");
        let symbol = Symbol::new("BTC", "USDT");
        let map = map_with(symbol.clone());
        let bus = TopicBus::new();
        let mut sub = bus.subscribe::<Quote>(Topic::quote(venue.clone(), symbol));

        let responder = tokio::spawn(async move {
            for _ in 0..3 {
                let msg = sent_rx.recv().await.expect("expected ping");
                assert!(matches!(msg, Message::Ping(_)));
                in_tx.send(Ok(Message::Pong(Vec::new()))).unwrap();
            }
            let text = r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100","B":"1","a":"101","A":"2"}}"#;
            in_tx.send(Ok(Message::Text(text.to_string()))).unwrap();
            in_tx.send(Ok(Message::Close(None))).unwrap();
        });

        tokio::time::timeout(
            Duration::from_millis(500),
            drive_ws_stream(
                &mut ws,
                &venue,
                &map,
                &bus,
                Duration::from_millis(25),
                Duration::from_millis(60),
            ),
        )
        .await
        .expect("stream should close cleanly after quote");
        responder.await.unwrap();

        let (_, quote) = tokio::time::timeout(Duration::from_millis(100), sub.next())
            .await
            .expect("timed out waiting for quote")
            .expect("quote should be published after surviving past idle_timeout");
        assert_eq!(quote.bid, Decimal::from(100));
    }
}
