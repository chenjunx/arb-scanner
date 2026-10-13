use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::exchange_info::bitget::{BITGET_USDT_FUTURES, bitget_symbol, parse_bitget_data};
use crate::net::connect_tcp;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{Kline, KlineInterval, KlineProvider, MarketDataSource, closed_klines, now_ms};

const WS_HOST: &str = "ws.bitget.com";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const REST_HOST: &str = "https://api.bitget.com";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Bitget 要求客户端每 30 秒发一次文本 `ping`(服务端回 `pong`)，2 分钟收不到就断开。
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);
/// `/api/v2/mix/market/candles` 单次最多 1000 根。
const CANDLES_LIMIT_MAX: usize = 1000;

/// Bitget USDT 本位永续一档盘口行情源：订阅公开频道 `books1`。发布的报价是
/// 交易所原始报价(一个 base 单位的价格)，挂单量单位是 base 币。
pub struct BitgetFuturesSource {
    venue: Venue,
    symbols: Vec<Symbol>,
    proxy: Option<String>,
}

impl BitgetFuturesSource {
    pub fn new(venue: Venue, symbols: Vec<Symbol>, proxy: Option<String>) -> Self {
        Self { venue, symbols, proxy }
    }

    fn symbol_map(&self) -> HashMap<String, Symbol> {
        self.symbols.iter().map(|s| (bitget_symbol(s), s.clone())).collect()
    }

    fn subscribe_message(&self) -> String {
        let args: Vec<serde_json::Value> = self
            .symbols
            .iter()
            .map(|s| serde_json::json!({"instType": BITGET_USDT_FUTURES, "channel": "books1", "instId": bitget_symbol(s)}))
            .collect();
        serde_json::json!({"op": "subscribe", "args": args}).to_string()
    }

    async fn connect(&self) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(WS_HOST, WS_PORT, self.proxy.as_deref()).await?;
        let (ws, _) = tokio_tungstenite::client_async_tls(WS_URL, tcp)
            .await
            .context("websocket handshake failed")?;
        Ok(ws)
    }
}

impl MarketDataSource for BitgetFuturesSource {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let symbol_map = self.symbol_map();
            let mut backoff = MIN_BACKOFF;

            loop {
                let mut ws = match self.connect().await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "bitget futures ws connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(self.subscribe_message())).await {
                    warn!(
                        "bitget futures ws subscribe failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                debug!("bitget futures ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                let mut ping = tokio::time::interval(WS_PING_INTERVAL);
                ping.tick().await;
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = ws.send(Message::Text("ping".to_string())).await {
                                warn!("bitget futures ws ping failed for venue={} err={err}", self.venue);
                                break;
                            }
                        }
                        msg = ws.next() => {
                            let msg = match msg {
                                Some(Ok(msg)) => msg,
                                Some(Err(err)) => {
                                    warn!("bitget futures ws error for venue={} err={err}", self.venue);
                                    break;
                                }
                                None => break,
                            };
                            let Message::Text(text) = msg else {
                                continue;
                            };
                            if let Some((symbol, quote)) = parse_books1_message(&text, &symbol_map, &self.venue) {
                                bus.publish(Topic::quote(self.venue.clone(), symbol), quote);
                            }
                        }
                    }
                }

                warn!("bitget futures ws disconnected for venue={}, reconnecting in {:?}", self.venue, backoff);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsArg {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    inst_id: String,
}

#[derive(Debug, Deserialize)]
struct BookEntry {
    /// 每档 `[price, size]`，都是字符串。
    #[serde(default)]
    asks: Vec<Vec<String>>,
    #[serde(default)]
    bids: Vec<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct WsPush {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    code: Option<serde_json::Value>,
    #[serde(default)]
    msg: Option<String>,
    arg: Option<WsArg>,
    #[serde(default)]
    data: Vec<BookEntry>,
}

fn top_level(levels: &[Vec<String>]) -> Option<(Decimal, Decimal)> {
    let level = levels.first()?;
    Some((level.first()?.parse().ok()?, level.get(1)?.parse().ok()?))
}

/// 解析一条 `books1` 推送。订阅出错(`event == "error"`)打 warn；订阅确认、
/// `pong` 等其它消息忽略。任意一侧盘口为空时丢弃这条。
fn parse_books1_message(text: &str, symbol_map: &HashMap<String, Symbol>, venue: &Venue) -> Option<(Symbol, Quote)> {
    let push: WsPush = serde_json::from_str(text).ok()?;
    if push.event.as_deref() == Some("error") {
        warn!(
            "bitget futures ws error for venue={venue}: code={} msg={}",
            push.code.map(|c| c.to_string()).unwrap_or_default(),
            push.msg.unwrap_or_default()
        );
        return None;
    }
    if push.event.is_some() {
        return None;
    }
    let arg = push.arg?;
    if arg.channel != "books1" {
        return None;
    }
    let symbol = symbol_map.get(&arg.inst_id)?;
    let entry = push.data.first()?;
    let (bid, bid_size) = top_level(&entry.bids)?;
    let (ask, ask_size) = top_level(&entry.asks)?;
    Some((symbol.clone(), Quote { bid, bid_size, ask, ask_size, ts_ms: now_ms() }))
}

/// Bitget USDT 本位永续的公开 K 线 / 24h 行情 REST 客户端，不需要签名。
pub struct BitgetFuturesKlineClient {
    venue: Venue,
    http: reqwest::Client,
}

impl BitgetFuturesKlineClient {
    pub fn new(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if let Some(proxy) = proxy {
            builder = builder.proxy(reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?);
        }
        let http = builder.build().context("failed to build bitget http client")?;
        Ok(Self { venue, http })
    }

    async fn public_get(&self, path: &str, query: &[(&str, String)]) -> anyhow::Result<String> {
        crate::ratelimit::throttle(REST_HOST).await;
        let resp = self
            .http
            .get(format!("{REST_HOST}{path}"))
            .query(query)
            .send()
            .await
            .with_context(|| format!("bitget request {path} failed"))?;
        resp.text().await.context("failed to read bitget response body")
    }
}

#[async_trait]
impl KlineProvider for BitgetFuturesKlineClient {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `GET /api/v2/mix/market/candles`，最后一根是当前未收盘的，所以多要 1 根
    /// 再交给 [`closed_klines`] 剔除。
    async fn klines(&self, symbol: &Symbol, interval: KlineInterval, limit: usize) -> anyhow::Result<Vec<Kline>> {
        let granularity = match interval {
            KlineInterval::M5 => "5m",
            KlineInterval::H1 => "1H",
        };
        let query = [
            ("symbol", bitget_symbol(symbol)),
            ("productType", BITGET_USDT_FUTURES.to_string()),
            ("granularity", granularity.to_string()),
            ("limit", (limit + 1).min(CANDLES_LIMIT_MAX).to_string()),
        ];
        let text = self.public_get("/api/v2/mix/market/candles", &query).await?;
        Ok(closed_klines(parse_candles(&text)?, interval, now_ms(), limit))
    }

    /// `GET /api/v2/mix/market/ticker` 返回只含一个元素的数组，取 `quoteVolume`。
    async fn quote_volume_24h(&self, symbol: &Symbol) -> anyhow::Result<Decimal> {
        let query = [("symbol", bitget_symbol(symbol)), ("productType", BITGET_USDT_FUTURES.to_string())];
        let text = self.public_get("/api/v2/mix/market/ticker", &query).await?;
        parse_quote_volume(&text)
    }
}

/// 每根是 `[ts(ms), o, h, l, c, baseVol, quoteVol]`，都是字符串。
fn parse_candles(text: &str) -> anyhow::Result<Vec<Kline>> {
    let rows: Vec<Vec<String>> = parse_bitget_data(text, "candles")?;
    rows.into_iter()
        .map(|row| {
            let open_time_ms: u64 = row.first().and_then(|t| t.parse().ok()).context("bitget candle has no valid ts")?;
            let close: Decimal = row.get(4).and_then(|c| c.parse().ok()).context("bitget candle has no valid close")?;
            Ok(Kline { open_time_ms, close })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerEntry {
    quote_volume: Decimal,
}

fn parse_quote_volume(text: &str) -> anyhow::Result<Decimal> {
    let tickers: Vec<TickerEntry> = parse_bitget_data(text, "ticker")?;
    tickers.into_iter().next().map(|t| t.quote_volume).context("bitget ticker response is empty")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::HOUR_MS;

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(bitget_symbol(&symbol), symbol);
        map
    }

    #[test]
    fn parses_books1_push() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        let venue = Venue::new("bitget_futures");
        let text = r#"{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"books1","instId":"BTCUSDT"},"data":[{"asks":[["27000.5","8.760"]],"bids":[["27000.0","2.710"]],"checksum":0,"ts":"1695716059516"}],"ts":1695716059516}"#;
        let (symbol, quote) = parse_books1_message(text, &map, &venue).unwrap();
        assert_eq!(symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(quote.bid, "27000.0".parse().unwrap());
        assert_eq!(quote.bid_size, "2.710".parse().unwrap());
        assert_eq!(quote.ask, "27000.5".parse().unwrap());
        assert_eq!(quote.ask_size, "8.760".parse().unwrap());
    }

    #[test]
    fn ignores_acks_errors_unknown_and_empty_books() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        let venue = Venue::new("bitget_futures");
        for text in [
            r#"{"event":"subscribe","arg":{"instType":"USDT-FUTURES","channel":"books1","instId":"BTCUSDT"}}"#,
            r#"{"event":"error","code":30001,"msg":"instType:USDT-FUTURES,channel:books1,instId:XXXUSDT doesn't exist"}"#,
            r#"{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"books1","instId":"ETHUSDT"},"data":[{"asks":[["1","1"]],"bids":[["1","1"]]}]}"#,
            r#"{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"books1","instId":"BTCUSDT"},"data":[{"asks":[],"bids":[["1","1"]]}]}"#,
            "pong",
        ] {
            assert!(parse_books1_message(text, &map, &venue).is_none(), "{text}");
        }
    }

    #[test]
    fn parses_candles_and_drops_open_bar() {
        let text = r#"{"code":"00000","msg":"success","requestTime":1,"data":[
            ["3600000","1","1","1","26194.5","26.26","687897.63"],
            ["7200000","1","1","1","26200.1","1","1"]
        ]}"#;
        let klines = parse_candles(text).unwrap();
        let closed = closed_klines(klines, KlineInterval::H1, 2 * HOUR_MS + 1, 10);
        assert_eq!(closed, vec![Kline { open_time_ms: HOUR_MS, close: "26194.5".parse().unwrap() }]);
        assert!(parse_candles(r#"{"code":"40034","msg":"Parameter does not exist","data":null}"#).is_err());
    }

    #[test]
    fn parses_ticker_quote_volume() {
        let text = r#"{"code":"00000","msg":"success","requestTime":1,"data":[{"symbol":"BTCUSDT","lastPr":"27000","baseVolume":"10","quoteVolume":"270000.5"}]}"#;
        assert_eq!(parse_quote_volume(text).unwrap(), "270000.5".parse().unwrap());
    }

    #[test]
    fn subscribe_message_uses_usdt_futures_books1() {
        let source = BitgetFuturesSource::new(Venue::new("bitget_futures"), vec![Symbol::new("1000BONK", "USDT")], None);
        let msg: serde_json::Value = serde_json::from_str(&source.subscribe_message()).unwrap();
        assert_eq!(
            msg["args"][0],
            serde_json::json!({"instType": "USDT-FUTURES", "channel": "books1", "instId": "1000BONKUSDT"})
        );
    }
}
