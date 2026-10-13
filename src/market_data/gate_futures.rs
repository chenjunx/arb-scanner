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

use crate::net::connect_tcp;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{Kline, KlineInterval, KlineProvider, MarketDataSource, closed_klines, now_ms};

const WS_HOST: &str = "fx-ws.gateio.ws";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
const REST_HOST: &str = "https://api.gateio.ws";
const API_PREFIX: &str = "/api/v4";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// `/futures/usdt/candlesticks` 单次最多返回 2000 根。
const CANDLESTICK_LIMIT_MAX: usize = 2000;

/// Gate 永续合约名是 `BASE_QUOTE`(如 `PEPE_USDT`)。
fn gate_contract(symbol: &Symbol) -> String {
    format!("{}_{}", symbol.base, symbol.quote).to_ascii_uppercase()
}

/// Gate.io USDT 本位永续 book_ticker 行情源：和 [`super::gate::GateSpotSource`]
/// 一样的订阅/重连方式，只是 host 换成合约域名、channel 换成 `futures.book_ticker`。
/// 发布的报价是交易所原始报价(每币价格)，挂单量单位是张。
pub struct GateFuturesSource {
    venue: Venue,
    symbols: Vec<Symbol>,
    proxy: Option<String>,
}

impl GateFuturesSource {
    pub fn new(venue: Venue, symbols: Vec<Symbol>, proxy: Option<String>) -> Self {
        Self { venue, symbols, proxy }
    }

    fn symbol_map(&self) -> HashMap<String, Symbol> {
        self.symbols.iter().map(|s| (gate_contract(s), s.clone())).collect()
    }

    fn subscribe_message(&self) -> String {
        let contracts: Vec<String> = self.symbols.iter().map(gate_contract).collect();
        let time_secs = now_ms() / 1000;
        serde_json::json!({
            "time": time_secs,
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "payload": contracts,
        })
        .to_string()
    }

    /// 建连:先拿到 TCP 流(直连或经代理隧道),再在其上做 TLS + WebSocket 握手。
    async fn connect(&self) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(WS_HOST, WS_PORT, self.proxy.as_deref()).await?;
        let (ws, _) = tokio_tungstenite::client_async_tls(WS_URL, tcp)
            .await
            .context("websocket handshake failed")?;
        Ok(ws)
    }
}

impl MarketDataSource for GateFuturesSource {
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
                            "gate futures ws connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(self.subscribe_message())).await {
                    warn!(
                        "gate futures ws subscribe failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                debug!("gate futures ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                while let Some(msg) = ws.next().await {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("gate futures ws error for venue={} err={err}", self.venue);
                            break;
                        }
                    };
                    let Message::Text(text) = msg else {
                        continue;
                    };
                    if let Some((symbol, quote)) = parse_book_ticker_message(&text, &symbol_map) {
                        bus.publish(Topic::quote(self.venue.clone(), symbol), quote);
                    }
                }

                warn!(
                    "gate futures ws disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        })
    }
}

/// `B`/`A` 是挂单张数，普通合约是 JSON 整数，开了小数张的合约可能是字符串；
/// `Decimal` 的默认反序列化两种都接受。
#[derive(Debug, Deserialize)]
struct BookTickerResult {
    s: String,
    b: Decimal,
    #[serde(rename = "B")]
    bid_size: Decimal,
    a: Decimal,
    #[serde(rename = "A")]
    ask_size: Decimal,
}

/// 解析一条 futures.book_ticker update 消息,查表得到内部 Symbol。忽略订阅确认、
/// pong 等非 update 消息，不打日志。盘口一侧为空时 Gate 会给空字符串，反序列化
/// 失败直接丢弃这条。
fn parse_book_ticker_message(text: &str, symbol_map: &HashMap<String, Symbol>) -> Option<(Symbol, Quote)> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    if value.get("channel").and_then(|c| c.as_str()) != Some("futures.book_ticker") {
        return None;
    }
    if value.get("event").and_then(|e| e.as_str()) != Some("update") {
        return None;
    }

    let result: BookTickerResult = match serde_json::from_value(value.get("result")?.clone()) {
        Ok(result) => result,
        Err(err) => {
            debug!("skipping gate futures book_ticker message: {err}");
            return None;
        }
    };

    let symbol = symbol_map.get(&result.s)?;
    Some((
        symbol.clone(),
        Quote {
            bid: result.b,
            bid_size: result.bid_size,
            ask: result.a,
            ask_size: result.ask_size,
            ts_ms: now_ms(),
        },
    ))
}

/// Gate USDT 本位永续的公开 K 线 / 24h 行情 REST 客户端，不需要签名。
pub struct GateFuturesKlineClient {
    venue: Venue,
    http: reqwest::Client,
}

impl GateFuturesKlineClient {
    pub fn new(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if let Some(proxy) = proxy {
            builder = builder.proxy(reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?);
        }
        let http = builder.build().context("failed to build gate futures http client")?;
        Ok(Self { venue, http })
    }

    async fn public_get(&self, path: &str, query: &[(&str, String)]) -> anyhow::Result<String> {
        crate::ratelimit::throttle(REST_HOST).await;
        let resp = self
            .http
            .get(format!("{REST_HOST}{API_PREFIX}{path}"))
            .query(query)
            .send()
            .await
            .with_context(|| format!("gate futures request {path} failed"))?;
        resp.text().await.context("failed to read gate futures response body")
    }
}

#[async_trait]
impl KlineProvider for GateFuturesKlineClient {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `GET /futures/usdt/candlesticks` 按时间升序返回，`t` 是开盘时间(秒)，最后
    /// 一根是当前未收盘的，所以多要 1 根再剔除。
    async fn klines(&self, symbol: &Symbol, interval: KlineInterval, limit: usize) -> anyhow::Result<Vec<Kline>> {
        let interval_param = match interval {
            KlineInterval::M5 => "5m",
            KlineInterval::H1 => "1h",
        };
        let query = [
            ("contract", gate_contract(symbol)),
            ("interval", interval_param.to_string()),
            ("limit", (limit + 1).min(CANDLESTICK_LIMIT_MAX).to_string()),
        ];
        let text = self.public_get("/futures/usdt/candlesticks", &query).await?;
        Ok(closed_klines(parse_candlesticks(&text)?, interval, now_ms(), limit))
    }

    /// `GET /futures/usdt/tickers?contract=X` 返回只含一个元素的数组。
    async fn quote_volume_24h(&self, symbol: &Symbol) -> anyhow::Result<Decimal> {
        let query = [("contract", gate_contract(symbol))];
        let text = self.public_get("/futures/usdt/tickers", &query).await?;
        parse_quote_volume(&text)
    }
}

#[derive(Debug, Deserialize)]
struct GateErrorResponse {
    label: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct CandlestickEntry {
    /// Gate 文档里标的是 double，按 f64 读再取整。
    t: f64,
    c: Decimal,
}

fn parse_candlesticks(text: &str) -> anyhow::Result<Vec<Kline>> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let entries: Vec<CandlestickEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate candlesticks, raw body: {text}"))?;
    Ok(entries
        .into_iter()
        .map(|e| Kline { open_time_ms: e.t as u64 * 1000, close: e.c })
        .collect())
}

#[derive(Debug, Deserialize)]
struct TickerEntry {
    volume_24h_quote: Decimal,
}

fn parse_quote_volume(text: &str) -> anyhow::Result<Decimal> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let tickers: Vec<TickerEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate futures tickers, raw body: {text}"))?;
    tickers
        .into_iter()
        .next()
        .map(|t| t.volume_24h_quote)
        .context("gate futures tickers response is empty")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(gate_contract(&symbol), symbol);
        map
    }

    #[test]
    fn parses_book_ticker_update_with_integer_sizes() {
        let symbol = Symbol::new("PEPE", "USDT");
        let map = map_with(symbol.clone());
        let text = r#"{
            "time": 1615366379,
            "time_ms": 1615366379123,
            "channel": "futures.book_ticker",
            "event": "update",
            "result": {"t": 1615366379123, "u": 2517661076, "s": "PEPE_USDT", "b": "0.000004036", "B": 37000, "a": "0.000004037", "A": 47061}
        }"#;

        let (parsed_symbol, quote) = parse_book_ticker_message(text, &map).expect("should parse");
        assert_eq!(parsed_symbol, symbol);
        assert_eq!(quote.bid, "0.000004036".parse::<Decimal>().unwrap());
        assert_eq!(quote.bid_size, Decimal::from(37000));
        assert_eq!(quote.ask, "0.000004037".parse::<Decimal>().unwrap());
        assert_eq!(quote.ask_size, Decimal::from(47061));
    }

    #[test]
    fn ignores_non_update_unmapped_and_empty_book_messages() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        assert!(parse_book_ticker_message(
            r#"{"time":1,"channel":"futures.book_ticker","event":"subscribe","result":{"status":"success"}}"#,
            &map
        )
        .is_none());
        assert!(parse_book_ticker_message(
            r#"{"time":1,"channel":"futures.book_ticker","event":"update","result":{"s":"ETH_USDT","b":"1","B":1,"a":"2","A":1}}"#,
            &map
        )
        .is_none());
        assert!(parse_book_ticker_message(
            r#"{"time":1,"channel":"futures.book_ticker","event":"update","result":{"s":"BTC_USDT","b":"","B":0,"a":"2","A":1}}"#,
            &map
        )
        .is_none());
        assert!(parse_book_ticker_message("not json", &map).is_none());
    }

    #[test]
    fn builds_subscribe_message() {
        let source = GateFuturesSource::new(Venue::new("gate_futures"), vec![Symbol::new("PEPE", "USDT")], None);
        let msg: serde_json::Value = serde_json::from_str(&source.subscribe_message()).unwrap();
        assert_eq!(msg["channel"], "futures.book_ticker");
        assert_eq!(msg["event"], "subscribe");
        assert_eq!(msg["payload"], serde_json::json!(["PEPE_USDT"]));
    }

    #[test]
    fn parses_candlesticks_converting_seconds_to_ms() {
        let text = r#"[
            {"o":"0.000004","v":15494,"t":1791435600,"c":"0.000004036","l":"0.000004","h":"0.000005","sum":"1"},
            {"o":"0.000004","v":15494,"t":1791439200,"c":"0.000004050","l":"0.000004","h":"0.000005","sum":"1"}
        ]"#;
        let klines = parse_candlesticks(text).expect("should parse");
        assert_eq!(klines[0], Kline { open_time_ms: 1791435600000, close: "0.000004036".parse().unwrap() });
        assert_eq!(klines[1].open_time_ms, 1791439200000);
    }

    #[test]
    fn parse_candlesticks_surfaces_error_response() {
        let err = parse_candlesticks(r#"{"label":"INVALID_PARAM_VALUE","message":"bad contract"}"#).unwrap_err();
        assert!(err.to_string().contains("bad contract"));
    }

    #[test]
    fn parses_tickers_quote_volume() {
        let text = r#"[{"contract":"PEPE_USDT","volume_24h_quote":"21076878","quanto_multiplier":"10000000"}]"#;
        assert_eq!(parse_quote_volume(text).unwrap(), Decimal::from(21076878));
        assert!(parse_quote_volume("[]").is_err());
    }
}
