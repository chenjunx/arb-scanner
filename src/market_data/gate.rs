use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
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

use super::{MarketDataSource, now_ms};

const WS_HOST: &str = "api.gateio.ws";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://api.gateio.ws/ws/v4/";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Gate.io 现货 book_ticker 行情源：通过公共 WebSocket v4 接口订阅一批交易对的
/// 最优买卖一档，断线后自动按指数退避重连。支持通过 HTTP CONNECT 代理出网。
pub struct GateSpotSource {
    venue: Venue,
    symbols: Vec<Symbol>,
    proxy: Option<String>,
}

impl GateSpotSource {
    pub fn new(venue: Venue, symbols: Vec<Symbol>, proxy: Option<String>) -> Self {
        Self { venue, symbols, proxy }
    }

    fn gate_symbol(symbol: &Symbol) -> String {
        format!("{}_{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }

    fn symbol_map(&self) -> HashMap<String, Symbol> {
        self.symbols
            .iter()
            .map(|s| (Self::gate_symbol(s), s.clone()))
            .collect()
    }

    fn subscribe_message(&self) -> String {
        let pairs: Vec<String> = self.symbols.iter().map(Self::gate_symbol).collect();
        let time_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        serde_json::json!({
            "time": time_secs,
            "channel": "spot.book_ticker",
            "event": "subscribe",
            "payload": pairs,
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

impl MarketDataSource for GateSpotSource {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let symbol_map = self.symbol_map();
            let subscribe_msg = self.subscribe_message();
            let mut backoff = MIN_BACKOFF;

            loop {
                let mut ws = match self.connect().await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "gate ws connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(subscribe_msg.clone())).await {
                    warn!(
                        "gate ws subscribe failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                debug!("gate ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                while let Some(msg) = ws.next().await {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("gate ws error for venue={} err={err}", self.venue);
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
                    "gate ws disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        })
    }
}

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

/// 解析一条 spot.book_ticker update 消息,查表得到内部 Symbol。纯函数,不依赖网络
/// 连接,便于脱离真实 WebSocket 连接做单元测试。忽略订阅确认(event=subscribe)、
/// spot.pong、spot.system 等非 book_ticker update 消息,不打日志,避免刷屏。
fn parse_book_ticker_message(text: &str, symbol_map: &HashMap<String, Symbol>) -> Option<(Symbol, Quote)> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    if value.get("channel").and_then(|c| c.as_str()) != Some("spot.book_ticker") {
        return None;
    }
    if value.get("event").and_then(|e| e.as_str()) != Some("update") {
        return None;
    }

    let result: BookTickerResult = match serde_json::from_value(value.get("result")?.clone()) {
        Ok(result) => result,
        Err(err) => {
            warn!("failed to parse gate book_ticker message: {err}");
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

#[cfg(test)]
mod tests {
    use super::*;

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(GateSpotSource::gate_symbol(&symbol), symbol);
        map
    }

    #[test]
    fn parses_book_ticker_update() {
        let symbol = Symbol::new("BTC", "USDT");
        let map = map_with(symbol.clone());
        let text = r#"{
            "time": 1606293275,
            "time_ms": 1606293275723,
            "channel": "spot.book_ticker",
            "event": "update",
            "result": {
                "t": 1606293275123,
                "u": 48733182,
                "s": "BTC_USDT",
                "b": "19177.79",
                "B": "0.0003341504",
                "a": "19179.38",
                "A": "0.09"
            }
        }"#;

        let (parsed_symbol, quote) = parse_book_ticker_message(text, &map).expect("should parse");
        assert_eq!(parsed_symbol, symbol);
        assert_eq!(quote.bid, "19177.79".parse::<Decimal>().unwrap());
        assert_eq!(quote.bid_size, "0.0003341504".parse::<Decimal>().unwrap());
        assert_eq!(quote.ask, "19179.38".parse::<Decimal>().unwrap());
        assert_eq!(quote.ask_size, "0.09".parse::<Decimal>().unwrap());
    }

    #[test]
    fn ignores_message_for_unmapped_symbol() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        let text = r#"{
            "time": 1606293275,
            "channel": "spot.book_ticker",
            "event": "update",
            "result": {"t": 1, "u": 1, "s": "ETH_USDT", "b": "1.0", "B": "1.0", "a": "1.1", "A": "1.0"}
        }"#;

        assert!(parse_book_ticker_message(text, &map).is_none());
    }

    #[test]
    fn ignores_non_update_or_non_book_ticker_messages() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        assert!(parse_book_ticker_message(
            r#"{"time":1,"channel":"spot.book_ticker","event":"subscribe","result":{"status":"success"}}"#,
            &map
        )
        .is_none());
        assert!(parse_book_ticker_message(
            r#"{"time":1,"channel":"spot.pong","event":"","result":null}"#,
            &map
        )
        .is_none());
        assert!(parse_book_ticker_message(
            r#"{"time":1,"channel":"spot.system","event":"update","result":{"type":"upgrade"}}"#,
            &map
        )
        .is_none());
    }

    #[test]
    fn ignores_malformed_message() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        assert!(parse_book_ticker_message("not json", &map).is_none());
    }

    #[test]
    fn builds_uppercase_underscore_pair_symbol() {
        assert_eq!(GateSpotSource::gate_symbol(&Symbol::new("BTC", "USDT")), "BTC_USDT");
        assert_eq!(GateSpotSource::gate_symbol(&Symbol::new("eth", "usdt")), "ETH_USDT");
    }

    #[test]
    fn builds_subscribe_message() {
        let source = GateSpotSource::new(
            Venue::new("gate"),
            vec![Symbol::new("BTC", "USDT"), Symbol::new("ETH", "USDT")],
            None,
        );

        let msg: serde_json::Value = serde_json::from_str(&source.subscribe_message()).unwrap();
        assert_eq!(msg["channel"], "spot.book_ticker");
        assert_eq!(msg["event"], "subscribe");
        assert_eq!(msg["payload"], serde_json::json!(["BTC_USDT", "ETH_USDT"]));
    }
}
