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

const WS_HOST: &str = "ws.kraken.com";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://ws.kraken.com/v2";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Kraken 现货 ticker 行情源：通过公共 WebSocket v2 接口订阅一批交易对的最优买卖一档，
/// 断线后自动按指数退避重连。支持通过 HTTP CONNECT 代理出网。
pub struct KrakenSpotSource {
    venue: Venue,
    symbols: Vec<Symbol>,
    proxy: Option<String>,
}

impl KrakenSpotSource {
    pub fn new(venue: Venue, symbols: Vec<Symbol>, proxy: Option<String>) -> Self {
        Self { venue, symbols, proxy }
    }

    fn kraken_symbol(symbol: &Symbol) -> String {
        format!("{}/{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }

    fn symbol_map(&self) -> HashMap<String, Symbol> {
        self.symbols
            .iter()
            .map(|s| (Self::kraken_symbol(s), s.clone()))
            .collect()
    }

    fn subscribe_message(&self) -> String {
        let pairs: Vec<String> = self.symbols.iter().map(Self::kraken_symbol).collect();
        serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": pairs,
                "event_trigger": "bbo",
            }
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

impl MarketDataSource for KrakenSpotSource {
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
                            "kraken ws connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(subscribe_msg.clone())).await {
                    warn!(
                        "kraken ws subscribe failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                debug!("kraken ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                while let Some(msg) = ws.next().await {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("kraken ws error for venue={} err={err}", self.venue);
                            break;
                        }
                    };
                    let Message::Text(text) = msg else {
                        continue;
                    };
                    for (symbol, quote) in parse_ticker_message(&text, &symbol_map) {
                        bus.publish(Topic::quote(self.venue.clone(), symbol), quote);
                    }
                }

                warn!(
                    "kraken ws disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        })
    }
}

#[derive(Debug, Deserialize)]
struct TickerData {
    symbol: String,
    bid: Decimal,
    bid_qty: Decimal,
    ask: Decimal,
    ask_qty: Decimal,
}

#[derive(Debug, Deserialize)]
struct TickerMessage {
    data: Vec<TickerData>,
}

/// 解析一条 v2 ticker 消息,查表得到内部 Symbol。纯函数,不依赖网络连接,便于脱离
/// 真实 WebSocket 连接做单元测试。忽略 heartbeat/订阅确认等非 ticker 消息,不打日志,
/// 避免刷屏。
fn parse_ticker_message(text: &str, symbol_map: &HashMap<String, Symbol>) -> Vec<(Symbol, Quote)> {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };
    if value.get("channel").and_then(|c| c.as_str()) != Some("ticker") {
        return Vec::new();
    }

    let msg: TickerMessage = match serde_json::from_value(value) {
        Ok(msg) => msg,
        Err(err) => {
            warn!("failed to parse kraken ticker message: {err}");
            return Vec::new();
        }
    };

    msg.data
        .iter()
        .filter_map(|d| {
            let symbol = symbol_map.get(&d.symbol)?;
            Some((
                symbol.clone(),
                Quote {
                    bid: d.bid,
                    bid_size: d.bid_qty,
                    ask: d.ask,
                    ask_size: d.ask_qty,
                    ts_ms: now_ms(),
                },
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(KrakenSpotSource::kraken_symbol(&symbol), symbol);
        map
    }

    #[test]
    fn parses_ticker_update() {
        let symbol = Symbol::new("BTC", "USD");
        let map = map_with(symbol.clone());
        let text = r#"{
            "channel": "ticker",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bid": 67000.1,
                    "bid_qty": 0.5,
                    "ask": 67000.2,
                    "ask_qty": 0.3,
                    "last": 67000.1,
                    "volume": 1234.5,
                    "vwap": 66950.0,
                    "low": 66000.0,
                    "high": 68000.0,
                    "change": 100.0,
                    "change_pct": 0.15
                }
            ]
        }"#;

        let parsed = parse_ticker_message(text, &map);
        assert_eq!(parsed.len(), 1);
        let (parsed_symbol, quote) = &parsed[0];
        assert_eq!(*parsed_symbol, symbol);
        assert_eq!(quote.bid, "67000.1".parse::<Decimal>().unwrap());
        assert_eq!(quote.bid_size, "0.5".parse::<Decimal>().unwrap());
        assert_eq!(quote.ask, "67000.2".parse::<Decimal>().unwrap());
        assert_eq!(quote.ask_size, "0.3".parse::<Decimal>().unwrap());
    }

    #[test]
    fn ignores_message_for_unmapped_symbol() {
        let map = map_with(Symbol::new("BTC", "USD"));
        let text = r#"{
            "channel": "ticker",
            "type": "update",
            "data": [
                {"symbol": "ETH/USD", "bid": 1.0, "bid_qty": 1.0, "ask": 1.1, "ask_qty": 1.0,
                 "last": 1.0, "volume": 1.0, "vwap": 1.0, "low": 1.0, "high": 1.0, "change": 0.0, "change_pct": 0.0}
            ]
        }"#;

        assert!(parse_ticker_message(text, &map).is_empty());
    }

    #[test]
    fn ignores_non_ticker_channel_messages() {
        let map = map_with(Symbol::new("BTC", "USD"));
        assert!(parse_ticker_message(r#"{"channel":"heartbeat"}"#, &map).is_empty());
        assert!(parse_ticker_message(r#"{"method":"subscribe","success":true,"result":{}}"#, &map).is_empty());
    }

    #[test]
    fn ignores_malformed_message() {
        let map = map_with(Symbol::new("BTC", "USD"));
        assert!(parse_ticker_message("not json", &map).is_empty());
    }

    #[test]
    fn builds_uppercase_slash_pair_symbol() {
        assert_eq!(KrakenSpotSource::kraken_symbol(&Symbol::new("BTC", "USD")), "BTC/USD");
        assert_eq!(KrakenSpotSource::kraken_symbol(&Symbol::new("eth", "usdt")), "ETH/USDT");
    }

    #[test]
    fn builds_subscribe_message() {
        let source = KrakenSpotSource::new(
            Venue::new("kraken"),
            vec![Symbol::new("BTC", "USD"), Symbol::new("ETH", "USD")],
            None,
        );

        let msg: serde_json::Value = serde_json::from_str(&source.subscribe_message()).unwrap();
        assert_eq!(msg["method"], "subscribe");
        assert_eq!(msg["params"]["channel"], "ticker");
        assert_eq!(msg["params"]["symbol"], serde_json::json!(["BTC/USD", "ETH/USD"]));
        assert_eq!(msg["params"]["event_trigger"], "bbo");
    }
}
