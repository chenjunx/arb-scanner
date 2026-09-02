use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use flate2::read::GzDecoder;
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

const WS_HOST: &str = "socket.coinex.com";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://socket.coinex.com/v2/spot";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const PING_INTERVAL: Duration = Duration::from_secs(30);
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// CoinEx 现货 BBO 行情源：通过 v2 公共 WebSocket 订阅一批交易对的最优买卖一档，
/// 断线后自动按指数退避重连。CoinEx 服务端推送一律用 gzip 压缩后以 Binary 帧
/// 发送，需要先解压再解析 JSON。支持通过 HTTP CONNECT 代理出网。
pub struct CoinexSpotSource {
    venue: Venue,
    symbols: Vec<Symbol>,
    proxy: Option<String>,
}

impl CoinexSpotSource {
    pub fn new(venue: Venue, symbols: Vec<Symbol>, proxy: Option<String>) -> Self {
        Self { venue, symbols, proxy }
    }

    fn coinex_symbol(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }

    fn symbol_map(&self) -> HashMap<String, Symbol> {
        self.symbols
            .iter()
            .map(|s| (Self::coinex_symbol(s), s.clone()))
            .collect()
    }

    fn subscribe_message(&self) -> String {
        let pairs: Vec<String> = self.symbols.iter().map(Self::coinex_symbol).collect();
        serde_json::json!({
            "method": "bbo.subscribe",
            "params": { "market_list": pairs },
            "id": 1,
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

impl MarketDataSource for CoinexSpotSource {
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
                            "coinex ws connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(subscribe_msg.clone())).await {
                    warn!(
                        "coinex ws subscribe failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                debug!("coinex ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                let mut ping_id: u64 = 2;
                let mut ping_interval = tokio::time::interval(PING_INTERVAL);
                ping_interval.tick().await; // 首次 tick 立即触发，跳过，避免刚连上就发一次多余的 ping

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            let ping = serde_json::json!({
                                "method": "server.ping",
                                "params": {},
                                "id": ping_id,
                            });
                            ping_id += 1;
                            if let Err(err) = ws.send(Message::Text(ping.to_string())).await {
                                warn!("coinex ws ping failed for venue={} err={err}", self.venue);
                                break;
                            }
                        }
                        msg = tokio::time::timeout(IDLE_TIMEOUT, ws.next()) => {
                            let msg = match msg {
                                Ok(Some(Ok(msg))) => msg,
                                Ok(Some(Err(err))) => {
                                    warn!("coinex ws error for venue={} err={err}", self.venue);
                                    break;
                                }
                                Ok(None) => break,
                                Err(_) => {
                                    warn!("coinex ws idle timeout for venue={}, no message in {IDLE_TIMEOUT:?}", self.venue);
                                    break;
                                }
                            };
                            let Message::Binary(bytes) = msg else {
                                continue;
                            };
                            let text = match decompress_gzip(&bytes) {
                                Ok(text) => text,
                                Err(err) => {
                                    warn!("coinex ws failed to decompress message for venue={} err={err:#}", self.venue);
                                    continue;
                                }
                            };
                            for (symbol, quote) in parse_bbo_message(&text, &symbol_map) {
                                bus.publish(Topic::quote(self.venue.clone(), symbol), quote);
                            }
                        }
                    }
                }

                warn!(
                    "coinex ws disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        })
    }
}

/// CoinEx v2 WS 服务端推送一律 gzip 压缩后以 Binary 帧发送,需要先解压出 JSON
/// 文本。纯函数,不依赖网络连接,便于脱离真实 WebSocket 连接做单元测试。
fn decompress_gzip(bytes: &[u8]) -> anyhow::Result<String> {
    let mut decoder = GzDecoder::new(bytes);
    let mut text = String::new();
    decoder.read_to_string(&mut text).context("failed to gunzip coinex ws message")?;
    Ok(text)
}

#[derive(Debug, Deserialize)]
struct BboData {
    market: String,
    best_bid_price: Decimal,
    best_bid_size: Decimal,
    best_ask_price: Decimal,
    best_ask_size: Decimal,
}

#[derive(Debug, Deserialize)]
struct BboMessage {
    data: BboData,
}

/// 解析一条解压后的 v2 消息,查表得到内部 Symbol。纯函数,不依赖网络连接,便于
/// 脱离真实 WebSocket 连接做单元测试。订阅确认(`{"id":..,"code":0,...}`)和
/// ping 的 pong 响应都没有 `method` 字段,天然被过滤掉,不需要额外特判。
fn parse_bbo_message(text: &str, symbol_map: &HashMap<String, Symbol>) -> Vec<(Symbol, Quote)> {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };
    if value.get("method").and_then(|m| m.as_str()) != Some("bbo.update") {
        return Vec::new();
    }

    let msg: BboMessage = match serde_json::from_value(value) {
        Ok(msg) => msg,
        Err(err) => {
            warn!("failed to parse coinex bbo message: {err}");
            return Vec::new();
        }
    };

    let Some(symbol) = symbol_map.get(&msg.data.market) else {
        return Vec::new();
    };
    vec![(
        symbol.clone(),
        Quote {
            bid: msg.data.best_bid_price,
            bid_size: msg.data.best_bid_size,
            ask: msg.data.best_ask_price,
            ask_size: msg.data.best_ask_size,
            ts_ms: now_ms(),
        },
    )]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(CoinexSpotSource::coinex_symbol(&symbol), symbol);
        map
    }

    #[test]
    fn parses_bbo_update() {
        let symbol = Symbol::new("BTC", "USDT");
        let map = map_with(symbol.clone());
        let text = r#"{
            "method": "bbo.update",
            "data": {
                "market": "BTCUSDT",
                "updated_at": 1642145331234,
                "best_bid_price": "67000.1",
                "best_bid_size": "0.5",
                "best_ask_price": "67000.2",
                "best_ask_size": "0.3"
            },
            "id": null
        }"#;

        let parsed = parse_bbo_message(text, &map);
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
        let map = map_with(Symbol::new("BTC", "USDT"));
        let text = r#"{
            "method": "bbo.update",
            "data": {
                "market": "ETHUSDT",
                "updated_at": 1,
                "best_bid_price": "1.0",
                "best_bid_size": "1.0",
                "best_ask_price": "1.1",
                "best_ask_size": "1.0"
            },
            "id": null
        }"#;

        assert!(parse_bbo_message(text, &map).is_empty());
    }

    #[test]
    fn ignores_non_bbo_method_messages() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        assert!(parse_bbo_message(r#"{"id":1,"code":0,"data":{},"message":"OK"}"#, &map).is_empty());
        assert!(
            parse_bbo_message(r#"{"id":2,"code":0,"data":{"result":"pong"},"message":"OK"}"#, &map).is_empty()
        );
    }

    #[test]
    fn ignores_malformed_message() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        assert!(parse_bbo_message("not json", &map).is_empty());
    }

    #[test]
    fn builds_uppercase_concatenated_symbol() {
        assert_eq!(CoinexSpotSource::coinex_symbol(&Symbol::new("BTC", "USDT")), "BTCUSDT");
        assert_eq!(CoinexSpotSource::coinex_symbol(&Symbol::new("eth", "usdt")), "ETHUSDT");
    }

    #[test]
    fn builds_subscribe_message() {
        let source = CoinexSpotSource::new(
            Venue::new("coinex"),
            vec![Symbol::new("BTC", "USDT"), Symbol::new("ETH", "USDT")],
            None,
        );

        let msg: serde_json::Value = serde_json::from_str(&source.subscribe_message()).unwrap();
        assert_eq!(msg["method"], "bbo.subscribe");
        assert_eq!(msg["params"]["market_list"], serde_json::json!(["BTCUSDT", "ETHUSDT"]));
    }

    #[test]
    fn gzip_roundtrip() {
        let original = r#"{"method":"bbo.update","data":{"market":"BTCUSDT"}}"#;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(original.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let decompressed = decompress_gzip(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }
}
