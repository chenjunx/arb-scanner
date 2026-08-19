use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures_util::StreamExt;
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

const MAINNET_WS_HOST: &str = "fstream.binance.com";
const MAINNET_WS_PORT: u16 = 443;
const TESTNET_WS_HOST: &str = "stream.binancefuture.com";
const TESTNET_WS_PORT: u16 = 443;
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// 币安 U 本位永续合约 bookTicker 行情源：通过 combined stream WebSocket 订阅一批
/// 交易对的最优买卖一档,断线后自动按指数退避重连。支持通过 HTTP CONNECT 代理出网。
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
            .map(|s| format!("{}@bookTicker", Self::binance_symbol(s).to_ascii_lowercase()))
            .collect::<Vec<_>>()
            .join("/");
        format!("wss://{}:{}/stream?streams={}", self.ws_host, self.ws_port, streams)
    }

    /// 建连:先拿到 TCP 流(直连或经代理隧道),再在其上做 TLS + WebSocket 握手。
    async fn connect(&self, url: &str) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(self.ws_host, self.ws_port, self.proxy.as_deref()).await?;
        let (ws, _) = tokio_tungstenite::client_async_tls(url, tcp)
            .await
            .context("websocket handshake failed")?;
        Ok(ws)
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

                while let Some(msg) = ws.next().await {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("binance futures ws error for venue={} err={err}", self.venue);
                            break;
                        }
                    };
                    let Message::Text(text) = msg else {
                        continue;
                    };
                    let Some((symbol, quote)) = parse_book_ticker(&text, &symbol_map) else {
                        continue;
                    };
                    bus.publish(Topic::quote(self.venue.clone(), symbol), quote);
                }

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
            "wss://fstream.binance.com:443/stream?streams=btcusdt@bookTicker/ethbtc@bookTicker"
        );
    }

    #[test]
    fn testnet_uses_testnet_host() {
        let source =
            BinanceFuturesSource::new(Venue::new("binance_futures"), vec![Symbol::new("BTC", "USDT")], true, None);

        assert!(source.stream_url().starts_with(&format!("wss://{TESTNET_WS_HOST}")));
    }
}
