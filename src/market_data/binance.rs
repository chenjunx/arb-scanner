use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures_util::StreamExt;
use log::{debug, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::market_data::orderbook::LocalOrderBook;
use crate::net::connect_tcp;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{MarketDataSource, now_ms};

const MAINNET_WS_HOST: &str = "stream.binance.com";
const MAINNET_WS_PORT: u16 = 9443;
const TESTNET_WS_HOST: &str = "testnet.binance.vision";
const TESTNET_WS_PORT: u16 = 443;
const MAINNET_REST_HOST: &str = "https://api.binance.com";
const TESTNET_REST_HOST: &str = "https://testnet.binance.vision";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// 深度增量续号断档/REST 快照拉取失败时整条连接重连重建；另外作为兜底，固定
/// 周期无条件重连一次(参考 `kraken.rs` 同名常量)，防止断档检测覆盖不到的
/// 错误状态一直带病运行。
const ORDER_BOOK_RESYNC_INTERVAL: Duration = Duration::from_secs(20 * 60);

/// 币安现货行情源：通过 combined stream WebSocket 订阅一批交易对的
/// bookTicker(最优买卖一档,发布为 `Quote`) + 增量深度(`<symbol>@depth@100ms`,
/// 按官方"REST 快照 + 续号校验"算法在本地合并成完整订单簿,发布为
/// `OrderBookLevels`)。断线后自动按指数退避重连。支持通过 HTTP CONNECT 代理出网。
pub struct BinanceSpotSource {
    venue: Venue,
    symbols: Vec<Symbol>,
    ws_host: &'static str,
    ws_port: u16,
    rest_host: &'static str,
    proxy: Option<String>,
    http: reqwest::Client,
}

impl BinanceSpotSource {
    pub fn new(venue: Venue, symbols: Vec<Symbol>, testnet: bool, proxy: Option<String>) -> anyhow::Result<Self> {
        let (ws_host, ws_port, rest_host) = if testnet {
            (TESTNET_WS_HOST, TESTNET_WS_PORT, TESTNET_REST_HOST)
        } else {
            (MAINNET_WS_HOST, MAINNET_WS_PORT, MAINNET_REST_HOST)
        };
        let http = build_http_client(proxy.as_deref())?;
        Ok(Self {
            venue,
            symbols,
            ws_host,
            ws_port,
            rest_host,
            proxy,
            http,
        })
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
                let lower = Self::binance_symbol(s).to_ascii_lowercase();
                [format!("{lower}@bookTicker"), format!("{lower}@depth@100ms")]
            })
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

impl MarketDataSource for BinanceSpotSource {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let symbol_map = self.symbol_map();
            let url = self.stream_url();
            let mut backoff = MIN_BACKOFF;

            'reconnect: loop {
                let mut ws = match self.connect(&url).await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "binance ws connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };
                debug!("binance ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                let mut books: HashMap<Symbol, LocalOrderBook> = HashMap::new();
                let mut sync: HashMap<Symbol, DepthSync> =
                    symbol_map.values().map(|s| (s.clone(), DepthSync::Buffering(Vec::new()))).collect();

                // 每个 symbol 并发拉一次 REST 快照，期间到达的增量先缓冲在
                // `DepthSync::Buffering` 里，快照回来后按官方算法重放缓冲、校验续号。
                let (snapshot_tx, mut snapshot_rx) = mpsc::unbounded_channel();
                for symbol in symbol_map.values().cloned().collect::<Vec<_>>() {
                    let tx = snapshot_tx.clone();
                    let http = self.http.clone();
                    let rest_host = self.rest_host;
                    let binance_symbol = Self::binance_symbol(&symbol);
                    tokio::spawn(async move {
                        let result = fetch_depth_snapshot(&http, rest_host, &binance_symbol).await;
                        let _ = tx.send((symbol, result));
                    });
                }
                drop(snapshot_tx);

                let mut resync_timer = tokio::time::interval(ORDER_BOOK_RESYNC_INTERVAL);
                resync_timer.tick().await;

                loop {
                    tokio::select! {
                        _ = resync_timer.tick() => {
                            debug!("binance ws periodic orderbook resync for venue={}, reconnecting", self.venue);
                            break;
                        }
                        snapshot = snapshot_rx.recv() => {
                            let Some((symbol, result)) = snapshot else { continue; };
                            match result {
                                Ok(snap) => {
                                    if !apply_snapshot(&symbol, snap, &mut books, &mut sync) {
                                        warn!(
                                            "binance depth snapshot/buffer gap for venue={} symbol={symbol}, reconnecting to resync",
                                            self.venue
                                        );
                                        break;
                                    }
                                    if let Some(book) = books.get(&symbol) {
                                        bus.publish(Topic::order_book(self.venue.clone(), symbol.clone()), book.snapshot(now_ms()));
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                        "binance depth snapshot fetch failed for venue={} symbol={symbol} err={err:#}, reconnecting to resync",
                                        self.venue
                                    );
                                    break;
                                }
                            }
                        }
                        msg = ws.next() => {
                            let msg = match msg {
                                Some(Ok(msg)) => msg,
                                Some(Err(err)) => {
                                    warn!("binance ws error for venue={} err={err}", self.venue);
                                    break;
                                }
                                None => break,
                            };
                            let Message::Text(text) = msg else {
                                continue;
                            };
                            let Ok(stream_only) = serde_json::from_str::<StreamOnly>(&text) else {
                                continue;
                            };
                            match stream_only.stream {
                                Some(s) if s.ends_with("@bookTicker") => {
                                    if let Some((symbol, quote)) = parse_book_ticker(&text, &symbol_map) {
                                        bus.publish(Topic::quote(self.venue.clone(), symbol), quote);
                                    }
                                }
                                Some(s) if s.ends_with("@depth@100ms") => {
                                    let Some((symbol, update)) = parse_depth_update(&text, &symbol_map) else {
                                        continue;
                                    };
                                    match sync.get_mut(&symbol) {
                                        Some(DepthSync::Buffering(buffer)) => buffer.push(update),
                                        Some(DepthSync::Synced { last_u }) => {
                                            if update.first_update_id != *last_u + 1 {
                                                warn!(
                                                    "binance depth update gap for venue={} symbol={symbol}, reconnecting to resync",
                                                    self.venue
                                                );
                                                break;
                                            }
                                            let book = books.entry(symbol.clone()).or_default();
                                            apply_levels(book, &update.b, &update.a);
                                            *last_u = update.u;
                                            bus.publish(Topic::order_book(self.venue.clone(), symbol), book.snapshot(now_ms()));
                                        }
                                        None => {}
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }

                warn!(
                    "binance ws disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue 'reconnect;
            }
        })
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
    if let Some(proxy) = proxy {
        builder = builder.proxy(reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?);
    }
    builder.build().context("failed to build binance http client")
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
            warn!("failed to parse binance book ticker message: {err}");
            return None;
        }
    };
    let payload = envelope.data;
    let Some(symbol) = symbol_map.get(&payload.s) else {
        warn!("binance book ticker for unknown symbol: {}", payload.s);
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

/// 只取 `stream` 字段，用来判断一条 combined stream 消息是 bookTicker 还是
/// depth 频道，再分流到各自的解析函数(两边的 `data` 结构完全不同，不能像
/// `parse_book_ticker` 那样直接无脑解析)。
#[derive(Debug, Deserialize)]
struct StreamOnly<'a> {
    stream: Option<&'a str>,
}

/// 增量深度更新的本地同步状态：REST 快照返回之前，先把收到的增量缓冲起来；
/// 快照应用成功后切到 `Synced`，记录上一条增量的 `u`，供下一条做续号校验
/// (`U == last_u + 1`)。
enum DepthSync {
    Buffering(Vec<DepthUpdatePayload>),
    Synced { last_u: u64 },
}

#[derive(Debug, Deserialize)]
struct DepthUpdatePayload {
    s: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    u: u64,
    #[serde(default)]
    b: Vec<(Decimal, Decimal)>,
    #[serde(default)]
    a: Vec<(Decimal, Decimal)>,
}

#[derive(Debug, Deserialize)]
struct DepthCombinedEnvelope {
    data: DepthUpdatePayload,
}

/// 解析一条 combined stream 的增量深度(`<symbol>@depth@100ms`)消息。纯函数，
/// 不依赖网络连接，便于单测。
fn parse_depth_update(text: &str, symbol_map: &HashMap<String, Symbol>) -> Option<(Symbol, DepthUpdatePayload)> {
    let envelope: DepthCombinedEnvelope = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(err) => {
            warn!("failed to parse binance depth update message: {err}");
            return None;
        }
    };
    let payload = envelope.data;
    let Some(symbol) = symbol_map.get(&payload.s) else {
        warn!("binance depth update for unknown symbol: {}", payload.s);
        return None;
    };
    Some((symbol.clone(), payload))
}

#[derive(Debug, Deserialize)]
struct DepthSnapshotPayload {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

/// 拉一次 `GET /api/v3/depth` REST 快照(limit=1000，币安现货深度接口单次上限)。
async fn fetch_depth_snapshot(http: &reqwest::Client, rest_host: &str, binance_symbol: &str) -> anyhow::Result<DepthSnapshotPayload> {
    crate::ratelimit::throttle(rest_host).await;
    let url = format!("{rest_host}/api/v3/depth?symbol={binance_symbol}&limit=1000");
    let resp = http.get(&url).send().await.context("binance depth snapshot request failed")?;
    let text = resp.text().await.context("failed to read binance depth snapshot response body")?;
    serde_json::from_str(&text).context("failed to parse binance depth snapshot response")
}

fn apply_levels(book: &mut LocalOrderBook, bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)]) {
    for (price, qty) in bids {
        book.apply_bid(*price, *qty);
    }
    for (price, qty) in asks {
        book.apply_ask(*price, *qty);
    }
}

/// 按官方算法("How to manage a local order book correctly")把 REST 快照应用
/// 到本地簿，再重放快照到达之前缓冲的增量：第一条重放的增量要求
/// `U <= lastUpdateId+1 <= u`，之后每条要求 `U == 上一条.u + 1`。重放中途断档
/// 返回 `false`，调用方据此整条连接重连重新来过(和 `kraken.rs` 一样，所有
/// resync 触发都统一走"断连重连"，不单独维护逐 symbol 的重新订阅流程)。
fn apply_snapshot(
    symbol: &Symbol,
    snapshot: DepthSnapshotPayload,
    books: &mut HashMap<Symbol, LocalOrderBook>,
    sync: &mut HashMap<Symbol, DepthSync>,
) -> bool {
    let buffered = match sync.remove(symbol) {
        Some(DepthSync::Buffering(buffer)) => buffer,
        _ => Vec::new(),
    };

    let book = books.entry(symbol.clone()).or_default();
    book.clear();
    apply_levels(book, &snapshot.bids, &snapshot.asks);

    let mut last_u = snapshot.last_update_id;
    let mut first = true;
    for update in buffered {
        if update.u <= snapshot.last_update_id {
            continue;
        }
        if first {
            if update.first_update_id > snapshot.last_update_id + 1 {
                return false;
            }
            first = false;
        } else if update.first_update_id != last_u + 1 {
            return false;
        }
        apply_levels(book, &update.b, &update.a);
        last_u = update.u;
    }

    sync.insert(symbol.clone(), DepthSync::Synced { last_u });
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(BinanceSpotSource::binance_symbol(&symbol), symbol);
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

    /// `parse_book_ticker` 是纯函数（不依赖网络/bus），直接测它本身的 CPU
    /// 开销：serde_json 反序列化 + Decimal 解析 + symbol_map 查表，不含任何
    /// WS 收包/调度耗时。
    #[test]
    fn measures_json_parse_latency() {
        use std::hint::black_box;
        use std::time::{Duration, Instant};

        let symbol = Symbol::new("BNB", "USDT");
        let map = map_with(symbol);
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

        for _ in 0..1_000 {
            black_box(parse_book_ticker(black_box(text), &map));
        }

        const ITERATIONS: usize = 20_000;
        let mut samples = Vec::with_capacity(ITERATIONS);
        for _ in 0..ITERATIONS {
            let start = Instant::now();
            let result = parse_book_ticker(black_box(text), &map);
            let elapsed = start.elapsed();
            black_box(result);
            samples.push(elapsed);
        }

        samples.sort();
        let sum: Duration = samples.iter().sum();
        let mean = sum / samples.len() as u32;
        let min = samples[0];
        let p50 = samples[samples.len() / 2];
        let p95 = samples[samples.len() * 95 / 100];
        let p99 = samples[samples.len() * 99 / 100];
        let max = *samples.last().unwrap();

        println!(
            "binance parse_book_ticker latency, n={}: min={min:?} p50={p50:?} mean={mean:?} p95={p95:?} p99={p99:?} max={max:?}",
            samples.len(),
        );
    }

    #[test]
    fn builds_lowercase_combined_stream_url() {
        let source = BinanceSpotSource::new(
            Venue::new("binance"),
            vec![Symbol::new("BTC", "USDT"), Symbol::new("ETH", "BTC")],
            false,
            None,
        )
        .unwrap();

        assert_eq!(
            source.stream_url(),
            "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms/ethbtc@bookTicker/ethbtc@depth@100ms"
        );
    }

    #[test]
    fn testnet_uses_testnet_host() {
        let source = BinanceSpotSource::new(Venue::new("binance"), vec![Symbol::new("BTC", "USDT")], true, None).unwrap();

        assert!(source.stream_url().starts_with(&format!("wss://{TESTNET_WS_HOST}")));
    }

    #[test]
    fn parses_depth_update_message() {
        let symbol = Symbol::new("BNB", "USDT");
        let map = map_with(symbol.clone());
        let text = r#"{
            "stream": "bnbusdt@depth@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1672515782136,
                "s": "BNBUSDT",
                "U": 157,
                "u": 160,
                "b": [["25.396", "10.000"]],
                "a": [["25.397", "5.000"]]
            }
        }"#;

        let (parsed_symbol, update) = parse_depth_update(text, &map).expect("should parse");
        assert_eq!(parsed_symbol, symbol);
        assert_eq!(update.first_update_id, 157);
        assert_eq!(update.u, 160);
        assert_eq!(update.b, vec![("25.396".parse().unwrap(), "10.000".parse().unwrap())]);
        assert_eq!(update.a, vec![("25.397".parse().unwrap(), "5.000".parse().unwrap())]);
    }

    #[test]
    fn apply_snapshot_replays_buffered_updates_after_lastupdateid() {
        let symbol = Symbol::new("BNB", "USDT");
        let mut books = HashMap::new();
        let mut sync = HashMap::new();
        sync.insert(
            symbol.clone(),
            DepthSync::Buffering(vec![
                // u <= lastUpdateId(150)，应被丢弃，不计入续号校验。
                DepthUpdatePayload {
                    s: "BNBUSDT".to_string(),
                    first_update_id: 145,
                    u: 150,
                    b: vec![],
                    a: vec![],
                },
                DepthUpdatePayload {
                    s: "BNBUSDT".to_string(),
                    first_update_id: 151,
                    u: 155,
                    b: vec![("99.0".parse().unwrap(), "2.0".parse().unwrap())],
                    a: vec![],
                },
                DepthUpdatePayload {
                    s: "BNBUSDT".to_string(),
                    first_update_id: 156,
                    u: 160,
                    b: vec![],
                    a: vec![("101.0".parse().unwrap(), "1.0".parse().unwrap())],
                },
            ]),
        );

        let snapshot = DepthSnapshotPayload {
            last_update_id: 150,
            bids: vec![("100.0".parse().unwrap(), "1.0".parse().unwrap())],
            asks: vec![],
        };

        assert!(apply_snapshot(&symbol, snapshot, &mut books, &mut sync));
        assert!(matches!(sync.get(&symbol), Some(DepthSync::Synced { last_u: 160 })));

        let book = books.get(&symbol).unwrap().snapshot(0);
        assert_eq!(
            book.bids.as_ref(),
            &vec![("100.0".parse().unwrap(), "1.0".parse().unwrap()), ("99.0".parse().unwrap(), "2.0".parse().unwrap())]
        );
        assert_eq!(book.asks.as_ref(), &vec![("101.0".parse().unwrap(), "1.0".parse().unwrap())]);
    }

    #[test]
    fn apply_snapshot_detects_gap_in_buffered_updates() {
        let symbol = Symbol::new("BNB", "USDT");
        let mut books = HashMap::new();
        let mut sync = HashMap::new();
        sync.insert(
            symbol.clone(),
            DepthSync::Buffering(vec![DepthUpdatePayload {
                s: "BNBUSDT".to_string(),
                // U(200) > lastUpdateId(150)+1，续号对不上，中间缺了数据。
                first_update_id: 200,
                u: 205,
                b: vec![],
                a: vec![],
            }]),
        );

        let snapshot = DepthSnapshotPayload {
            last_update_id: 150,
            bids: vec![],
            asks: vec![],
        };

        assert!(!apply_snapshot(&symbol, snapshot, &mut books, &mut sync));
    }
}
