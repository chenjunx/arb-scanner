use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::market_data::orderbook::{LocalOrderBook, OrderBookLevels};
use crate::net::connect_tcp;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{MarketDataSource, now_ms};

const WS_HOST: &str = "ws.kraken.com";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://ws.kraken.com/v2";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// `book` 频道允许的深度档位之一(10/25/100/500/1000)，选 100 档。
const BOOK_DEPTH: u32 = 100;
/// 定期强制重连重新拉订单簿快照，兜底 checksum 覆盖不到的深档悄悄跑偏
/// (checksum 只校验前 10 档)；不依赖任何错误检测，纯定时触发。
const ORDER_BOOK_RESYNC_INTERVAL: Duration = Duration::from_secs(20 * 60);

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

    /// 订阅多档订单簿频道，和 `ticker` 频道并存在同一条连接上。
    fn subscribe_book_message(&self) -> String {
        let pairs: Vec<String> = self.symbols.iter().map(Self::kraken_symbol).collect();
        serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol": pairs,
                "depth": BOOK_DEPTH,
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
                if let Err(err) = ws.send(Message::Text(self.subscribe_book_message())).await {
                    warn!(
                        "kraken ws book subscribe failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                debug!("kraken ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                let mut books: HashMap<Symbol, LocalOrderBook> = HashMap::new();
                let mut resync = tokio::time::interval(ORDER_BOOK_RESYNC_INTERVAL);
                resync.tick().await;
                'conn: loop {
                    tokio::select! {
                        _ = resync.tick() => {
                            debug!("kraken ws periodic orderbook resync for venue={}, reconnecting", self.venue);
                            break 'conn;
                        }
                        msg = ws.next() => {
                            let msg = match msg {
                                Some(Ok(msg)) => msg,
                                Some(Err(err)) => {
                                    warn!("kraken ws error for venue={} err={err}", self.venue);
                                    break 'conn;
                                }
                                None => break 'conn,
                            };
                            let Message::Text(text) = msg else {
                                continue;
                            };
                            for (symbol, quote) in parse_ticker_message(&text, &symbol_map) {
                                bus.publish(Topic::quote(self.venue.clone(), symbol), quote);
                            }
                            let mut needs_resync = false;
                            for event in apply_book_message(&text, &symbol_map, &mut books) {
                                match event {
                                    BookEvent::Updated(symbol, levels) => {
                                        bus.publish(Topic::order_book(self.venue.clone(), symbol), levels);
                                    }
                                    BookEvent::ChecksumMismatch(symbol) => {
                                        warn!(
                                            "kraken orderbook checksum mismatch for venue={} symbol={symbol}, reconnecting to resync",
                                            self.venue
                                        );
                                        needs_resync = true;
                                    }
                                }
                            }
                            if needs_resync {
                                break 'conn;
                            }
                        }
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

// `price`/`qty` 用 `RawValue` 接住原始 JSON 数字文本(而不是直接声明成
// `Decimal` 让 serde 走它自己的数字反序列化)，是因为 Kraken 的 bare JSON
// number(如 `100.00000`)经由 serde_json 的 f64 路径反序列化会丢失末尾的 0
// (变成 `Decimal("100")`)，而 checksum 算法要求的是线上原样的数字字符串。
// `RawValue::get()` 拿到的文本原样喂给 `Decimal::from_str` 能保留精度，
// 之后 `Decimal::to_string()` 就能还原出一致的字符串用于 checksum。
#[derive(Debug, Deserialize)]
struct BookLevel<'a> {
    #[serde(borrow)]
    price: &'a RawValue,
    #[serde(borrow)]
    qty: &'a RawValue,
}

impl BookLevel<'_> {
    fn price(&self) -> Decimal {
        self.price.get().parse().unwrap_or_default()
    }

    fn qty(&self) -> Decimal {
        self.qty.get().parse().unwrap_or_default()
    }
}

#[derive(Debug, Deserialize)]
struct BookData<'a> {
    symbol: String,
    #[serde(default, borrow)]
    bids: Vec<BookLevel<'a>>,
    #[serde(default, borrow)]
    asks: Vec<BookLevel<'a>>,
    #[serde(default)]
    checksum: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct BookMessage<'a> {
    #[serde(rename = "type")]
    kind: String,
    #[serde(borrow)]
    data: Vec<BookData<'a>>,
}

/// 只取 `channel` 字段的轻量结构，用于在真正按 `BookMessage` 解析之前先判断
/// 是不是 `book` 频道的消息——`BookMessage` 借用了 `RawValue`，必须直接从
/// 原始文本反序列化(不能像 `parse_ticker_message` 那样先转成 `serde_json::Value`
/// 再 `from_value`，那样会丢失 `RawValue` 需要的原始数字文本)。
#[derive(Debug, Deserialize)]
struct ChannelOnly<'a> {
    channel: Option<&'a str>,
}

#[derive(Debug, PartialEq)]
enum BookEvent {
    Updated(Symbol, OrderBookLevels),
    ChecksumMismatch(Symbol),
}

/// 把一个 Decimal 格式化成 Kraken checksum 算法要求的样子：去掉小数点、去掉
/// 前导零(按 Kraken 文档描述的 book checksum 计算方式)。
fn checksum_token(value: Decimal) -> String {
    let no_dot: String = value.to_string().chars().filter(|c| *c != '.').collect();
    let trimmed = no_dot.trim_start_matches('0');
    if trimmed.is_empty() { "0".to_string() } else { trimmed.to_string() }
}

/// 按 Kraken 文档的 book checksum 算法：取前 10 档 asks(升序)+前 10 档
/// bids(降序)，价格和数量各自去掉小数点/前导零后拼接成一个字符串，算 CRC32。
fn book_checksum(book: &LocalOrderBook) -> u32 {
    let snapshot = book.snapshot(0);
    let mut buf = String::new();
    for (price, qty) in snapshot.asks.iter().take(10) {
        buf.push_str(&checksum_token(*price));
        buf.push_str(&checksum_token(*qty));
    }
    for (price, qty) in snapshot.bids.iter().take(10) {
        buf.push_str(&checksum_token(*price));
        buf.push_str(&checksum_token(*qty));
    }
    crc32fast::hash(buf.as_bytes())
}

/// 解析一条 v2 `book` 频道消息(snapshot/update 共用)，合并进 `books` 里对应
/// symbol 的本地订单簿，返回这次消息触发的更新/校验和不一致事件。忽略非
/// `book` 频道的消息(ticker/心跳/订阅确认等)，不打日志。
fn apply_book_message(text: &str, symbol_map: &HashMap<String, Symbol>, books: &mut HashMap<Symbol, LocalOrderBook>) -> Vec<BookEvent> {
    match serde_json::from_str::<ChannelOnly>(text) {
        Ok(channel_only) if channel_only.channel == Some("book") => {}
        _ => return Vec::new(),
    }

    let msg: BookMessage = match serde_json::from_str(text) {
        Ok(msg) => msg,
        Err(err) => {
            warn!("failed to parse kraken book message: {err}");
            return Vec::new();
        }
    };

    let mut events = Vec::new();
    for entry in msg.data {
        let Some(symbol) = symbol_map.get(&entry.symbol) else {
            continue;
        };
        let book = books.entry(symbol.clone()).or_default();
        if msg.kind == "snapshot" {
            book.clear();
        }
        for level in &entry.bids {
            book.apply_bid(level.price(), level.qty());
        }
        for level in &entry.asks {
            book.apply_ask(level.price(), level.qty());
        }

        if let Some(expected) = entry.checksum {
            if book_checksum(book) != expected {
                book.clear();
                events.push(BookEvent::ChecksumMismatch(symbol.clone()));
                continue;
            }
        }
        events.push(BookEvent::Updated(symbol.clone(), book.snapshot(now_ms())));
    }
    events
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

    #[test]
    fn builds_book_subscribe_message() {
        let source = KrakenSpotSource::new(Venue::new("kraken"), vec![Symbol::new("BTC", "USD")], None);

        let msg: serde_json::Value = serde_json::from_str(&source.subscribe_book_message()).unwrap();
        assert_eq!(msg["method"], "subscribe");
        assert_eq!(msg["params"]["channel"], "book");
        assert_eq!(msg["params"]["symbol"], serde_json::json!(["BTC/USD"]));
        assert_eq!(msg["params"]["depth"], BOOK_DEPTH);
    }

    #[test]
    fn checksum_token_strips_dot_and_leading_zeros() {
        assert_eq!(checksum_token("067000.10000".parse().unwrap()), "6700010000");
        assert_eq!(checksum_token("0.5".parse().unwrap()), "5");
        assert_eq!(checksum_token("0".parse().unwrap()), "0");
    }

    #[test]
    fn apply_book_message_builds_sorted_snapshot_from_snapshot_frame() {
        let symbol = Symbol::new("BTC", "USD");
        let map = map_with(symbol.clone());
        let mut books = HashMap::new();
        let text = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "bids": [{"price": 99.0, "qty": 2.0}, {"price": 100.0, "qty": 1.0}],
                    "asks": [{"price": 101.0, "qty": 1.0}, {"price": 102.0, "qty": 2.0}]
                }
            ]
        }"#;

        let events = apply_book_message(text, &map, &mut books);
        assert_eq!(events.len(), 1);
        match &events[0] {
            BookEvent::Updated(s, levels) => {
                assert_eq!(s, &symbol);
                assert_eq!(levels.bids.as_ref(), &vec![("100.0".parse().unwrap(), "1.0".parse().unwrap()), ("99.0".parse().unwrap(), "2.0".parse().unwrap())]);
                assert_eq!(levels.asks.as_ref(), &vec![("101.0".parse().unwrap(), "1.0".parse().unwrap()), ("102.0".parse().unwrap(), "2.0".parse().unwrap())]);
            }
            BookEvent::ChecksumMismatch(_) => panic!("expected Updated event"),
        }
    }

    #[test]
    fn apply_book_message_update_frame_merges_into_existing_book() {
        let symbol = Symbol::new("BTC", "USD");
        let map = map_with(symbol.clone());
        let mut books = HashMap::new();
        let snapshot = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [{"symbol": "BTC/USD", "bids": [{"price": 100.0, "qty": 1.0}], "asks": [{"price": 101.0, "qty": 1.0}]}]
        }"#;
        apply_book_message(snapshot, &map, &mut books);

        let update = r#"{
            "channel": "book",
            "type": "update",
            "data": [{"symbol": "BTC/USD", "bids": [{"price": 100.0, "qty": 0.0}, {"price": 99.5, "qty": 3.0}], "asks": []}]
        }"#;
        let events = apply_book_message(update, &map, &mut books);

        match &events[0] {
            BookEvent::Updated(_, levels) => {
                assert_eq!(levels.bids.as_ref(), &vec![("99.5".parse().unwrap(), "3.0".parse().unwrap())]);
                assert_eq!(levels.asks.as_ref(), &vec![("101.0".parse().unwrap(), "1.0".parse().unwrap())]);
            }
            BookEvent::ChecksumMismatch(_) => panic!("expected Updated event"),
        }
    }

    #[test]
    fn apply_book_message_detects_checksum_mismatch_and_clears_book() {
        let symbol = Symbol::new("BTC", "USD");
        let map = map_with(symbol.clone());
        let mut books = HashMap::new();
        let text = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [{"symbol": "BTC/USD", "bids": [{"price": 100.0, "qty": 1.0}], "asks": [{"price": 101.0, "qty": 1.0}], "checksum": 1}]
        }"#;

        let events = apply_book_message(text, &map, &mut books);
        assert_eq!(events, vec![BookEvent::ChecksumMismatch(symbol.clone())]);
        assert!(books.get(&symbol).unwrap().is_empty());
    }

    #[test]
    fn apply_book_message_accepts_matching_checksum() {
        let symbol = Symbol::new("BTC", "USD");
        let map = map_with(symbol.clone());

        let mut reference = LocalOrderBook::new();
        reference.apply_bid("100.0".parse().unwrap(), "1.0".parse().unwrap());
        reference.apply_ask("101.0".parse().unwrap(), "1.0".parse().unwrap());
        let expected_checksum = book_checksum(&reference);

        let mut books = HashMap::new();
        let text = format!(
            r#"{{
                "channel": "book",
                "type": "snapshot",
                "data": [{{"symbol": "BTC/USD", "bids": [{{"price": 100.0, "qty": 1.0}}], "asks": [{{"price": 101.0, "qty": 1.0}}], "checksum": {expected_checksum}}}]
            }}"#
        );

        let events = apply_book_message(&text, &map, &mut books);
        assert!(matches!(events.as_slice(), [BookEvent::Updated(s, _)] if s == &symbol));
    }

    #[test]
    fn apply_book_message_ignores_non_book_channel() {
        let map = map_with(Symbol::new("BTC", "USD"));
        let mut books = HashMap::new();
        assert!(apply_book_message(r#"{"channel":"ticker","type":"update","data":[]}"#, &map, &mut books).is_empty());
        assert!(apply_book_message("not json", &map, &mut books).is_empty());
    }
}
