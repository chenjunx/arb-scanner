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
use crate::market_data::orderbook::{LocalOrderBook, OrderBookLevels};
use crate::net::connect_tcp;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{
    Kline, KlineInterval, KlineProvider, MarketDataSource, RANGE_MAX_PAGES, closed_klines, closed_klines_in_range, now_ms,
};

const WS_HOST: &str = "ws.bitget.com";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const REST_HOST: &str = "https://api.bitget.com";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Bitget 要求客户端每 30 秒发一次文本 `ping`(服务端回 `pong`)，2 分钟收不到就断开。
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);
/// 定期强制重连重新订阅 `books`，兜底 checksum 覆盖不到的问题；不依赖任何
/// 错误检测，纯定时触发。
const ORDER_BOOK_RESYNC_INTERVAL: Duration = Duration::from_secs(20 * 60);
/// `/api/v2/mix/market/candles` 单次最多 1000 根。
const CANDLES_LIMIT_MAX: usize = 1000;
/// `/api/v2/mix/market/history-candles` 单次最多 200 根。
const HISTORY_CANDLES_LIMIT_MAX: usize = 200;
/// `history-candles` 在 host 级限流之外再单独限流，见 [`crate::ratelimit`]。
const HISTORY_CANDLES_LIMIT_KEY: &str = "bitget:/api/v2/mix/market/history-candles";

fn candle_granularity(interval: KlineInterval) -> &'static str {
    match interval {
        KlineInterval::M5 => "5m",
        KlineInterval::H1 => "1H",
    }
}

/// Bitget USDT 本位永续行情源：订阅公开频道 `books1` 发布 `Topic::Quote`，
/// 同时订阅 `books`(多档增量簿)维护本地订单簿发布 `Topic::OrderBook`。发布的
/// 报价/档位都是交易所原始报价(一个 base 单位的价格)，挂单量单位是 base 币。
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

    /// 每个 symbol 相邻地订阅 `books1` + `books` 两个频道，同一个 `args`
    /// 数组、同一个 `op:"subscribe"` 请求。
    fn subscribe_message(&self) -> String {
        let args: Vec<serde_json::Value> = self
            .symbols
            .iter()
            .flat_map(|s| {
                let inst_id = bitget_symbol(s);
                [
                    serde_json::json!({"instType": BITGET_USDT_FUTURES, "channel": "books1", "instId": inst_id}),
                    serde_json::json!({"instType": BITGET_USDT_FUTURES, "channel": "books", "instId": inst_id}),
                ]
            })
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

                let mut books: HashMap<Symbol, LocalOrderBook> = HashMap::new();

                let mut ping = tokio::time::interval(WS_PING_INTERVAL);
                ping.tick().await;
                let mut resync = tokio::time::interval(ORDER_BOOK_RESYNC_INTERVAL);
                resync.tick().await;
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = ws.send(Message::Text("ping".to_string())).await {
                                warn!("bitget futures ws ping failed for venue={} err={err}", self.venue);
                                break;
                            }
                        }
                        _ = resync.tick() => {
                            debug!("bitget futures ws periodic orderbook resync for venue={}, reconnecting", self.venue);
                            break;
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
                            let mut needs_resync = false;
                            for event in apply_book_message(&text, &symbol_map, &self.venue, &mut books) {
                                match event {
                                    BookEvent::Updated(symbol, levels) => {
                                        bus.publish(Topic::order_book(self.venue.clone(), symbol), levels);
                                    }
                                    BookEvent::ChecksumMismatch(symbol) => {
                                        warn!(
                                            "bitget futures orderbook checksum mismatch for venue={} symbol={symbol}, reconnecting to resync",
                                            self.venue
                                        );
                                        needs_resync = true;
                                    }
                                }
                            }
                            if needs_resync {
                                break;
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

fn parse_level_rows(rows: &[Vec<String>]) -> Vec<(Decimal, Decimal)> {
    rows.iter()
        .filter_map(|row| {
            let price = row.first()?.parse().ok()?;
            let qty = row.get(1)?.parse().ok()?;
            Some((price, qty))
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct BooksEntry {
    #[serde(default)]
    asks: Vec<Vec<String>>,
    #[serde(default)]
    bids: Vec<Vec<String>>,
    #[serde(default)]
    checksum: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct BooksPush {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    code: Option<serde_json::Value>,
    #[serde(default)]
    msg: Option<String>,
    arg: Option<WsArg>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    data: Vec<BooksEntry>,
}

#[derive(Debug, PartialEq)]
enum BookEvent {
    Updated(Symbol, OrderBookLevels),
    ChecksumMismatch(Symbol),
}

/// Bitget `books` 频道的 checksum 算法(参考 ccxt 的实现)：按档位序号从 0
/// 到 24，依次取 `bid[i].price:bid[i].qty`(没有该档就跳过)，再取
/// `ask[i].price:ask[i].qty`，把整个数组用 `:` 拼接成一个字符串算 CRC32，
/// 按有符号 32 位整数解读(和推送里可能是负数的 `checksum` 字段对齐)。
/// price/qty 直接用 `Decimal::to_string()`，不像 Kraken 那样要去掉小数点/
/// 前导零——因为 Bitget 这边价格/数量在 JSON 里本来就是字符串而不是裸数字，
/// `rust_decimal` 从字符串反序列化不会丢失原始精度，`to_string()` 能还原
/// 出和推送一致的文本。
fn book_checksum(book: &LocalOrderBook) -> i32 {
    let snapshot = book.snapshot(0);
    let mut tokens: Vec<String> = Vec::new();
    for i in 0..25 {
        if let Some((price, qty)) = snapshot.bids.get(i) {
            tokens.push(price.to_string());
            tokens.push(qty.to_string());
        }
        if let Some((price, qty)) = snapshot.asks.get(i) {
            tokens.push(price.to_string());
            tokens.push(qty.to_string());
        }
    }
    crc32fast::hash(tokens.join(":").as_bytes()) as i32
}

/// 解析一条 `books` 频道推送(snapshot/update 共用)，合并进 `books` 里对应
/// symbol 的本地订单簿。`action:"snapshot"` 先清空本地簿再灌入全量档位，
/// `action:"update"` 增量合并(qty=0 删除该档)。校验和不一致就清空本地簿、
/// 等重连重新走一次 snapshot。忽略非 `books` 频道的消息，不打日志(除非是
/// 该频道自己的订阅错误)。
fn apply_book_message(
    text: &str,
    symbol_map: &HashMap<String, Symbol>,
    venue: &Venue,
    books: &mut HashMap<Symbol, LocalOrderBook>,
) -> Vec<BookEvent> {
    let push: BooksPush = match serde_json::from_str(text) {
        Ok(push) => push,
        Err(_) => return Vec::new(),
    };
    let is_books_channel = push.arg.as_ref().map(|a| a.channel.as_str()) == Some("books");
    if push.event.as_deref() == Some("error") {
        if is_books_channel {
            warn!(
                "bitget futures ws books subscribe error for venue={venue}: code={} msg={}",
                push.code.map(|c| c.to_string()).unwrap_or_default(),
                push.msg.unwrap_or_default()
            );
        }
        return Vec::new();
    }
    if push.event.is_some() || !is_books_channel {
        return Vec::new();
    }
    let Some(arg) = push.arg else { return Vec::new() };
    let Some(symbol) = symbol_map.get(&arg.inst_id) else { return Vec::new() };
    let is_snapshot = push.action.as_deref() == Some("snapshot");

    let mut events = Vec::new();
    for entry in &push.data {
        let book = books.entry(symbol.clone()).or_default();
        if is_snapshot {
            book.clear();
        }
        for (price, qty) in parse_level_rows(&entry.bids) {
            book.apply_bid(price, qty);
        }
        for (price, qty) in parse_level_rows(&entry.asks) {
            book.apply_ask(price, qty);
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
        let query = [
            ("symbol", bitget_symbol(symbol)),
            ("productType", BITGET_USDT_FUTURES.to_string()),
            ("granularity", candle_granularity(interval).to_string()),
            ("limit", (limit + 1).min(CANDLES_LIMIT_MAX).to_string()),
        ];
        let text = self.public_get("/api/v2/mix/market/candles", &query).await?;
        Ok(closed_klines(parse_candles(&text)?, interval, now_ms(), limit))
    }

    /// `GET /api/v2/mix/market/history-candles`，单次最多 200 根，只带 `endTime`
    /// 从 `end_ms` 往前翻页到 `start_ms`(同时带 `startTime` 时交易所对超长区间
    /// 返回哪一段没有文档保证，所以不带)。
    async fn klines_range(
        &self,
        symbol: &Symbol,
        interval: KlineInterval,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<Kline>> {
        // `endTime` 按 K 线**收盘时间**过滤(收盘 ≤ endTime 的才返回)，所以要拿到
        // 开盘时间 = t 的那根得传 t + 周期；下一页从本页最老一根的开盘时间(即前一根
        // 的收盘时间)接着往前。
        let mut klines: Vec<Kline> = Vec::new();
        let mut end = end_ms + interval.millis();
        for _ in 0..RANGE_MAX_PAGES {
            let query = [
                ("symbol", bitget_symbol(symbol)),
                ("productType", BITGET_USDT_FUTURES.to_string()),
                ("granularity", candle_granularity(interval).to_string()),
                ("endTime", end.to_string()),
                ("limit", HISTORY_CANDLES_LIMIT_MAX.to_string()),
            ];
            crate::ratelimit::throttle(HISTORY_CANDLES_LIMIT_KEY).await;
            let text = self.public_get("/api/v2/mix/market/history-candles", &query).await?;
            let page = parse_candles(&text)?;
            let Some(oldest) = page.iter().map(|k| k.open_time_ms).min() else {
                break;
            };
            klines.extend(page);
            if oldest <= start_ms || oldest >= end {
                break;
            }
            end = oldest;
        }
        Ok(closed_klines_in_range(klines, interval, now_ms(), start_ms, end_ms))
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
            let volume: Decimal =
                row.get(5).and_then(|v| v.parse().ok()).context("bitget candle has no valid baseVol")?;
            Ok(Kline { open_time_ms, close, volume })
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
        assert_eq!(
            closed,
            vec![Kline { open_time_ms: HOUR_MS, close: "26194.5".parse().unwrap(), volume: "26.26".parse().unwrap() }]
        );
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
        assert_eq!(
            msg["args"][1],
            serde_json::json!({"instType": "USDT-FUTURES", "channel": "books", "instId": "1000BONKUSDT"})
        );
    }

    #[test]
    fn apply_book_message_builds_sorted_snapshot_from_snapshot_frame() {
        let symbol = Symbol::new("BTC", "USDT");
        let map = map_with(symbol.clone());
        let venue = Venue::new("bitget_futures");
        let mut books = HashMap::new();
        let text = r#"{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"books","instId":"BTCUSDT"},
            "data":[{"asks":[["101","1"],["102","2"]],"bids":[["100","1"],["99","2"]],"ts":"1"}]}"#;

        let events = apply_book_message(text, &map, &venue, &mut books);
        assert_eq!(events.len(), 1);
        match &events[0] {
            BookEvent::Updated(s, levels) => {
                assert_eq!(s, &symbol);
                assert_eq!(levels.bids.as_ref(), &vec![("100".parse().unwrap(), "1".parse().unwrap()), ("99".parse().unwrap(), "2".parse().unwrap())]);
                assert_eq!(levels.asks.as_ref(), &vec![("101".parse().unwrap(), "1".parse().unwrap()), ("102".parse().unwrap(), "2".parse().unwrap())]);
            }
            BookEvent::ChecksumMismatch(_) => panic!("expected Updated event"),
        }
    }

    #[test]
    fn apply_book_message_update_frame_merges_into_existing_book() {
        let symbol = Symbol::new("BTC", "USDT");
        let map = map_with(symbol.clone());
        let venue = Venue::new("bitget_futures");
        let mut books = HashMap::new();
        let snapshot = r#"{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"books","instId":"BTCUSDT"},
            "data":[{"asks":[["101","1"]],"bids":[["100","1"]]}]}"#;
        apply_book_message(snapshot, &map, &venue, &mut books);

        let update = r#"{"action":"update","arg":{"instType":"USDT-FUTURES","channel":"books","instId":"BTCUSDT"},
            "data":[{"asks":[],"bids":[["100","0"],["99.5","3"]]}]}"#;
        let events = apply_book_message(update, &map, &venue, &mut books);

        match &events[0] {
            BookEvent::Updated(_, levels) => {
                assert_eq!(levels.bids.as_ref(), &vec![("99.5".parse().unwrap(), "3".parse().unwrap())]);
                assert_eq!(levels.asks.as_ref(), &vec![("101".parse().unwrap(), "1".parse().unwrap())]);
            }
            BookEvent::ChecksumMismatch(_) => panic!("expected Updated event"),
        }
    }

    #[test]
    fn apply_book_message_detects_checksum_mismatch_and_clears_book() {
        let symbol = Symbol::new("BTC", "USDT");
        let map = map_with(symbol.clone());
        let venue = Venue::new("bitget_futures");
        let mut books = HashMap::new();
        let text = r#"{"action":"update","arg":{"instType":"USDT-FUTURES","channel":"books","instId":"BTCUSDT"},
            "data":[{"asks":[["101","1"]],"bids":[["100","1"]],"checksum":1}]}"#;

        let events = apply_book_message(text, &map, &venue, &mut books);
        assert_eq!(events, vec![BookEvent::ChecksumMismatch(symbol.clone())]);
        assert!(books.get(&symbol).unwrap().is_empty());
    }

    #[test]
    fn apply_book_message_accepts_matching_checksum() {
        let symbol = Symbol::new("BTC", "USDT");
        let map = map_with(symbol.clone());
        let venue = Venue::new("bitget_futures");

        let mut reference = LocalOrderBook::new();
        reference.apply_bid("100".parse().unwrap(), "1".parse().unwrap());
        reference.apply_ask("101".parse().unwrap(), "1".parse().unwrap());
        let expected_checksum = book_checksum(&reference);

        let mut books = HashMap::new();
        let text = format!(
            r#"{{"action":"update","arg":{{"instType":"USDT-FUTURES","channel":"books","instId":"BTCUSDT"}},
            "data":[{{"asks":[["101","1"]],"bids":[["100","1"]],"checksum":{expected_checksum}}}]}}"#
        );

        let events = apply_book_message(&text, &map, &venue, &mut books);
        assert!(matches!(events.as_slice(), [BookEvent::Updated(s, _)] if s == &symbol));
    }

    #[test]
    fn apply_book_message_ignores_non_books_channel_and_malformed() {
        let map = map_with(Symbol::new("BTC", "USDT"));
        let venue = Venue::new("bitget_futures");
        let mut books = HashMap::new();
        let books1 = r#"{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"books1","instId":"BTCUSDT"},"data":[{"asks":[["1","1"]],"bids":[["1","1"]]}]}"#;
        assert!(apply_book_message(books1, &map, &venue, &mut books).is_empty());
        assert!(apply_book_message("not json", &map, &venue, &mut books).is_empty());
        assert!(apply_book_message(
            r#"{"event":"error","code":30001,"msg":"bad","arg":{"instType":"USDT-FUTURES","channel":"books","instId":"BTCUSDT"}}"#,
            &map, &venue, &mut books
        ).is_empty());
    }
}
