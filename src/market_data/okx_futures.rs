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

use crate::exchange_info::okx::{okx_swap_inst_id, parse_okx_data};
use crate::market_data::orderbook::{LocalOrderBook, OrderBookLevels};
use crate::net::connect_tcp;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{
    Kline, KlineInterval, KlineProvider, MarketDataSource, RANGE_MAX_PAGES, closed_klines, closed_klines_in_range, now_ms,
};

const WS_HOST: &str = "ws.okx.com";
const WS_PORT: u16 = 8443;
const WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const REST_HOST: &str = "https://www.okx.com";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// OKX 30 秒没有任何收发就断开连接，定时发文本 `ping` 保活(服务端回 `pong`)。
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);
/// 定期强制重连重新订阅 `books`，兜底 seqId/prevSeqId 续号校验覆盖不到的问题；
/// 不依赖任何错误检测，纯定时触发。
const ORDER_BOOK_RESYNC_INTERVAL: Duration = Duration::from_secs(20 * 60);
/// `/api/v5/market/candles` 单次最多 300 根。
const CANDLES_LIMIT_MAX: usize = 300;
/// 翻页上限，防止交易所一直返回满页时死循环(10 页 = 3000 根，远超用量)。
const CANDLES_MAX_PAGES: usize = 10;
/// `/api/v5/market/history-candles` 单次最多 100 根。
const HISTORY_CANDLES_LIMIT_MAX: usize = 100;

fn candle_bar(interval: KlineInterval) -> &'static str {
    match interval {
        KlineInterval::M5 => "5m",
        KlineInterval::H1 => "1H",
    }
}

/// OKX USDT 本位永续行情源：订阅公开频道 `bbo-tbt`(逐笔推送的最优买卖价)
/// 发布 `Topic::Quote`，同时订阅 `books`(多档增量簿)维护本地订单簿发布
/// `Topic::OrderBook`。发布的报价/档位都是交易所原始报价(每币价格)，挂单量
/// 单位是张。
pub struct OkxFuturesSource {
    venue: Venue,
    symbols: Vec<Symbol>,
    proxy: Option<String>,
}

impl OkxFuturesSource {
    pub fn new(venue: Venue, symbols: Vec<Symbol>, proxy: Option<String>) -> Self {
        Self { venue, symbols, proxy }
    }

    fn symbol_map(&self) -> HashMap<String, Symbol> {
        self.symbols.iter().map(|s| (okx_swap_inst_id(s), s.clone())).collect()
    }

    /// 每个 symbol 相邻地订阅 `bbo-tbt` + `books` 两个频道，同一个 `args`
    /// 数组、同一个 `op:"subscribe"` 请求。
    fn subscribe_message(&self) -> String {
        let args: Vec<serde_json::Value> = self
            .symbols
            .iter()
            .flat_map(|s| {
                let inst_id = okx_swap_inst_id(s);
                [
                    serde_json::json!({"channel": "bbo-tbt", "instId": inst_id}),
                    serde_json::json!({"channel": "books", "instId": inst_id}),
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

impl MarketDataSource for OkxFuturesSource {
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
                        warn!("okx futures ws connect failed for venue={} err={err:#}, retrying in {:?}", self.venue, backoff);
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(self.subscribe_message())).await {
                    warn!("okx futures ws subscribe failed for venue={} err={err}, retrying in {:?}", self.venue, backoff);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                debug!("okx futures ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                let mut books: HashMap<Symbol, LocalOrderBook> = HashMap::new();
                let mut seq_ids: HashMap<Symbol, i64> = HashMap::new();

                let mut ping = tokio::time::interval(WS_PING_INTERVAL);
                ping.tick().await;
                let mut resync = tokio::time::interval(ORDER_BOOK_RESYNC_INTERVAL);
                resync.tick().await;
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = ws.send(Message::Text("ping".to_string())).await {
                                warn!("okx futures ws ping failed for venue={} err={err}", self.venue);
                                break;
                            }
                        }
                        _ = resync.tick() => {
                            debug!("okx futures ws periodic orderbook resync for venue={}, reconnecting", self.venue);
                            break;
                        }
                        msg = ws.next() => {
                            let msg = match msg {
                                Some(Ok(msg)) => msg,
                                Some(Err(err)) => {
                                    warn!("okx futures ws error for venue={} err={err}", self.venue);
                                    break;
                                }
                                None => break,
                            };
                            let Message::Text(text) = msg else {
                                continue;
                            };
                            if let Some((symbol, quote)) = parse_bbo_message(&text, &symbol_map, &self.venue) {
                                bus.publish(Topic::quote(self.venue.clone(), symbol), quote);
                            }
                            let mut needs_resync = false;
                            for event in apply_book_message(&text, &symbol_map, &self.venue, &mut books, &mut seq_ids) {
                                match event {
                                    BookEvent::Updated(symbol, levels) => {
                                        bus.publish(Topic::order_book(self.venue.clone(), symbol), levels);
                                    }
                                    BookEvent::Gap(symbol) => {
                                        warn!(
                                            "okx futures orderbook sequence gap for venue={} symbol={symbol}, reconnecting to resync",
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

                warn!("okx futures ws disconnected for venue={}, reconnecting in {:?}", self.venue, backoff);
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
struct BboEntry {
    /// 每档 `[price, size, 废弃字段, 订单数]`，都是字符串。
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
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
    arg: Option<WsArg>,
    #[serde(default)]
    data: Vec<BboEntry>,
}

fn top_level(levels: &[Vec<String>]) -> Option<(Decimal, Decimal)> {
    let level = levels.first()?;
    Some((level.first()?.parse().ok()?, level.get(1)?.parse().ok()?))
}

/// 解析一条 `bbo-tbt` 推送。订阅出错(`event == "error"`)打 warn；订阅确认、
/// `pong` 等其它消息忽略。任意一侧盘口为空时丢弃这条。
fn parse_bbo_message(text: &str, symbol_map: &HashMap<String, Symbol>, venue: &Venue) -> Option<(Symbol, Quote)> {
    let push: WsPush = serde_json::from_str(text).ok()?;
    if push.event.as_deref() == Some("error") {
        warn!(
            "okx futures ws error for venue={venue}: code={} msg={}",
            push.code.unwrap_or_default(),
            push.msg.unwrap_or_default()
        );
        return None;
    }
    if push.event.is_some() {
        return None;
    }
    let arg = push.arg?;
    if arg.channel != "bbo-tbt" {
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
#[serde(rename_all = "camelCase")]
struct BookEntry {
    #[serde(default)]
    asks: Vec<Vec<String>>,
    #[serde(default)]
    bids: Vec<Vec<String>>,
    #[serde(default)]
    seq_id: i64,
    #[serde(default)]
    prev_seq_id: i64,
}

#[derive(Debug, Deserialize)]
struct BookPush {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
    arg: Option<WsArg>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    data: Vec<BookEntry>,
}

#[derive(Debug, PartialEq)]
enum BookEvent {
    Updated(Symbol, OrderBookLevels),
    Gap(Symbol),
}

/// 解析一条 `books` 频道推送(snapshot/update 共用)，合并进 `books` 里对应
/// symbol 的本地订单簿。OKX 已废弃 checksum 字段(恒为 0，不再校验)，改用
/// `seqId`/`prevSeqId` 续号校验：`update` 帧的 `prevSeqId` 必须等于上一帧
/// (snapshot 或 update)的 `seqId`，不等就判定断档、清空该 symbol 本地簿等
/// 重连重新走一次 snapshot。忽略非 `books` 频道的消息，不打日志(除非是该
/// 频道自己的订阅错误)。
fn apply_book_message(
    text: &str,
    symbol_map: &HashMap<String, Symbol>,
    venue: &Venue,
    books: &mut HashMap<Symbol, LocalOrderBook>,
    seq_ids: &mut HashMap<Symbol, i64>,
) -> Vec<BookEvent> {
    let push: BookPush = match serde_json::from_str(text) {
        Ok(push) => push,
        Err(_) => return Vec::new(),
    };
    let is_books_channel = push.arg.as_ref().map(|a| a.channel.as_str()) == Some("books");
    if push.event.as_deref() == Some("error") {
        if is_books_channel {
            warn!(
                "okx futures ws books subscribe error for venue={venue}: code={} msg={}",
                push.code.unwrap_or_default(),
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

    let mut events = Vec::new();
    for entry in &push.data {
        match push.action.as_deref() {
            Some("snapshot") => {
                let book = books.entry(symbol.clone()).or_default();
                book.clear();
                for (price, qty) in parse_level_rows(&entry.bids) {
                    book.apply_bid(price, qty);
                }
                for (price, qty) in parse_level_rows(&entry.asks) {
                    book.apply_ask(price, qty);
                }
                seq_ids.insert(symbol.clone(), entry.seq_id);
                events.push(BookEvent::Updated(symbol.clone(), book.snapshot(now_ms())));
            }
            Some("update") => {
                let Some(last_seq_id) = seq_ids.get(symbol).copied() else {
                    // 还没收到过 snapshot，丢弃这条 update，等重连后重新走 snapshot。
                    continue;
                };
                if entry.prev_seq_id != last_seq_id {
                    seq_ids.remove(symbol);
                    books.remove(symbol);
                    events.push(BookEvent::Gap(symbol.clone()));
                    continue;
                }
                let book = books.entry(symbol.clone()).or_default();
                for (price, qty) in parse_level_rows(&entry.bids) {
                    book.apply_bid(price, qty);
                }
                for (price, qty) in parse_level_rows(&entry.asks) {
                    book.apply_ask(price, qty);
                }
                seq_ids.insert(symbol.clone(), entry.seq_id);
                events.push(BookEvent::Updated(symbol.clone(), book.snapshot(now_ms())));
            }
            _ => {}
        }
    }
    events
}

/// OKX USDT 本位永续的公开 K 线 / 24h 行情 REST 客户端，不需要签名。
pub struct OkxFuturesKlineClient {
    venue: Venue,
    http: reqwest::Client,
}

impl OkxFuturesKlineClient {
    pub fn new(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if let Some(proxy) = proxy {
            builder = builder.proxy(reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?);
        }
        let http = builder.build().context("failed to build okx http client")?;
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
            .with_context(|| format!("okx request {path} failed"))?;
        resp.text().await.context("failed to read okx response body")
    }
}

#[async_trait]
impl KlineProvider for OkxFuturesKlineClient {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `GET /api/v5/market/candles` 按时间倒序返回，第一根是当前未收盘的，所以
    /// 多要 1 根再交给 [`closed_klines`] 排序、剔除。单次最多 300 根，不够就用
    /// `after`(早于该时间戳)往前翻页。
    async fn klines(&self, symbol: &Symbol, interval: KlineInterval, limit: usize) -> anyhow::Result<Vec<Kline>> {
        let bar = candle_bar(interval);
        let want = limit + 1;
        let mut klines: Vec<Kline> = Vec::with_capacity(want);
        let mut after: Option<u64> = None;
        for _ in 0..CANDLES_MAX_PAGES {
            let page_limit = (want - klines.len()).min(CANDLES_LIMIT_MAX);
            let mut query = vec![
                ("instId", okx_swap_inst_id(symbol)),
                ("bar", bar.to_string()),
                ("limit", page_limit.to_string()),
            ];
            if let Some(ts) = after {
                query.push(("after", ts.to_string()));
            }
            let text = self.public_get("/api/v5/market/candles", &query).await?;
            let page = parse_candles(&text)?;
            let page_len = page.len();
            after = page.iter().map(|k| k.open_time_ms).min();
            klines.extend(page);
            if klines.len() >= want || page_len < page_limit {
                break;
            }
        }
        Ok(closed_klines(klines, interval, now_ms(), limit))
    }

    /// `GET /api/v5/market/history-candles`(一年以前的也能拿到，普通 candles 接口
    /// 只保留近期)，倒序返回，单次最多 100 根，从 `end_ms` 用 `after`(严格早于)
    /// 往前翻页到 `start_ms`。
    async fn klines_range(
        &self,
        symbol: &Symbol,
        interval: KlineInterval,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<Kline>> {
        let mut klines: Vec<Kline> = Vec::new();
        let mut after = end_ms + 1;
        for _ in 0..RANGE_MAX_PAGES {
            let query = vec![
                ("instId", okx_swap_inst_id(symbol)),
                ("bar", candle_bar(interval).to_string()),
                ("after", after.to_string()),
                ("limit", HISTORY_CANDLES_LIMIT_MAX.to_string()),
            ];
            let text = self.public_get("/api/v5/market/history-candles", &query).await?;
            let page = parse_candles(&text)?;
            let Some(oldest) = page.iter().map(|k| k.open_time_ms).min() else {
                break;
            };
            klines.extend(page);
            if oldest <= start_ms {
                break;
            }
            after = oldest;
        }
        Ok(closed_klines_in_range(klines, interval, now_ms(), start_ms, end_ms))
    }

    /// `GET /api/v5/market/ticker`：永续的 `volCcy24h` 是 base 币数量，没有直接
    /// 的计价币成交额，按 `volCcy24h × last` 估算。
    async fn quote_volume_24h(&self, symbol: &Symbol) -> anyhow::Result<Decimal> {
        let text = self.public_get("/api/v5/market/ticker", &[("instId", okx_swap_inst_id(symbol))]).await?;
        parse_quote_volume(&text)
    }
}

/// 每根是 `[ts(ms), o, h, l, c, vol, volCcy, volCcyQuote, confirm]`，都是字符串。
fn parse_candles(text: &str) -> anyhow::Result<Vec<Kline>> {
    let rows: Vec<Vec<String>> = parse_okx_data(text, "candles")?;
    rows.into_iter()
        .map(|row| {
            let open_time_ms: u64 = row.first().and_then(|t| t.parse().ok()).context("okx candle has no valid ts")?;
            let close: Decimal = row.get(4).and_then(|c| c.parse().ok()).context("okx candle has no valid close")?;
            let volume: Decimal = row.get(5).and_then(|v| v.parse().ok()).context("okx candle has no valid vol")?;
            Ok(Kline { open_time_ms, close, volume })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerEntry {
    last: String,
    vol_ccy24h: String,
}

fn parse_quote_volume(text: &str) -> anyhow::Result<Decimal> {
    let tickers: Vec<TickerEntry> = parse_okx_data(text, "ticker")?;
    let ticker = tickers.into_iter().next().context("okx ticker response is empty")?;
    let last: Decimal = ticker.last.parse().context("okx ticker has invalid last")?;
    let volume: Decimal = ticker.vol_ccy24h.parse().context("okx ticker has invalid volCcy24h")?;
    Ok(volume * last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::HOUR_MS;

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(okx_swap_inst_id(&symbol), symbol);
        map
    }

    #[test]
    fn parses_bbo_push() {
        let map = map_with(Symbol::new("PEPE", "USDT"));
        let venue = Venue::new("okx_futures");
        let text = r#"{"arg":{"channel":"bbo-tbt","instId":"PEPE-USDT-SWAP"},"data":[{"asks":[["0.00000404","120","0","3"]],"bids":[["0.00000403","95","0","2"]],"ts":"1700000000000","seqId":1}]}"#;
        let (symbol, quote) = parse_bbo_message(text, &map, &venue).unwrap();
        assert_eq!(symbol, Symbol::new("PEPE", "USDT"));
        assert_eq!(quote.bid, "0.00000403".parse().unwrap());
        assert_eq!(quote.bid_size, Decimal::from(95));
        assert_eq!(quote.ask, "0.00000404".parse().unwrap());
        assert_eq!(quote.ask_size, Decimal::from(120));
    }

    #[test]
    fn ignores_acks_errors_unknown_and_empty_books() {
        let map = map_with(Symbol::new("PEPE", "USDT"));
        let venue = Venue::new("okx_futures");
        for text in [
            r#"{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"PEPE-USDT-SWAP"},"connId":"a"}"#,
            r#"{"event":"error","code":"60012","msg":"Invalid request","connId":"a"}"#,
            r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["1","1","0","1"]],"bids":[["1","1","0","1"]]}]}"#,
            r#"{"arg":{"channel":"bbo-tbt","instId":"PEPE-USDT-SWAP"},"data":[{"asks":[],"bids":[["1","1","0","1"]]}]}"#,
            "pong",
        ] {
            assert!(parse_bbo_message(text, &map, &venue).is_none(), "{text}");
        }
    }

    #[test]
    fn parses_candles_newest_first() {
        let text = r#"{"code":"0","msg":"","data":[
            ["7200000","1","1","1","0.000004","1","1","1","0"],
            ["3600000","1","1","1","0.0000039","1","1","1","1"]
        ]}"#;
        let klines = parse_candles(text).unwrap();
        assert_eq!(
            klines[0],
            Kline { open_time_ms: 2 * HOUR_MS, close: "0.000004".parse().unwrap(), volume: Decimal::ONE }
        );
        let closed = closed_klines(klines, KlineInterval::H1, 2 * HOUR_MS + 1, 10);
        assert_eq!(
            closed,
            vec![Kline { open_time_ms: HOUR_MS, close: "0.0000039".parse().unwrap(), volume: Decimal::ONE }]
        );
        assert!(parse_candles(r#"{"code":"51001","msg":"Instrument ID does not exist","data":[]}"#).is_err());
    }

    #[test]
    fn estimates_quote_volume_from_base_volume() {
        let text = r#"{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","last":"50000","vol24h":"1000","volCcy24h":"10"}]}"#;
        assert_eq!(parse_quote_volume(text).unwrap(), Decimal::from(500_000));
    }

    #[test]
    fn subscribe_message_lists_all_inst_ids() {
        let source = OkxFuturesSource::new(
            Venue::new("okx_futures"),
            vec![Symbol::new("BTC", "USDT"), Symbol::new("PEPE", "USDT")],
            None,
        );
        let msg: serde_json::Value = serde_json::from_str(&source.subscribe_message()).unwrap();
        assert_eq!(msg["op"], "subscribe");
        assert_eq!(msg["args"][0], serde_json::json!({"channel": "bbo-tbt", "instId": "BTC-USDT-SWAP"}));
        assert_eq!(msg["args"][1], serde_json::json!({"channel": "books", "instId": "BTC-USDT-SWAP"}));
        assert_eq!(msg["args"][2], serde_json::json!({"channel": "bbo-tbt", "instId": "PEPE-USDT-SWAP"}));
        assert_eq!(msg["args"][3], serde_json::json!({"channel": "books", "instId": "PEPE-USDT-SWAP"}));
    }

    #[test]
    fn apply_book_message_builds_book_from_snapshot_frame() {
        let symbol = Symbol::new("PEPE", "USDT");
        let map = map_with(symbol.clone());
        let venue = Venue::new("okx_futures");
        let mut books = HashMap::new();
        let mut seq_ids = HashMap::new();
        let text = r#"{"arg":{"channel":"books","instId":"PEPE-USDT-SWAP"},"action":"snapshot",
            "data":[{"asks":[["101","1","0","1"],["102","2","0","1"]],"bids":[["100","1","0","1"],["99","2","0","1"]],"ts":"1","checksum":0,"prevSeqId":-1,"seqId":10}]}"#;

        let events = apply_book_message(text, &map, &venue, &mut books, &mut seq_ids);
        assert_eq!(events.len(), 1);
        match &events[0] {
            BookEvent::Updated(s, levels) => {
                assert_eq!(s, &symbol);
                assert_eq!(levels.bids.as_ref(), &vec![("100".parse().unwrap(), "1".parse().unwrap()), ("99".parse().unwrap(), "2".parse().unwrap())]);
                assert_eq!(levels.asks.as_ref(), &vec![("101".parse().unwrap(), "1".parse().unwrap()), ("102".parse().unwrap(), "2".parse().unwrap())]);
            }
            BookEvent::Gap(_) => panic!("expected Updated event"),
        }
        assert_eq!(seq_ids[&symbol], 10);
    }

    #[test]
    fn apply_book_message_update_frame_merges_when_prev_seq_id_matches() {
        let symbol = Symbol::new("PEPE", "USDT");
        let map = map_with(symbol.clone());
        let venue = Venue::new("okx_futures");
        let mut books = HashMap::new();
        let mut seq_ids = HashMap::new();
        let snapshot = r#"{"arg":{"channel":"books","instId":"PEPE-USDT-SWAP"},"action":"snapshot",
            "data":[{"asks":[["101","1","0","1"]],"bids":[["100","1","0","1"]],"prevSeqId":-1,"seqId":10}]}"#;
        apply_book_message(snapshot, &map, &venue, &mut books, &mut seq_ids);

        let update = r#"{"arg":{"channel":"books","instId":"PEPE-USDT-SWAP"},"action":"update",
            "data":[{"asks":[],"bids":[["100","0","0","0"],["99.5","3","0","1"]],"prevSeqId":10,"seqId":11}]}"#;
        let events = apply_book_message(update, &map, &venue, &mut books, &mut seq_ids);

        match &events[0] {
            BookEvent::Updated(_, levels) => {
                assert_eq!(levels.bids.as_ref(), &vec![("99.5".parse().unwrap(), "3".parse().unwrap())]);
                assert_eq!(levels.asks.as_ref(), &vec![("101".parse().unwrap(), "1".parse().unwrap())]);
            }
            BookEvent::Gap(_) => panic!("expected Updated event"),
        }
        assert_eq!(seq_ids[&symbol], 11);
    }

    #[test]
    fn apply_book_message_detects_prev_seq_id_gap_and_clears_book() {
        let symbol = Symbol::new("PEPE", "USDT");
        let map = map_with(symbol.clone());
        let venue = Venue::new("okx_futures");
        let mut books = HashMap::new();
        let mut seq_ids = HashMap::new();
        let snapshot = r#"{"arg":{"channel":"books","instId":"PEPE-USDT-SWAP"},"action":"snapshot",
            "data":[{"asks":[["101","1","0","1"]],"bids":[["100","1","0","1"]],"prevSeqId":-1,"seqId":10}]}"#;
        apply_book_message(snapshot, &map, &venue, &mut books, &mut seq_ids);

        let update = r#"{"arg":{"channel":"books","instId":"PEPE-USDT-SWAP"},"action":"update",
            "data":[{"asks":[],"bids":[["99.5","3","0","1"]],"prevSeqId":999,"seqId":1000}]}"#;
        let events = apply_book_message(update, &map, &venue, &mut books, &mut seq_ids);

        assert_eq!(events, vec![BookEvent::Gap(symbol.clone())]);
        assert!(!seq_ids.contains_key(&symbol));
        assert!(!books.contains_key(&symbol));
    }

    #[test]
    fn apply_book_message_ignores_non_books_channel_and_malformed() {
        let map = map_with(Symbol::new("PEPE", "USDT"));
        let venue = Venue::new("okx_futures");
        let mut books = HashMap::new();
        let mut seq_ids = HashMap::new();
        let bbo = r#"{"arg":{"channel":"bbo-tbt","instId":"PEPE-USDT-SWAP"},"data":[{"asks":[["1","1"]],"bids":[["1","1"]]}]}"#;
        assert!(apply_book_message(bbo, &map, &venue, &mut books, &mut seq_ids).is_empty());
        assert!(apply_book_message("not json", &map, &venue, &mut books, &mut seq_ids).is_empty());
        assert!(apply_book_message(
            r#"{"event":"error","code":"60012","msg":"bad","arg":{"channel":"books","instId":"PEPE-USDT-SWAP"}}"#,
            &map, &venue, &mut books, &mut seq_ids
        ).is_empty());
    }
}
