use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
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

use super::{
    Kline, KlineInterval, KlineProvider, MarketDataSource, RANGE_MAX_PAGES, closed_klines, closed_klines_in_range, now_ms,
};

const WS_HOST: &str = "fx-ws.gateio.ws";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
const REST_HOST: &str = "https://api.gateio.ws";
const API_PREFIX: &str = "/api/v4";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// `/futures/usdt/candlesticks` 单次最多返回 2000 根。
const CANDLESTICK_LIMIT_MAX: usize = 2000;
/// 增量深度续号断档/REST 快照拉取失败时整条连接重连重建；另外作为兜底，固定
/// 周期无条件重连一次(参考 `binance.rs`/`kraken.rs` 同名常量)，防止断档检测
/// 覆盖不到的错误状态一直带病运行。
const ORDER_BOOK_RESYNC_INTERVAL: Duration = Duration::from_secs(20 * 60);
/// `futures.order_book_update` 订阅深度档位(Gate 文档里非现货频道上限 100)。
const ORDER_BOOK_DEPTH_LIMIT: &str = "100";

fn candlestick_interval(interval: KlineInterval) -> &'static str {
    match interval {
        KlineInterval::M5 => "5m",
        KlineInterval::H1 => "1h",
    }
}

/// Gate 永续合约名是 `BASE_QUOTE`(如 `PEPE_USDT`)。
fn gate_contract(symbol: &Symbol) -> String {
    format!("{}_{}", symbol.base, symbol.quote).to_ascii_uppercase()
}

/// Gate.io USDT 本位永续行情源：和 [`super::gate::GateSpotSource`] 一样的
/// 订阅/重连方式，只是 host 换成合约域名。同时订阅 `futures.book_ticker`
/// (最优一档,发布为 `Quote`) 和 `futures.order_book_update`(全量增量深度，
/// 按官方"REST 快照(`with_id=true`) + `U`/`u` 续号校验"算法在本地合并成完整
/// 订单簿,发布为 `OrderBookLevels`)。发布的报价是交易所原始报价(每币价格)，
/// 挂单量单位是张。
pub struct GateFuturesSource {
    venue: Venue,
    symbols: Vec<Symbol>,
    proxy: Option<String>,
    http: reqwest::Client,
}

impl GateFuturesSource {
    pub fn new(venue: Venue, symbols: Vec<Symbol>, proxy: Option<String>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy.as_deref())?;
        Ok(Self { venue, symbols, proxy, http })
    }

    fn symbol_map(&self) -> HashMap<String, Symbol> {
        self.symbols.iter().map(|s| (gate_contract(s), s.clone())).collect()
    }

    fn book_ticker_subscribe_message(&self) -> String {
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

    /// `futures.order_book_update` 的订阅 payload 是单个合约(`[contract,
    /// interval, limit]`)，不像 `book_ticker` 那样能把所有合约塞进一条消息，
    /// 所以每个 symbol 要单独发一条订阅帧。
    fn order_book_subscribe_message(symbol: &Symbol) -> String {
        let contract = gate_contract(symbol);
        let time_secs = now_ms() / 1000;
        serde_json::json!({
            "time": time_secs,
            "channel": "futures.order_book_update",
            "event": "subscribe",
            "payload": [contract, "100ms", ORDER_BOOK_DEPTH_LIMIT],
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

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
    if let Some(proxy) = proxy {
        builder = builder.proxy(reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?);
    }
    builder.build().context("failed to build gate futures http client")
}

impl MarketDataSource for GateFuturesSource {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let symbol_map = self.symbol_map();
            let mut backoff = MIN_BACKOFF;

            'reconnect: loop {
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

                if let Err(err) = ws.send(Message::Text(self.book_ticker_subscribe_message())).await {
                    warn!(
                        "gate futures ws subscribe failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                for symbol in &self.symbols {
                    let msg = Self::order_book_subscribe_message(symbol);
                    if let Err(err) = ws.send(Message::Text(msg)).await {
                        warn!(
                            "gate futures ws order book subscribe failed for venue={} symbol={symbol} err={err}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue 'reconnect;
                    }
                }

                debug!("gate futures ws connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;

                let mut books: HashMap<Symbol, LocalOrderBook> = HashMap::new();
                let mut sync: HashMap<Symbol, DepthSync> =
                    symbol_map.values().map(|s| (s.clone(), DepthSync::Buffering(Vec::new()))).collect();

                // 每个 symbol 并发拉一次 REST 快照(`with_id=true`)，期间到达的增量
                // 先缓冲在 `DepthSync::Buffering` 里，快照回来后按 `U`/`u` 续号
                // 校验重放缓冲(算法和 `binance.rs` 的 `apply_snapshot` 完全一致)。
                let (snapshot_tx, mut snapshot_rx) = mpsc::unbounded_channel();
                for symbol in symbol_map.values().cloned().collect::<Vec<_>>() {
                    let tx = snapshot_tx.clone();
                    let http = self.http.clone();
                    let contract = gate_contract(&symbol);
                    tokio::spawn(async move {
                        let result = fetch_order_book_snapshot(&http, &contract).await;
                        let _ = tx.send((symbol, result));
                    });
                }
                drop(snapshot_tx);

                let mut resync_timer = tokio::time::interval(ORDER_BOOK_RESYNC_INTERVAL);
                resync_timer.tick().await;

                loop {
                    tokio::select! {
                        _ = resync_timer.tick() => {
                            debug!("gate futures ws periodic orderbook resync for venue={}, reconnecting", self.venue);
                            break;
                        }
                        snapshot = snapshot_rx.recv() => {
                            let Some((symbol, result)) = snapshot else { continue; };
                            match result {
                                Ok(snap) => {
                                    if !apply_snapshot(&symbol, snap, &mut books, &mut sync) {
                                        warn!(
                                            "gate futures order book snapshot/buffer gap for venue={} symbol={symbol}, reconnecting to resync",
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
                                        "gate futures order book snapshot fetch failed for venue={} symbol={symbol} err={err:#}, reconnecting to resync",
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
                                    warn!("gate futures ws error for venue={} err={err}", self.venue);
                                    break;
                                }
                                None => break,
                            };
                            let Message::Text(text) = msg else {
                                continue;
                            };
                            match classify_message(&text) {
                                Some(GateChannel::BookTicker) => {
                                    if let Some((symbol, quote)) = parse_book_ticker_message(&text, &symbol_map) {
                                        bus.publish(Topic::quote(self.venue.clone(), symbol), quote);
                                    }
                                }
                                Some(GateChannel::OrderBookUpdate) => {
                                    let Some((symbol, update)) = parse_order_book_update(&text, &symbol_map) else {
                                        continue;
                                    };
                                    match sync.get_mut(&symbol) {
                                        Some(DepthSync::Buffering(buffer)) => buffer.push(update),
                                        Some(DepthSync::Synced { last_u }) => {
                                            if update.u <= *last_u {
                                                continue;
                                            }
                                            if update.first_update_id != *last_u + 1 {
                                                warn!(
                                                    "gate futures order book update gap for venue={} symbol={symbol}, reconnecting to resync",
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
                                None => {}
                            }
                        }
                    }
                }

                warn!(
                    "gate futures ws disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue 'reconnect;
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

/// 只看 `channel` 字段，判断一条推送是 `book_ticker` 还是 `order_book_update`，
/// 再分流到各自的解析函数(两边 `result` 结构完全不同，不能像
/// `parse_book_ticker_message` 那样直接无脑解析)。忽略订阅确认等其他事件。
#[derive(Debug, Deserialize)]
struct ChannelEvent<'a> {
    channel: Option<&'a str>,
    event: Option<&'a str>,
}

enum GateChannel {
    BookTicker,
    OrderBookUpdate,
}

fn classify_message(text: &str) -> Option<GateChannel> {
    let envelope: ChannelEvent = serde_json::from_str(text).ok()?;
    if envelope.event != Some("update") {
        return None;
    }
    match envelope.channel {
        Some("futures.book_ticker") => Some(GateChannel::BookTicker),
        Some("futures.order_book_update") => Some(GateChannel::OrderBookUpdate),
        _ => None,
    }
}

/// 深度档位，REST 快照和 WS 增量推送共用同一个形状(`{"p": "price", "s": size}`)。
#[derive(Debug, Deserialize)]
struct BookLevel {
    p: Decimal,
    s: Decimal,
}

#[derive(Debug, Deserialize)]
struct OrderBookUpdateResult {
    s: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    u: u64,
    #[serde(default)]
    b: Vec<BookLevel>,
    #[serde(default)]
    a: Vec<BookLevel>,
}

#[derive(Debug, Deserialize)]
struct OrderBookUpdateEnvelope {
    result: OrderBookUpdateResult,
}

/// 增量深度更新的本地同步状态：REST 快照返回之前，先把收到的增量缓冲起来；
/// 快照应用成功后切到 `Synced`，记录上一条增量的 `u`，供下一条做续号校验
/// (`U == last_u + 1`)。和 `binance.rs` 的 `DepthSync` 同构。
enum DepthSync {
    Buffering(Vec<DepthUpdate>),
    Synced { last_u: u64 },
}

#[derive(Debug, Clone)]
struct DepthUpdate {
    first_update_id: u64,
    u: u64,
    b: Vec<(Decimal, Decimal)>,
    a: Vec<(Decimal, Decimal)>,
}

/// 解析一条 `futures.order_book_update` update 消息，查表得到内部 Symbol。
/// 纯函数，不依赖网络连接，便于单测。
fn parse_order_book_update(text: &str, symbol_map: &HashMap<String, Symbol>) -> Option<(Symbol, DepthUpdate)> {
    let envelope: OrderBookUpdateEnvelope = serde_json::from_str(text).ok()?;
    let result = envelope.result;
    let symbol = symbol_map.get(&result.s)?.clone();
    Some((
        symbol,
        DepthUpdate {
            first_update_id: result.first_update_id,
            u: result.u,
            b: result.b.into_iter().map(|l| (l.p, l.s)).collect(),
            a: result.a.into_iter().map(|l| (l.p, l.s)).collect(),
        },
    ))
}

fn apply_levels(book: &mut LocalOrderBook, bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)]) {
    for (price, qty) in bids {
        book.apply_bid(*price, *qty);
    }
    for (price, qty) in asks {
        book.apply_ask(*price, *qty);
    }
}

#[derive(Debug)]
struct OrderBookSnapshot {
    id: u64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
}

#[derive(Debug, Deserialize)]
struct OrderBookSnapshotPayload {
    id: u64,
    #[serde(default)]
    bids: Vec<BookLevel>,
    #[serde(default)]
    asks: Vec<BookLevel>,
}

/// 拉一次 `GET /futures/usdt/order_book` REST 快照(`with_id=true` 带上基准
/// `id`，供增量续号校验用)。
async fn fetch_order_book_snapshot(http: &reqwest::Client, contract: &str) -> anyhow::Result<OrderBookSnapshot> {
    crate::ratelimit::throttle(REST_HOST).await;
    let url =
        format!("{REST_HOST}{API_PREFIX}/futures/usdt/order_book?contract={contract}&with_id=true&limit={ORDER_BOOK_DEPTH_LIMIT}");
    let resp = http.get(&url).send().await.context("gate futures order book snapshot request failed")?;
    let text = resp.text().await.context("failed to read gate futures order book snapshot response body")?;
    parse_order_book_snapshot(&text)
}

fn parse_order_book_snapshot(text: &str) -> anyhow::Result<OrderBookSnapshot> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let payload: OrderBookSnapshotPayload =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate futures order book snapshot, raw body: {text}"))?;
    Ok(OrderBookSnapshot {
        id: payload.id,
        bids: payload.bids.into_iter().map(|l| (l.p, l.s)).collect(),
        asks: payload.asks.into_iter().map(|l| (l.p, l.s)).collect(),
    })
}

/// 按官方算法把 REST 快照应用到本地簿，再重放快照到达之前缓冲的增量：第一条
/// 重放的增量要求 `U <= id+1 <= u`，之后每条要求 `U == 上一条.u + 1`。重放中途
/// 断档返回 `false`，调用方据此整条连接重连重新来过(和 `binance.rs` 一样，
/// 所有 resync 触发都统一走"断连重连")。
fn apply_snapshot(
    symbol: &Symbol,
    snapshot: OrderBookSnapshot,
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

    let mut last_u = snapshot.id;
    let mut first = true;
    for update in buffered {
        if update.u <= snapshot.id {
            continue;
        }
        if first {
            if update.first_update_id > snapshot.id + 1 {
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
        let query = [
            ("contract", gate_contract(symbol)),
            ("interval", candlestick_interval(interval).to_string()),
            ("limit", (limit + 1).min(CANDLESTICK_LIMIT_MAX).to_string()),
        ];
        let text = self.public_get("/futures/usdt/candlesticks", &query).await?;
        Ok(closed_klines(parse_candlesticks(&text)?, interval, now_ms(), limit))
    }

    /// 同一个接口改用 `from`/`to`(秒，两端都含；和 `limit` 互斥)，单次区间不能
    /// 超过 2000 根，按 `CANDLESTICK_LIMIT_MAX − 1` 根一段从 `start_ms` 往后切。
    async fn klines_range(
        &self,
        symbol: &Symbol,
        interval: KlineInterval,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<Kline>> {
        let step_ms = interval.millis();
        let chunk_ms = (CANDLESTICK_LIMIT_MAX as u64 - 1) * step_ms;
        let mut klines: Vec<Kline> = Vec::new();
        let mut from = start_ms;
        for _ in 0..RANGE_MAX_PAGES {
            if from > end_ms {
                break;
            }
            let to = (from + chunk_ms).min(end_ms);
            // `to` 恰好落在某根的开盘时间时，Gate 把这一根截到 `to` 为止，返回一根
            // v=0、价格等于上一根收盘的空 K 线(历史区间也一样)。所以 `to` 取最后
            // 一根结束前 1 秒，让它完整返回。
            let query = [
                ("contract", gate_contract(symbol)),
                ("interval", candlestick_interval(interval).to_string()),
                ("from", (from / 1000).to_string()),
                ("to", ((to + step_ms) / 1000 - 1).to_string()),
            ];
            let text = self.public_get("/futures/usdt/candlesticks", &query).await?;
            klines.extend(parse_candlesticks(&text)?);
            from = to + step_ms;
        }
        Ok(closed_klines_in_range(klines, interval, now_ms(), start_ms, end_ms))
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
    /// 成交张数(整数)。
    v: Decimal,
}

fn parse_candlesticks(text: &str) -> anyhow::Result<Vec<Kline>> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let entries: Vec<CandlestickEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate candlesticks, raw body: {text}"))?;
    Ok(entries
        .into_iter()
        .map(|e| Kline { open_time_ms: e.t as u64 * 1000, close: e.c, volume: e.v })
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
        let source = GateFuturesSource::new(Venue::new("gate_futures"), vec![Symbol::new("PEPE", "USDT")], None).unwrap();
        let msg: serde_json::Value = serde_json::from_str(&source.book_ticker_subscribe_message()).unwrap();
        assert_eq!(msg["channel"], "futures.book_ticker");
        assert_eq!(msg["event"], "subscribe");
        assert_eq!(msg["payload"], serde_json::json!(["PEPE_USDT"]));
    }

    #[test]
    fn builds_order_book_subscribe_message() {
        let symbol = Symbol::new("PEPE", "USDT");
        let msg: serde_json::Value =
            serde_json::from_str(&GateFuturesSource::order_book_subscribe_message(&symbol)).unwrap();
        assert_eq!(msg["channel"], "futures.order_book_update");
        assert_eq!(msg["event"], "subscribe");
        assert_eq!(msg["payload"], serde_json::json!(["PEPE_USDT", "100ms", "100"]));
    }

    #[test]
    fn classifies_book_ticker_and_order_book_update_messages() {
        assert!(matches!(
            classify_message(r#"{"channel":"futures.book_ticker","event":"update","result":{}}"#),
            Some(GateChannel::BookTicker)
        ));
        assert!(matches!(
            classify_message(r#"{"channel":"futures.order_book_update","event":"update","result":{}}"#),
            Some(GateChannel::OrderBookUpdate)
        ));
        assert!(classify_message(r#"{"channel":"futures.order_book_update","event":"subscribe"}"#).is_none());
        assert!(classify_message("not json").is_none());
    }

    #[test]
    fn parses_order_book_update_message() {
        let symbol = Symbol::new("GMT", "USDT");
        let map = map_with(symbol.clone());
        let text = r#"{
            "id": null,
            "time": 1650188898,
            "channel": "futures.order_book_update",
            "event": "update",
            "error": null,
            "result": {
                "t": 1650188898938,
                "s": "GMT_USDT",
                "U": 1577718307,
                "u": 1577719254,
                "b": [{"p": "2.5178", "s": 0}, {"p": "2.5179", "s": 100}],
                "a": [{"p": "2.52", "s": 200}]
            }
        }"#;

        let (parsed_symbol, update) = parse_order_book_update(text, &map).expect("should parse");
        assert_eq!(parsed_symbol, symbol);
        assert_eq!(update.first_update_id, 1577718307);
        assert_eq!(update.u, 1577719254);
        assert_eq!(
            update.b,
            vec![
                ("2.5178".parse().unwrap(), Decimal::ZERO),
                ("2.5179".parse().unwrap(), Decimal::from(100)),
            ]
        );
        assert_eq!(update.a, vec![("2.52".parse().unwrap(), Decimal::from(200))]);
    }

    #[test]
    fn parses_order_book_snapshot_response() {
        let text = r#"{
            "id": 6358770031,
            "current": 1634350208.745,
            "asks": [{"s": 24909, "p": "61264.8"}],
            "bids": [{"s": 41844, "p": "61264.7"}],
            "update": 1634350208.724
        }"#;

        let snapshot = parse_order_book_snapshot(text).expect("should parse");
        assert_eq!(snapshot.id, 6358770031);
        assert_eq!(snapshot.bids, vec![("61264.7".parse().unwrap(), Decimal::from(41844))]);
        assert_eq!(snapshot.asks, vec![("61264.8".parse().unwrap(), Decimal::from(24909))]);
    }

    #[test]
    fn order_book_snapshot_surfaces_error_response() {
        let err = parse_order_book_snapshot(r#"{"label":"INVALID_PARAM_VALUE","message":"bad contract"}"#).unwrap_err();
        assert!(err.to_string().contains("bad contract"));
    }

    #[test]
    fn apply_snapshot_replays_buffered_updates_after_baseline_id() {
        let symbol = Symbol::new("GMT", "USDT");
        let mut books = HashMap::new();
        let mut sync = HashMap::new();
        sync.insert(
            symbol.clone(),
            DepthSync::Buffering(vec![
                // u <= id(150)，应被丢弃，不计入续号校验。
                DepthUpdate { first_update_id: 145, u: 150, b: vec![], a: vec![] },
                DepthUpdate {
                    first_update_id: 151,
                    u: 155,
                    b: vec![("99.0".parse().unwrap(), "2.0".parse().unwrap())],
                    a: vec![],
                },
                DepthUpdate {
                    first_update_id: 156,
                    u: 160,
                    b: vec![],
                    a: vec![("101.0".parse().unwrap(), "1.0".parse().unwrap())],
                },
            ]),
        );

        let snapshot = OrderBookSnapshot {
            id: 150,
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
        let symbol = Symbol::new("GMT", "USDT");
        let mut books = HashMap::new();
        let mut sync = HashMap::new();
        sync.insert(
            symbol.clone(),
            DepthSync::Buffering(vec![DepthUpdate {
                // U(200) > id(150)+1，续号对不上，中间缺了数据。
                first_update_id: 200,
                u: 205,
                b: vec![],
                a: vec![],
            }]),
        );

        let snapshot = OrderBookSnapshot { id: 150, bids: vec![], asks: vec![] };

        assert!(!apply_snapshot(&symbol, snapshot, &mut books, &mut sync));
    }

    #[test]
    fn parses_candlesticks_converting_seconds_to_ms() {
        let text = r#"[
            {"o":"0.000004","v":15494,"t":1791435600,"c":"0.000004036","l":"0.000004","h":"0.000005","sum":"1"},
            {"o":"0.000004","v":15494,"t":1791439200,"c":"0.000004050","l":"0.000004","h":"0.000005","sum":"1"}
        ]"#;
        let klines = parse_candlesticks(text).expect("should parse");
        assert_eq!(
            klines[0],
            Kline { open_time_ms: 1791435600000, close: "0.000004036".parse().unwrap(), volume: Decimal::from(15494) }
        );
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
