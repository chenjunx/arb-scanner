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
use crate::net::connect_tcp;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{Kline, KlineInterval, KlineProvider, MarketDataSource, closed_klines, now_ms};

const WS_HOST: &str = "ws.okx.com";
const WS_PORT: u16 = 8443;
const WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const REST_HOST: &str = "https://www.okx.com";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// OKX 30 秒没有任何收发就断开连接，定时发文本 `ping` 保活(服务端回 `pong`)。
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);
/// `/api/v5/market/candles` 单次最多 300 根。
const CANDLES_LIMIT_MAX: usize = 300;
/// 翻页上限，防止交易所一直返回满页时死循环(10 页 = 3000 根，远超用量)。
const CANDLES_MAX_PAGES: usize = 10;

/// OKX USDT 本位永续一档盘口行情源：订阅公开频道 `bbo-tbt`(逐笔推送的最优
/// 买卖价)。发布的报价是交易所原始报价(每币价格)，挂单量单位是张。
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

    fn subscribe_message(&self) -> String {
        let args: Vec<serde_json::Value> = self
            .symbols
            .iter()
            .map(|s| serde_json::json!({"channel": "bbo-tbt", "instId": okx_swap_inst_id(s)}))
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

                let mut ping = tokio::time::interval(WS_PING_INTERVAL);
                ping.tick().await;
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = ws.send(Message::Text("ping".to_string())).await {
                                warn!("okx futures ws ping failed for venue={} err={err}", self.venue);
                                break;
                            }
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
        let bar = match interval {
            KlineInterval::M5 => "5m",
            KlineInterval::H1 => "1H",
        };
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
            Ok(Kline { open_time_ms, close })
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
        assert_eq!(klines[0], Kline { open_time_ms: 2 * HOUR_MS, close: "0.000004".parse().unwrap() });
        let closed = closed_klines(klines, KlineInterval::H1, 2 * HOUR_MS + 1, 10);
        assert_eq!(closed, vec![Kline { open_time_ms: HOUR_MS, close: "0.0000039".parse().unwrap() }]);
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
        assert_eq!(msg["args"][1], serde_json::json!({"channel": "bbo-tbt", "instId": "PEPE-USDT-SWAP"}));
    }
}
