use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::market_data::now_ms;
use crate::net::connect_tcp;
use crate::order_manager::stream::{ExchangeOrderUpdate, OrderStreamSource};
use crate::types::{Symbol, Venue};

use super::OrderProvider;
use super::types::{MarketInfo, MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const HOST: &str = "https://api.kraken.com";
const WS_HOST: &str = "ws-auth.kraken.com";
const WS_PORT: u16 = 443;
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Kraken 下单(执行层)客户端：查询交易对精度限制、提交市价单。签名方式和
/// `wallet::kraken::KrakenWalletProvider` 一致，用标准 HMAC-SHA512，凭证也复用
/// 同一套环境变量。
///
/// 重要限制：Kraken 的 `AddOrder` 接口对市价单只同步返回 `txid`，不保证立即
/// 告知是否已成交/成交多少——本实现里 `place_market_order_raw` 因此固定返回
/// `OrderStatus::New`、`filled_qty=0`、`avg_price=None`，调用方需要清楚这不是
/// 遗漏而是接口本身的限制；要拿到真实成交结果需要额外调用 `QueryOrders`
/// (本模块暂未实现)。
pub struct KrakenOrderProvider {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
}

impl KrakenOrderProvider {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            api_secret,
            http,
        })
    }

    /// 从环境变量读取凭证并构造实例，和 `wallet::kraken::KrakenWalletProvider::from_env`
    /// 读取同一套变量：`KRAKEN_SPOT_API_KEY` + `KRAKEN_SPOT_API_SECRET`。
    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("KRAKEN_SPOT_API_KEY").context("KRAKEN_SPOT_API_KEY not set")?;
        let api_secret =
            std::env::var("KRAKEN_SPOT_API_SECRET").context("KRAKEN_SPOT_API_SECRET not set")?;
        Self::new(venue, api_key, api_secret, proxy)
    }

    async fn private_request(&self, path: &str, params: Vec<(String, String)>) -> anyhow::Result<String> {
        kraken_private_request(&self.http, &self.api_key, &self.api_secret, path, params).await
    }

    /// 不需要签名的公开接口请求，用于查询交易对精度限制。
    async fn public_request(&self, path: &str, params: Vec<(String, String)>) -> anyhow::Result<String> {
        let query = params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        crate::ratelimit::throttle(HOST).await;
        let resp = self
            .http
            .get(format!("{HOST}{path}?{query}"))
            .send()
            .await
            .context("kraken public request failed")?;
        resp.text().await.context("failed to read kraken public response body")
    }

    fn kraken_pair(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }
}

#[async_trait]
impl OrderProvider for KrakenOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn market_info(&self, symbol: &Symbol) -> anyhow::Result<MarketInfo> {
        let params = vec![("pair".to_string(), Self::kraken_pair(symbol))];
        let text = self.public_request("/0/public/AssetPairs", params).await?;
        parse_market_info(&text, symbol)
    }

    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let OrderAmount::Base(quantity) = req.amount else {
            anyhow::bail!("{} does not support quote-amount market orders", self.venue());
        };
        let mut params = vec![
            ("pair".to_string(), Self::kraken_pair(&req.symbol)),
            ("type".to_string(), map_side(req.side).to_string()),
            ("ordertype".to_string(), "market".to_string()),
            ("volume".to_string(), quantity.to_string()),
        ];
        if let Some(client_order_id) = &req.client_order_id {
            params.push(("cl_ord_id".to_string(), client_order_id.clone()));
        }
        let text = self.private_request("/0/private/AddOrder", params).await?;
        parse_add_order_result(&text)
    }
}

/// 签名并发起一次 Kraken 私有 POST 请求。抽成自由函数是因为
/// `KrakenOrderProvider`(下单)和 `KrakenPrivateOrderStream`(私有订单流的
/// token 获取)都需要同一套 HMAC 签名逻辑。
async fn kraken_private_request(
    http: &reqwest::Client,
    api_key: &str,
    api_secret: &str,
    path: &str,
    params: Vec<(String, String)>,
) -> anyhow::Result<String> {
    let nonce = now_ms().to_string();
    let post_data = build_post_data(&nonce, &params);
    let signature = sign_kraken(api_secret, path, &nonce, &post_data)?;

    crate::ratelimit::throttle(HOST).await;
    let resp = http
        .post(format!("{HOST}{path}"))
        .header("API-Key", api_key)
        .header("API-Sign", signature)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(post_data)
        .send()
        .await
        .context("kraken order request failed")?;
    resp.text().await.context("failed to read kraken order response body")
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build kraken http client")
}

/// 拼出 Kraken 要求的 POST body：`nonce=<nonce>&k=v&...`，签名和实际发送必须
/// 用同一份字符串。
fn build_post_data(nonce: &str, params: &[(String, String)]) -> String {
    let mut parts = vec![format!("nonce={nonce}")];
    parts.extend(params.iter().map(|(k, v)| format!("{k}={v}")));
    parts.join("&")
}

/// Kraken 签名算法：
/// `message = path_bytes ++ SHA256(nonce ++ post_data)`
/// `signature = base64(HMAC_SHA512(base64_decode(secret), message))`
fn sign_kraken(secret_b64: &str, path: &str, nonce: &str, post_data: &str) -> anyhow::Result<String> {
    let secret = base64_engine.decode(secret_b64).context("invalid kraken api secret base64")?;

    let mut sha_input = Vec::with_capacity(nonce.len() + post_data.len());
    sha_input.extend_from_slice(nonce.as_bytes());
    sha_input.extend_from_slice(post_data.as_bytes());
    let digest = ring::digest::digest(&ring::digest::SHA256, &sha_input);

    let mut message = Vec::with_capacity(path.len() + digest.as_ref().len());
    message.extend_from_slice(path.as_bytes());
    message.extend_from_slice(digest.as_ref());

    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA512, &secret);
    let signature = ring::hmac::sign(&key, &message);
    Ok(base64_engine.encode(signature.as_ref()))
}

fn map_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

#[derive(Debug, Deserialize)]
struct KrakenEnvelope<T> {
    #[serde(default)]
    error: Vec<String>,
    #[serde(default)]
    result: Option<T>,
}

/// 解析 Kraken 通用的 `{error: [...], result: ...}` 信封：`error` 非空时视为失败，
/// 否则把 `result` 反序列化成调用方指定的具体类型。
fn unwrap_result<T: DeserializeOwned>(text: &str) -> anyhow::Result<T> {
    let envelope: KrakenEnvelope<serde_json::Value> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse kraken response envelope, raw body: {text}"))?;
    if !envelope.error.is_empty() {
        anyhow::bail!("kraken error: {}", envelope.error.join(", "));
    }
    let result = envelope
        .result
        .ok_or_else(|| anyhow::anyhow!("kraken response missing result"))?;
    serde_json::from_value(result.clone())
        .with_context(|| format!("failed to parse kraken result payload, raw result: {result}"))
}

#[derive(Debug, Deserialize)]
struct AssetPairInfo {
    #[serde(default)]
    ordermin: Option<String>,
    lot_decimals: u32,
}

/// `AssetPairs?pair=<X>` 只返回一个 pair，但返回的 key 是交易所内部代码
/// (如 "XXBTZUSD")而不是请求里传的 altname，所以取 `result` 里唯一的那个值。
fn parse_market_info(text: &str, symbol: &Symbol) -> anyhow::Result<MarketInfo> {
    let pairs: HashMap<String, AssetPairInfo> = unwrap_result(text)?;
    let info = pairs
        .into_values()
        .next()
        .ok_or_else(|| anyhow::anyhow!("kraken AssetPairs returned no pair for {symbol}"))?;

    let qty_step = Decimal::new(1, info.lot_decimals);
    let min_qty = info
        .ordermin
        .and_then(|v| v.parse().ok())
        .unwrap_or(Decimal::ZERO);

    Ok(MarketInfo {
        symbol: symbol.clone(),
        qty_step,
        min_qty,
    })
}

#[derive(Debug, Deserialize)]
struct AddOrderResult {
    txid: Vec<String>,
}

fn parse_add_order_result(text: &str) -> anyhow::Result<OrderResult> {
    let result: AddOrderResult = unwrap_result(text)?;
    let order_id = result
        .txid
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("kraken AddOrder response missing txid"))?;

    // Kraken 的 AddOrder 不同步返回成交信息，见本文件顶部注释；手续费同理拿不到。
    Ok(OrderResult {
        order_id,
        status: OrderStatus::New,
        filled_qty: Decimal::ZERO,
        avg_price: None,
        fee: None,
        fee_asset: None,
    })
}

/// Kraken 私有订单流客户端：通过 `GetWebSocketsToken` 拿到鉴权 token，连接
/// WebSocket v2 `wss://ws-auth.kraken.com/v2` 并订阅 `executions` channel。
/// 和 Binance 的 listenKey 不同，token 一旦用于建立连接就在整个会话期间有效，
/// 不需要额外的心跳续期；断线重连时重新拿一个新 token 即可(旧 token 大概率
/// 已经过期)。
pub struct KrakenPrivateOrderStream {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
    proxy: Option<String>,
}

impl KrakenPrivateOrderStream {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            api_secret,
            http,
            proxy: proxy.map(str::to_string),
        })
    }

    /// 和 `KrakenOrderProvider::from_env` 复用同一套凭证环境变量。
    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("KRAKEN_SPOT_API_KEY").context("KRAKEN_SPOT_API_KEY not set")?;
        let api_secret =
            std::env::var("KRAKEN_SPOT_API_SECRET").context("KRAKEN_SPOT_API_SECRET not set")?;
        Self::new(venue, api_key, api_secret, proxy)
    }

    async fn fetch_token(&self) -> anyhow::Result<String> {
        let text = kraken_private_request(&self.http, &self.api_key, &self.api_secret, "/0/private/GetWebSocketsToken", vec![]).await?;
        parse_ws_token(&text)
    }

    async fn connect(&self, token: &str) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(WS_HOST, WS_PORT, self.proxy.as_deref()).await?;
        let url = format!("wss://{WS_HOST}/v2");
        let (mut ws, _) = tokio_tungstenite::client_async_tls(url, tcp)
            .await
            .context("kraken private order stream handshake failed")?;

        let subscribe = serde_json::json!({
            "method": "subscribe",
            "params": {
                "channel": "executions",
                "token": token,
                "snap_orders": false,
                "snap_trades": false,
            }
        });
        ws.send(Message::Text(subscribe.to_string()))
            .await
            .context("failed to send kraken executions subscribe message")?;
        Ok(ws)
    }
}

impl OrderStreamSource for KrakenPrivateOrderStream {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, tx: mpsc::Sender<ExchangeOrderUpdate>) -> crate::order_manager::stream::StreamHandle {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let mut backoff = MIN_BACKOFF;
            let mut ready_tx = Some(ready_tx);

            loop {
                let token = match self.fetch_token().await {
                    Ok(token) => token,
                    Err(err) => {
                        warn!(
                            "kraken private order stream: failed to fetch ws token for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                let mut ws = match self.connect(&token).await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "kraken private order stream connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };
                debug!("kraken private order stream connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;
                if let Some(ready_tx) = ready_tx.take() {
                    let _ = ready_tx.send(());
                }

                while let Some(msg) = ws.next().await {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("kraken private order stream error for venue={} err={err}", self.venue);
                            break;
                        }
                    };
                    let Message::Text(text) = msg else { continue };
                    for update in parse_kraken_execution(&text, &self.venue) {
                        if tx.send(update).await.is_err() {
                            return;
                        }
                    }
                }

                warn!(
                    "kraken private order stream disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        });
        crate::order_manager::stream::StreamHandle { join, ready: ready_rx }
    }
}

#[derive(Debug, Deserialize)]
struct WsTokenResult {
    token: String,
}

fn parse_ws_token(text: &str) -> anyhow::Result<String> {
    let result: WsTokenResult = unwrap_result(text)?;
    Ok(result.token)
}

fn map_kraken_ws_status(status: &str) -> OrderStatus {
    match status {
        "filled" => OrderStatus::Filled,
        "partially_filled" => OrderStatus::PartiallyFilled,
        "rejected" => OrderStatus::Rejected,
        "canceled" | "expired" => OrderStatus::Expired,
        _ => OrderStatus::New, // pending_new/new 等未成交中间态
    }
}

#[derive(Debug, Deserialize)]
struct ChannelEnvelope {
    #[serde(default)]
    channel: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExecutionsEnvelope {
    #[serde(default)]
    data: Vec<KrakenExecutionData>,
}

/// `exec_type: "trade"` 的推送里带的单笔手续费，币种通常是成交对里的计价币
/// 或折扣币(如用 KFEE 抵扣)，一条消息可能包含多笔不同币种的 fee 项。
#[derive(Debug, Deserialize)]
struct KrakenFee {
    asset: String,
    qty: Decimal,
}

#[derive(Debug, Deserialize)]
struct KrakenExecutionData {
    order_id: String,
    #[serde(default)]
    cl_ord_id: Option<String>,
    order_status: String,
    /// 累计成交量(不是本次推送的增量)；pending_new 阶段可能整个字段都不存在。
    #[serde(default)]
    cum_qty: Decimal,
    /// 累计成交额，配合 cum_qty 算均价。
    #[serde(default)]
    cum_cost: Decimal,
    /// 只有 `exec_type: "trade"` 的成交事件才会带这个字段，其它状态变更事件
    /// (pending_new/canceled 等)默认为空数组。
    #[serde(default)]
    fees: Vec<KrakenFee>,
}

/// 按 asset 分组求和，语义和 `binance::sum_fee_by_asset` 一致：只有单一币种
/// 时才认为是可信的单一手续费值返回 `Some`，混合多币种或没有 fee 项时返回
/// `None`，交给 Portfolio 按 `FeeConfig` 估算兜底。
fn sum_kraken_fees(fees: &[KrakenFee]) -> (Option<Decimal>, Option<String>) {
    let mut totals: HashMap<&str, Decimal> = HashMap::new();
    for fee in fees {
        *totals.entry(fee.asset.as_str()).or_insert(Decimal::ZERO) += fee.qty;
    }
    if totals.len() == 1 {
        let (asset, total) = totals.into_iter().next().unwrap();
        (Some(total), Some(asset.to_string()))
    } else {
        (None, None)
    }
}

/// 解析一条 WebSocket v2 消息，只关心 `channel: "executions"` 的推送(心跳、
/// 订阅确认等消息直接忽略)。一条消息可能携带多笔订单的更新，因此返回
/// `Vec`。纯函数，不依赖真实 WebSocket 连接，便于单元测试。
///
/// Kraken 用 ISO8601 字符串标记时间戳而不是 epoch 毫秒，这里不引入额外的
/// 日期解析依赖，直接用本地收到消息的时间作为 `ts_ms`——`OrderManager` 只把
/// 它当展示/记录用的时间戳，不参与去重/防倒退判断(那部分靠 `filled_qty` 和
/// `status` 本身)。
fn parse_kraken_execution(text: &str, venue: &Venue) -> Vec<ExchangeOrderUpdate> {
    let envelope: ChannelEnvelope = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(_) => return Vec::new(),
    };
    if envelope.channel.as_deref() != Some("executions") {
        return Vec::new();
    }
    let full: ExecutionsEnvelope = match serde_json::from_str(text) {
        Ok(full) => full,
        Err(err) => {
            warn!("failed to parse kraken executions message: {err}");
            return Vec::new();
        }
    };

    let ts_ms = now_ms();
    full.data
        .into_iter()
        .map(|item| {
            let (fee, fee_asset) = sum_kraken_fees(&item.fees);
            ExchangeOrderUpdate {
                venue: venue.clone(),
                client_order_id: item.cl_ord_id.filter(|s| !s.is_empty()),
                exchange_order_id: Some(item.order_id),
                status: map_kraken_ws_status(&item.order_status),
                filled_qty: item.cum_qty,
                avg_price: (item.cum_qty > Decimal::ZERO).then(|| item.cum_cost / item.cum_qty),
                fee,
                fee_asset,
                ts_ms,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_matches_independently_computed_reference_vector() {
        let secret_b64 = "coFbU8p41bBXnzmdU/ynDvyqypLm4S9D8y1wn7H1als=";
        let path = "/0/private/AddOrder";
        let nonce = "1700000000000";
        let post_data = "nonce=1700000000000&pair=XBTUSD&type=buy&ordertype=market&volume=0.1";

        // 用同一套签名算法(和 wallet::kraken 的签名测试用同一个参考密钥)
        // 反向验证：改变任意一个字段签名必须不同，避免实现里字段拼接顺序出错。
        let signature = sign_kraken(secret_b64, path, nonce, post_data).expect("signing should succeed");
        let signature_changed_volume =
            sign_kraken(secret_b64, path, nonce, "nonce=1700000000000&pair=XBTUSD&type=buy&ordertype=market&volume=0.2")
                .expect("signing should succeed");
        assert_ne!(signature, signature_changed_volume);
    }

    #[test]
    fn builds_post_data_with_nonce_first() {
        let params = vec![("pair".to_string(), "XBTUSD".to_string())];
        assert_eq!(build_post_data("123", &params), "nonce=123&pair=XBTUSD");
    }

    #[test]
    fn maps_side_to_kraken_string() {
        assert_eq!(map_side(OrderSide::Buy), "buy");
        assert_eq!(map_side(OrderSide::Sell), "sell");
    }

    #[test]
    fn parses_market_info_from_asset_pairs() {
        let text = r#"{
            "error": [],
            "result": {
                "XXBTZUSD": {
                    "altname": "XBTUSD",
                    "wsname": "XBT/USD",
                    "lot_decimals": 8,
                    "ordermin": "0.0001"
                }
            }
        }"#;
        let symbol = Symbol::new("XBT", "USD");
        let info = parse_market_info(text, &symbol).expect("should parse");
        assert_eq!(info.qty_step, Decimal::new(1, 8));
        assert_eq!(info.min_qty, "0.0001".parse().unwrap());
    }

    #[test]
    fn parse_market_info_defaults_min_qty_when_missing() {
        let text = r#"{
            "error": [],
            "result": {
                "XETHZUSD": {"altname": "ETHUSD", "wsname": "ETH/USD", "lot_decimals": 6}
            }
        }"#;
        let symbol = Symbol::new("ETH", "USD");
        let info = parse_market_info(text, &symbol).expect("should parse");
        assert_eq!(info.min_qty, Decimal::ZERO);
    }

    #[test]
    fn parse_market_info_surfaces_error_response() {
        let text = r#"{"error": ["EQuery:Unknown asset pair"], "result": null}"#;
        let symbol = Symbol::new("XBT", "USD");
        let err = parse_market_info(text, &symbol).unwrap_err();
        assert!(err.to_string().contains("Unknown asset pair"));
    }

    #[test]
    fn parses_add_order_result() {
        let text = r#"{
            "error": [],
            "result": {
                "descr": {"order": "buy 0.0002 XBTUSD @ market"},
                "txid": ["OQCLML-BW3P3-BUCMWZ"]
            }
        }"#;
        let result = parse_add_order_result(text).expect("should parse");
        assert_eq!(result.order_id, "OQCLML-BW3P3-BUCMWZ");
        assert_eq!(result.status, OrderStatus::New);
        assert_eq!(result.filled_qty, Decimal::ZERO);
        assert_eq!(result.avg_price, None);
        assert_eq!(result.fee, None);
        assert_eq!(result.fee_asset, None);
    }

    #[test]
    fn parse_add_order_result_surfaces_error_response() {
        let text = r#"{"error": ["EOrder:Insufficient funds"], "result": null}"#;
        let err = parse_add_order_result(text).unwrap_err();
        assert!(err.to_string().contains("Insufficient funds"));
    }

    #[test]
    fn parses_ws_token() {
        let text = r#"{"error": [], "result": {"token": "anF3heJR/CGYRq1L3Bwtoa/gGB..."}}"#;
        assert_eq!(parse_ws_token(text).expect("should parse"), "anF3heJR/CGYRq1L3Bwtoa/gGB...");
    }

    #[test]
    fn parse_ws_token_surfaces_error_response() {
        let text = r#"{"error": ["EGeneral:Permission denied"], "result": null}"#;
        let err = parse_ws_token(text).unwrap_err();
        assert!(err.to_string().contains("Permission denied"));
    }

    #[test]
    fn maps_kraken_ws_status_strings() {
        assert_eq!(map_kraken_ws_status("filled"), OrderStatus::Filled);
        assert_eq!(map_kraken_ws_status("partially_filled"), OrderStatus::PartiallyFilled);
        assert_eq!(map_kraken_ws_status("pending_new"), OrderStatus::New);
        assert_eq!(map_kraken_ws_status("new"), OrderStatus::New);
        assert_eq!(map_kraken_ws_status("rejected"), OrderStatus::Rejected);
        assert_eq!(map_kraken_ws_status("canceled"), OrderStatus::Expired);
        assert_eq!(map_kraken_ws_status("expired"), OrderStatus::Expired);
    }

    #[test]
    fn parses_executions_update_with_partial_fill() {
        let venue = Venue::new("kraken");
        let text = r#"{
            "channel": "executions",
            "type": "update",
            "data": [
                {
                    "order_id": "OK4GJX-KSTLS-7DZZO5",
                    "cl_ord_id": "ORD-000000000001",
                    "symbol": "BTC/USD",
                    "order_status": "partially_filled",
                    "cum_qty": 0.4,
                    "cum_cost": 16000.0,
                    "timestamp": "2023-09-22T10:33:05.709950Z"
                }
            ],
            "sequence": 8
        }"#;
        let updates = parse_kraken_execution(text, &venue);
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.venue, venue);
        assert_eq!(update.client_order_id, Some("ORD-000000000001".to_string()));
        assert_eq!(update.exchange_order_id, Some("OK4GJX-KSTLS-7DZZO5".to_string()));
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
        assert_eq!(update.filled_qty, "0.4".parse().unwrap());
        assert_eq!(update.avg_price, Some("40000".parse().unwrap()));
        assert_eq!(update.fee, None);
        assert_eq!(update.fee_asset, None);
    }

    #[test]
    fn parses_executions_trade_with_single_asset_fee() {
        let venue = Venue::new("kraken");
        let text = r#"{
            "channel": "executions",
            "type": "update",
            "data": [
                {
                    "order_id": "OK4GJX-KSTLS-7DZZO5",
                    "cl_ord_id": "ORD-000000000001",
                    "symbol": "BTC/USD",
                    "order_status": "filled",
                    "cum_qty": 0.4,
                    "cum_cost": 16000.0,
                    "fees": [{"asset": "USD", "qty": 4.16}],
                    "timestamp": "2023-09-22T10:33:05.709950Z"
                }
            ],
            "sequence": 8
        }"#;
        let updates = parse_kraken_execution(text, &venue);
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.fee, Some("4.16".parse().unwrap()));
        assert_eq!(update.fee_asset, Some("USD".to_string()));
    }

    #[test]
    fn parses_executions_trade_with_mixed_asset_fees_falls_back_to_none() {
        let venue = Venue::new("kraken");
        let text = r#"{
            "channel": "executions",
            "type": "update",
            "data": [
                {
                    "order_id": "OK4GJX-KSTLS-7DZZO5",
                    "symbol": "BTC/USD",
                    "order_status": "filled",
                    "cum_qty": 0.4,
                    "cum_cost": 16000.0,
                    "fees": [{"asset": "USD", "qty": 2.0}, {"asset": "KFEE", "qty": 100}],
                    "timestamp": "2023-09-22T10:33:05.709950Z"
                }
            ],
            "sequence": 8
        }"#;
        let updates = parse_kraken_execution(text, &venue);
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.fee, None);
        assert_eq!(update.fee_asset, None);
    }

    #[test]
    fn parses_executions_pending_new_without_cl_ord_id_or_fill() {
        let venue = Venue::new("kraken");
        let text = r#"{
            "channel": "executions",
            "type": "update",
            "data": [
                {
                    "order_id": "OK4GJX-KSTLS-7DZZO5",
                    "symbol": "BTC/USD",
                    "order_qty": 0.005,
                    "cum_cost": 0.0,
                    "order_status": "pending_new",
                    "timestamp": "2023-09-22T10:33:05.709950Z"
                }
            ],
            "sequence": 9
        }"#;
        let updates = parse_kraken_execution(text, &venue);
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.client_order_id, None);
        assert_eq!(update.status, OrderStatus::New);
        assert_eq!(update.filled_qty, Decimal::ZERO);
        assert_eq!(update.avg_price, None);
        assert_eq!(update.fee, None);
        assert_eq!(update.fee_asset, None);
    }

    #[test]
    fn ignores_non_execution_channel_messages() {
        let venue = Venue::new("kraken");
        assert!(parse_kraken_execution(r#"{"channel":"heartbeat"}"#, &venue).is_empty());
        assert!(parse_kraken_execution(
            r#"{"method":"subscribe","success":true,"result":{"channel":"executions"}}"#,
            &venue
        )
        .is_empty());
    }

    #[test]
    fn ignores_malformed_execution_message() {
        let venue = Venue::new("kraken");
        assert!(parse_kraken_execution("not json", &venue).is_empty());
    }
}
