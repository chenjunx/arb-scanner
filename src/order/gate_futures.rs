use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::market_data::now_ms;
use crate::net::connect_tcp;
use crate::order_manager::OrderManager;
use crate::order_manager::stream::{ExchangeOrderUpdate, OrderStreamSource};
use crate::types::{Symbol, Venue};

use super::OrderProvider;
use super::types::{MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const HOST: &str = "https://api.gateio.ws";
const API_PREFIX: &str = "/api/v4";
const WS_HOST: &str = "fx-ws.gateio.ws";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// 私有订单流可能很久没有推送，定时发应用层 `futures.ping` 保活。
const WS_PING_INTERVAL: Duration = Duration::from_secs(15);
/// Gate 自定义单号前缀，见 [`gate_client_order_id`]。
const CLIENT_ID_PREFIX: &str = "t-";

/// Gate.io USDT 本位永续下单客户端(`/futures/usdt/*`)。凭证和现货共用
/// `GATE_API_KEY`/`GATE_API_SECRET`。签名算法和 `order::gate` 完全相同，按既有
/// 约定各文件独立一份。
///
/// 下单量单位是**张**(1 张 = `quanto_multiplier` 个币，见
/// `exchange_info::gate` 的 `perpetual_contract_spec`)，只支持整数张；
/// 方向用 `size` 的正负表示(买正卖负)。要求账户是单向持仓模式，杠杆/保证金
/// 模式由用户在交易所预先设置好，这里不修改。
pub struct GateFuturesOrderProvider {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
}

impl GateFuturesOrderProvider {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self { venue, api_key, api_secret, http })
    }

    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("GATE_API_KEY").context("GATE_API_KEY not set")?;
        let api_secret = std::env::var("GATE_API_SECRET").context("GATE_API_SECRET not set")?;
        Self::new(venue, api_key, api_secret, proxy)
    }

    async fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<String> {
        signed_request(&self.http, &self.api_key, &self.api_secret, method, path, body).await
    }
}

async fn signed_request(
    http: &reqwest::Client,
    api_key: &str,
    api_secret: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> anyhow::Result<String> {
    let body_str = body.as_ref().map(|b| b.to_string()).unwrap_or_default();
    let timestamp = (now_ms() / 1000).to_string();
    let full_path = format!("{API_PREFIX}{path}");
    let signature = gate_sign(api_secret, method.as_str(), &full_path, "", &body_str, &timestamp);

    crate::ratelimit::throttle(HOST).await;
    let mut request = http
        .request(method, format!("{HOST}{full_path}"))
        .header("KEY", api_key)
        .header("Timestamp", &timestamp)
        .header("SIGN", &signature);
    if body.is_some() {
        request = request.header("Content-Type", "application/json").body(body_str);
    }
    let resp = request.send().await.context("gate futures request failed")?;
    resp.text().await.context("failed to read gate futures response body")
}

#[async_trait]
impl OrderProvider for GateFuturesOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `POST /futures/usdt/orders`，市价单 = `price: "0"` + `tif: "ioc"`。
    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let OrderAmount::Base(quantity) = req.amount else {
            anyhow::bail!("{} does not support quote-amount market orders", self.venue());
        };
        let body = build_market_order_body(&req.symbol, req.side, quantity, req.client_order_id.as_deref())?;
        let text = self.signed_request(reqwest::Method::POST, "/futures/usdt/orders", Some(body)).await?;
        parse_order_response(&text)
    }

    /// `GET /futures/usdt/orders/{id}`，合约订单号全局唯一，不需要 symbol。
    async fn query_order(&self, _symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<OrderResult> {
        let path = format!("/futures/usdt/orders/{exchange_order_id}");
        let text = self.signed_request(reqwest::Method::GET, &path, None).await?;
        parse_order_response(&text)
    }

    /// `GET /futures/usdt/positions/{contract}`：单向持仓模式下 `size` 是净持仓
    /// 张数(多正空负)；双向持仓模式下这个接口报错，正好当作前置条件检查。
    async fn perp_position(&self, symbol: &Symbol) -> anyhow::Result<Decimal> {
        let path = format!("/futures/usdt/positions/{}", gate_contract(symbol));
        let text = self.signed_request(reqwest::Method::GET, &path, None).await?;
        parse_position(&text)
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build gate futures http client")
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 和 `order::gate::gate_sign` 完全相同的算法(REST v4 HMAC-SHA512)。
fn gate_sign(secret: &str, method: &str, url_path: &str, query_string: &str, body: &str, timestamp: &str) -> String {
    let body_hash = hex_encode(ring::digest::digest(&ring::digest::SHA512, body.as_bytes()).as_ref());
    let payload = format!("{method}\n{url_path}\n{query_string}\n{body_hash}\n{timestamp}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA512, secret.as_bytes());
    let signature = ring::hmac::sign(&key, payload.as_bytes());
    hex_encode(signature.as_ref())
}

fn gate_ws_auth(api_key: &str, api_secret: &str, channel: &str, event: &str, time_secs: u64) -> serde_json::Value {
    let sign_str = format!("channel={channel}&event={event}&time={time_secs}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA512, api_secret.as_bytes());
    let signature = ring::hmac::sign(&key, sign_str.as_bytes());
    serde_json::json!({
        "method": "api_key",
        "KEY": api_key,
        "SIGN": hex_encode(signature.as_ref()),
    })
}

fn gate_contract(symbol: &Symbol) -> String {
    format!("{}_{}", symbol.base, symbol.quote).to_ascii_uppercase()
}

/// Gate `text` 自定义单号：必须以 `t-` 开头，前缀之后不超过 28 字节。超长时
/// 保留末尾(时间戳+随机数)截掉开头，规则同 `order::gate::gate_client_order_id`。
/// 推送回来时 [`parse_futures_order_update`] 会去掉 `t-` 前缀，所以只要内部
/// client_order_id 不超过 28 字节(如 `cross_funding` 自己生成的短 id)，就能按
/// client_order_id 原样关联回内部订单。
fn gate_client_order_id(client_order_id: Option<&str>) -> Option<String> {
    let id = client_order_id?;
    if id.is_empty() {
        return None;
    }
    let chars: Vec<char> = id.chars().collect();
    let truncated: String = if chars.len() > 28 { chars[chars.len() - 28..].iter().collect() } else { id.to_string() };
    Some(format!("{CLIENT_ID_PREFIX}{truncated}"))
}

/// `size` 必须是整数张，买正卖负。
fn build_market_order_body(
    symbol: &Symbol,
    side: OrderSide,
    quantity: Decimal,
    client_order_id: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    if quantity.fract() != Decimal::ZERO {
        anyhow::bail!("gate futures order size must be whole contracts, got {quantity}");
    }
    let contracts = quantity.to_i64().with_context(|| format!("gate futures order size out of range: {quantity}"))?;
    let size = match side {
        OrderSide::Buy => contracts,
        OrderSide::Sell => -contracts,
    };
    let mut body = serde_json::json!({
        "contract": gate_contract(symbol),
        "size": size,
        "price": "0",
        "tif": "ioc",
    });
    if let Some(text) = gate_client_order_id(client_order_id) {
        body["text"] = serde_json::Value::String(text);
    }
    Ok(body)
}

#[derive(Debug, Deserialize)]
struct GateErrorResponse {
    label: String,
    message: String,
}

/// REST 和 WS 推送共用的合约订单字段。`id` 是整数，`size`/`left` 普通合约是
/// 整数、开了小数张的合约可能是字符串，`fill_price` REST 是字符串、WS 是数字——
/// `Decimal` 的默认反序列化都接受。
#[derive(Debug, Deserialize)]
struct FuturesOrder {
    id: serde_json::Value,
    #[serde(default)]
    contract: Option<String>,
    #[serde(default)]
    size: Decimal,
    #[serde(default)]
    left: Decimal,
    #[serde(default)]
    fill_price: Option<Decimal>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    finish_as: Option<String>,
    #[serde(default)]
    text: Option<String>,
}

impl FuturesOrder {
    fn id_string(&self) -> String {
        match &self.id {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        }
    }

    /// 已成交张数(绝对值)。
    fn filled_qty(&self) -> Decimal {
        (self.size.abs() - self.left.abs()).max(Decimal::ZERO)
    }

    fn avg_price(&self) -> Option<Decimal> {
        self.fill_price.filter(|p| *p > Decimal::ZERO)
    }

    /// `finished` + `finish_as=filled` 才是全部成交；其它终态原因(`ioc` 剩余
    /// 部分被撤、`cancelled`、`liquidated`、`stp` 等)统一按 Expired，成交量如实
    /// 保留在 filled_qty 里，和 `order::gate` 的既有约定一致。
    fn status(&self) -> OrderStatus {
        let has_fill = self.filled_qty() > Decimal::ZERO;
        match self.status.as_deref() {
            Some("finished") => match self.finish_as.as_deref() {
                Some("filled") => OrderStatus::Filled,
                _ => OrderStatus::Expired,
            },
            _ if has_fill => OrderStatus::PartiallyFilled,
            _ => OrderStatus::New,
        }
    }

    /// 去掉 `t-` 前缀还原内部 client_order_id；不是 `t-` 开头的(如网页下单的
    /// `web`、`api`)不是我们下的单，不带 client_order_id，交给 exchange_order_id 兜底。
    fn client_order_id(&self) -> Option<String> {
        self.text
            .as_deref()
            .and_then(|t| t.strip_prefix(CLIENT_ID_PREFIX))
            .filter(|t| !t.is_empty())
            .map(str::to_string)
    }
}

/// 合约订单 REST 响应不带实际手续费(只有费率 `tkfr`/`mkfr`)，fee 留空交给
/// 估算兜底。
fn parse_order_response(text: &str) -> anyhow::Result<OrderResult> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate futures error {}: {}", err.label, err.message);
    }
    let order: FuturesOrder =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate futures order response, raw body: {text}"))?;
    Ok(OrderResult {
        order_id: order.id_string(),
        status: order.status(),
        filled_qty: order.filled_qty(),
        avg_price: order.avg_price(),
        fee: None,
        fee_asset: None,
    })
}

#[derive(Debug, Deserialize)]
struct PositionResponse {
    size: Decimal,
}

fn parse_position(text: &str) -> anyhow::Result<Decimal> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate futures error {}: {}", err.label, err.message);
    }
    let position: PositionResponse =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate futures position response, raw body: {text}"))?;
    Ok(position.size)
}

#[derive(Debug, Deserialize)]
struct AccountResponse {
    user: serde_json::Value,
}

fn parse_account_user_id(text: &str) -> anyhow::Result<String> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate futures error {}: {}", err.label, err.message);
    }
    let account: AccountResponse =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate futures account response, raw body: {text}"))?;
    match account.user {
        serde_json::Value::String(s) if !s.is_empty() => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => anyhow::bail!("gate futures account response has invalid user id: {other}"),
    }
}

/// Gate.io USDT 本位永续私有订单流：订阅 `futures.orders`，payload 是
/// `[user_id, "!all"]`(全部合约)，所以启动时先用 `GET /futures/usdt/accounts`
/// 查出 user id。鉴权方式和现货 `spot.orders` 相同(内嵌在订阅请求的 `auth` 里)。
/// 推送里的数量单位是张，和下单单位一致。
pub struct GateFuturesPrivateOrderStream {
    venue: Venue,
    api_key: String,
    api_secret: String,
    proxy: Option<String>,
    http: reqwest::Client,
}

impl GateFuturesPrivateOrderStream {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self { venue, api_key, api_secret, proxy: proxy.map(str::to_string), http })
    }

    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("GATE_API_KEY").context("GATE_API_KEY not set")?;
        let api_secret = std::env::var("GATE_API_SECRET").context("GATE_API_SECRET not set")?;
        Self::new(venue, api_key, api_secret, proxy)
    }

    async fn fetch_user_id(&self) -> anyhow::Result<String> {
        let text = signed_request(
            &self.http,
            &self.api_key,
            &self.api_secret,
            reqwest::Method::GET,
            "/futures/usdt/accounts",
            None,
        )
        .await?;
        parse_account_user_id(&text)
    }

    fn subscribe_message(&self, user_id: &str) -> serde_json::Value {
        let time_secs = now_ms() / 1000;
        let auth = gate_ws_auth(&self.api_key, &self.api_secret, "futures.orders", "subscribe", time_secs);
        serde_json::json!({
            "time": time_secs,
            "channel": "futures.orders",
            "event": "subscribe",
            "payload": [user_id, "!all"],
            "auth": auth,
        })
    }

    async fn connect(&self) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(WS_HOST, WS_PORT, self.proxy.as_deref()).await?;
        let (ws, _) = tokio_tungstenite::client_async_tls(WS_URL, tcp)
            .await
            .context("gate futures private order stream handshake failed")?;
        Ok(ws)
    }
}

impl OrderStreamSource for GateFuturesPrivateOrderStream {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, order_manager: Arc<OrderManager>) -> crate::order_manager::stream::StreamHandle {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let mut backoff = MIN_BACKOFF;
            let mut ready_tx = Some(ready_tx);
            let mut user_id: Option<String> = None;

            loop {
                let uid = match &user_id {
                    Some(uid) => uid.clone(),
                    None => match self.fetch_user_id().await {
                        Ok(uid) => {
                            user_id = Some(uid.clone());
                            uid
                        }
                        Err(err) => {
                            warn!(
                                "gate futures user id query failed for venue={} err={err:#}, retrying in {:?}",
                                self.venue, backoff
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(MAX_BACKOFF);
                            continue;
                        }
                    },
                };

                let mut ws = match self.connect().await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "gate futures private order stream connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(self.subscribe_message(&uid).to_string())).await {
                    warn!(
                        "gate futures private order stream subscribe failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                debug!("gate futures private order stream connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;
                if let Some(ready_tx) = ready_tx.take() {
                    let _ = ready_tx.send(());
                }

                let mut ping = tokio::time::interval(WS_PING_INTERVAL);
                ping.tick().await;
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            let msg = serde_json::json!({"time": now_ms() / 1000, "channel": "futures.ping"});
                            if let Err(err) = ws.send(Message::Text(msg.to_string())).await {
                                warn!("gate futures private order stream ping failed for venue={} err={err}", self.venue);
                                break;
                            }
                        }
                        msg = ws.next() => {
                            let msg = match msg {
                                Some(Ok(msg)) => msg,
                                Some(Err(err)) => {
                                    warn!("gate futures private order stream error for venue={} err={err}", self.venue);
                                    break;
                                }
                                None => break,
                            };
                            let Message::Text(text) = msg else {
                                continue;
                            };
                            for update in parse_futures_order_update(&text, &self.venue) {
                                order_manager.handle_exchange_update(update).await;
                            }
                        }
                    }
                }

                warn!(
                    "gate futures private order stream disconnected for venue={}, reconnecting in {:?}",
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
struct WsEnvelope {
    #[serde(default)]
    channel: Option<String>,
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    error: Option<serde_json::Value>,
    #[serde(default)]
    result: serde_json::Value,
}

/// 解析一条 `futures.orders` 推送。订阅确认里带 `error` 时(如鉴权失败)打
/// warn，否则只处理 `event == "update"`。合约订单推送没有手续费字段，fee 留空。
fn parse_futures_order_update(text: &str, venue: &Venue) -> Vec<ExchangeOrderUpdate> {
    let Ok(envelope) = serde_json::from_str::<WsEnvelope>(text) else {
        return Vec::new();
    };
    if envelope.channel.as_deref() != Some("futures.orders") {
        return Vec::new();
    }
    if let Some(error) = envelope.error.filter(|e| !e.is_null()) {
        warn!("gate futures.orders error for venue={venue}: {error}");
        return Vec::new();
    }
    if envelope.event.as_deref() != Some("update") {
        return Vec::new();
    }
    let orders: Vec<FuturesOrder> = match serde_json::from_value(envelope.result) {
        Ok(orders) => orders,
        Err(err) => {
            warn!("failed to parse gate futures.orders message: {err}");
            return Vec::new();
        }
    };

    let ts_ms = now_ms();
    orders
        .into_iter()
        .map(|order| {
            let symbol = order
                .contract
                .as_deref()
                .and_then(|c| c.split_once('_'))
                .map(|(base, quote)| Symbol::new(base, quote));
            ExchangeOrderUpdate {
                venue: venue.clone(),
                symbol,
                client_order_id: order.client_order_id(),
                exchange_order_id: Some(order.id_string()),
                status: order.status(),
                filled_qty: order.filled_qty(),
                avg_price: order.avg_price(),
                fee: None,
                fee_asset: None,
                ts_ms,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_market_order_body_with_signed_size() {
        let symbol = Symbol::new("PEPE", "USDT");
        let buy = build_market_order_body(&symbol, OrderSide::Buy, Decimal::from(3), Some("cf-1-abcde")).unwrap();
        assert_eq!(buy["contract"], "PEPE_USDT");
        assert_eq!(buy["size"], 3);
        assert_eq!(buy["price"], "0");
        assert_eq!(buy["tif"], "ioc");
        assert_eq!(buy["text"], "t-cf-1-abcde");

        let sell = build_market_order_body(&symbol, OrderSide::Sell, Decimal::from(3), None).unwrap();
        assert_eq!(sell["size"], -3);
        assert!(sell.get("text").is_none());
    }

    #[test]
    fn market_order_body_rejects_fractional_contracts() {
        let err = build_market_order_body(&Symbol::new("PEPE", "USDT"), OrderSide::Buy, "1.5".parse().unwrap(), None)
            .unwrap_err();
        assert!(err.to_string().contains("whole contracts"));
    }

    #[test]
    fn gate_client_order_id_truncates_from_front() {
        let long = "x".repeat(10) + &"y".repeat(28);
        assert_eq!(gate_client_order_id(Some(&long)).unwrap(), format!("t-{}", "y".repeat(28)));
        assert_eq!(gate_client_order_id(Some("")), None);
    }

    #[test]
    fn parses_filled_order_response() {
        let text = r#"{"id":15675394,"contract":"PEPE_USDT","size":-3,"left":0,"fill_price":"0.000004036","status":"finished","finish_as":"filled","text":"t-cf-1-abcde"}"#;
        let result = parse_order_response(text).unwrap();
        assert_eq!(result.order_id, "15675394");
        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_qty, Decimal::from(3));
        assert_eq!(result.avg_price, Some("0.000004036".parse().unwrap()));
        assert_eq!(result.fee, None);
    }

    #[test]
    fn parses_partially_filled_ioc_as_expired() {
        let text = r#"{"id":1,"contract":"PEPE_USDT","size":10,"left":4,"fill_price":"0.000004","status":"finished","finish_as":"ioc"}"#;
        let result = parse_order_response(text).unwrap();
        assert_eq!(result.status, OrderStatus::Expired);
        assert_eq!(result.filled_qty, Decimal::from(6));
    }

    #[test]
    fn parse_order_response_surfaces_error() {
        let err = parse_order_response(r#"{"label":"INSUFFICIENT_AVAILABLE","message":"balance not enough"}"#).unwrap_err();
        assert!(err.to_string().contains("INSUFFICIENT_AVAILABLE"));
    }

    #[test]
    fn parses_position_size() {
        assert_eq!(parse_position(r#"{"contract":"PEPE_USDT","size":-12,"mode":"single"}"#).unwrap(), Decimal::from(-12));
        assert_eq!(parse_position(r#"{"contract":"PEPE_USDT","size":0}"#).unwrap(), Decimal::ZERO);
        assert!(parse_position(r#"{"label":"POSITION_DUAL_MODE","message":"dual mode"}"#).is_err());
    }

    #[test]
    fn parses_account_user_id() {
        assert_eq!(parse_account_user_id(r#"{"user":1666,"currency":"USDT","total":"10"}"#).unwrap(), "1666");
        assert!(parse_account_user_id(r#"{"label":"INVALID_KEY","message":"bad key"}"#).is_err());
    }

    #[test]
    fn parses_futures_orders_update_stripping_text_prefix() {
        let venue = Venue::new("gate_futures");
        let text = r#"{
            "time": 1541505434,
            "channel": "futures.orders",
            "event": "update",
            "result": [
                {"id": 3369218, "contract": "PEPE_USDT", "size": 5, "left": 0, "fill_price": 0.000004036,
                 "status": "finished", "finish_as": "filled", "text": "t-cf-1700000000000-abcde", "user": "1666"},
                {"id": 3369219, "contract": "PEPE_USDT", "size": -5, "left": -2, "fill_price": 0.000004,
                 "status": "open", "finish_as": "_new", "text": "web"}
            ]
        }"#;
        let updates = parse_futures_order_update(text, &venue);
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].client_order_id.as_deref(), Some("cf-1700000000000-abcde"));
        assert_eq!(updates[0].exchange_order_id.as_deref(), Some("3369218"));
        assert_eq!(updates[0].status, OrderStatus::Filled);
        assert_eq!(updates[0].filled_qty, Decimal::from(5));
        assert_eq!(updates[0].symbol, Some(Symbol::new("PEPE", "USDT")));
        assert_eq!(updates[1].client_order_id, None);
        assert_eq!(updates[1].status, OrderStatus::PartiallyFilled);
        assert_eq!(updates[1].filled_qty, Decimal::from(3));
    }

    #[test]
    fn ignores_subscribe_ack_pong_and_other_channels() {
        let venue = Venue::new("gate_futures");
        assert!(parse_futures_order_update(
            r#"{"time":1,"channel":"futures.orders","event":"subscribe","error":null,"result":{"status":"success"}}"#,
            &venue
        )
        .is_empty());
        assert!(parse_futures_order_update(
            r#"{"time":1,"channel":"futures.orders","event":"subscribe","error":{"code":2,"message":"invalid key"},"result":null}"#,
            &venue
        )
        .is_empty());
        assert!(parse_futures_order_update(r#"{"time":1,"channel":"futures.pong","event":"","result":null}"#, &venue).is_empty());
        assert!(parse_futures_order_update("not json", &venue).is_empty());
    }

    #[test]
    fn subscribe_message_uses_user_id_and_all_contracts() {
        let stream = GateFuturesPrivateOrderStream::new(Venue::new("gate_futures"), "k".into(), "s".into(), None).unwrap();
        let msg = stream.subscribe_message("1666");
        assert_eq!(msg["channel"], "futures.orders");
        assert_eq!(msg["payload"], serde_json::json!(["1666", "!all"]));
        assert_eq!(msg["auth"]["method"], "api_key");
    }
}
