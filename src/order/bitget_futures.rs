use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::exchange_info::bitget::{BITGET_USDT_FUTURES, bitget_symbol, opt_decimal, parse_bitget_data};
use crate::market_data::now_ms;
use crate::net::connect_tcp;
use crate::order_manager::OrderManager;
use crate::order_manager::stream::{ExchangeOrderUpdate, OrderStreamSource};
use crate::types::{Symbol, Venue};

use super::OrderProvider;
use super::types::{MarginMode, MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const HOST: &str = "https://api.bitget.com";
const WS_HOST: &str = "ws.bitget.com";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://ws.bitget.com/v2/ws/private";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Bitget 要求客户端每 30 秒发一次文本 `ping`(服务端回 `pong`)，2 分钟收不到就断开。
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);
const MARGIN_COIN: &str = "USDT";

/// Bitget USDT 本位永续下单客户端(v2 `/api/v2/mix/*`，`productType=USDT-FUTURES`)。
/// 凭证读 `BITGET_API_KEY` / `BITGET_API_SECRET` / `BITGET_API_PASSPHRASE`。
///
/// 下单量单位是 base 币(合约名里的 base，含乘数前缀)。要求账户是单向持仓
/// 模式(不传 `tradeSide`)；保证金模式 `marginMode` 每笔必填，由构造参数
/// 给出，必须和用户在交易所设置的一致。杠杆、持仓模式由用户预先设置，这里不修改。
pub struct BitgetFuturesOrderProvider {
    venue: Venue,
    credentials: BitgetCredentials,
    margin_mode: MarginMode,
    http: reqwest::Client,
}

#[derive(Clone)]
struct BitgetCredentials {
    api_key: String,
    api_secret: String,
    passphrase: String,
}

impl BitgetCredentials {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            api_key: std::env::var("BITGET_API_KEY").context("BITGET_API_KEY not set")?,
            api_secret: std::env::var("BITGET_API_SECRET").context("BITGET_API_SECRET not set")?,
            passphrase: std::env::var("BITGET_API_PASSPHRASE").context("BITGET_API_PASSPHRASE not set")?,
        })
    }
}

impl BitgetFuturesOrderProvider {
    pub fn new(
        venue: Venue,
        api_key: String,
        api_secret: String,
        passphrase: String,
        margin_mode: MarginMode,
        proxy: Option<&str>,
    ) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self { venue, credentials: BitgetCredentials { api_key, api_secret, passphrase }, margin_mode, http })
    }

    pub fn from_env(venue: Venue, margin_mode: MarginMode, proxy: Option<&str>) -> anyhow::Result<Self> {
        let c = BitgetCredentials::from_env()?;
        Self::new(venue, c.api_key, c.api_secret, c.passphrase, margin_mode, proxy)
    }

    /// `query` 直接拼进 URL 参与签名，所以调用方传入的值不能含需要 URL 编码的
    /// 字符(symbol / orderId 都满足)。
    async fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<String> {
        let request_path = path_with_query(path, query);
        let body_str = body.as_ref().map(|b| b.to_string()).unwrap_or_default();
        let timestamp = now_ms().to_string();
        let signature = bitget_sign(
            &self.credentials.api_secret,
            &format!("{timestamp}{}{request_path}{body_str}", method.as_str()),
        );

        crate::ratelimit::throttle(HOST).await;
        let mut request = self
            .http
            .request(method, format!("{HOST}{request_path}"))
            .header("ACCESS-KEY", &self.credentials.api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", &timestamp)
            .header("ACCESS-PASSPHRASE", &self.credentials.passphrase)
            .header("Content-Type", "application/json")
            .header("locale", "en-US");
        if body.is_some() {
            request = request.body(body_str);
        }
        let resp = request.send().await.context("bitget request failed")?;
        resp.text().await.context("failed to read bitget response body")
    }
}

#[async_trait]
impl OrderProvider for BitgetFuturesOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `POST /api/v2/mix/order/place-order`，`orderType=market`。下单接口只返回
    /// 订单号，成交走私有订单流。
    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let OrderAmount::Base(quantity) = req.amount else {
            anyhow::bail!("{} does not support quote-amount market orders", self.venue());
        };
        let body =
            build_market_order_body(&req.symbol, req.side, quantity, self.margin_mode, req.client_order_id.as_deref())?;
        let text = self.signed_request(reqwest::Method::POST, "/api/v2/mix/order/place-order", &[], Some(body)).await?;
        parse_place_order_response(&text)
    }

    /// `GET /api/v2/mix/order/detail?symbol=&productType=&orderId=`。
    async fn query_order(&self, symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<OrderResult> {
        let symbol = bitget_symbol(symbol);
        let query = [("symbol", symbol.as_str()), ("productType", BITGET_USDT_FUTURES), ("orderId", exchange_order_id)];
        let text = self.signed_request(reqwest::Method::GET, "/api/v2/mix/order/detail", &query, None).await?;
        parse_order_detail_response(&text)
    }

    /// `GET /api/v2/mix/position/single-position`：没有持仓时返回空数组；按
    /// `holdSide`(long/short)定方向后求和。单位是 base 币。
    async fn perp_position(&self, symbol: &Symbol) -> anyhow::Result<Decimal> {
        let symbol = bitget_symbol(symbol);
        let query = [("symbol", symbol.as_str()), ("productType", BITGET_USDT_FUTURES), ("marginCoin", MARGIN_COIN)];
        let text = self.signed_request(reqwest::Method::GET, "/api/v2/mix/position/single-position", &query, None).await?;
        parse_positions(&text)
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build bitget http client")
}

fn path_with_query(path: &str, query: &[(&str, &str)]) -> String {
    if query.is_empty() {
        return path.to_string();
    }
    let qs: Vec<String> = query.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{path}?{}", qs.join("&"))
}

/// base64(HMAC-SHA256(secret, prehash))，REST 和 WS 登录共用。
fn bitget_sign(secret: &str, prehash: &str) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    base64_engine.encode(ring::hmac::sign(&key, prehash.as_bytes()).as_ref())
}

fn margin_mode_param(margin_mode: MarginMode) -> &'static str {
    match margin_mode {
        MarginMode::Cross => "crossed",
        MarginMode::Isolated => "isolated",
    }
}

fn build_market_order_body(
    symbol: &Symbol,
    side: OrderSide,
    quantity: Decimal,
    margin_mode: MarginMode,
    client_order_id: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    if quantity <= Decimal::ZERO {
        anyhow::bail!("bitget order size must be positive, got {quantity}");
    }
    let mut body = serde_json::json!({
        "symbol": bitget_symbol(symbol),
        "productType": BITGET_USDT_FUTURES,
        "marginMode": margin_mode_param(margin_mode),
        "marginCoin": MARGIN_COIN,
        "size": quantity.normalize().to_string(),
        "side": match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        },
        "orderType": "market",
    });
    if let Some(id) = client_order_id.filter(|id| !id.is_empty()) {
        body["clientOid"] = serde_json::Value::String(id.to_string());
    }
    Ok(body)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderData {
    order_id: String,
}

fn parse_place_order_response(text: &str) -> anyhow::Result<OrderResult> {
    let data: PlaceOrderData = parse_bitget_data(text, "place order")?;
    if data.order_id.is_empty() {
        anyhow::bail!("bitget place order response has no orderId, raw body: {text}");
    }
    Ok(OrderResult {
        order_id: data.order_id,
        status: OrderStatus::New,
        filled_qty: Decimal::ZERO,
        avg_price: None,
        fee: None,
        fee_asset: None,
    })
}

/// v2 订单状态：`live`/`new`(挂单中)、`partially_filled`、`filled`、
/// `canceled`。市价单剩余部分被撤按 Expired，成交量如实保留。
fn map_state(state: &str, filled: Decimal) -> OrderStatus {
    match state {
        "filled" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Expired,
        "partially_filled" => OrderStatus::PartiallyFilled,
        _ if filled > Decimal::ZERO => OrderStatus::PartiallyFilled,
        _ => OrderStatus::New,
    }
}

/// REST 查单字段(和 WS 推送的字段名不同：成交量是 `baseVolume`、状态是 `state`)。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderDetail {
    #[serde(default)]
    order_id: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    base_volume: String,
    #[serde(default)]
    price_avg: String,
}

fn parse_order_detail_response(text: &str) -> anyhow::Result<OrderResult> {
    let order: OrderDetail = parse_bitget_data(text, "order detail")?;
    let filled_qty = opt_decimal(&order.base_volume).unwrap_or(Decimal::ZERO);
    Ok(OrderResult {
        order_id: order.order_id,
        status: map_state(&order.state, filled_qty),
        filled_qty,
        avg_price: opt_decimal(&order.price_avg).filter(|p| *p > Decimal::ZERO),
        fee: None,
        fee_asset: None,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionEntry {
    #[serde(default)]
    hold_side: String,
    #[serde(default)]
    total: String,
}

fn parse_positions(text: &str) -> anyhow::Result<Decimal> {
    let positions: Vec<PositionEntry> = parse_bitget_data(text, "position")?;
    Ok(positions
        .iter()
        .map(|p| {
            let total = opt_decimal(&p.total).unwrap_or(Decimal::ZERO).abs();
            if p.hold_side == "short" { -total } else { total }
        })
        .sum())
}

/// Bitget USDT 本位永续私有订单流：连上 `/v2/ws/private` 后先 `login`，成功后
/// 订阅 `orders`(`instId=default`，全部合约)，收到订阅确认才算 ready。推送
/// 里的数量单位是 base 币，和下单单位一致。
pub struct BitgetFuturesPrivateOrderStream {
    venue: Venue,
    credentials: BitgetCredentials,
    proxy: Option<String>,
}

impl BitgetFuturesPrivateOrderStream {
    pub fn new(venue: Venue, api_key: String, api_secret: String, passphrase: String, proxy: Option<&str>) -> Self {
        Self {
            venue,
            credentials: BitgetCredentials { api_key, api_secret, passphrase },
            proxy: proxy.map(str::to_string),
        }
    }

    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let c = BitgetCredentials::from_env()?;
        Ok(Self::new(venue, c.api_key, c.api_secret, c.passphrase, proxy))
    }

    fn login_message(&self, time_secs: u64) -> serde_json::Value {
        let timestamp = time_secs.to_string();
        let sign = bitget_sign(&self.credentials.api_secret, &format!("{timestamp}GET/user/verify"));
        serde_json::json!({
            "op": "login",
            "args": [{
                "apiKey": self.credentials.api_key,
                "passphrase": self.credentials.passphrase,
                "timestamp": timestamp,
                "sign": sign,
            }],
        })
    }

    fn subscribe_message() -> serde_json::Value {
        serde_json::json!({
            "op": "subscribe",
            "args": [{"instType": BITGET_USDT_FUTURES, "channel": "orders", "instId": "default"}],
        })
    }

    async fn connect(&self) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(WS_HOST, WS_PORT, self.proxy.as_deref()).await?;
        let (ws, _) = tokio_tungstenite::client_async_tls(WS_URL, tcp)
            .await
            .context("bitget private order stream handshake failed")?;
        Ok(ws)
    }
}

impl OrderStreamSource for BitgetFuturesPrivateOrderStream {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, order_manager: Arc<OrderManager>) -> crate::order_manager::stream::StreamHandle {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let mut backoff = MIN_BACKOFF;
            let mut ready_tx = Some(ready_tx);

            loop {
                let mut ws = match self.connect().await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "bitget private order stream connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(self.login_message(now_ms() / 1000).to_string())).await {
                    warn!(
                        "bitget private order stream login send failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                let mut ping = tokio::time::interval(WS_PING_INTERVAL);
                ping.tick().await;
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = ws.send(Message::Text("ping".to_string())).await {
                                warn!("bitget private order stream ping failed for venue={} err={err}", self.venue);
                                break;
                            }
                        }
                        msg = ws.next() => {
                            let msg = match msg {
                                Some(Ok(msg)) => msg,
                                Some(Err(err)) => {
                                    warn!("bitget private order stream error for venue={} err={err}", self.venue);
                                    break;
                                }
                                None => break,
                            };
                            let Message::Text(text) = msg else {
                                continue;
                            };
                            match parse_private_message(&text, &self.venue) {
                                PrivateMessage::LoggedIn => {
                                    if let Err(err) = ws.send(Message::Text(Self::subscribe_message().to_string())).await {
                                        warn!("bitget private order stream subscribe failed for venue={} err={err}", self.venue);
                                        break;
                                    }
                                }
                                PrivateMessage::Subscribed => {
                                    debug!("bitget private order stream subscribed for venue={}", self.venue);
                                    backoff = MIN_BACKOFF;
                                    if let Some(ready_tx) = ready_tx.take() {
                                        let _ = ready_tx.send(());
                                    }
                                }
                                PrivateMessage::Error(err) => {
                                    warn!("bitget private order stream error event for venue={}: {err}", self.venue);
                                    break;
                                }
                                PrivateMessage::Orders(updates) => {
                                    for update in updates {
                                        order_manager.handle_exchange_update(update).await;
                                    }
                                }
                                PrivateMessage::Other => {}
                            }
                        }
                    }
                }

                warn!(
                    "bitget private order stream disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        });
        crate::order_manager::stream::StreamHandle { join, ready: ready_rx }
    }
}

#[derive(Debug)]
enum PrivateMessage {
    LoggedIn,
    Subscribed,
    /// 登录失败、订阅失败等，断开重连。
    Error(String),
    Orders(Vec<ExchangeOrderUpdate>),
    Other,
}

#[derive(Debug, Deserialize)]
struct WsArg {
    #[serde(default)]
    channel: String,
}

/// WS 推送里的订单字段。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsOrder {
    #[serde(default)]
    inst_id: String,
    #[serde(default)]
    order_id: String,
    #[serde(default)]
    client_oid: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    acc_base_volume: String,
    #[serde(default)]
    price_avg: String,
}

#[derive(Debug, Deserialize)]
struct WsMessage {
    #[serde(default)]
    event: Option<String>,
    /// 登录/错误响应里的 `code` 文档是数字，实际也见过字符串，两种都接受。
    #[serde(default)]
    code: Option<serde_json::Value>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    arg: Option<WsArg>,
    #[serde(default)]
    data: Option<Vec<WsOrder>>,
}

fn code_is_zero(code: &Option<serde_json::Value>) -> bool {
    match code {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Number(n)) => n.as_i64() == Some(0),
        Some(serde_json::Value::String(s)) => s == "0",
        Some(_) => false,
    }
}

/// `BTCUSDT` → `BTC/USDT`。
fn parse_inst_id(inst_id: &str) -> Option<Symbol> {
    let base = inst_id.strip_suffix("USDT")?;
    (!base.is_empty()).then(|| Symbol::new(base, "USDT"))
}

fn parse_private_message(text: &str, venue: &Venue) -> PrivateMessage {
    let Ok(msg) = serde_json::from_str::<WsMessage>(text) else {
        return PrivateMessage::Other;
    };
    match msg.event.as_deref() {
        Some("login") if code_is_zero(&msg.code) => return PrivateMessage::LoggedIn,
        Some("subscribe") => return PrivateMessage::Subscribed,
        Some("login") | Some("error") => {
            let code = msg.code.map(|c| c.to_string()).unwrap_or_default();
            return PrivateMessage::Error(format!("code={code} msg={}", msg.msg.unwrap_or_default()));
        }
        Some(_) => return PrivateMessage::Other,
        None => {}
    }
    if msg.arg.map(|a| a.channel).as_deref() != Some("orders") {
        return PrivateMessage::Other;
    }
    let ts_ms = now_ms();
    let updates = msg
        .data
        .unwrap_or_default()
        .into_iter()
        .map(|order| {
            let filled_qty = opt_decimal(&order.acc_base_volume).unwrap_or(Decimal::ZERO);
            ExchangeOrderUpdate {
                venue: venue.clone(),
                symbol: parse_inst_id(&order.inst_id),
                client_order_id: Some(order.client_oid).filter(|id| !id.is_empty()),
                exchange_order_id: Some(order.order_id).filter(|id| !id.is_empty()),
                status: map_state(&order.status, filled_qty),
                filled_qty,
                avg_price: opt_decimal(&order.price_avg).filter(|p| *p > Decimal::ZERO),
                // 推送里的 `fillFee`/`feeDetail` 暂不解析，fee 留空交给估算兜底。
                fee: None,
                fee_asset: None,
                ts_ms,
            }
        })
        .collect();
    PrivateMessage::Orders(updates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_and_builds_paths() {
        assert_eq!(bitget_sign("secret", "msg"), "/k+cQY9oPwNPavkNHdW4asA1XdljMsWcx0WY0HNhB/Y=");
        assert_eq!(
            path_with_query("/api/v2/mix/order/detail", &[("symbol", "BTCUSDT"), ("orderId", "1")]),
            "/api/v2/mix/order/detail?symbol=BTCUSDT&orderId=1"
        );
    }

    #[test]
    fn builds_market_order_body() {
        let symbol = Symbol::new("1000BONK", "USDT");
        let body = build_market_order_body(&symbol, OrderSide::Buy, "1200.0".parse().unwrap(), MarginMode::Cross, Some("cf1"))
            .unwrap();
        assert_eq!(body["symbol"], "1000BONKUSDT");
        assert_eq!(body["productType"], "USDT-FUTURES");
        assert_eq!(body["marginMode"], "crossed");
        assert_eq!(body["marginCoin"], "USDT");
        assert_eq!(body["size"], "1200");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["orderType"], "market");
        assert_eq!(body["clientOid"], "cf1");

        let body = build_market_order_body(&symbol, OrderSide::Sell, Decimal::ONE, MarginMode::Isolated, None).unwrap();
        assert_eq!(body["marginMode"], "isolated");
        assert_eq!(body["side"], "sell");
        assert!(body.get("clientOid").is_none());
        assert!(build_market_order_body(&symbol, OrderSide::Sell, Decimal::ZERO, MarginMode::Cross, None).is_err());
    }

    #[test]
    fn parses_place_order_response() {
        let ok = r#"{"code":"00000","msg":"success","requestTime":1,"data":{"clientOid":"cf1","orderId":"121211212122"}}"#;
        let result = parse_place_order_response(ok).unwrap();
        assert_eq!(result.order_id, "121211212122");
        assert_eq!(result.status, OrderStatus::New);

        let err = parse_place_order_response(r#"{"code":"40762","msg":"The order amount exceeds the balance","requestTime":1,"data":null}"#)
            .unwrap_err();
        assert!(err.to_string().contains("40762"));
    }

    #[test]
    fn parses_order_detail() {
        let text = r#"{"code":"00000","msg":"success","requestTime":1,"data":{"symbol":"BTCUSDT","size":"0.01","orderId":"1","clientOid":"cf1","baseVolume":"0.01","priceAvg":"27000.5","state":"filled","side":"buy"}}"#;
        let result = parse_order_detail_response(text).unwrap();
        assert_eq!(result.order_id, "1");
        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_qty, "0.01".parse().unwrap());
        assert_eq!(result.avg_price, Some("27000.5".parse().unwrap()));

        let text = r#"{"code":"00000","msg":"success","requestTime":1,"data":{"orderId":"1","baseVolume":"0","priceAvg":"","state":"canceled"}}"#;
        let result = parse_order_detail_response(text).unwrap();
        assert_eq!(result.status, OrderStatus::Expired);
        assert_eq!(result.avg_price, None);
    }

    #[test]
    fn sums_positions_by_hold_side() {
        assert_eq!(parse_positions(r#"{"code":"00000","msg":"success","data":[]}"#).unwrap(), Decimal::ZERO);
        assert_eq!(
            parse_positions(r#"{"code":"00000","msg":"success","data":[{"symbol":"BTCUSDT","holdSide":"short","total":"0.02"}]}"#)
                .unwrap(),
            "-0.02".parse().unwrap()
        );
        assert_eq!(
            parse_positions(r#"{"code":"00000","msg":"success","data":[{"holdSide":"long","total":"3"},{"holdSide":"short","total":"1"}]}"#)
                .unwrap(),
            Decimal::from(2)
        );
        assert!(parse_positions(r#"{"code":"40037","msg":"Apikey does not exist","data":null}"#).is_err());
    }

    #[test]
    fn parses_private_ws_messages() {
        let venue = Venue::new("bitget_futures");
        assert!(matches!(parse_private_message(r#"{"event":"login","code":0,"msg":""}"#, &venue), PrivateMessage::LoggedIn));
        assert!(matches!(parse_private_message(r#"{"event":"login","code":"0","msg":""}"#, &venue), PrivateMessage::LoggedIn));
        assert!(matches!(
            parse_private_message(r#"{"event":"error","code":30005,"msg":"Logged in failed"}"#, &venue),
            PrivateMessage::Error(_)
        ));
        assert!(matches!(
            parse_private_message(r#"{"event":"subscribe","arg":{"instType":"USDT-FUTURES","channel":"orders","instId":"default"}}"#, &venue),
            PrivateMessage::Subscribed
        ));
        assert!(matches!(parse_private_message("pong", &venue), PrivateMessage::Other));

        let push = r#"{"action":"snapshot","arg":{"instType":"USDT-FUTURES","channel":"orders","instId":"default"},"data":[
            {"accBaseVolume":"0.01","clientOid":"cf170000000000012345","instId":"BTCUSDT","orderId":"13333333333333333333",
             "priceAvg":"27000.5","status":"filled","size":"0.01","side":"buy","orderType":"market"},
            {"accBaseVolume":"0","clientOid":"","instId":"1000BONKUSDT","orderId":"2","priceAvg":"","status":"live"}
        ],"ts":1695716059516}"#;
        let PrivateMessage::Orders(updates) = parse_private_message(push, &venue) else {
            panic!("expected orders");
        };
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].symbol, Some(Symbol::new("BTC", "USDT")));
        assert_eq!(updates[0].client_order_id.as_deref(), Some("cf170000000000012345"));
        assert_eq!(updates[0].exchange_order_id.as_deref(), Some("13333333333333333333"));
        assert_eq!(updates[0].status, OrderStatus::Filled);
        assert_eq!(updates[0].filled_qty, "0.01".parse().unwrap());
        assert_eq!(updates[0].avg_price, Some("27000.5".parse().unwrap()));
        assert_eq!(updates[1].symbol, Some(Symbol::new("1000BONK", "USDT")));
        assert_eq!(updates[1].client_order_id, None);
        assert_eq!(updates[1].status, OrderStatus::New);
    }

    #[test]
    fn login_message_has_signature() {
        let stream =
            BitgetFuturesPrivateOrderStream::new(Venue::new("bitget_futures"), "k".into(), "s".into(), "p".into(), None);
        let msg = stream.login_message(1_538_054_050);
        assert_eq!(msg["args"][0]["timestamp"], "1538054050");
        assert_eq!(msg["args"][0]["sign"], bitget_sign("s", "1538054050GET/user/verify"));
        assert_eq!(BitgetFuturesPrivateOrderStream::subscribe_message()["args"][0]["instId"], "default");
    }
}
