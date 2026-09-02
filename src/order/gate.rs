use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::market_data::now_ms;
use crate::net::connect_tcp;
use crate::order_manager::OrderManager;
use crate::order_manager::stream::{ExchangeOrderUpdate, OrderStreamSource};
use crate::types::{Symbol, Venue};

use super::OrderProvider;
use super::types::{LimitIocOrderRequest, LimitOrderRequest, MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const HOST: &str = "https://api.gateio.ws";
const API_PREFIX: &str = "/api/v4";
const WS_HOST: &str = "api.gateio.ws";
const WS_PORT: u16 = 443;
const WS_URL: &str = "wss://api.gateio.ws/ws/v4/";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Gate.io 下单(执行层)客户端：REST v4 签名下单/查询，行情查询
/// (`quote_usdt_price`)公开接口不需要签名。凭证读 `GATE_API_KEY`/
/// `GATE_API_SECRET`(仿照 Binance 的简单命名，Gate 现货是唯一场景，不需要像
/// Kraken 那样区分 spot/futures)。
///
/// 重要限制：Gate 市价单 `amount` 字段含义随 `side` 变化——`side=sell` 时
/// `amount` 是 base 数量，`side=buy` 时 `amount` 是 quote 数量(花多少 quote 去
/// 买)，和限价单永远用 base 数量不同。对应到 [`OrderAmount`]：只支持
/// `Sell+Base`/`Buy+Quote`，`Sell+Quote`/`Buy+Base` 直接报错，这是 Gate 接口
/// 本身的限制，不是本实现遗漏。市价单 `time_in_force` 只支持 `ioc`/`fok`。
pub struct GateOrderProvider {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
}

impl GateOrderProvider {
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
        query_params: &[(String, String)],
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<String> {
        let query_string = query_params.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        let body_str = body.as_ref().map(|b| b.to_string()).unwrap_or_default();
        let timestamp = now_secs().to_string();
        let full_path = format!("{API_PREFIX}{path}");
        let signature = gate_sign(&self.api_secret, method.as_str(), &full_path, &query_string, &body_str, &timestamp);

        let url = if query_string.is_empty() {
            format!("{HOST}{full_path}")
        } else {
            format!("{HOST}{full_path}?{query_string}")
        };

        crate::ratelimit::throttle(HOST).await;
        let mut request = self
            .http
            .request(method, &url)
            .header("KEY", &self.api_key)
            .header("Timestamp", &timestamp)
            .header("SIGN", &signature);
        if let Some(body) = &body {
            request = request.header("Content-Type", "application/json").body(body.to_string());
        }
        let resp = request.send().await.context("gate order request failed")?;
        resp.text().await.context("failed to read gate order response body")
    }
}

#[async_trait]
impl OrderProvider for GateOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let body = build_market_order_body(&req.symbol, req.side, req.amount, req.client_order_id.as_deref())?;
        let text = self.signed_request(reqwest::Method::POST, "/spot/orders", &[], Some(body)).await?;
        parse_order_response(&text)
    }

    async fn place_limit_ioc_order_raw(&self, req: &LimitIocOrderRequest) -> anyhow::Result<OrderResult> {
        let body = build_limit_ioc_order_body(&req.symbol, req.side, req.quantity, req.price, req.client_order_id.as_deref());
        let text = self.signed_request(reqwest::Method::POST, "/spot/orders", &[], Some(body)).await?;
        parse_order_response(&text)
    }

    async fn place_limit_order_raw(&self, req: &LimitOrderRequest) -> anyhow::Result<OrderResult> {
        let body = build_limit_order_body(&req.symbol, req.side, req.quantity, req.price, req.client_order_id.as_deref());
        let text = self.signed_request(reqwest::Method::POST, "/spot/orders", &[], Some(body)).await?;
        parse_order_response(&text)
    }

    /// `DELETE /spot/orders/{id}?currency_pair=...&account=spot`，撤单指令
    /// 是否被接受由响应体是否能解析出 `GateErrorResponse` 判断，真正的终态
    /// 仍然只信任 `spot.orders` 私有 WS 推送。
    async fn cancel_order(&self, symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<()> {
        let query = vec![
            ("currency_pair".to_string(), gate_symbol(symbol)),
            ("account".to_string(), "spot".to_string()),
        ];
        let path = format!("/spot/orders/{exchange_order_id}");
        let text = self.signed_request(reqwest::Method::DELETE, &path, &query, None).await?;
        if let Ok(err) = serde_json::from_str::<GateErrorResponse>(&text) {
            anyhow::bail!("gate error {}: {}", err.label, err.message);
        }
        Ok(())
    }

    /// `GET /spot/orders/{id}?currency_pair=...&account=spot`。
    async fn query_order(&self, symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<OrderResult> {
        let query = vec![
            ("currency_pair".to_string(), gate_symbol(symbol)),
            ("account".to_string(), "spot".to_string()),
        ];
        let path = format!("/spot/orders/{exchange_order_id}");
        let text = self.signed_request(reqwest::Method::GET, &path, &query, None).await?;
        parse_order_response(&text)
    }

    /// `GET /spot/tickers?currency_pair={ASSET}_USDT`，公开行情接口不需要签名。
    async fn quote_usdt_price(&self, asset: &str) -> anyhow::Result<Decimal> {
        let pair = format!("{}_USDT", asset.to_ascii_uppercase());
        let text = gate_public_get(&self.http, "/spot/tickers", &[("currency_pair".to_string(), pair)]).await?;
        parse_ticker_price(&text)
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build gate http client")
}

async fn gate_public_get(http: &reqwest::Client, path: &str, query_params: &[(String, String)]) -> anyhow::Result<String> {
    let query_string = query_params.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let url = if query_string.is_empty() {
        format!("{HOST}{API_PREFIX}{path}")
    } else {
        format!("{HOST}{API_PREFIX}{path}?{query_string}")
    };
    crate::ratelimit::throttle(HOST).await;
    let resp = http.get(&url).send().await.context("gate public request failed")?;
    resp.text().await.context("failed to read gate public response body")
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Gate REST v4 签名：
/// `payload = Method + "\n" + URL(含 /api/v4 前缀，不含 query) + "\n" + QueryString + "\n" + HexEncode(SHA512(body)) + "\n" + Timestamp`
/// `SIGN = HexEncode(HMAC_SHA512(secret, payload))`。`secret` 是普通 UTF-8
/// 字符串(不像 Kraken 那样是 base64)，直接当 HMAC key 用。
fn gate_sign(secret: &str, method: &str, url_path: &str, query_string: &str, body: &str, timestamp: &str) -> String {
    let body_hash = hex_encode(ring::digest::digest(&ring::digest::SHA512, body.as_bytes()).as_ref());
    let payload = format!("{method}\n{url_path}\n{query_string}\n{body_hash}\n{timestamp}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA512, secret.as_bytes());
    let signature = ring::hmac::sign(&key, payload.as_bytes());
    hex_encode(signature.as_ref())
}

fn gate_symbol(symbol: &Symbol) -> String {
    format!("{}_{}", symbol.base, symbol.quote).to_ascii_uppercase()
}

fn map_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

/// Gate `text` 自定义单号规则：必须以 `t-` 开头，前缀之后不超过 28 字节，只能
/// 含 `0-9A-Za-z_-.`。内部生成的 client_order_id(见 `Strategy::generate_client_order_id`)
/// 形如 `{strategy_name}-{ms}-{rand}`，字符集本身已经合法，只需要处理超长
/// 截断——保留末尾的时间戳+随机数后缀(比开头的策略名更能保证短时间内不
/// 撞车)，超过 28 字节从前面截掉。截断后的值和 `OrderManager` 里存的原始
/// client_order_id 不再逐字相等，靠 `exchange_order_id` 兜底关联(见
/// `order_manager::stream::ExchangeOrderUpdate` 字段注释)，不影响正确性。
fn gate_client_order_id(client_order_id: Option<&str>) -> Option<String> {
    let id = client_order_id?;
    if id.is_empty() {
        return None;
    }
    let chars: Vec<char> = id.chars().collect();
    let truncated: String = if chars.len() > 28 { chars[chars.len() - 28..].iter().collect() } else { id.to_string() };
    Some(format!("t-{truncated}"))
}

fn build_market_order_body(
    symbol: &Symbol,
    side: OrderSide,
    amount: OrderAmount,
    client_order_id: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    let amount_value = match (side, amount) {
        (OrderSide::Sell, OrderAmount::Base(qty)) => qty,
        (OrderSide::Buy, OrderAmount::Quote(quote_amount)) => quote_amount,
        (OrderSide::Sell, OrderAmount::Quote(_)) => {
            anyhow::bail!("gate market sell orders only support base-amount, not quote-amount");
        }
        (OrderSide::Buy, OrderAmount::Base(_)) => {
            anyhow::bail!("gate market buy orders only support quote-amount, not base-amount");
        }
    };
    let mut body = serde_json::json!({
        "currency_pair": gate_symbol(symbol),
        "type": "market",
        "side": map_side(side),
        "amount": amount_value.to_string(),
        "time_in_force": "ioc",
    });
    if let Some(text) = gate_client_order_id(client_order_id) {
        body["text"] = serde_json::Value::String(text);
    }
    Ok(body)
}

fn build_limit_ioc_order_body(
    symbol: &Symbol,
    side: OrderSide,
    quantity: Decimal,
    price: Decimal,
    client_order_id: Option<&str>,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "currency_pair": gate_symbol(symbol),
        "type": "limit",
        "side": map_side(side),
        "amount": quantity.to_string(),
        "price": price.to_string(),
        "time_in_force": "ioc",
    });
    if let Some(text) = gate_client_order_id(client_order_id) {
        body["text"] = serde_json::Value::String(text);
    }
    body
}

fn build_limit_order_body(
    symbol: &Symbol,
    side: OrderSide,
    quantity: Decimal,
    price: Decimal,
    client_order_id: Option<&str>,
) -> serde_json::Value {
    let mut body = serde_json::json!({
        "currency_pair": gate_symbol(symbol),
        "type": "limit",
        "side": map_side(side),
        "amount": quantity.to_string(),
        "price": price.to_string(),
        "time_in_force": "gtc",
    });
    if let Some(text) = gate_client_order_id(client_order_id) {
        body["text"] = serde_json::Value::String(text);
    }
    body
}

#[derive(Debug, Deserialize)]
struct GateErrorResponse {
    label: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct GateOrderResponse {
    id: String,
    status: String,
    #[serde(default)]
    filled_total: Decimal,
    #[serde(default)]
    avg_deal_price: Decimal,
    #[serde(default)]
    fee: Option<Decimal>,
    #[serde(default)]
    fee_currency: Option<String>,
}

/// Gate 现货订单状态只有 open/closed/cancelled 三种(不像 Binance 有专门的
/// PARTIALLY_FILLED 状态)，`open` 时用 `filled_total` 是否 >0 区分
/// New/PartiallyFilled；`cancelled` 统一映射到 Expired(不管是否有部分成交)，
/// 和 Binance EXPIRED/CANCELED、Kraken canceled/expired 的既有约定一致——
/// 终态是"被取消"比"是否有部分成交"更重要，成交量本身已经在 filled_qty 里
/// 如实保留，不会因为映射到 Expired 就被抹掉。
fn map_order_status(status: &str, has_fill: bool) -> OrderStatus {
    match status {
        "closed" => OrderStatus::Filled,
        "cancelled" => OrderStatus::Expired,
        "open" => {
            if has_fill {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::New
            }
        }
        _ => OrderStatus::New,
    }
}

/// 响应里没有单独的"成交基础币数量"字段，用 `filled_total`(累计成交额,quote
/// 计价) / `avg_deal_price`(累计均价) 反推，和 Binance
/// `cummulativeQuoteQty / executedQty` 算均价的思路相反(这里是已知额和价反推
/// 量)。`avg_deal_price` 为 0 说明还没有任何成交，均价留 `None`。
fn parse_order_response(text: &str) -> anyhow::Result<OrderResult> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let resp: GateOrderResponse =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate order response, raw body: {text}"))?;

    let has_fill = resp.filled_total > Decimal::ZERO;
    let filled_qty = if resp.avg_deal_price > Decimal::ZERO {
        resp.filled_total / resp.avg_deal_price
    } else {
        Decimal::ZERO
    };
    let avg_price = (resp.avg_deal_price > Decimal::ZERO).then_some(resp.avg_deal_price);

    Ok(OrderResult {
        order_id: resp.id,
        status: map_order_status(&resp.status, has_fill),
        filled_qty,
        avg_price,
        fee: resp.fee,
        fee_asset: resp.fee_currency,
    })
}

#[derive(Debug, Deserialize)]
struct TickerEntry {
    last: Decimal,
}

fn parse_ticker_price(text: &str) -> anyhow::Result<Decimal> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let entries: Vec<TickerEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate tickers response, raw body: {text}"))?;
    entries
        .into_iter()
        .next()
        .map(|e| e.last)
        .ok_or_else(|| anyhow::anyhow!("gate tickers response has no entries"))
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

/// Gate.io 私有订单流客户端：连接公共 WebSocket v4 端点并订阅 `spot.orders`
/// channel，鉴权内嵌在订阅请求的 `auth` 字段里(不是单独的 login 消息)。
pub struct GatePrivateOrderStream {
    venue: Venue,
    api_key: String,
    api_secret: String,
    proxy: Option<String>,
    symbols: Vec<Symbol>,
}

impl GatePrivateOrderStream {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>, symbols: Vec<Symbol>) -> Self {
        Self { venue, api_key, api_secret, proxy: proxy.map(str::to_string), symbols }
    }

    pub fn from_env(venue: Venue, proxy: Option<&str>, symbols: Vec<Symbol>) -> anyhow::Result<Self> {
        let api_key = std::env::var("GATE_API_KEY").context("GATE_API_KEY not set")?;
        let api_secret = std::env::var("GATE_API_SECRET").context("GATE_API_SECRET not set")?;
        Ok(Self::new(venue, api_key, api_secret, proxy, symbols))
    }

    fn symbol_map(&self) -> HashMap<String, Symbol> {
        self.symbols.iter().map(|s| (gate_symbol(s), s.clone())).collect()
    }

    fn subscribe_message(&self) -> serde_json::Value {
        let pairs: Vec<String> = self.symbols.iter().map(gate_symbol).collect();
        let time_secs = now_secs();
        let auth = gate_ws_auth(&self.api_key, &self.api_secret, "spot.orders", "subscribe", time_secs);
        serde_json::json!({
            "time": time_secs,
            "channel": "spot.orders",
            "event": "subscribe",
            "payload": pairs,
            "auth": auth,
        })
    }

    async fn connect(&self) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(WS_HOST, WS_PORT, self.proxy.as_deref()).await?;
        let (ws, _) = tokio_tungstenite::client_async_tls(WS_URL, tcp)
            .await
            .context("gate private order stream handshake failed")?;
        Ok(ws)
    }
}

impl OrderStreamSource for GatePrivateOrderStream {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, order_manager: Arc<OrderManager>) -> crate::order_manager::stream::StreamHandle {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let symbol_map = self.symbol_map();
            let subscribe_msg = self.subscribe_message();
            let mut last_fee_by_order: HashMap<String, Decimal> = HashMap::new();
            let mut backoff = MIN_BACKOFF;
            let mut ready_tx = Some(ready_tx);

            loop {
                let mut ws = match self.connect().await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "gate private order stream connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(subscribe_msg.to_string())).await {
                    warn!(
                        "gate private order stream subscribe failed for venue={} err={err}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                debug!("gate private order stream connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;
                if let Some(ready_tx) = ready_tx.take() {
                    let _ = ready_tx.send(());
                }

                while let Some(msg) = ws.next().await {
                    let msg = match msg {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("gate private order stream error for venue={} err={err}", self.venue);
                            break;
                        }
                    };
                    let Message::Text(text) = msg else {
                        continue;
                    };
                    for update in parse_gate_order_update(&text, &self.venue, &symbol_map, &mut last_fee_by_order) {
                        order_manager.handle_exchange_update(update).await;
                    }
                }

                warn!(
                    "gate private order stream disconnected for venue={}, reconnecting in {:?}",
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
}

#[derive(Debug, Deserialize)]
struct OrdersUpdateEnvelope {
    #[serde(default)]
    result: Vec<GateOrderPush>,
}

#[derive(Debug, Deserialize)]
struct GateOrderPush {
    id: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    currency_pair: Option<String>,
    /// 单笔订单的事件类型：`put`(新单进入订单簿)/`update`(部分成交或其它更新)/
    /// `finish`(订单结束，具体原因看 `finish_as`)。和外层信封的 `event`
    /// (`subscribe`/`update`)是两个不同层级的字段，命名恰好相同容易混淆。
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    finish_as: Option<String>,
    #[serde(default)]
    filled_total: Decimal,
    #[serde(default)]
    avg_deal_price: Decimal,
    #[serde(default)]
    fee: Option<Decimal>,
    #[serde(default)]
    fee_currency: Option<String>,
}

/// 和 REST `map_order_status` 的状态集合不同，WS 推送按单笔订单的 `event`
/// (`put`/`update`/`finish`)+`finish_as` 判断，没有独立的 status 字段。
/// `finish_as="filled"` 才是真正全部成交；`cancelled`/`ioc`(IOC 未完全成交
/// 被撤销剩余部分)/其它终态原因统一按 Expired 处理，和 REST 侧的既有约定
/// 一致——终态是否被取消比是否有部分成交更重要，filled_qty 如实保留。
fn map_ws_status(event: &str, finish_as: Option<&str>, has_fill: bool) -> OrderStatus {
    match event {
        "finish" => match finish_as {
            Some("filled") => OrderStatus::Filled,
            Some(_) => OrderStatus::Expired,
            None => {
                if has_fill {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::Expired
                }
            }
        },
        "put" => OrderStatus::New,
        _ => {
            if has_fill {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::New
            }
        }
    }
}

/// 解析一条 `spot.orders` 推送，只关心 `channel=="spot.orders" &&
/// event=="update"`(订阅确认、`spot.pong` 等消息直接忽略)。一条消息可能携带
/// 多笔订单的更新，因此返回 `Vec`。
///
/// `fee` 字段和 `filled_total`/`left` 放在一起，字段命名规律推断大概率是
/// 订单级别累计值(而不是本次推送的增量)，但没有查到权威文档明确说明——用
/// `last_fee_by_order` 记录"上次看到的累计 fee"，每次推送用
/// `当前fee - 上次fee` 算出 `ExchangeOrderUpdate.fee` 要求的增量语义(见
/// `order_manager::stream` 的字段注释)，这个假设标注为未经真实接口核对的
/// best-effort，和 `exchange_info::kraken::kraken_perpetual_pair` 现有的
/// 注释同一风格。订单进入终态后从 map 里移除，避免长期运行下 map 无限增长。
fn parse_gate_order_update(
    text: &str,
    venue: &Venue,
    symbol_map: &HashMap<String, Symbol>,
    last_fee_by_order: &mut HashMap<String, Decimal>,
) -> Vec<ExchangeOrderUpdate> {
    let envelope: WsEnvelope = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(_) => return Vec::new(),
    };
    if envelope.channel.as_deref() != Some("spot.orders") || envelope.event.as_deref() != Some("update") {
        return Vec::new();
    }
    let full: OrdersUpdateEnvelope = match serde_json::from_str(text) {
        Ok(full) => full,
        Err(err) => {
            warn!("failed to parse gate spot.orders message: {err}");
            return Vec::new();
        }
    };

    let ts_ms = now_ms();
    full.result
        .into_iter()
        .map(|item| {
            let symbol = item.currency_pair.as_deref().and_then(|p| symbol_map.get(p)).cloned();
            let has_fill = item.filled_total > Decimal::ZERO;
            let status = map_ws_status(item.event.as_deref().unwrap_or(""), item.finish_as.as_deref(), has_fill);
            let filled_qty = if item.avg_deal_price > Decimal::ZERO {
                item.filled_total / item.avg_deal_price
            } else {
                Decimal::ZERO
            };
            let avg_price = (item.avg_deal_price > Decimal::ZERO).then_some(item.avg_deal_price);

            let incremental_fee = item.fee.map(|cumulative_fee| {
                let previous_fee = last_fee_by_order.get(&item.id).copied().unwrap_or(Decimal::ZERO);
                cumulative_fee - previous_fee
            });
            if matches!(status, OrderStatus::Filled | OrderStatus::Expired | OrderStatus::Rejected) {
                last_fee_by_order.remove(&item.id);
            } else if let Some(cumulative_fee) = item.fee {
                last_fee_by_order.insert(item.id.clone(), cumulative_fee);
            }

            ExchangeOrderUpdate {
                venue: venue.clone(),
                symbol,
                client_order_id: item.text.filter(|s| !s.is_empty()),
                exchange_order_id: Some(item.id),
                status,
                filled_qty,
                avg_price,
                fee: incremental_fee,
                fee_asset: item.fee_currency,
                ts_ms,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_sign_changes_with_any_field() {
        let secret = "test-secret";
        let base = gate_sign(secret, "POST", "/api/v4/spot/orders", "", "{}", "1700000000");

        assert_ne!(base, gate_sign(secret, "GET", "/api/v4/spot/orders", "", "{}", "1700000000"));
        assert_ne!(base, gate_sign(secret, "POST", "/api/v4/spot/orders/1", "", "{}", "1700000000"));
        assert_ne!(base, gate_sign(secret, "POST", "/api/v4/spot/orders", "a=b", "{}", "1700000000"));
        assert_ne!(base, gate_sign(secret, "POST", "/api/v4/spot/orders", "", "{\"a\":1}", "1700000000"));
        assert_ne!(base, gate_sign(secret, "POST", "/api/v4/spot/orders", "", "{}", "1700000001"));
        assert_ne!(base, gate_sign("other-secret", "POST", "/api/v4/spot/orders", "", "{}", "1700000000"));
    }

    #[test]
    fn maps_side_to_gate_string() {
        assert_eq!(map_side(OrderSide::Buy), "buy");
        assert_eq!(map_side(OrderSide::Sell), "sell");
    }

    #[test]
    fn builds_uppercase_underscore_pair_symbol() {
        assert_eq!(gate_symbol(&Symbol::new("BTC", "USDT")), "BTC_USDT");
        assert_eq!(gate_symbol(&Symbol::new("eth", "usdt")), "ETH_USDT");
    }

    #[test]
    fn gate_client_order_id_adds_prefix() {
        assert_eq!(gate_client_order_id(Some("cid-1")), Some("t-cid-1".to_string()));
    }

    #[test]
    fn gate_client_order_id_truncates_long_id_keeping_suffix() {
        let long_id = "cross_exchange_execution-1700000000000-12345";
        let result = gate_client_order_id(Some(long_id)).unwrap();
        assert!(result.starts_with("t-"));
        assert_eq!(result.len(), 2 + 28);
        assert!(long_id.ends_with(&result[2..]));
    }

    #[test]
    fn gate_client_order_id_none_when_missing_or_empty() {
        assert_eq!(gate_client_order_id(None), None);
        assert_eq!(gate_client_order_id(Some("")), None);
    }

    #[test]
    fn builds_market_order_body_sell_base() {
        let body = build_market_order_body(&Symbol::new("BTC", "USDT"), OrderSide::Sell, OrderAmount::Base("0.1".parse().unwrap()), Some("cid-1"))
            .expect("should build");
        assert_eq!(body["currency_pair"], "BTC_USDT");
        assert_eq!(body["type"], "market");
        assert_eq!(body["side"], "sell");
        assert_eq!(body["amount"], "0.1");
        assert_eq!(body["time_in_force"], "ioc");
        assert_eq!(body["text"], "t-cid-1");
        assert!(body.get("price").is_none());
    }

    #[test]
    fn builds_market_order_body_buy_quote() {
        let body = build_market_order_body(&Symbol::new("BTC", "USDT"), OrderSide::Buy, OrderAmount::Quote("100".parse().unwrap()), None)
            .expect("should build");
        assert_eq!(body["side"], "buy");
        assert_eq!(body["amount"], "100");
        assert!(body.get("text").is_none());
    }

    #[test]
    fn build_market_order_body_sell_quote_bails() {
        let err = build_market_order_body(&Symbol::new("BTC", "USDT"), OrderSide::Sell, OrderAmount::Quote("100".parse().unwrap()), None)
            .unwrap_err();
        assert!(err.to_string().contains("sell orders only support base-amount"));
    }

    #[test]
    fn build_market_order_body_buy_base_bails() {
        let err = build_market_order_body(&Symbol::new("BTC", "USDT"), OrderSide::Buy, OrderAmount::Base("0.1".parse().unwrap()), None)
            .unwrap_err();
        assert!(err.to_string().contains("buy orders only support quote-amount"));
    }

    #[test]
    fn builds_limit_ioc_order_body_with_price_and_tif() {
        let body = build_limit_ioc_order_body(
            &Symbol::new("BTC", "USDT"),
            OrderSide::Sell,
            "0.2".parse().unwrap(),
            "30000".parse().unwrap(),
            None,
        );
        assert_eq!(body["type"], "limit");
        assert_eq!(body["side"], "sell");
        assert_eq!(body["amount"], "0.2");
        assert_eq!(body["price"], "30000");
        assert_eq!(body["time_in_force"], "ioc");
        assert!(body.get("text").is_none());
    }

    #[test]
    fn parses_order_response_filled() {
        let text = r#"{"id":"12345","text":"t-cid-1","status":"closed","filled_total":"3000.0","avg_deal_price":"30000.0","fee":"0.5","fee_currency":"USDT"}"#;
        let result = parse_order_response(text).expect("should parse");
        assert_eq!(result.order_id, "12345");
        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_qty, "0.1".parse().unwrap());
        assert_eq!(result.avg_price, Some("30000.0".parse().unwrap()));
        assert_eq!(result.fee, Some("0.5".parse().unwrap()));
        assert_eq!(result.fee_asset, Some("USDT".to_string()));
    }

    #[test]
    fn parses_order_response_open_with_no_fill() {
        let text = r#"{"id":"12345","status":"open","filled_total":"0","avg_deal_price":"0"}"#;
        let result = parse_order_response(text).expect("should parse");
        assert_eq!(result.status, OrderStatus::New);
        assert_eq!(result.filled_qty, Decimal::ZERO);
        assert_eq!(result.avg_price, None);
    }

    #[test]
    fn parses_order_response_cancelled_with_partial_fill() {
        let text = r#"{"id":"12345","status":"cancelled","filled_total":"1500.0","avg_deal_price":"30000.0"}"#;
        let result = parse_order_response(text).expect("should parse");
        assert_eq!(result.status, OrderStatus::Expired);
        assert_eq!(result.filled_qty, "0.05".parse().unwrap());
    }

    #[test]
    fn parse_order_response_surfaces_error() {
        let text = r#"{"label":"INVALID_PARAM_VALUE","message":"no valid parameter"}"#;
        let err = parse_order_response(text).unwrap_err();
        assert!(err.to_string().contains("no valid parameter"));
    }

    #[test]
    fn parses_ticker_price_response() {
        let text = r#"[{"currency_pair":"BTC_USDT","last":"30306.10","lowest_ask":"30307","highest_bid":"30305"}]"#;
        let price = parse_ticker_price(text).expect("should parse");
        assert_eq!(price, "30306.10".parse().unwrap());
    }

    #[test]
    fn parse_ticker_price_surfaces_error() {
        let text = r#"{"label":"INVALID_CURRENCY_PAIR","message":"currency pair not found"}"#;
        let err = parse_ticker_price(text).unwrap_err();
        assert!(err.to_string().contains("currency pair not found"));
    }

    #[test]
    fn gate_ws_auth_changes_with_time() {
        let a = gate_ws_auth("key", "secret", "spot.orders", "subscribe", 1700000000);
        let b = gate_ws_auth("key", "secret", "spot.orders", "subscribe", 1700000001);
        assert_eq!(a["method"], "api_key");
        assert_eq!(a["KEY"], "key");
        assert_ne!(a["SIGN"], b["SIGN"]);
    }

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(gate_symbol(&symbol), symbol);
        map
    }

    #[test]
    fn parses_gate_order_update_put_event_as_new() {
        let venue = Venue::new("gate_spot");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let mut last_fee = HashMap::new();
        let text = r#"{
            "channel": "spot.orders",
            "event": "update",
            "result": [
                {"id":"1","text":"t-cid-1","currency_pair":"BTC_USDT","event":"put","filled_total":"0","avg_deal_price":"0"}
            ]
        }"#;
        let updates = parse_gate_order_update(text, &venue, &map, &mut last_fee);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].status, OrderStatus::New);
        assert_eq!(updates[0].client_order_id, Some("t-cid-1".to_string()));
        assert_eq!(updates[0].exchange_order_id, Some("1".to_string()));
        assert_eq!(updates[0].symbol, Some(Symbol::new("BTC", "USDT")));
    }

    #[test]
    fn parses_gate_order_update_finish_filled() {
        let venue = Venue::new("gate_spot");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let mut last_fee = HashMap::new();
        let text = r#"{
            "channel": "spot.orders",
            "event": "update",
            "result": [
                {"id":"1","currency_pair":"BTC_USDT","event":"finish","finish_as":"filled","filled_total":"3000.0","avg_deal_price":"30000.0","fee":"0.5","fee_currency":"USDT"}
            ]
        }"#;
        let updates = parse_gate_order_update(text, &venue, &map, &mut last_fee);
        assert_eq!(updates[0].status, OrderStatus::Filled);
        assert_eq!(updates[0].filled_qty, "0.1".parse().unwrap());
        assert_eq!(updates[0].avg_price, Some("30000.0".parse().unwrap()));
        assert_eq!(updates[0].fee, Some("0.5".parse().unwrap()));
    }

    #[test]
    fn parses_gate_order_update_finish_cancelled_with_partial_fill() {
        let venue = Venue::new("gate_spot");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let mut last_fee = HashMap::new();
        let text = r#"{
            "channel": "spot.orders",
            "event": "update",
            "result": [
                {"id":"1","currency_pair":"BTC_USDT","event":"finish","finish_as":"cancelled","filled_total":"1500.0","avg_deal_price":"30000.0"}
            ]
        }"#;
        let updates = parse_gate_order_update(text, &venue, &map, &mut last_fee);
        assert_eq!(updates[0].status, OrderStatus::Expired);
        assert_eq!(updates[0].filled_qty, "0.05".parse().unwrap());
    }

    #[test]
    fn parses_gate_order_update_computes_incremental_fee_across_pushes() {
        let venue = Venue::new("gate_spot");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let mut last_fee = HashMap::new();
        let first = r#"{
            "channel": "spot.orders",
            "event": "update",
            "result": [
                {"id":"1","currency_pair":"BTC_USDT","event":"update","filled_total":"1000.0","avg_deal_price":"30000.0","fee":"1.0","fee_currency":"USDT"}
            ]
        }"#;
        let second = r#"{
            "channel": "spot.orders",
            "event": "update",
            "result": [
                {"id":"1","currency_pair":"BTC_USDT","event":"finish","finish_as":"filled","filled_total":"3000.0","avg_deal_price":"30000.0","fee":"1.5","fee_currency":"USDT"}
            ]
        }"#;
        let updates_1 = parse_gate_order_update(first, &venue, &map, &mut last_fee);
        assert_eq!(updates_1[0].fee, Some("1.0".parse().unwrap()));

        let updates_2 = parse_gate_order_update(second, &venue, &map, &mut last_fee);
        assert_eq!(updates_2[0].fee, Some("0.5".parse().unwrap()));

        // 终态之后 map 里的记录应该被清理掉。
        assert!(!last_fee.contains_key("1"));
    }

    #[test]
    fn keeps_update_with_missing_symbol_as_none_instead_of_dropping() {
        let venue = Venue::new("gate_spot");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let mut last_fee = HashMap::new();
        let text = r#"{
            "channel": "spot.orders",
            "event": "update",
            "result": [
                {"id":"1","currency_pair":"ETH_USDT","event":"put","filled_total":"0","avg_deal_price":"0"}
            ]
        }"#;
        let updates = parse_gate_order_update(text, &venue, &map, &mut last_fee);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].symbol, None);
    }

    #[test]
    fn ignores_non_update_event_messages() {
        let venue = Venue::new("gate_spot");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let mut last_fee = HashMap::new();
        assert!(parse_gate_order_update(
            r#"{"channel":"spot.orders","event":"subscribe","result":{"status":"success"}}"#,
            &venue,
            &map,
            &mut last_fee
        )
        .is_empty());
        assert!(parse_gate_order_update(r#"{"channel":"spot.pong","event":"","result":null}"#, &venue, &map, &mut last_fee).is_empty());
    }

    #[test]
    fn ignores_malformed_message() {
        let venue = Venue::new("gate_spot");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let mut last_fee = HashMap::new();
        assert!(parse_gate_order_update("not json", &venue, &map, &mut last_fee).is_empty());
    }

    #[test]
    fn builds_ws_subscribe_message_with_auth() {
        let stream =
            GatePrivateOrderStream::new(Venue::new("gate_spot"), "key".to_string(), "secret".to_string(), None, vec![
                Symbol::new("BTC", "USDT"),
            ]);
        let msg = stream.subscribe_message();
        assert_eq!(msg["channel"], "spot.orders");
        assert_eq!(msg["event"], "subscribe");
        assert_eq!(msg["payload"], serde_json::json!(["BTC_USDT"]));
        assert_eq!(msg["auth"]["method"], "api_key");
        assert_eq!(msg["auth"]["KEY"], "key");
        assert!(msg["auth"]["SIGN"].is_string());
    }
}
