use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::market_data::now_ms;
use crate::net::connect_tcp;
use crate::order_manager::OrderManager;
use crate::order_manager::stream::{ExchangeOrderUpdate, OrderStreamSource};
use crate::types::{Symbol, Venue};

use super::OrderProvider;
use super::types::{LimitIocOrderRequest, MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const HOST: &str = "https://api.kraken.com";
/// `pub(crate)`：`accounting::kraken::KrakenBalanceStream` 复用同一个私有 WS 端点。
pub(crate) const WS_HOST: &str = "ws-auth.kraken.com";
pub(crate) const WS_PORT: u16 = 443;
pub(crate) const MIN_BACKOFF: Duration = Duration::from_secs(1);
pub(crate) const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Kraken 官方建议客户端至少每 60s 主动 ping 一次，用来探测那些应用层
/// heartbeat 还在推送、但中间代理/NAT 已经悄悄杀掉的"假死"连接。
/// `pub(crate)`：`accounting::kraken::KrakenBalanceStream` 复用同一套读循环。
pub(crate) const PING_INTERVAL: Duration = Duration::from_secs(30);
/// 给 3 倍 ping 间隔的余量再判定连接假死，避免单次网络抖动/服务端瞬时延迟
/// 就误触发重连。
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

/// Kraken 下单(执行层)客户端：查询交易对精度限制、提交市价单/限价 IOC 单。
/// 下单走 WS v2 私有连接(`KrakenOrderWsClient`)的 `add_order` 方法而不是 REST
/// `AddOrder`，省掉每次下单的 TCP+TLS+HTTP 握手延迟；行情查询(`quote_usdt_price`)
/// 仍走 REST，签名方式和 `wallet::kraken::KrakenWalletProvider` 一致，用标准
/// HMAC-SHA512，凭证也复用同一套环境变量。
///
/// 重要限制：Kraken 的 `add_order` 对市价单只同步返回 order_id，不保证立即
/// 告知是否已成交/成交多少(REST `AddOrder` 同样如此)——本实现里
/// `place_market_order_raw` 因此固定返回 `OrderStatus::New`、`filled_qty=0`、
/// `avg_price=None`，调用方需要清楚这不是遗漏而是接口本身的限制。限价 IOC 单
/// (`place_limit_ioc_order_raw`)同样适用这个限制；真实成交结果要靠
/// `KrakenPrivateOrderStream` 的 `executions` WS 推送获取，其
/// `map_kraken_ws_status` 已覆盖 filled/partially_filled/canceled/expired，
/// 不需要改动。
pub struct KrakenOrderProvider {
    venue: Venue,
    http: reqwest::Client,
    order_ws: KrakenOrderWsClient,
}

impl KrakenOrderProvider {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        let order_ws = KrakenOrderWsClient::new(venue.clone(), api_key, api_secret, proxy)?;
        Ok(Self { venue, http, order_ws })
    }

    /// 从环境变量读取凭证并构造实例，和 `wallet::kraken::KrakenWalletProvider::from_env`
    /// 读取同一套变量：`KRAKEN_SPOT_API_KEY` + `KRAKEN_SPOT_API_SECRET`。
    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("KRAKEN_SPOT_API_KEY").context("KRAKEN_SPOT_API_KEY not set")?;
        let api_secret =
            std::env::var("KRAKEN_SPOT_API_SECRET").context("KRAKEN_SPOT_API_SECRET not set")?;
        Self::new(venue, api_key, api_secret, proxy)
    }

    fn kraken_pair(symbol: &Symbol) -> String {
        format!("{}/{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }
}

#[async_trait]
impl OrderProvider for KrakenOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let OrderAmount::Base(quantity) = req.amount else {
            anyhow::bail!("{} does not support quote-amount market orders", self.venue());
        };
        let params = build_market_add_order_params(
            &Self::kraken_pair(&req.symbol),
            req.side,
            quantity,
            req.client_order_id.as_deref(),
        );
        let result = self.order_ws.add_order(params).await?;
        Ok(order_result_from_ws(result))
    }

    async fn place_limit_ioc_order_raw(&self, req: &LimitIocOrderRequest) -> anyhow::Result<OrderResult> {
        let params = build_limit_ioc_add_order_params(
            &Self::kraken_pair(&req.symbol),
            req.side,
            req.quantity,
            req.price,
            req.client_order_id.as_deref(),
        );
        let result = self.order_ws.add_order(params).await?;
        Ok(order_result_from_ws(result))
    }

    /// `GET /0/public/Ticker?pair={ASSET}USD`(查 USD 不是 USDT：Kraken 山寨币
    /// 现货对多是 `*USD`，本系统里 USD 按 1:1 近似当 USDT 用，见
    /// `pricing::is_usdt_equivalent`)。公开行情接口不需要签名。`KFEE` 等没有
    /// 可交易对的资产查询会在 `unwrap_result` 里自然报错，交给调用方按失败
    /// 兜底处理，不特殊硬编码。
    async fn quote_usdt_price(&self, asset: &str) -> anyhow::Result<Decimal> {
        let pair = format!("{}USD", asset.to_ascii_uppercase());
        let text = kraken_public_get(&self.http, "/0/public/Ticker", vec![("pair".to_string(), pair)]).await?;
        parse_ticker_price(&text)
    }
}

/// 签名并发起一次 Kraken 私有 POST 请求。抽成自由函数是因为
/// `KrakenOrderProvider`(下单)、`KrakenPrivateOrderStream`(私有订单流的
/// token 获取)和 `accounting::kraken::KrakenBalanceStream` 都需要同一套 HMAC
/// 签名逻辑，因此是 `pub(crate)`。
pub(crate) async fn kraken_private_request(
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

/// 不需要签名的 Kraken 公开接口请求。抽成自由函数而不是挂在
/// `KrakenOrderProvider` 上，是因为它只需要 `http` 客户端，不涉及签名/凭证，
/// 和 `kraken_private_request` 的抽取理由一致。
async fn kraken_public_get(http: &reqwest::Client, path: &str, params: Vec<(String, String)>) -> anyhow::Result<String> {
    let query = params.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let url = if query.is_empty() { format!("{HOST}{path}") } else { format!("{HOST}{path}?{query}") };
    crate::ratelimit::throttle(HOST).await;
    let resp = http.get(url).send().await.context("kraken public request failed")?;
    resp.text().await.context("failed to read kraken public response body")
}

pub(crate) fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
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

/// WS `add_order` 只同步返回 order_id，不带成交信息，见本文件顶部注释；
/// 手续费同理拿不到，统一在这里组装成固定语义的 `OrderResult`。
fn order_result_from_ws(result: AddOrderWsResult) -> OrderResult {
    OrderResult {
        order_id: result.order_id,
        status: OrderStatus::New,
        filled_qty: Decimal::ZERO,
        avg_price: None,
        fee: None,
        fee_asset: None,
    }
}

/// Kraken WS v2 `add_order` 要求 `order_qty`/`limit_price` 是 JSON number(其
/// 校验器认的是 `number_float`)，不能像 REST 表单参数那样传字符串；而项目里
/// `rust_decimal` 开的是 `serde-with-str` 特性，直接序列化 `Decimal` 只会得到
/// 字符串，所以这里手动转成 `f64` 再包成 `serde_json::Number`。仅用于拼这几个
/// WS 请求字段，不影响其它地方 Decimal 的序列化行为。
fn decimal_to_json_number(value: Decimal) -> serde_json::Value {
    value
        .to_string()
        .parse::<f64>()
        .ok()
        .and_then(serde_json::Number::from_f64)
        .map(serde_json::Value::Number)
        .unwrap_or_else(|| serde_json::Value::String(value.to_string()))
}

/// 组装 WS v2 `add_order` 市价单参数(不含 `token`——由 `KrakenOrderWsClient::add_order`
/// 在实际发送前填入当前连接的 token，调用方不需要关心 token 何时刷新)。
fn build_market_add_order_params(pair: &str, side: OrderSide, quantity: Decimal, client_order_id: Option<&str>) -> serde_json::Value {
    let mut params = serde_json::json!({
        "order_type": "market",
        "side": map_side(side),
        "order_qty": decimal_to_json_number(quantity),
        "symbol": pair,
    });
    if let Some(client_order_id) = client_order_id {
        params["cl_ord_id"] = serde_json::Value::String(client_order_id.to_string());
    }
    params
}

/// 组装 WS v2 `add_order` 限价 IOC 单参数，同样不含 `token`。
fn build_limit_ioc_add_order_params(
    pair: &str,
    side: OrderSide,
    quantity: Decimal,
    price: Decimal,
    client_order_id: Option<&str>,
) -> serde_json::Value {
    let mut params = serde_json::json!({
        "order_type": "limit",
        "side": map_side(side),
        "order_qty": decimal_to_json_number(quantity),
        "limit_price": decimal_to_json_number(price),
        "time_in_force": "ioc",
        "symbol": pair,
    });
    if let Some(client_order_id) = client_order_id {
        params["cl_ord_id"] = serde_json::Value::String(client_order_id.to_string());
    }
    params
}

#[derive(Debug)]
pub(crate) struct AddOrderWsResult {
    pub(crate) order_id: String,
}

#[derive(Debug, Deserialize)]
struct AddOrderWsResponse {
    #[serde(default)]
    req_id: Option<u64>,
    #[serde(default)]
    success: bool,
    #[serde(default)]
    result: Option<AddOrderWsResultRaw>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AddOrderWsResultRaw {
    order_id: String,
}

/// 解析一条 `add_order` 响应消息。用 `req_id` 是否存在来判断这条消息是不是
/// 对某次下单请求的应答——`ping` 的 `pong` 回复、心跳等消息都不带 `req_id`，
/// 天然被过滤掉，不需要额外判断 `method` 字段。纯函数，不依赖真实连接，
/// 便于单元测试。
fn parse_add_order_ws_response(text: &str) -> Option<(u64, anyhow::Result<AddOrderWsResult>)> {
    let resp: AddOrderWsResponse = serde_json::from_str(text).ok()?;
    let req_id = resp.req_id?;
    if resp.success {
        let result = resp.result?;
        Some((req_id, Ok(AddOrderWsResult { order_id: result.order_id })))
    } else {
        let err = resp.error.unwrap_or_else(|| "unknown kraken add_order error".to_string());
        Some((req_id, Err(anyhow::anyhow!("kraken add_order error: {err}"))))
    }
}

/// 下单专用的私有 WS 长连接：和 `KrakenPrivateOrderStream`(只管接收
/// `executions` 推送)完全解耦、各自独立重连。用 `req_id` 关联请求/响应——
/// `add_order` 把待应答的 `oneshot::Sender` 存进 `pending`，后台读循环收到
/// 带匹配 `req_id` 的响应后取出并回传。
///
/// 连接状态用 `active: Mutex<Option<ActiveConnection>>` 显式表达：只有连接
/// 建立成功期间才是 `Some`，一旦读循环判定断线(错误/空闲超时)就立刻置回
/// `None` 再进入重连退避——`add_order` 据此判断"未连接"必须快速失败，
/// 不能靠一个跨重连世代持续存活的 channel 把请求悄悄排队等重连，那样会
/// 违反"快速失败、不排队"的约定。
pub(crate) struct KrakenOrderWsClient {
    venue: Venue,
    next_req_id: AtomicU64,
    active: Arc<Mutex<Option<ActiveConnection>>>,
    pending: Arc<DashMap<u64, oneshot::Sender<anyhow::Result<AddOrderWsResult>>>>,
}

struct ActiveConnection {
    sender: mpsc::UnboundedSender<Message>,
    token: String,
}

impl KrakenOrderWsClient {
    fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        let active = Arc::new(Mutex::new(None));
        let pending = Arc::new(DashMap::new());
        tokio::spawn(run_kraken_order_ws(
            venue.clone(),
            api_key,
            api_secret,
            http,
            proxy.map(str::to_string),
            active.clone(),
            pending.clone(),
        ));
        Ok(Self { venue, next_req_id: AtomicU64::new(1), active, pending })
    }

    /// 提交一次下单请求并等待响应。`params` 不需要包含 `token`——这里读取
    /// 当前连接世代的 token 填进去，调用方不用感知 token 刷新/重连。
    /// 未连接(还没首次连上/正在重连)或消息发送失败都直接快速返回错误，
    /// 不排队等待重连。
    pub(crate) async fn add_order(&self, mut params: serde_json::Value) -> anyhow::Result<AddOrderWsResult> {
        let (sender, token) = {
            let guard = self.active.lock().unwrap();
            let conn = guard
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("kraken order ws for venue={} not connected", self.venue))?;
            (conn.sender.clone(), conn.token.clone())
        };
        params["token"] = serde_json::Value::String(token);

        let req_id = self.next_req_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(req_id, tx);

        let request = serde_json::json!({"method": "add_order", "params": params, "req_id": req_id});
        if sender.send(Message::Text(request.to_string())).is_err() {
            self.pending.remove(&req_id);
            anyhow::bail!("kraken order ws for venue={} connection just closed", self.venue);
        }

        match rx.await {
            Ok(result) => result,
            Err(_) => anyhow::bail!("kraken order ws for venue={} response channel closed", self.venue),
        }
    }
}

/// 后台连接循环：结构对齐 `KrakenPrivateOrderStream::spawn`，复用同样的
/// token 获取/连接/ping/空闲超时判定逻辑，多一个 `select!` 分支处理
/// `add_order` 发来的待发送消息。每次连接建立/断开都会更新 `active`，
/// 断线时清空 `pending`——所有还没应答的请求立刻收到错误，不排队等重连。
async fn run_kraken_order_ws(
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
    proxy: Option<String>,
    active: Arc<Mutex<Option<ActiveConnection>>>,
    pending: Arc<DashMap<u64, oneshot::Sender<anyhow::Result<AddOrderWsResult>>>>,
) {
    let mut backoff = MIN_BACKOFF;

    loop {
        let token = match kraken_private_request(&http, &api_key, &api_secret, "/0/private/GetWebSocketsToken", vec![])
            .await
            .and_then(|text| parse_ws_token(&text))
        {
            Ok(token) => token,
            Err(err) => {
                warn!("kraken order ws: failed to fetch ws token for venue={venue} err={err:#}, retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };

        let tcp = match connect_tcp(WS_HOST, WS_PORT, proxy.as_deref()).await {
            Ok(tcp) => tcp,
            Err(err) => {
                warn!("kraken order ws connect failed for venue={venue} err={err:#}, retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };
        let url = format!("wss://{WS_HOST}/v2");
        let mut ws = match tokio_tungstenite::client_async_tls(url, tcp).await {
            Ok((ws, _)) => ws,
            Err(err) => {
                warn!("kraken order ws handshake failed for venue={venue} err={err:#}, retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };
        debug!("kraken order ws connected for venue={venue}");
        backoff = MIN_BACKOFF;

        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel::<Message>();
        *active.lock().unwrap() = Some(ActiveConnection { sender: conn_tx, token });

        let mut ping_interval = tokio::time::interval(PING_INTERVAL);
        ping_interval.tick().await; // 首次 tick 立即触发，跳过，避免刚连上就发一次多余的 ping

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    let ping = serde_json::json!({"method": "ping"});
                    if let Err(err) = ws.send(Message::Text(ping.to_string())).await {
                        warn!("kraken order ws: failed to send ping for venue={venue} err={err}");
                        break;
                    }
                }
                outgoing = conn_rx.recv() => {
                    // `conn_tx`/`conn_rx` 生命周期和这层内循环一致，只有 break 出去时
                    // 才会一起被丢弃，`recv()` 返回 `None` 在正常运行时不会发生。
                    let Some(msg) = outgoing else { break };
                    if let Err(err) = ws.send(msg).await {
                        warn!("kraken order ws: failed to send add_order request for venue={venue} err={err}");
                        break;
                    }
                }
                msg = tokio::time::timeout(IDLE_TIMEOUT, ws.next()) => {
                    let msg = match msg {
                        Ok(Some(Ok(msg))) => msg,
                        Ok(Some(Err(err))) => {
                            warn!("kraken order ws error for venue={venue} err={err}");
                            break;
                        }
                        Ok(None) => break,
                        Err(_) => {
                            warn!("kraken order ws idle timeout for venue={venue}, no message in {IDLE_TIMEOUT:?}");
                            break;
                        }
                    };
                    let Message::Text(text) = msg else { continue };
                    if let Some((req_id, result)) = parse_add_order_ws_response(&text) {
                        if let Some((_, tx)) = pending.remove(&req_id) {
                            let _ = tx.send(result);
                        }
                    }
                }
            }
        }

        *active.lock().unwrap() = None;
        let stale_req_ids: Vec<u64> = pending.iter().map(|entry| *entry.key()).collect();
        for req_id in stale_req_ids {
            if let Some((_, tx)) = pending.remove(&req_id) {
                let _ = tx.send(Err(anyhow::anyhow!("kraken order ws for venue={venue} disconnected, reconnecting")));
            }
        }

        warn!("kraken order ws disconnected for venue={venue}, reconnecting in {backoff:?}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

#[derive(Debug, Deserialize)]
struct TickerEntry {
    /// 最近成交 `[价格, 量]`，只取价格。
    c: Vec<Decimal>,
}

/// `GET /0/public/Ticker` 响应的 `result` 是按 Kraken 内部资产代码(如
/// `XXBTZUSD`)为 key 的 map，和 `exchange_info::kraken::parse_trading_fee`
/// 同样的原因(不是按请求传入的 pair 字符串做 key)，用 `.into_values().next()`
/// 取唯一的那个值，不按 key 精确匹配。
fn parse_ticker_price(text: &str) -> anyhow::Result<Decimal> {
    let result: HashMap<String, TickerEntry> = unwrap_result(text)?;
    let entry = result
        .into_values()
        .next()
        .ok_or_else(|| anyhow::anyhow!("kraken Ticker response has no entries"))?;
    entry
        .c
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("kraken Ticker response missing last trade price"))
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

    fn spawn(self: Box<Self>, order_manager: Arc<OrderManager>) -> crate::order_manager::stream::StreamHandle {
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

                let mut ping_interval = tokio::time::interval(PING_INTERVAL);
                ping_interval.tick().await; // 首次 tick 立即触发，跳过，避免刚连上就发一次多余的 ping

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            let ping = serde_json::json!({"method": "ping"});
                            if let Err(err) = ws.send(Message::Text(ping.to_string())).await {
                                warn!("kraken private order stream: failed to send ping for venue={} err={err}", self.venue);
                                break;
                            }
                        }
                        msg = tokio::time::timeout(IDLE_TIMEOUT, ws.next()) => {
                            let msg = match msg {
                                Ok(Some(Ok(msg))) => msg,
                                Ok(Some(Err(err))) => {
                                    warn!("kraken private order stream error for venue={} err={err}", self.venue);
                                    break;
                                }
                                Ok(None) => break,
                                Err(_) => {
                                    warn!(
                                        "kraken private order stream idle timeout for venue={}, no message in {:?}",
                                        self.venue, IDLE_TIMEOUT
                                    );
                                    break;
                                }
                            };
                            let Message::Text(text) = msg else { continue };
                            for update in parse_kraken_execution(&text, &self.venue) {
                                order_manager.handle_exchange_update(update).await;
                            }
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

pub(crate) fn parse_ws_token(text: &str) -> anyhow::Result<String> {
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
pub(crate) struct ChannelEnvelope {
    #[serde(default)]
    pub(crate) channel: Option<String>,
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
    #[serde(default)]
    symbol: Option<String>,
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
/// `None`。
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

/// Kraken 用 `"BTC/USD"` 这种带分隔符的格式表示交易对，不像 Binance 那样需要
/// 反查表——直接按 `/` 切分成 base/quote 即可。
fn parse_kraken_symbol(raw: &str) -> Option<Symbol> {
    let (base, quote) = raw.split_once('/')?;
    if base.is_empty() || quote.is_empty() {
        return None;
    }
    Some(Symbol::new(base, quote))
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
            let symbol = item.symbol.as_deref().and_then(parse_kraken_symbol);
            if symbol.is_none() {
                warn!(
                    "kraken private order stream: missing/malformed symbol {:?}, raw message: {text}",
                    item.symbol
                );
            }
            let (fee, fee_asset) = sum_kraken_fees(&item.fees);
            ExchangeOrderUpdate {
                venue: venue.clone(),
                symbol,
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
    fn builds_market_add_order_params_without_token() {
        let params = build_market_add_order_params("BTC/USD", OrderSide::Buy, "0.1".parse().unwrap(), Some("cid-1"));
        assert_eq!(params["order_type"], "market");
        assert_eq!(params["side"], "buy");
        assert!(params["order_qty"].is_number(), "order_qty must be a JSON number, got {:?}", params["order_qty"]);
        assert_eq!(params["order_qty"], 0.1);
        assert_eq!(params["symbol"], "BTC/USD");
        assert_eq!(params["cl_ord_id"], "cid-1");
        assert!(params.get("token").is_none());
        assert!(params.get("limit_price").is_none());
    }

    #[test]
    fn builds_limit_ioc_add_order_params_with_price_and_tif() {
        let params = build_limit_ioc_add_order_params(
            "BTC/USD",
            OrderSide::Sell,
            "0.2".parse().unwrap(),
            "30000".parse().unwrap(),
            None,
        );
        assert_eq!(params["order_type"], "limit");
        assert_eq!(params["side"], "sell");
        assert!(params["order_qty"].is_number(), "order_qty must be a JSON number, got {:?}", params["order_qty"]);
        assert_eq!(params["order_qty"], 0.2);
        assert!(params["limit_price"].is_number(), "limit_price must be a JSON number, got {:?}", params["limit_price"]);
        assert_eq!(params["limit_price"], 30000.0);
        assert_eq!(params["time_in_force"], "ioc");
        assert!(params.get("cl_ord_id").is_none());
    }

    #[test]
    fn parses_successful_add_order_ws_response() {
        let text = r#"{"method":"add_order","req_id":7,"success":true,"result":{"order_id":"OQCLML-BW3P3-BUCMWZ"}}"#;
        let (req_id, result) = parse_add_order_ws_response(text).expect("should parse");
        assert_eq!(req_id, 7);
        assert_eq!(result.expect("should be ok").order_id, "OQCLML-BW3P3-BUCMWZ");
    }

    #[test]
    fn parses_failed_add_order_ws_response() {
        let text = r#"{"method":"add_order","req_id":8,"success":false,"error":"Insufficient funds"}"#;
        let (req_id, result) = parse_add_order_ws_response(text).expect("should parse");
        assert_eq!(req_id, 8);
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Insufficient funds"));
    }

    #[test]
    fn ignores_add_order_ws_messages_without_req_id() {
        assert!(parse_add_order_ws_response(r#"{"method":"pong"}"#).is_none());
        assert!(parse_add_order_ws_response("not json").is_none());
    }

    #[test]
    fn parses_ticker_price_response() {
        let text = r#"{
            "error": [],
            "result": {
                "XXBTZUSD": {
                    "a": ["30306.10000", "1", "1.000"],
                    "b": ["30305.90000", "1", "1.000"],
                    "c": ["30306.10000", "0.00067643"],
                    "v": ["4083.67001100", "4412.73601799"],
                    "p": ["30297.50968", "30310.75658"],
                    "t": [23329, 25344],
                    "l": ["29868.30000", "29868.30000"],
                    "h": ["30720.70000", "30820.50000"],
                    "o": "30502.30000"
                }
            }
        }"#;
        let price = parse_ticker_price(text).expect("should parse");
        assert_eq!(price, "30306.10000".parse().unwrap());
    }

    #[test]
    fn parse_ticker_price_surfaces_error_response() {
        let text = r#"{"error": ["EQuery:Unknown asset pair"], "result": null}"#;
        let err = parse_ticker_price(text).unwrap_err();
        assert!(err.to_string().contains("Unknown asset pair"));
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
        assert_eq!(update.symbol, Some(Symbol::new("BTC", "USD")));
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
        assert_eq!(update.symbol, Some(Symbol::new("BTC", "USD")));
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
        assert_eq!(update.symbol, Some(Symbol::new("BTC", "USD")));
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
        assert_eq!(update.symbol, Some(Symbol::new("BTC", "USD")));
        assert_eq!(update.client_order_id, None);
        assert_eq!(update.status, OrderStatus::New);
        assert_eq!(update.filled_qty, Decimal::ZERO);
        assert_eq!(update.avg_price, None);
        assert_eq!(update.fee, None);
        assert_eq!(update.fee_asset, None);
    }

    #[test]
    fn keeps_execution_with_missing_symbol_as_none_instead_of_dropping() {
        let venue = Venue::new("kraken");
        let text = r#"{
            "channel": "executions",
            "type": "update",
            "data": [
                {
                    "order_id": "OK4GJX-KSTLS-7DZZO5",
                    "order_status": "partially_filled",
                    "cum_qty": 0.4,
                    "cum_cost": 16000.0,
                    "timestamp": "2023-09-22T10:33:05.709950Z"
                },
                {
                    "order_id": "OK4GJX-KSTLS-7DZZO6",
                    "symbol": "ETH/USD",
                    "order_status": "filled",
                    "cum_qty": 1.0,
                    "cum_cost": 3000.0,
                    "timestamp": "2023-09-22T10:33:05.709950Z"
                }
            ],
            "sequence": 8
        }"#;
        let updates = parse_kraken_execution(text, &venue);
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].exchange_order_id, Some("OK4GJX-KSTLS-7DZZO5".to_string()));
        assert_eq!(updates[0].symbol, None);
        assert_eq!(updates[0].status, OrderStatus::PartiallyFilled);
        assert_eq!(updates[1].exchange_order_id, Some("OK4GJX-KSTLS-7DZZO6".to_string()));
        assert_eq!(updates[1].symbol, Some(Symbol::new("ETH", "USD")));
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
