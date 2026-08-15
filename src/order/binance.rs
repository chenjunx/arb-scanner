use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use futures_util::{SinkExt, StreamExt};
use log::{debug, warn};
use ring::signature::Ed25519KeyPair;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::market_data::now_ms;
use crate::net::connect_tcp;
use crate::order_manager::stream::{ExchangeOrderUpdate, OrderStreamSource};
use crate::types::{Symbol, Venue};

use super::OrderProvider;
use super::types::{MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const MAINNET_HOST: &str = "https://api.binance.com";
const TESTNET_HOST: &str = "https://testnet.binance.vision";
// 现货 listenKey REST 接口(POST/PUT/DELETE /api/v3/userDataStream)已在
// 2026-02-20 下线，User Data Stream 改成在 WS API 连接内做 session.logon
// 签名鉴权 + userDataStream.subscribe，见 BinanceUserDataStream。
const WS_API_MAINNET_HOST: &str = "ws-api.binance.com";
const WS_API_TESTNET_HOST: &str = "ws-api.testnet.binance.vision";
const WS_API_PORT: u16 = 443;
const WS_API_PATH: &str = "/ws-api/v3";
// 5000 曾在并发下三条腿一起发请求时因调度延迟触发过一次 -1022(签名无效)，
// 实际是时间戳超出 recvWindow 但被币安网关报成了签名错误，调大留出冗余。
const RECV_WINDOW_MS: u64 = 10_000;
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// 币安下单(执行层)客户端：查询交易对精度限制、提交市价单。签名方式和
/// `wallet::binance::BinanceWalletProvider` 一致，用 Ed25519，凭证也复用同一套
/// 环境变量(交易和提币通常用同一个 API Key，只是权限勾选不同)。
pub struct BinanceOrderProvider {
    venue: Venue,
    api_key: String,
    key_pair: Ed25519KeyPair,
    host: &'static str,
    http: reqwest::Client,
}

impl BinanceOrderProvider {
    pub fn new(venue: Venue, api_key: String, private_key_pem: &str, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let key_pair = load_ed25519_key(private_key_pem)?;
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            key_pair,
            host: if testnet { TESTNET_HOST } else { MAINNET_HOST },
            http,
        })
    }

    /// 从环境变量读取凭证并构造实例，和 `wallet::binance::BinanceWalletProvider::from_env`
    /// 读取同一套变量：`BINANCE_API_KEY` + `BINANCE_API_SECRET`。
    pub fn from_env(venue: Venue, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY")
            .context("BINANCE_API_KEY not set")?;
        let private_key_pem = std::env::var("BINANCE_API_SECRET")
            .context("BINANCE_API_SECRET not set")?;
        Self::new(venue, api_key, &private_key_pem, testnet, proxy)
    }

    /// 对参数做签名并发起一次已签名请求(query string 里带 timestamp/recvWindow/signature)。
    async fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        mut params: Vec<(String, String)>,
    ) -> anyhow::Result<String> {
        params.push(("timestamp".to_string(), now_ms().to_string()));
        params.push(("recvWindow".to_string(), RECV_WINDOW_MS.to_string()));
        let query = build_query_string(&params);
        let signature = sign_ed25519(&self.key_pair, &query);
        let url = format!("{}{}?{}&signature={}", self.host, path, query, percent_encode(&signature));

        crate::ratelimit::throttle(self.host).await;
        let resp = self
            .http
            .request(method, &url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("binance order request failed")?;
        let text = resp.text().await.context("failed to read binance order response body")?;
        Ok(text)
    }

    fn binance_symbol(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }
}

#[async_trait]
impl OrderProvider for BinanceOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let mut params = vec![
            ("symbol".to_string(), Self::binance_symbol(&req.symbol)),
            ("side".to_string(), map_side(req.side).to_string()),
            ("type".to_string(), "MARKET".to_string()),
        ];
        match req.amount {
            OrderAmount::Base(quantity) => params.push(("quantity".to_string(), quantity.to_string())),
            OrderAmount::Quote(quote_amount) => params.push(("quoteOrderQty".to_string(), quote_amount.to_string())),
        }
        if let Some(client_order_id) = &req.client_order_id {
            params.push(("newClientOrderId".to_string(), client_order_id.clone()));
        }
        let text = self.signed_request(reqwest::Method::POST, "/api/v3/order", params).await?;
        parse_order_response(&text)
    }

    /// `GET /api/v3/order` 按 orderId 查询。响应不带 `avgPrice`/`fills`，但带
    /// `cummulativeQuoteQty`，`parse_order_response` 本来就是用
    /// `cummulativeQuoteQty / executedQty` 算均价(而不是依赖 `avgPrice` 字段)，
    /// `fills` 靠 `#[serde(default)]` 缺省为空即可直接复用。这个接口本身不带
    /// 手续费(币安故意不在订单汇总查询里给逐笔成交明细)，成交了就再查一次
    /// `GET /api/v3/myTrades` 补真实手续费；那一步失败也不影响这里返回订单
    /// 状态本身，只是手续费留空，交给 Portfolio 按 `FeeConfig` 估算兜底。
    /// 用于 `wait_for_fill` 的 REST 兜底核对，以及 `reconcile-order` 命令。
    async fn query_order(&self, symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<OrderResult> {
        let params = vec![
            ("symbol".to_string(), Self::binance_symbol(symbol)),
            ("orderId".to_string(), exchange_order_id.to_string()),
        ];
        let text = self.signed_request(reqwest::Method::GET, "/api/v3/order", params).await?;
        let mut result = parse_order_response(&text)?;

        if result.fee.is_none() && result.filled_qty > Decimal::ZERO {
            match self.query_order_fee(symbol, exchange_order_id).await {
                Ok((fee, fee_asset)) => {
                    result.fee = fee;
                    result.fee_asset = fee_asset;
                }
                Err(err) => {
                    warn!(
                        "query_order: 补查 myTrades 手续费失败(order_id={exchange_order_id})，手续费留空交给估算兜底: {err:#}"
                    );
                }
            }
        }

        Ok(result)
    }
}

impl BinanceOrderProvider {
    /// `GET /api/v3/myTrades` 按 orderId 查这笔订单的逐笔成交明细，汇总出真实
    /// 手续费。只在 `query_order` 里、订单确实有成交但拿不到手续费时调用。
    async fn query_order_fee(
        &self,
        symbol: &Symbol,
        exchange_order_id: &str,
    ) -> anyhow::Result<(Option<Decimal>, Option<String>)> {
        let params = vec![
            ("symbol".to_string(), Self::binance_symbol(symbol)),
            ("orderId".to_string(), exchange_order_id.to_string()),
        ];
        let text = self.signed_request(reqwest::Method::GET, "/api/v3/myTrades", params).await?;
        parse_my_trades_response(&text)
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build binance http client")
}

/// 按插入顺序拼接 `k=v&k=v...`(value 做 percent-encode，签名必须覆盖和实际
/// 发送完全一致的 query string——base64 编码的 `signature` 本身也要在拼进
/// URL 时percent-encode，否则其中的 `+` 会被币安网关当 query string 里的
/// 空格解析，导致签名校验失败(`-1022`)，实测已复现)。
fn build_query_string(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{k}={}", percent_encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// 解析 PKCS8 PEM 文本(过滤 BEGIN/END 行,拼接剩余 base64 并解码),交给
/// `Ed25519KeyPair::from_pkcs8_maybe_unchecked` 加载私钥。用 `_maybe_unchecked`
/// 而不是 `from_pkcs8`：`openssl genpkey -algorithm ed25519`(Binance 官方文档
/// 推荐的生成方式)默认产出的是不带内嵌公钥的 PKCS8 v1，`from_pkcs8` 严格要求
/// 带公钥的 v2 格式,遇到 v1 会报 `KeyRejected` 的 "VersionNotSupported"；
/// `_maybe_unchecked` 同时兼容 v1/v2,v1 时只是跳过"内嵌公钥与私钥一致"的校验。
fn load_ed25519_key(pem: &str) -> anyhow::Result<Ed25519KeyPair> {
    let der = parse_pem_pkcs8(pem)?;
    Ed25519KeyPair::from_pkcs8_maybe_unchecked(&der)
        .map_err(|err| anyhow::anyhow!("invalid ed25519 pkcs8 key: {err}"))
}

fn parse_pem_pkcs8(pem: &str) -> anyhow::Result<Vec<u8>> {
    let body: String = pem
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("-----"))
        .collect();
    base64_engine.decode(body).context("failed to base64-decode PEM body")
}

/// 对 payload 做 Ed25519 签名，返回 base64 编码结果。
fn sign_ed25519(key_pair: &Ed25519KeyPair, payload: &str) -> String {
    let signature = key_pair.sign(payload.as_bytes());
    base64_engine.encode(signature.as_ref())
}

fn map_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    }
}

fn map_status(status: &str) -> OrderStatus {
    match status {
        "FILLED" => OrderStatus::Filled,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Expired,
        _ => OrderStatus::New,
    }
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    code: i64,
    msg: String,
}

#[derive(Debug, Deserialize)]
struct OrderFill {
    commission: Decimal,
    #[serde(rename = "commissionAsset")]
    commission_asset: String,
}

#[derive(Debug, Deserialize)]
struct OrderResponse {
    #[serde(rename = "orderId")]
    order_id: i64,
    status: String,
    #[serde(rename = "executedQty")]
    executed_qty: Decimal,
    #[serde(rename = "cummulativeQuoteQty")]
    cummulative_quote_qty: Decimal,
    /// MARKET 单默认 `newOrderRespType=FULL` 时带这个字段，每笔成交各自的
    /// 手续费，按币种汇总见 `sum_fee_by_asset`。
    #[serde(default)]
    fills: Vec<OrderFill>,
}

/// 按 `commissionAsset` 分组求和；只有单一币种时才认为是可信的单一手续费值
/// 返回 `Some`，混合多币种(如 BNB 抵扣额度中途用完)时返回 `None`，交给
/// Portfolio 按 `FeeConfig` 估算兜底，不做加权处理。
fn sum_fee_by_asset(fills: &[OrderFill]) -> (Option<Decimal>, Option<String>) {
    let mut totals: HashMap<&str, Decimal> = HashMap::new();
    for fill in fills {
        *totals.entry(fill.commission_asset.as_str()).or_insert(Decimal::ZERO) += fill.commission;
    }
    if totals.len() == 1 {
        let (asset, total) = totals.into_iter().next().unwrap();
        (Some(total), Some(asset.to_string()))
    } else {
        (None, None)
    }
}

fn parse_order_response(text: &str) -> anyhow::Result<OrderResult> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let resp: OrderResponse = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance order response, raw body: {text}"))?;
    let avg_price = (resp.executed_qty > Decimal::ZERO).then(|| resp.cummulative_quote_qty / resp.executed_qty);
    let (fee, fee_asset) = sum_fee_by_asset(&resp.fills);

    Ok(OrderResult {
        order_id: resp.order_id.to_string(),
        status: map_status(&resp.status),
        filled_qty: resp.executed_qty,
        avg_price,
        fee,
        fee_asset,
    })
}

/// `GET /api/v3/myTrades` 响应是一个成交明细数组，每一项的 `commission`/
/// `commissionAsset` 字段名和下单响应里 `fills` 数组的字段完全一样，直接复用
/// `OrderFill`/`sum_fee_by_asset`。
fn parse_my_trades_response(text: &str) -> anyhow::Result<(Option<Decimal>, Option<String>)> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let fills: Vec<OrderFill> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance myTrades response, raw body: {text}"))?;
    Ok(sum_fee_by_asset(&fills))
}

/// 币安现货 User Data Stream 客户端：在 WS API 连接内做 `session.logon`
/// (Ed25519 签名鉴权) + `userDataStream.subscribe`，把推送的 `executionReport`
/// 转换成 `ExchangeOrderUpdate`。2026-02-20 起旧的 listenKey REST 接口
/// (POST/PUT/DELETE /api/v3/userDataStream) 已下线，不能再用。
pub struct BinanceUserDataStream {
    venue: Venue,
    api_key: String,
    key_pair: Ed25519KeyPair,
    ws_host: &'static str,
    ws_port: u16,
    proxy: Option<String>,
}

impl BinanceUserDataStream {
    pub fn new(venue: Venue, api_key: String, private_key_pem: &str, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let key_pair = load_ed25519_key(private_key_pem)?;
        let ws_host = if testnet { WS_API_TESTNET_HOST } else { WS_API_MAINNET_HOST };
        Ok(Self {
            venue,
            api_key,
            key_pair,
            ws_host,
            ws_port: WS_API_PORT,
            proxy: proxy.map(str::to_string),
        })
    }

    /// 从环境变量读取凭证，和 `BinanceOrderProvider::from_env` 复用同一套
    /// `BINANCE_API_KEY` + `BINANCE_API_SECRET`：session.logon 需要用私钥签名，
    /// 不再是 listenKey 时代只需要 API Key 的轻量鉴权。
    pub fn from_env(venue: Venue, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY").context("BINANCE_API_KEY not set")?;
        let private_key_pem = std::env::var("BINANCE_API_SECRET").context("BINANCE_API_SECRET not set")?;
        Self::new(venue, api_key, &private_key_pem, testnet, proxy)
    }

    async fn connect(&self) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(self.ws_host, self.ws_port, self.proxy.as_deref()).await?;
        let url = format!("wss://{}:{}{}", self.ws_host, self.ws_port, WS_API_PATH);
        let (ws, _) = tokio_tungstenite::client_async_tls(url, tcp)
            .await
            .context("binance user data stream handshake failed")?;
        Ok(ws)
    }

    /// 按字母序拼接 `k=v&k=v...`(value 做 percent-encode，币安 2026-01-15 起
    /// 要求签名前编码，见 WS API request security 文档)，用下单同一把 Ed25519
    /// 私钥签名。
    fn sign_ws_params(&self, params: &[(&str, &str)]) -> String {
        let mut sorted = params.to_vec();
        sorted.sort_by_key(|(k, _)| *k);
        let payload = sorted
            .iter()
            .map(|(k, v)| format!("{k}={}", percent_encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        sign_ed25519(&self.key_pair, &payload)
    }

    async fn session_logon(&self, ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> anyhow::Result<()> {
        let timestamp = now_ms();
        let timestamp_str = timestamp.to_string();
        let signature = self.sign_ws_params(&[("apiKey", &self.api_key), ("timestamp", &timestamp_str)]);
        let req = serde_json::json!({
            "id": "logon",
            "method": "session.logon",
            "params": {
                "apiKey": self.api_key,
                "signature": signature,
                "timestamp": timestamp,
            }
        });
        send_ws_api_request(ws, "logon", &req).await.context("session.logon failed")
    }

    async fn subscribe_user_data(&self, ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>) -> anyhow::Result<()> {
        let req = serde_json::json!({"id": "sub", "method": "userDataStream.subscribe"});
        send_ws_api_request(ws, "sub", &req).await.context("userDataStream.subscribe failed")
    }
}

impl OrderStreamSource for BinanceUserDataStream {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, tx: mpsc::Sender<ExchangeOrderUpdate>) -> crate::order_manager::stream::StreamHandle {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let join = tokio::spawn(async move {
            let mut backoff = MIN_BACKOFF;
            let mut ready_tx = Some(ready_tx);

            loop {
                let mut ws = match self.connect().await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "binance user data stream connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = self.session_logon(&mut ws).await {
                    warn!(
                        "binance user data stream: session.logon failed for venue={} err={err:#}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
                if let Err(err) = self.subscribe_user_data(&mut ws).await {
                    warn!(
                        "binance user data stream: userDataStream.subscribe failed for venue={} err={err:#}, retrying in {:?}",
                        self.venue, backoff
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
                debug!("binance user data stream connected and subscribed for venue={}", self.venue);
                backoff = MIN_BACKOFF;
                if let Some(ready_tx) = ready_tx.take() {
                    let _ = ready_tx.send(());
                }

                loop {
                    let msg = match ws.next().await {
                        Some(Ok(msg)) => msg,
                        Some(Err(err)) => {
                            warn!("binance user data stream error for venue={} err={err}", self.venue);
                            break;
                        }
                        None => break,
                    };
                    match msg {
                        // WS API 20s 一次 ping / 60s 无 pong 就断连，比旧的
                        // listenKey(60 分钟)严格得多，必须及时应答。
                        Message::Ping(payload) => {
                            if ws.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Message::Text(text) => {
                            let Some(update) = parse_execution_report(&text, &self.venue) else { continue };
                            if tx.send(update).await.is_err() {
                                return;
                            }
                        }
                        _ => {}
                    }
                }

                warn!(
                    "binance user data stream disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        });
        crate::order_manager::stream::StreamHandle { join, ready: ready_rx }
    }
}

/// RFC 3986 未保留字符集之外的字节一律 percent-encode。
fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[derive(Debug, Deserialize)]
struct WsApiResponse {
    id: Option<String>,
    #[serde(default)]
    status: i64,
}

/// 判断一条 WS API 响应文本是否是我们在等的那个请求的结果：`id` 不匹配(或
/// 干脆不是响应，比如推送事件)返回 `None`，忽略即可；`id` 匹配时返回
/// `Some(status == 200)`。纯函数，不依赖真实连接，便于单元测试。
fn match_ws_api_response(text: &str, expected_id: &str) -> Option<bool> {
    let resp: WsApiResponse = serde_json::from_str(text).ok()?;
    if resp.id.as_deref() != Some(expected_id) {
        return None;
    }
    Some(resp.status == 200)
}

/// 发送一条 WS API 请求并阻塞等待匹配 `id` 的响应；`status` 非 200 时把响应体
/// 透传成错误。等待期间收到 `Ping` 帧会先回 `Pong`，避免在鉴权/订阅阶段就被
/// 服务端因超时断连。
async fn send_ws_api_request(
    ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    expected_id: &str,
    req: &serde_json::Value,
) -> anyhow::Result<()> {
    ws.send(Message::Text(req.to_string())).await.context("failed to send websocket api request")?;
    loop {
        let msg = ws
            .next()
            .await
            .context("connection closed while waiting for websocket api response")?
            .context("websocket error while waiting for response")?;
        match msg {
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload)).await.context("failed to send pong")?;
            }
            Message::Text(text) => match match_ws_api_response(&text, expected_id) {
                Some(true) => return Ok(()),
                Some(false) => anyhow::bail!("binance websocket api error: {text}"),
                None => continue,
            },
            _ => {}
        }
    }
}

#[derive(Debug, Deserialize)]
struct UserDataEventEnvelope {
    #[serde(rename = "e")]
    event_type: String,
}

#[derive(Debug, Deserialize)]
struct ExecutionReport {
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "i")]
    exchange_order_id: i64,
    #[serde(rename = "X")]
    order_status: String,
    /// 累计成交量(不是本次推送的增量)。
    #[serde(rename = "z")]
    cumulative_filled_qty: Decimal,
    /// 累计成交额，配合 cumulative_filled_qty 算均价。
    #[serde(rename = "Z")]
    cumulative_quote_qty: Decimal,
    /// 本次成交(增量)的手续费，配合 `N` 币种一起使用；非成交类事件(如纯状态
    /// 变更)可能不带这两个字段，落到 `None`。
    #[serde(rename = "n", default)]
    commission: Option<Decimal>,
    #[serde(rename = "N", default)]
    commission_asset: Option<String>,
    #[serde(rename = "E")]
    event_time_ms: u64,
}

/// 解析一条 User Data Stream 消息，只关心 `executionReport` 事件(其它如
/// `outboundAccountPosition`/`balanceUpdate`，以及 WS API 请求响应，直接忽略)。
/// WS API 订阅推送把原始事件包了一层 `{"subscriptionId":N,"event":{...}}`，
/// 先取出内层 `event`(取不到就把原文当兜底，兼容旧格式测试数据)。纯函数，
/// 不依赖真实 WebSocket 连接，便于单元测试。
fn parse_execution_report(text: &str, venue: &Venue) -> Option<ExchangeOrderUpdate> {
    let raw: serde_json::Value = match serde_json::from_str(text) {
        Ok(raw) => raw,
        Err(err) => {
            warn!("failed to parse binance user data stream message: {err}");
            return None;
        }
    };
    let event = raw.get("event").cloned().unwrap_or(raw);

    let envelope: UserDataEventEnvelope = serde_json::from_value(event.clone()).ok()?;
    if envelope.event_type != "executionReport" {
        return None;
    }
    let report: ExecutionReport = match serde_json::from_value(event) {
        Ok(report) => report,
        Err(err) => {
            warn!("failed to parse binance executionReport: {err}");
            return None;
        }
    };

    let avg_price = (report.cumulative_filled_qty > Decimal::ZERO)
        .then(|| report.cumulative_quote_qty / report.cumulative_filled_qty);

    Some(ExchangeOrderUpdate {
        venue: venue.clone(),
        client_order_id: Some(report.client_order_id).filter(|s| !s.is_empty()),
        exchange_order_id: Some(report.exchange_order_id.to_string()),
        status: map_status(&report.order_status),
        filled_qty: report.cumulative_filled_qty,
        avg_price,
        fee: report.commission,
        fee_asset: report.commission_asset.filter(|s| !s.is_empty()),
        ts_ms: report.event_time_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{KeyPair, UnparsedPublicKey};

    fn generate_test_pem() -> (String, Ed25519KeyPair) {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("load pkcs8");
        let body = base64_engine.encode(pkcs8.as_ref());
        let pem = format!("-----BEGIN PRIVATE KEY-----\n{body}\n-----END PRIVATE KEY-----\n");
        (pem, key_pair)
    }

    #[test]
    fn loads_ed25519_key_from_pem_roundtrip() {
        let (pem, _) = generate_test_pem();
        let loaded = load_ed25519_key(&pem).expect("should load key from PEM");
        assert_eq!(loaded.public_key().as_ref().len(), 32);
    }

    #[test]
    fn signs_payload_verifiable_by_public_key() {
        let (pem, key_pair) = generate_test_pem();
        let loaded = load_ed25519_key(&pem).expect("should load key from PEM");

        let payload = "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=0.01&timestamp=1700000000000";
        let signature_b64 = sign_ed25519(&loaded, payload);
        let signature = base64_engine.decode(signature_b64).expect("valid base64 signature");

        let public_key = UnparsedPublicKey::new(&ring::signature::ED25519, key_pair.public_key().as_ref());
        public_key
            .verify(payload.as_bytes(), &signature)
            .expect("signature should verify against the same keypair's public key");
    }

    #[test]
    fn builds_query_string_in_insertion_order() {
        let params = vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
            ("side".to_string(), "BUY".to_string()),
        ];
        assert_eq!(build_query_string(&params), "symbol=BTCUSDT&side=BUY");
    }

    #[test]
    fn build_query_string_percent_encodes_reserved_characters_in_values() {
        let params = vec![("signature".to_string(), "a+b/c=d".to_string())];
        assert_eq!(build_query_string(&params), "signature=a%2Bb%2Fc%3Dd");
    }

    #[test]
    fn maps_side_to_binance_string() {
        assert_eq!(map_side(OrderSide::Buy), "BUY");
        assert_eq!(map_side(OrderSide::Sell), "SELL");
    }

    #[test]
    fn maps_status_strings() {
        assert_eq!(map_status("FILLED"), OrderStatus::Filled);
        assert_eq!(map_status("PARTIALLY_FILLED"), OrderStatus::PartiallyFilled);
        assert_eq!(map_status("NEW"), OrderStatus::New);
        assert_eq!(map_status("REJECTED"), OrderStatus::Rejected);
        assert_eq!(map_status("EXPIRED"), OrderStatus::Expired);
    }

    #[test]
    fn parses_order_response_with_avg_price() {
        let text = r#"{
            "symbol": "BTCUSDT",
            "orderId": 28,
            "clientOrderId": "abc",
            "transactTime": 1507725176595,
            "price": "0.00000000",
            "origQty": "10.00000000",
            "executedQty": "10.00000000",
            "cummulativeQuoteQty": "1000.00000000",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "SELL"
        }"#;
        let result = parse_order_response(text).expect("should parse");
        assert_eq!(result.order_id, "28");
        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_qty, "10.00000000".parse().unwrap());
        assert_eq!(result.avg_price, Some("100".parse().unwrap()));
        assert_eq!(result.fee, None);
        assert_eq!(result.fee_asset, None);
    }

    #[test]
    fn parses_order_response_with_single_asset_fills_as_real_fee() {
        let text = r#"{
            "symbol": "BTCUSDT",
            "orderId": 28,
            "clientOrderId": "abc",
            "transactTime": 1507725176595,
            "price": "0.00000000",
            "origQty": "10.00000000",
            "executedQty": "10.00000000",
            "cummulativeQuoteQty": "1000.00000000",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "SELL",
            "fills": [
                {"price": "100", "qty": "6", "commission": "0.006", "commissionAsset": "BNB", "tradeId": 1},
                {"price": "100", "qty": "4", "commission": "0.004", "commissionAsset": "BNB", "tradeId": 2}
            ]
        }"#;
        let result = parse_order_response(text).expect("should parse");
        assert_eq!(result.fee, Some("0.010".parse().unwrap()));
        assert_eq!(result.fee_asset, Some("BNB".to_string()));
    }

    #[test]
    fn parses_order_response_with_mixed_asset_fills_falls_back_to_none() {
        let text = r#"{
            "symbol": "BTCUSDT",
            "orderId": 28,
            "clientOrderId": "abc",
            "transactTime": 1507725176595,
            "price": "0.00000000",
            "origQty": "10.00000000",
            "executedQty": "10.00000000",
            "cummulativeQuoteQty": "1000.00000000",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "SELL",
            "fills": [
                {"price": "100", "qty": "6", "commission": "0.006", "commissionAsset": "BNB", "tradeId": 1},
                {"price": "100", "qty": "4", "commission": "0.4", "commissionAsset": "USDT", "tradeId": 2}
            ]
        }"#;
        let result = parse_order_response(text).expect("should parse");
        assert_eq!(result.fee, None);
        assert_eq!(result.fee_asset, None);
    }

    #[test]
    fn parse_order_response_surfaces_error_response() {
        let text = r#"{"code":-2010,"msg":"Account has insufficient balance for requested action."}"#;
        let err = parse_order_response(text).unwrap_err();
        assert!(err.to_string().contains("insufficient balance"));
    }

    /// `GET /api/v3/order`(query_order 用的接口)不带 `fills` 字段，也没有
    /// `avgPrice` 字段——确认这种响应形状也能正确解析出成交量/均价(均价靠
    /// cummulativeQuoteQty/executedQty 算，不依赖 fills)。
    #[test]
    fn parses_get_order_response_without_fills_field() {
        let text = r#"{
            "symbol": "BTCUSDT",
            "orderId": 28,
            "clientOrderId": "abc",
            "price": "0.00000000",
            "origQty": "10.00000000",
            "executedQty": "10.00000000",
            "cummulativeQuoteQty": "500000.00000000",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "BUY",
            "time": 1507725176595,
            "updateTime": 1507725176595,
            "isWorking": true
        }"#;
        let result = parse_order_response(text).expect("should parse");
        assert_eq!(result.order_id, "28");
        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_qty, Decimal::new(10, 0));
        assert_eq!(result.avg_price, Some(Decimal::new(50000, 0)));
        assert_eq!(result.fee, None);
        assert_eq!(result.fee_asset, None);
    }

    /// `GET /api/v3/myTrades`(query_order 内部为补手续费而调用的接口)返回的是
    /// 成交明细数组，字段名和下单响应里的 `fills` 一样，按 commissionAsset
    /// 汇总求和即可。
    #[test]
    fn parses_my_trades_response_and_sums_commission() {
        let text = r#"[
            {"symbol":"BTCUSDT","id":1,"orderId":28,"orderListId":-1,"price":"50000.00","qty":"6.00000000","quoteQty":"300000.00","commission":"0.006","commissionAsset":"BNB","time":1507725176595,"isBuyer":true,"isMaker":false,"isBestMatch":true},
            {"symbol":"BTCUSDT","id":2,"orderId":28,"orderListId":-1,"price":"50000.00","qty":"4.00000000","quoteQty":"200000.00","commission":"0.004","commissionAsset":"BNB","time":1507725176595,"isBuyer":true,"isMaker":false,"isBestMatch":true}
        ]"#;
        let (fee, fee_asset) = parse_my_trades_response(text).expect("should parse");
        assert_eq!(fee, Some("0.010".parse().unwrap()));
        assert_eq!(fee_asset, Some("BNB".to_string()));
    }

    #[test]
    fn parse_my_trades_response_surfaces_error_response() {
        let text = r#"{"code":-1121,"msg":"Invalid symbol."}"#;
        let err = parse_my_trades_response(text).unwrap_err();
        assert!(err.to_string().contains("Invalid symbol"));
    }

    #[test]
    fn percent_encodes_reserved_characters() {
        assert_eq!(percent_encode("abcXYZ019-_.~"), "abcXYZ019-_.~");
        assert_eq!(percent_encode("a+b/c=d"), "a%2Bb%2Fc%3Dd");
    }

    #[test]
    fn matches_ws_api_response_success() {
        let text = r#"{"id":"logon","status":200,"result":{}}"#;
        assert_eq!(match_ws_api_response(text, "logon"), Some(true));
    }

    #[test]
    fn matches_ws_api_response_failure() {
        let text = r#"{"id":"logon","status":400,"error":{"code":-1022,"msg":"Signature for this request is not valid."}}"#;
        assert_eq!(match_ws_api_response(text, "logon"), Some(false));
    }

    #[test]
    fn ignores_ws_api_response_with_different_id() {
        let text = r#"{"id":"sub","status":200,"result":{}}"#;
        assert_eq!(match_ws_api_response(text, "logon"), None);
    }

    #[test]
    fn ignores_malformed_ws_api_response() {
        assert_eq!(match_ws_api_response("not json", "logon"), None);
    }

    #[test]
    fn parses_execution_report_partial_fill() {
        let venue = Venue::new("binance");
        let text = r#"{
            "subscriptionId": 0,
            "event": {
                "e": "executionReport",
                "E": 1700000000123,
                "s": "BTCUSDT",
                "c": "ORD-000000000001",
                "S": "BUY",
                "o": "MARKET",
                "X": "PARTIALLY_FILLED",
                "i": 123456,
                "z": "0.40000000",
                "Z": "16000.00000000"
            }
        }"#;
        let update = parse_execution_report(text, &venue).expect("should parse");
        assert_eq!(update.venue, venue);
        assert_eq!(update.client_order_id, Some("ORD-000000000001".to_string()));
        assert_eq!(update.exchange_order_id, Some("123456".to_string()));
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
        assert_eq!(update.filled_qty, "0.40000000".parse().unwrap());
        assert_eq!(update.avg_price, Some("40000".parse().unwrap()));
        assert_eq!(update.ts_ms, 1700000000123);
        assert_eq!(update.fee, None);
        assert_eq!(update.fee_asset, None);
    }

    #[test]
    fn parses_execution_report_with_commission() {
        let venue = Venue::new("binance");
        let text = r#"{
            "subscriptionId": 0,
            "event": {
                "e": "executionReport",
                "E": 1700000000123,
                "s": "BTCUSDT",
                "c": "ORD-000000000001",
                "S": "BUY",
                "o": "MARKET",
                "X": "PARTIALLY_FILLED",
                "i": 123456,
                "z": "0.40000000",
                "Z": "16000.00000000",
                "n": "0.0004",
                "N": "BTC"
            }
        }"#;
        let update = parse_execution_report(text, &venue).expect("should parse");
        assert_eq!(update.fee, Some("0.0004".parse().unwrap()));
        assert_eq!(update.fee_asset, Some("BTC".to_string()));
    }

    #[test]
    fn parses_execution_report_new_order_without_fill() {
        let venue = Venue::new("binance");
        let text = r#"{
            "subscriptionId": 0,
            "event": {
                "e": "executionReport",
                "E": 1700000000000,
                "s": "BTCUSDT",
                "c": "ORD-000000000002",
                "S": "BUY",
                "o": "MARKET",
                "X": "NEW",
                "i": 654321,
                "z": "0.00000000",
                "Z": "0.00000000"
            }
        }"#;
        let update = parse_execution_report(text, &venue).expect("should parse");
        assert_eq!(update.status, OrderStatus::New);
        assert_eq!(update.filled_qty, Decimal::ZERO);
        assert_eq!(update.avg_price, None);
        assert_eq!(update.fee, None);
        assert_eq!(update.fee_asset, None);
    }

    #[test]
    fn ignores_non_execution_report_events() {
        let venue = Venue::new("binance");
        let text = r#"{"subscriptionId":0,"event":{"e":"outboundAccountPosition","E":1700000000000,"u":1700000000000,"B":[]}}"#;
        assert!(parse_execution_report(text, &venue).is_none());
    }

    #[test]
    fn ignores_ws_api_response_messages_in_execution_report_parsing() {
        let venue = Venue::new("binance");
        let text = r#"{"id":"logon","status":200,"result":{}}"#;
        assert!(parse_execution_report(text, &venue).is_none());
    }

    #[test]
    fn ignores_malformed_user_data_stream_message() {
        let venue = Venue::new("binance");
        assert!(parse_execution_report("not json", &venue).is_none());
    }
}
