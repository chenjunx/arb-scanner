use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use futures_util::StreamExt;
use log::{debug, warn};
use ring::signature::Ed25519KeyPair;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::accounting::provider::FundingIncomeRecord;
use crate::market_data::now_ms;
use crate::net::connect_tcp;
use crate::order_manager::stream::{ExchangeOrderUpdate, OrderStreamSource};
use crate::order_manager::OrderManager;
use crate::types::{Symbol, Venue};

use super::OrderProvider;
use super::types::{MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const MAINNET_HOST: &str = "https://fapi.binance.com";
const TESTNET_HOST: &str = "https://testnet.binancefuture.com";
const MAINNET_WS_HOST: &str = "fstream.binance.com";
const TESTNET_WS_HOST: &str = "stream.binancefuture.com";
const WS_PORT: u16 = 443;
// 5000 曾在并发下三条腿一起发请求时因调度延迟触发过一次 -1022(签名无效)，
// 实际是时间戳超出 recvWindow 但被币安网关报成了签名错误，调大留出冗余。
const RECV_WINDOW_MS: u64 = 10_000;
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
// listenKey 60 分钟不活动会过期，30 分钟续期一次留足冗余。
const LISTEN_KEY_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// 币安 USDT-M 永续合约下单(执行层)客户端：查询交易对精度限制、提交市价单。
/// 签名方式和 `order::binance::BinanceOrderProvider`(现货)一致，用 Ed25519，
/// 凭证也复用同一套环境变量(同一个 API Key 上勾选现货+合约交易权限即可)。
///
/// 只支持币安账户默认的单向持仓模式(One-way Mode)：不传 `positionSide`。
/// 如果账户被手动切换成双向持仓模式(Hedge Mode)，下单会报错，需要用户自行
/// 确保账户模式和这里的假设一致。
pub struct BinanceFuturesOrderProvider {
    venue: Venue,
    api_key: String,
    key_pair: Ed25519KeyPair,
    host: &'static str,
    http: reqwest::Client,
}

impl BinanceFuturesOrderProvider {
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

    /// 从环境变量读取凭证并构造实例，和 `order::binance::BinanceOrderProvider::from_env`
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
            .context("binance futures order request failed")?;
        let text = resp.text().await.context("failed to read binance futures order response body")?;
        Ok(text)
    }

    /// 拉取该 symbol 从 `start_time_ms`(含)起的资金费结算流水，按 `tran_id`
    /// 升序返回，供 `accounting::FundingFeeTracker` 轮询使用。
    /// `GET /fapi/v1/income?incomeType=FUNDING_FEE`，单次最多返回 1000 条。
    pub(crate) async fn income_history(
        &self,
        symbol: &Symbol,
        start_time_ms: Option<u64>,
    ) -> anyhow::Result<Vec<FundingIncomeRecord>> {
        let mut params = vec![
            ("symbol".to_string(), binance_symbol(symbol)),
            ("incomeType".to_string(), "FUNDING_FEE".to_string()),
            ("limit".to_string(), "1000".to_string()),
        ];
        if let Some(start) = start_time_ms {
            params.push(("startTime".to_string(), start.to_string()));
        }
        let text = self.signed_request(reqwest::Method::GET, "/fapi/v1/income", params).await?;
        parse_funding_income(&text, symbol)
    }
}

fn binance_symbol(symbol: &Symbol) -> String {
    format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
}

/// 从已知的 symbols 列表构建 "BTCUSDT" -> Symbol 反查表，供解析 User Data
/// Stream 推送时把交易所的拼接式 symbol 还原成结构化的 `Symbol`。
fn binance_futures_symbol_map(symbols: &[Symbol]) -> HashMap<String, Symbol> {
    symbols.iter().map(|s| (binance_symbol(s), s.clone())).collect()
}

#[async_trait]
impl OrderProvider for BinanceFuturesOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let OrderAmount::Base(quantity) = req.amount else {
            anyhow::bail!("{} does not support quote-amount market orders", self.venue());
        };
        let mut params = vec![
            ("symbol".to_string(), binance_symbol(&req.symbol)),
            ("side".to_string(), map_side(req.side).to_string()),
            ("type".to_string(), "MARKET".to_string()),
            ("quantity".to_string(), quantity.to_string()),
            // 默认 MARKET 单只同步返回 ACK(无成交信息)，必须显式要求 RESULT
            // 才能在下单响应里同步拿到 avgPrice/executedQty。
            ("newOrderRespType".to_string(), "RESULT".to_string()),
        ];
        if let Some(client_order_id) = &req.client_order_id {
            params.push(("newClientOrderId".to_string(), client_order_id.clone()));
        }
        let text = self.signed_request(reqwest::Method::POST, "/fapi/v1/order", params).await?;
        parse_order_response(&text)
    }

    /// `GET /fapi/v1/order` 按 orderId 查询，响应字段(orderId/status/
    /// executedQty/avgPrice)和下单 RESULT 响应一致，直接复用
    /// `parse_order_response`——但这个接口本身不带手续费(和现货一样，订单
    /// 汇总查询不给逐笔成交明细)，成交了就再查一次 `GET /fapi/v1/userTrades`
    /// 补真实手续费；那一步失败不影响这里返回订单状态本身，只是手续费留空。
    /// 用于 `wait_for_fill` 的 REST 兜底核对，以及 `reconcile-order` 命令。
    async fn query_order(&self, symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<OrderResult> {
        let params = vec![
            ("symbol".to_string(), binance_symbol(symbol)),
            ("orderId".to_string(), exchange_order_id.to_string()),
        ];
        let text = self.signed_request(reqwest::Method::GET, "/fapi/v1/order", params).await?;
        let mut result = parse_order_response(&text)?;

        if result.fee.is_none() && result.filled_qty > Decimal::ZERO {
            match self.query_order_fee(symbol, exchange_order_id).await {
                Ok((fee, fee_asset)) => {
                    result.fee = fee;
                    result.fee_asset = fee_asset;
                }
                Err(err) => {
                    warn!(
                        "query_order: 补查 userTrades 手续费失败(order_id={exchange_order_id})，手续费留空交给估算兜底: {err:#}"
                    );
                }
            }
        }

        Ok(result)
    }

    /// `GET /fapi/v1/ticker/price?symbol={ASSET}USDT`，公开行情接口不需要签名。
    async fn quote_usdt_price(&self, asset: &str) -> anyhow::Result<Decimal> {
        let symbol = format!("{}USDT", asset.to_ascii_uppercase());
        let url = format!("{}/fapi/v1/ticker/price?symbol={symbol}", self.host);
        crate::ratelimit::throttle(self.host).await;
        let resp = self.http.get(&url).send().await.context("binance futures ticker price request failed")?;
        let text = resp.text().await.context("failed to read binance futures ticker price response body")?;
        parse_ticker_price(&text)
    }
}

impl BinanceFuturesOrderProvider {
    /// `GET /fapi/v1/userTrades` 按 orderId 查这笔订单的逐笔成交明细，汇总出
    /// 真实手续费。只在 `query_order` 里、订单确实有成交但拿不到手续费时调用。
    async fn query_order_fee(
        &self,
        symbol: &Symbol,
        exchange_order_id: &str,
    ) -> anyhow::Result<(Option<Decimal>, Option<String>)> {
        let params = vec![
            ("symbol".to_string(), binance_symbol(symbol)),
            ("orderId".to_string(), exchange_order_id.to_string()),
        ];
        let text = self.signed_request(reqwest::Method::GET, "/fapi/v1/userTrades", params).await?;
        parse_user_trades_response(&text)
    }
}

/// 币安 U 本位合约 User Data Stream 客户端：管理 listenKey 生命周期、维护私有
/// 订单 WebSocket 连接，把 `ORDER_TRADE_UPDATE` 推送转换成 `ExchangeOrderUpdate`。
/// listenKey 的获取/续期只需要 `X-MBX-APIKEY` 头，不需要签名，和现货一致。
pub struct BinanceFuturesUserDataStream {
    venue: Venue,
    api_key: String,
    host: &'static str,
    ws_host: &'static str,
    ws_port: u16,
    http: reqwest::Client,
    proxy: Option<String>,
    symbols: Vec<Symbol>,
}

impl BinanceFuturesUserDataStream {
    pub fn new(
        venue: Venue,
        api_key: String,
        testnet: bool,
        proxy: Option<&str>,
        symbols: Vec<Symbol>,
    ) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        let (host, ws_host) = if testnet { (TESTNET_HOST, TESTNET_WS_HOST) } else { (MAINNET_HOST, MAINNET_WS_HOST) };
        Ok(Self {
            venue,
            api_key,
            host,
            ws_host,
            ws_port: WS_PORT,
            http,
            proxy: proxy.map(str::to_string),
            symbols,
        })
    }

    /// 从环境变量读取凭证，和 `BinanceFuturesOrderProvider::from_env` 复用同一个
    /// `BINANCE_API_KEY`（listenKey 接口不需要 API secret/签名）。
    pub fn from_env(venue: Venue, testnet: bool, proxy: Option<&str>, symbols: Vec<Symbol>) -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY").context("BINANCE_API_KEY not set")?;
        Self::new(venue, api_key, testnet, proxy, symbols)
    }

    async fn create_listen_key(&self) -> anyhow::Result<String> {
        crate::ratelimit::throttle(self.host).await;
        let resp = self
            .http
            .post(format!("{}/fapi/v1/listenKey", self.host))
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("failed to create binance futures listenKey")?;
        let text = resp.text().await.context("failed to read binance futures listenKey response")?;
        parse_listen_key(&text)
    }

    async fn keepalive_listen_key(&self, listen_key: &str) -> anyhow::Result<()> {
        crate::ratelimit::throttle(self.host).await;
        self.http
            .put(format!("{}/fapi/v1/listenKey?listenKey={}", self.host, listen_key))
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("failed to keepalive binance futures listenKey")?;
        Ok(())
    }

    async fn connect(&self, listen_key: &str) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(self.ws_host, self.ws_port, self.proxy.as_deref()).await?;
        // 2026-04-23 起币安把私有推流迁到了 `/private` 路由前缀下，不带前缀的
        // 旧连接虽然还能握手成功，但收不到任何 ORDER_TRADE_UPDATE 消息(静默失效)。
        let url = format!("wss://{}:{}/private/ws/{}", self.ws_host, self.ws_port, listen_key);
        let (ws, _) = tokio_tungstenite::client_async_tls(url, tcp)
            .await
            .context("binance futures user data stream handshake failed")?;
        Ok(ws)
    }
}

impl OrderStreamSource for BinanceFuturesUserDataStream {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, order_manager: Arc<OrderManager>) -> crate::order_manager::stream::StreamHandle {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let symbol_map = binance_futures_symbol_map(&self.symbols);
        let join = tokio::spawn(async move {
            let mut backoff = MIN_BACKOFF;
            let mut ready_tx = Some(ready_tx);

            loop {
                let listen_key = match self.create_listen_key().await {
                    Ok(key) => key,
                    Err(err) => {
                        warn!(
                            "binance futures user data stream: failed to create listenKey for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                let mut ws = match self.connect(&listen_key).await {
                    Ok(ws) => ws,
                    Err(err) => {
                        warn!(
                            "binance futures user data stream connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };
                debug!("binance futures user data stream connected for venue={}", self.venue);
                backoff = MIN_BACKOFF;
                if let Some(ready_tx) = ready_tx.take() {
                    let _ = ready_tx.send(());
                }

                let mut keepalive_ticker = tokio::time::interval(LISTEN_KEY_KEEPALIVE_INTERVAL);
                // 第一次 tick 立即完成(interval 语义)，先消费掉避免刚连上就续期一次。
                keepalive_ticker.tick().await;

                loop {
                    tokio::select! {
                        _ = keepalive_ticker.tick() => {
                            if let Err(err) = self.keepalive_listen_key(&listen_key).await {
                                warn!(
                                    "binance futures user data stream: listenKey keepalive failed for venue={} err={err:#}",
                                    self.venue
                                );
                            }
                        }
                        msg = ws.next() => {
                            let Some(msg) = msg else { break };
                            let msg = match msg {
                                Ok(msg) => msg,
                                Err(err) => {
                                    warn!("binance futures user data stream error for venue={} err={err}", self.venue);
                                    break;
                                }
                            };
                            let Message::Text(text) = msg else { continue };
                            let Some(update) = parse_order_trade_update(&text, &self.venue, &symbol_map) else { continue };
                            order_manager.handle_exchange_update(update).await;
                        }
                    }
                }

                warn!(
                    "binance futures user data stream disconnected for venue={}, reconnecting in {:?}",
                    self.venue, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        });
        crate::order_manager::stream::StreamHandle { join, ready: ready_rx }
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build binance futures http client")
}

/// 按插入顺序拼接 `k=v&k=v...`(value 做 percent-encode，签名必须覆盖和实际
/// 发送完全一致的 query string——base64 编码的 `signature` 本身也要在拼进
/// URL 时 percent-encode，否则其中的 `+` 会被币安网关当 query string 里的
/// 空格解析，导致签名校验失败(`-1022`)，实测已复现)。
fn build_query_string(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{k}={}", percent_encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// 只保留 RFC 3986 unreserved 字符，其余一律 `%XX` 转义——用于 query string
/// 里可能出现 `+`/`/`/`=` 等保留字符的字段(尤其是 base64 编码的签名)。
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
        "EXPIRED" | "EXPIRED_IN_MATCH" | "CANCELED" => OrderStatus::Expired,
        _ => OrderStatus::New,
    }
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    code: i64,
    msg: String,
}

#[derive(Debug, Deserialize)]
struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

fn parse_listen_key(text: &str) -> anyhow::Result<String> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let resp: ListenKeyResponse =
        serde_json::from_str(text).with_context(|| format!("failed to parse binance futures listenKey response, raw body: {text}"))?;
    Ok(resp.listen_key)
}

#[derive(Debug, Deserialize)]
struct UserDataEventEnvelope {
    #[serde(rename = "e")]
    event_type: String,
}

#[derive(Debug, Deserialize)]
struct OrderTradeUpdateOrder {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "i")]
    exchange_order_id: i64,
    #[serde(rename = "X")]
    order_status: String,
    /// 累计成交量(不是本次推送的增量)。
    #[serde(rename = "z")]
    cumulative_filled_qty: Decimal,
    /// 订单累计均价，合约推送直接给出，不用像现货那样用成交额/成交量反算。
    #[serde(rename = "ap")]
    avg_price: Decimal,
    /// 本次成交(增量)的手续费，配合 `N` 币种一起使用；非成交类事件(如纯状态
    /// 变更)可能不带这两个字段，落到 `None`。
    #[serde(rename = "n", default)]
    commission: Option<Decimal>,
    #[serde(rename = "N", default)]
    commission_asset: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrderTradeUpdateEvent {
    #[serde(rename = "E")]
    event_time_ms: u64,
    #[serde(rename = "o")]
    order: OrderTradeUpdateOrder,
}

/// 解析一条 User Data Stream 消息，只关心 `ORDER_TRADE_UPDATE` 事件(其它如
/// `ACCOUNT_UPDATE`/`MARGIN_CALL` 直接忽略)。纯函数，不依赖真实 WebSocket
/// 连接，便于单元测试。
fn parse_order_trade_update(
    text: &str,
    venue: &Venue,
    symbol_map: &HashMap<String, Symbol>,
) -> Option<ExchangeOrderUpdate> {
    let envelope: UserDataEventEnvelope = match serde_json::from_str(text) {
        Ok(envelope) => envelope,
        Err(err) => {
            warn!("failed to parse binance futures user data stream message: {err}");
            return None;
        }
    };
    if envelope.event_type != "ORDER_TRADE_UPDATE" {
        return None;
    }
    let event: OrderTradeUpdateEvent = match serde_json::from_str(text) {
        Ok(event) => event,
        Err(err) => {
            warn!("failed to parse binance futures ORDER_TRADE_UPDATE: {err}");
            return None;
        }
    };
    let order = event.order;
    let Some(symbol) = symbol_map.get(&order.symbol) else {
        warn!("binance futures user data stream: unmapped symbol {}, dropping update", order.symbol);
        return None;
    };
    let avg_price = (order.avg_price > Decimal::ZERO).then_some(order.avg_price);

    Some(ExchangeOrderUpdate {
        venue: venue.clone(),
        symbol: symbol.clone(),
        client_order_id: Some(order.client_order_id).filter(|s| !s.is_empty()),
        exchange_order_id: Some(order.exchange_order_id.to_string()),
        status: map_status(&order.order_status),
        filled_qty: order.cumulative_filled_qty,
        avg_price,
        fee: order.commission,
        fee_asset: order.commission_asset.filter(|s| !s.is_empty()),
        ts_ms: event.event_time_ms,
    })
}

#[derive(Debug, Deserialize)]
struct OrderResponse {
    #[serde(rename = "orderId")]
    order_id: i64,
    status: String,
    #[serde(rename = "executedQty")]
    executed_qty: Decimal,
    #[serde(rename = "avgPrice", default)]
    avg_price: Decimal,
}

fn parse_order_response(text: &str) -> anyhow::Result<OrderResult> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let resp: OrderResponse = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance futures order response, raw body: {text}"))?;
    let avg_price = (resp.avg_price > Decimal::ZERO).then_some(resp.avg_price);

    Ok(OrderResult {
        order_id: resp.order_id.to_string(),
        status: map_status(&resp.status),
        filled_qty: resp.executed_qty,
        avg_price,
        // 合约下单响应(RESULT)不带手续费信息，也没有对应的私有流可补。
        fee: None,
        fee_asset: None,
    })
}

#[derive(Debug, Deserialize)]
struct TradeFill {
    commission: Decimal,
    #[serde(rename = "commissionAsset")]
    commission_asset: String,
}

/// 按 `commissionAsset` 分组求和；只有单一币种时才认为是可信的单一手续费值
/// 返回 `Some`，混合多币种时返回 `None`，不做加权处理(和现货 `binance.rs`
/// 里的同名逻辑一致)。
fn sum_fee_by_asset(fills: &[TradeFill]) -> (Option<Decimal>, Option<String>) {
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

/// `GET /fapi/v1/userTrades` 响应是一个成交明细数组，`commission`/
/// `commissionAsset` 字段名和现货 `myTrades` 一致。
fn parse_user_trades_response(text: &str) -> anyhow::Result<(Option<Decimal>, Option<String>)> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let fills: Vec<TradeFill> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance futures userTrades response, raw body: {text}"))?;
    Ok(sum_fee_by_asset(&fills))
}

#[derive(Debug, Deserialize)]
struct TickerPriceResponse {
    price: Decimal,
}

fn parse_ticker_price(text: &str) -> anyhow::Result<Decimal> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let resp: TickerPriceResponse = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance futures ticker price response, raw body: {text}"))?;
    Ok(resp.price)
}

#[derive(Debug, Deserialize)]
struct IncomeEntry {
    #[serde(rename = "incomeType")]
    income_type: String,
    income: Decimal,
    time: u64,
    #[serde(rename = "tranId")]
    tran_id: i64,
}

/// `symbol` 来自调用方(而不是响应里的 `symbol` 字段)，因为查询已经按 symbol
/// 过滤过；响应可能混入非 `FUNDING_FEE` 的条目(理论上不会，因为请求带了
/// `incomeType` 过滤，这里再过滤一遍是防御性的)。
fn parse_funding_income(text: &str, symbol: &Symbol) -> anyhow::Result<Vec<FundingIncomeRecord>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let entries: Vec<IncomeEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse binance futures income response, raw body: {text}"))?;
    Ok(entries
        .into_iter()
        .filter(|e| e.income_type == "FUNDING_FEE")
        .map(|e| FundingIncomeRecord {
            symbol: symbol.clone(),
            income: e.income,
            time_ms: e.time,
            tran_id: e.tran_id,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{KeyPair, UnparsedPublicKey};

    fn map_with(symbol: Symbol) -> HashMap<String, Symbol> {
        let mut map = HashMap::new();
        map.insert(binance_symbol(&symbol), symbol);
        map
    }

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
        assert_eq!(map_status("CANCELED"), OrderStatus::Expired);
    }

    #[test]
    fn parses_order_response_with_avg_price() {
        let text = r#"{
            "orderId": 28,
            "symbol": "BTCUSDT",
            "status": "FILLED",
            "clientOrderId": "abc",
            "price": "0",
            "avgPrice": "100.00000",
            "origQty": "10",
            "executedQty": "10",
            "cumQuote": "1000",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "SELL",
            "positionSide": "BOTH"
        }"#;
        let result = parse_order_response(text).expect("should parse");
        assert_eq!(result.order_id, "28");
        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_qty, "10".parse().unwrap());
        assert_eq!(result.avg_price, Some("100.00000".parse().unwrap()));
    }

    #[test]
    fn parse_order_response_surfaces_error_response() {
        let text = r#"{"code":-2019,"msg":"Margin is insufficient."}"#;
        let err = parse_order_response(text).unwrap_err();
        assert!(err.to_string().contains("Margin is insufficient"));
    }

    /// `GET /fapi/v1/userTrades`(query_order 内部为补手续费而调用的接口)返回
    /// 的是成交明细数组，字段名和现货 myTrades 一样，按 commissionAsset 汇总
    /// 求和即可。
    #[test]
    fn parses_user_trades_response_and_sums_commission() {
        let text = r#"[
            {"symbol":"BTCUSDT","id":1,"orderId":28,"side":"SELL","price":"100.0","qty":"6","realizedPnl":"0","marginAsset":"USDT","quoteQty":"600","commission":"0.006","commissionAsset":"USDT","time":1,"positionSide":"BOTH","buyer":false,"maker":false},
            {"symbol":"BTCUSDT","id":2,"orderId":28,"side":"SELL","price":"100.0","qty":"4","realizedPnl":"0","marginAsset":"USDT","quoteQty":"400","commission":"0.004","commissionAsset":"USDT","time":1,"positionSide":"BOTH","buyer":false,"maker":false}
        ]"#;
        let (fee, fee_asset) = parse_user_trades_response(text).expect("should parse");
        assert_eq!(fee, Some("0.010".parse().unwrap()));
        assert_eq!(fee_asset, Some("USDT".to_string()));
    }

    #[test]
    fn parse_user_trades_response_surfaces_error_response() {
        let text = r#"{"code":-1121,"msg":"Invalid symbol."}"#;
        let err = parse_user_trades_response(text).unwrap_err();
        assert!(err.to_string().contains("Invalid symbol"));
    }

    #[test]
    fn parses_listen_key_response() {
        let text = r#"{"listenKey":"abc123"}"#;
        assert_eq!(parse_listen_key(text).expect("should parse"), "abc123");
    }

    #[test]
    fn parse_listen_key_surfaces_error_response() {
        let text = r#"{"code":-2014,"msg":"API-key format invalid."}"#;
        let err = parse_listen_key(text).unwrap_err();
        assert!(err.to_string().contains("API-key format invalid"));
    }

    #[test]
    fn parses_order_trade_update_partial_fill() {
        let venue = Venue::new("binance_futures");
        let symbol = Symbol::new("BTC", "USDT");
        let map = map_with(symbol.clone());
        let text = r#"{
            "e": "ORDER_TRADE_UPDATE",
            "E": 1700000000123,
            "o": {
                "s": "BTCUSDT",
                "c": "ORD-000000000001",
                "S": "SELL",
                "o": "MARKET",
                "X": "PARTIALLY_FILLED",
                "i": 123456,
                "z": "0.40000000",
                "ap": "40000",
                "l": "0.40000000"
            }
        }"#;
        let update = parse_order_trade_update(text, &venue, &map).expect("should parse");
        assert_eq!(update.venue, venue);
        assert_eq!(update.symbol, symbol);
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
    fn parses_order_trade_update_with_commission() {
        let venue = Venue::new("binance_futures");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let text = r#"{
            "e": "ORDER_TRADE_UPDATE",
            "E": 1700000000123,
            "o": {
                "s": "BTCUSDT",
                "c": "ORD-000000000001",
                "S": "SELL",
                "o": "MARKET",
                "X": "PARTIALLY_FILLED",
                "i": 123456,
                "z": "0.40000000",
                "ap": "40000",
                "l": "0.40000000",
                "n": "0.016",
                "N": "USDT"
            }
        }"#;
        let update = parse_order_trade_update(text, &venue, &map).expect("should parse");
        assert_eq!(update.fee, Some("0.016".parse().unwrap()));
        assert_eq!(update.fee_asset, Some("USDT".to_string()));
    }

    #[test]
    fn parses_order_trade_update_new_order_without_fill() {
        let venue = Venue::new("binance_futures");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let text = r#"{
            "e": "ORDER_TRADE_UPDATE",
            "E": 1700000000000,
            "o": {
                "s": "BTCUSDT",
                "c": "ORD-000000000002",
                "S": "SELL",
                "o": "MARKET",
                "X": "NEW",
                "i": 654321,
                "z": "0.00000000",
                "ap": "0",
                "l": "0"
            }
        }"#;
        let update = parse_order_trade_update(text, &venue, &map).expect("should parse");
        assert_eq!(update.status, OrderStatus::New);
        assert_eq!(update.filled_qty, Decimal::ZERO);
        assert_eq!(update.avg_price, None);
        assert_eq!(update.fee, None);
        assert_eq!(update.fee_asset, None);
    }

    #[test]
    fn ignores_non_order_trade_update_events() {
        let venue = Venue::new("binance_futures");
        let map = map_with(Symbol::new("BTC", "USDT"));
        let text = r#"{"e":"ACCOUNT_UPDATE","E":1700000000000,"a":{}}"#;
        assert!(parse_order_trade_update(text, &venue, &map).is_none());
    }

    #[test]
    fn ignores_order_trade_update_for_unmapped_symbol() {
        let venue = Venue::new("binance_futures");
        let map = map_with(Symbol::new("ETH", "USDT"));
        let text = r#"{
            "e": "ORDER_TRADE_UPDATE",
            "E": 1700000000123,
            "o": {
                "s": "BTCUSDT",
                "c": "ORD-000000000001",
                "S": "SELL",
                "o": "MARKET",
                "X": "PARTIALLY_FILLED",
                "i": 123456,
                "z": "0.40000000",
                "ap": "40000",
                "l": "0.40000000"
            }
        }"#;
        assert!(parse_order_trade_update(text, &venue, &map).is_none());
    }

    #[test]
    fn ignores_malformed_user_data_stream_message() {
        let venue = Venue::new("binance_futures");
        let map = map_with(Symbol::new("BTC", "USDT"));
        assert!(parse_order_trade_update("not json", &venue, &map).is_none());
    }

    #[test]
    fn parses_funding_income_entries_and_filters_other_income_types() {
        let text = r#"[
            {
                "symbol": "BTCUSDT",
                "incomeType": "FUNDING_FEE",
                "income": "-0.00500000",
                "asset": "USDT",
                "time": 1700000000000,
                "tranId": 111111,
                "tradeId": ""
            },
            {
                "symbol": "BTCUSDT",
                "incomeType": "COMMISSION",
                "income": "-0.10000000",
                "asset": "USDT",
                "time": 1700000001000,
                "tranId": 222222,
                "tradeId": "9999"
            },
            {
                "symbol": "BTCUSDT",
                "incomeType": "FUNDING_FEE",
                "income": "0.00800000",
                "asset": "USDT",
                "time": 1700000002000,
                "tranId": 333333,
                "tradeId": ""
            }
        ]"#;
        let symbol = Symbol::new("BTC", "USDT");
        let records = parse_funding_income(text, &symbol).expect("should parse");

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].symbol, symbol);
        assert_eq!(records[0].income, "-0.00500000".parse().unwrap());
        assert_eq!(records[0].time_ms, 1700000000000);
        assert_eq!(records[0].tran_id, 111111);
        assert_eq!(records[1].income, "0.00800000".parse().unwrap());
        assert_eq!(records[1].tran_id, 333333);
    }

    #[test]
    fn parses_ticker_price_response() {
        let text = r#"{"symbol":"BTCUSDT","price":"40000.50"}"#;
        let price = parse_ticker_price(text).expect("should parse");
        assert_eq!(price, "40000.50".parse().unwrap());
    }

    #[test]
    fn parse_ticker_price_surfaces_error_response() {
        let text = r#"{"code":-1121,"msg":"Invalid symbol."}"#;
        let err = parse_ticker_price(text).unwrap_err();
        assert!(err.to_string().contains("Invalid symbol"));
    }

    #[test]
    fn parse_funding_income_surfaces_error_response() {
        let text = r#"{"code":-1121,"msg":"Invalid symbol."}"#;
        let symbol = Symbol::new("BTC", "USDT");
        let err = parse_funding_income(text, &symbol).unwrap_err();
        assert!(err.to_string().contains("-1121"));
    }
}
