use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use futures_util::{SinkExt, StreamExt};
use log::{debug, error, warn};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::exchange_info::okx::{OkxResponse, okx_swap_inst_id, opt_decimal, parse_okx_data, parse_okx_swap_inst_id};
use crate::accounting::FundingIncomeRecord;
use crate::market_data::now_ms;
use crate::net::connect_tcp;
use crate::order_manager::OrderManager;
use crate::order_manager::stream::{ExchangeOrderUpdate, OrderStreamSource};
use crate::types::{Symbol, Venue};

use super::OrderProvider;
use super::contract_size::ContractSizeCache;
use super::types::{MarginMode, MarketOrderRequest, OrderAmount, OrderResult, OrderSide, OrderStatus};

const HOST: &str = "https://www.okx.com";
const WS_HOST: &str = "ws.okx.com";
const WS_PORT: u16 = 8443;
const WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/private";
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// OKX 30 秒没有任何收发就断开连接，定时发文本 `ping` 保活(服务端回 `pong`)。
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);
/// 资金费流水每页条数(OKX 上限 100)和最多翻几页。7 天内最密 1h 结算一次是 168 条，
/// 20 页绰绰有余；超过说明翻页逻辑出了问题，宁可报错也不返回不完整的结果。
const FUNDING_PAGE_LIMIT: &str = "100";
const FUNDING_MAX_PAGES: usize = 20;
/// `clOrdId` 只能是字母数字，1~32 位。
const CL_ORD_ID_MAX_LEN: usize = 32;

/// OKX USDT 本位永续下单客户端(`instType=SWAP`)。凭证读 `OKX_API_KEY` /
/// `OKX_API_SECRET` / `OKX_API_PASSPHRASE`。
///
/// 对外(下单请求、成交结果、持仓)的数量单位是**币**，和系统其它部分一致；
/// OKX 接口的单位是**张**(1 张 = `ctVal` 个币)，换算只在这一层做，见
/// [`ContractSizeCache`]。要求账户是单向持仓(`net`)模式；保证金模式
/// `tdMode` 每笔必填，由构造参数给出，必须和用户在交易所设置的一致。杠杆、
/// 持仓模式由用户预先设置，这里不修改。
pub struct OkxFuturesOrderProvider {
    venue: Venue,
    credentials: OkxCredentials,
    margin_mode: MarginMode,
    http: reqwest::Client,
    contract_sizes: Arc<ContractSizeCache>,
}

#[derive(Clone)]
struct OkxCredentials {
    api_key: String,
    api_secret: String,
    passphrase: String,
}

impl OkxCredentials {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            api_key: std::env::var("OKX_API_KEY").context("OKX_API_KEY not set")?,
            api_secret: std::env::var("OKX_API_SECRET").context("OKX_API_SECRET not set")?,
            passphrase: std::env::var("OKX_API_PASSPHRASE").context("OKX_API_PASSPHRASE not set")?,
        })
    }
}

impl OkxFuturesOrderProvider {
    pub fn new(
        venue: Venue,
        api_key: String,
        api_secret: String,
        passphrase: String,
        margin_mode: MarginMode,
        contract_sizes: Arc<ContractSizeCache>,
        proxy: Option<&str>,
    ) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            credentials: OkxCredentials { api_key, api_secret, passphrase },
            margin_mode,
            http,
            contract_sizes,
        })
    }

    pub fn from_env(
        venue: Venue,
        margin_mode: MarginMode,
        contract_sizes: Arc<ContractSizeCache>,
        proxy: Option<&str>,
    ) -> anyhow::Result<Self> {
        let c = OkxCredentials::from_env()?;
        Self::new(venue, c.api_key, c.api_secret, c.passphrase, margin_mode, contract_sizes, proxy)
    }

    /// `query` 直接拼进 request path 参与签名，所以调用方传入的值不能含需要
    /// URL 编码的字符(instId / ordId 都满足)。
    async fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<String> {
        let request_path = path_with_query(path, query);
        let body_str = body.as_ref().map(|b| b.to_string()).unwrap_or_default();
        let timestamp = iso8601_ms(now_ms());
        let signature = okx_sign(
            &self.credentials.api_secret,
            &format!("{timestamp}{}{request_path}{body_str}", method.as_str()),
        );

        crate::ratelimit::throttle(HOST).await;
        let mut request = self
            .http
            .request(method, format!("{HOST}{request_path}"))
            .header("OK-ACCESS-KEY", &self.credentials.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", &timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.credentials.passphrase)
            .header("Content-Type", "application/json");
        if body.is_some() {
            request = request.body(body_str);
        }
        let resp = request.send().await.context("okx request failed")?;
        resp.text().await.context("failed to read okx response body")
    }

    /// `GET /api/v5/account/bills?type=8`(资金费)：该合约从 `start_time_ms` 起的资金费
    /// 流水，供 `accounting::FundingFeeTracker` 使用。只能查最近 7 天。
    ///
    /// 每页最多 100 条、按时间倒序，用 `after=<最后一条 billId>` 往前翻页。`tran_id`
    /// 用结算时间(毫秒)——同一合约每个结算时点只有一条资金费记录。
    pub(crate) async fn funding_history(
        &self,
        symbol: &Symbol,
        start_time_ms: Option<u64>,
    ) -> anyhow::Result<Vec<FundingIncomeRecord>> {
        let inst_id = okx_swap_inst_id(symbol);
        let begin = start_time_ms.map(|t| t.to_string());
        let mut after: Option<String> = None;
        let mut records = Vec::new();
        for _ in 0..FUNDING_MAX_PAGES {
            let mut query = vec![
                ("instType", "SWAP"),
                ("instId", inst_id.as_str()),
                ("type", "8"),
                ("limit", FUNDING_PAGE_LIMIT),
            ];
            if let Some(begin) = &begin {
                query.push(("begin", begin.as_str()));
            }
            if let Some(after) = &after {
                query.push(("after", after.as_str()));
            }
            let text = self.signed_request(reqwest::Method::GET, "/api/v5/account/bills", &query, None).await?;
            let (page, last_bill_id) = parse_funding_bills(&text, symbol)?;
            let full_page = page.len() >= FUNDING_PAGE_LIMIT.parse::<usize>().unwrap_or(100);
            records.extend(page);
            match last_bill_id {
                Some(id) if full_page => after = Some(id),
                _ => {
                    records.sort_by_key(|r: &FundingIncomeRecord| r.tran_id);
                    return Ok(records);
                }
            }
        }
        anyhow::bail!("okx funding bills for {inst_id} exceed {FUNDING_MAX_PAGES} pages, refusing partial result")
    }
}

#[async_trait]
impl OrderProvider for OkxFuturesOrderProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `POST /api/v5/trade/order`，`ordType=market`。下单接口只返回订单号，
    /// 成交走私有订单流。
    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
        let OrderAmount::Base(quantity) = req.amount else {
            anyhow::bail!("{} does not support quote-amount market orders", self.venue());
        };
        let contracts = self.contract_sizes.to_contracts(&req.symbol, quantity).await?;
        let body =
            build_market_order_body(&req.symbol, req.side, contracts, self.margin_mode, req.client_order_id.as_deref())
                .with_context(|| format!("{quantity} {} 换算成 {contracts} 张", req.symbol.base))?;
        let text = self.signed_request(reqwest::Method::POST, "/api/v5/trade/order", &[], Some(body)).await?;
        self.contract_sizes.convert_result(&req.symbol, parse_place_order_response(&text)?).await
    }

    /// `GET /api/v5/trade/order?instId=&ordId=`。
    async fn query_order(&self, symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<OrderResult> {
        let inst_id = okx_swap_inst_id(symbol);
        let query = [("instId", inst_id.as_str()), ("ordId", exchange_order_id)];
        let text = self.signed_request(reqwest::Method::GET, "/api/v5/trade/order", &query, None).await?;
        self.contract_sizes.convert_result(symbol, parse_query_order_response(&text)?).await
    }

    /// 先 `GET /api/v5/account/config` 确认是单向持仓(`net_mode`，下单不带
    /// `posSide`，双向持仓下会被拒)，再 `GET /api/v5/account/positions`：没有
    /// 持仓时返回空数组；`pos` 自带正负，单位是张，换成币数返回。
    async fn perp_position(&self, symbol: &Symbol) -> anyhow::Result<Decimal> {
        let text = self.signed_request(reqwest::Method::GET, "/api/v5/account/config", &[], None).await?;
        ensure_net_mode(&text)?;
        let inst_id = okx_swap_inst_id(symbol);
        let query = [("instType", "SWAP"), ("instId", inst_id.as_str())];
        let text = self.signed_request(reqwest::Method::GET, "/api/v5/account/positions", &query, None).await?;
        self.contract_sizes.from_contracts(symbol, parse_positions(&text)?).await
    }
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build okx http client")
}

fn path_with_query(path: &str, query: &[(&str, &str)]) -> String {
    if query.is_empty() {
        return path.to_string();
    }
    let qs: Vec<String> = query.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("{path}?{}", qs.join("&"))
}

/// base64(HMAC-SHA256(secret, prehash))，REST 和 WS 登录共用。
fn okx_sign(secret: &str, prehash: &str) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    base64_engine.encode(ring::hmac::sign(&key, prehash.as_bytes()).as_ref())
}

/// Unix 毫秒 → `2020-12-08T09:08:57.715Z`(OKX REST 要求的 UTC ISO 8601 时间戳)。
fn iso8601_ms(ms: u64) -> String {
    let secs = ms / 1000;
    let sod = secs % 86_400;
    // 公历日期换算(Howard Hinnant 的 civil_from_days)。
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60,
        ms % 1000
    )
}

fn td_mode(margin_mode: MarginMode) -> &'static str {
    match margin_mode {
        MarginMode::Cross => "cross",
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
        anyhow::bail!("okx order size must be positive, got {quantity}");
    }
    let mut body = serde_json::json!({
        "instId": okx_swap_inst_id(symbol),
        "tdMode": td_mode(margin_mode),
        "side": match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        },
        "ordType": "market",
        "sz": quantity.normalize().to_string(),
    });
    if let Some(id) = client_order_id.filter(|id| !id.is_empty()) {
        // 不合规的 id 交易所会拒单；与其截断/改写导致推送关联不上，不如直接报错。
        if id.len() > CL_ORD_ID_MAX_LEN || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
            anyhow::bail!("okx clOrdId must be 1-{CL_ORD_ID_MAX_LEN} alphanumeric chars, got {id:?}");
        }
        body["clOrdId"] = serde_json::Value::String(id.to_string());
    }
    Ok(body)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderAck {
    #[serde(default)]
    ord_id: String,
    #[serde(default)]
    s_code: String,
    #[serde(default)]
    s_msg: String,
}

/// 下单失败时外层 `code` 是 `1`/`51xxx`，具体原因在 `data[0].sCode/sMsg`，优先报它。
fn parse_place_order_response(text: &str) -> anyhow::Result<OrderResult> {
    let resp: OkxResponse<PlaceOrderAck> =
        serde_json::from_str(text).with_context(|| format!("failed to parse okx place order response, raw body: {text}"))?;
    if let Some(ack) = resp.data.first()
        && !ack.s_code.is_empty()
        && ack.s_code != "0"
    {
        anyhow::bail!("okx order rejected {}: {}", ack.s_code, ack.s_msg);
    }
    if resp.code != "0" {
        anyhow::bail!("okx error {}: {}", resp.code, resp.msg);
    }
    let ack = resp.data.into_iter().next().context("okx place order response has no data")?;
    if ack.ord_id.is_empty() {
        anyhow::bail!("okx place order response has no ordId, raw body: {text}");
    }
    Ok(OrderResult {
        order_id: ack.ord_id,
        status: OrderStatus::New,
        filled_qty: Decimal::ZERO,
        avg_price: None,
        fee: None,
        fee_asset: None,
    })
}

/// REST 查单和 WS `orders` 推送共用的字段，数值都是字符串(可能为空串)。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkxOrder {
    #[serde(default)]
    inst_id: String,
    #[serde(default)]
    ord_id: String,
    #[serde(default)]
    cl_ord_id: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    acc_fill_sz: String,
    #[serde(default)]
    avg_px: String,
    /// WS 推送：本次推送对应成交的手续费，负数是扣费、正数是返佣。REST 查单里
    /// 这个字段语义不同(文档注明两边不一致)，只在 [`Self::into_update`] 用。
    #[serde(default)]
    fill_fee: String,
    #[serde(default)]
    fill_fee_ccy: String,
}

impl OkxOrder {
    fn filled_qty(&self) -> Decimal {
        opt_decimal(&self.acc_fill_sz).unwrap_or(Decimal::ZERO)
    }

    fn avg_price(&self) -> Option<Decimal> {
        opt_decimal(&self.avg_px).filter(|p| *p > Decimal::ZERO)
    }

    /// 市价单剩余部分被撤(`canceled`/`mmp_canceled`)按 Expired，成交量如实保留。
    fn status(&self) -> OrderStatus {
        match self.state.as_str() {
            "filled" => OrderStatus::Filled,
            "canceled" | "mmp_canceled" => OrderStatus::Expired,
            "partially_filled" => OrderStatus::PartiallyFilled,
            _ => OrderStatus::New,
        }
    }

    /// 手续费取 `fillFee`(本次推送的单笔值)而不是累计的 `fee`，和
    /// `ExchangeOrderUpdate::fee` 的增量语义一致；取反成"正数是成本"，返佣就是
    /// 负成本。没有成交的推送(如撤单)即使带着上一笔的 `fillFee`，
    /// `OrderManager` 也只在 fill_delta > 0 时记手续费，不会重复。
    fn into_update(self, venue: &Venue, ts_ms: u64) -> ExchangeOrderUpdate {
        let fee = opt_decimal(&self.fill_fee).filter(|_| !self.fill_fee_ccy.is_empty());
        ExchangeOrderUpdate {
            venue: venue.clone(),
            symbol: parse_okx_swap_inst_id(&self.inst_id),
            client_order_id: Some(self.cl_ord_id.clone()).filter(|id| !id.is_empty()),
            exchange_order_id: Some(self.ord_id.clone()).filter(|id| !id.is_empty()),
            status: self.status(),
            filled_qty: self.filled_qty(),
            avg_price: self.avg_price(),
            fee: fee.map(|f| -f),
            fee_asset: fee.map(|_| self.fill_fee_ccy.clone()),
            ts_ms,
        }
    }
}

fn parse_query_order_response(text: &str) -> anyhow::Result<OrderResult> {
    let orders: Vec<OkxOrder> = parse_okx_data(text, "order")?;
    let order = orders.into_iter().next().context("okx order response has no data")?;
    Ok(OrderResult {
        order_id: order.ord_id.clone(),
        status: order.status(),
        filled_qty: order.filled_qty(),
        avg_price: order.avg_price(),
        fee: None,
        fee_asset: None,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PositionEntry {
    #[serde(default)]
    pos: String,
    #[serde(default)]
    pos_side: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BillEntry {
    bill_id: String,
    bal_chg: String,
    ts: String,
}

/// 解析一页资金费账单，返回记录和本页最后一条(最早)的 `billId`，供翻页用。
fn parse_funding_bills(text: &str, symbol: &Symbol) -> anyhow::Result<(Vec<FundingIncomeRecord>, Option<String>)> {
    let bills: Vec<BillEntry> = parse_okx_data(text, "bills")?;
    let last_bill_id = bills.last().map(|b| b.bill_id.clone());
    let records = bills
        .into_iter()
        .map(|b| {
            let time_ms: u64 = b.ts.parse().with_context(|| format!("invalid okx bill ts '{}'", b.ts))?;
            let income = opt_decimal(&b.bal_chg).with_context(|| format!("invalid okx bill balChg '{}'", b.bal_chg))?;
            Ok(FundingIncomeRecord { symbol: symbol.clone(), income, time_ms, tran_id: time_ms as i64 })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((records, last_bill_id))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AccountConfig {
    #[serde(default)]
    pos_mode: String,
}

fn ensure_net_mode(text: &str) -> anyhow::Result<()> {
    let configs: Vec<AccountConfig> = parse_okx_data(text, "account config")?;
    let pos_mode = configs.first().map(|c| c.pos_mode.as_str()).unwrap_or_default();
    if pos_mode != "net_mode" {
        anyhow::bail!("okx 账户持仓模式是 '{pos_mode}'，需要先在交易所改成单向持仓(net_mode)");
    }
    Ok(())
}

fn parse_positions(text: &str) -> anyhow::Result<Decimal> {
    let positions: Vec<PositionEntry> = parse_okx_data(text, "positions")?;
    Ok(positions
        .iter()
        .map(|p| {
            let pos = opt_decimal(&p.pos).unwrap_or(Decimal::ZERO);
            match p.pos_side.as_str() {
                "long" => pos.abs(),
                "short" => -pos.abs(),
                _ => pos,
            }
        })
        .sum())
}

/// OKX USDT 本位永续私有订单流：连上 `/ws/v5/private` 后先 `login`，成功后
/// 订阅 `orders`(`instType=SWAP`，全部永续)，收到订阅确认才算 ready。推送
/// 里的数量单位是张，交给 `OrderManager` 之前用和下单网关同一份
/// [`ContractSizeCache`] 换成币数。
pub struct OkxFuturesPrivateOrderStream {
    venue: Venue,
    credentials: OkxCredentials,
    proxy: Option<String>,
    contract_sizes: Arc<ContractSizeCache>,
}

impl OkxFuturesPrivateOrderStream {
    pub fn new(
        venue: Venue,
        api_key: String,
        api_secret: String,
        passphrase: String,
        contract_sizes: Arc<ContractSizeCache>,
        proxy: Option<&str>,
    ) -> Self {
        Self {
            venue,
            credentials: OkxCredentials { api_key, api_secret, passphrase },
            proxy: proxy.map(str::to_string),
            contract_sizes,
        }
    }

    pub fn from_env(venue: Venue, contract_sizes: Arc<ContractSizeCache>, proxy: Option<&str>) -> anyhow::Result<Self> {
        let c = OkxCredentials::from_env()?;
        Ok(Self::new(venue, c.api_key, c.api_secret, c.passphrase, contract_sizes, proxy))
    }

    fn login_message(&self, time_secs: u64) -> serde_json::Value {
        let timestamp = time_secs.to_string();
        let sign = okx_sign(&self.credentials.api_secret, &format!("{timestamp}GET/users/self/verify"));
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
        serde_json::json!({"op": "subscribe", "args": [{"channel": "orders", "instType": "SWAP"}]})
    }

    async fn connect(&self) -> anyhow::Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>> {
        let tcp = connect_tcp(WS_HOST, WS_PORT, self.proxy.as_deref()).await?;
        let (ws, _) = tokio_tungstenite::client_async_tls(WS_URL, tcp)
            .await
            .context("okx private order stream handshake failed")?;
        Ok(ws)
    }
}

impl OrderStreamSource for OkxFuturesPrivateOrderStream {
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
                            "okx private order stream connect failed for venue={} err={err:#}, retrying in {:?}",
                            self.venue, backoff
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                if let Err(err) = ws.send(Message::Text(self.login_message(now_ms() / 1000).to_string())).await {
                    warn!(
                        "okx private order stream login send failed for venue={} err={err}, retrying in {:?}",
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
                                warn!("okx private order stream ping failed for venue={} err={err}", self.venue);
                                break;
                            }
                        }
                        msg = ws.next() => {
                            let msg = match msg {
                                Some(Ok(msg)) => msg,
                                Some(Err(err)) => {
                                    warn!("okx private order stream error for venue={} err={err}", self.venue);
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
                                        warn!("okx private order stream subscribe failed for venue={} err={err}", self.venue);
                                        break;
                                    }
                                }
                                PrivateMessage::Subscribed => {
                                    debug!("okx private order stream subscribed for venue={}", self.venue);
                                    backoff = MIN_BACKOFF;
                                    if let Some(ready_tx) = ready_tx.take() {
                                        let _ = ready_tx.send(());
                                    }
                                }
                                PrivateMessage::Error(err) => {
                                    warn!("okx private order stream error event for venue={}: {err}", self.venue);
                                    break;
                                }
                                PrivateMessage::Orders(updates) => {
                                    for update in updates {
                                        let order_ref =
                                            format!("{:?}/{:?}", update.client_order_id, update.exchange_order_id);
                                        match self.contract_sizes.convert_update(update).await {
                                            Ok(update) => order_manager.handle_exchange_update(update).await,
                                            Err(err) => error!(
                                                "okx order update {order_ref} for venue={} dropped: 张数无法换算成币数 ({err:#})；若是本系统的订单，仓位需要用 set-position 人工修正",
                                                self.venue
                                            ),
                                        }
                                    }
                                }
                                PrivateMessage::Other => {}
                            }
                        }
                    }
                }

                warn!("okx private order stream disconnected for venue={}, reconnecting in {:?}", self.venue, backoff);
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

#[derive(Debug, Deserialize)]
struct WsMessage {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    arg: Option<WsArg>,
    #[serde(default)]
    data: Option<Vec<OkxOrder>>,
}

fn parse_private_message(text: &str, venue: &Venue) -> PrivateMessage {
    let Ok(msg) = serde_json::from_str::<WsMessage>(text) else {
        return PrivateMessage::Other;
    };
    let code_ok = msg.code.as_deref().is_none_or(|c| c == "0");
    match msg.event.as_deref() {
        Some("login") if code_ok => return PrivateMessage::LoggedIn,
        Some("subscribe") => return PrivateMessage::Subscribed,
        Some("login") | Some("error") => {
            return PrivateMessage::Error(format!(
                "code={} msg={}",
                msg.code.unwrap_or_default(),
                msg.msg.unwrap_or_default()
            ));
        }
        Some(_) => return PrivateMessage::Other,
        None => {}
    }
    if msg.arg.map(|a| a.channel).as_deref() != Some("orders") {
        return PrivateMessage::Other;
    }
    let ts_ms = now_ms();
    PrivateMessage::Orders(msg.data.unwrap_or_default().into_iter().map(|o| o.into_update(venue, ts_ms)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn net_mode_check_rejects_long_short_mode() {
        ensure_net_mode(r#"{"code":"0","msg":"","data":[{"posMode":"net_mode","acctLv":"2"}]}"#).unwrap();
        let err = ensure_net_mode(r#"{"code":"0","msg":"","data":[{"posMode":"long_short_mode"}]}"#).unwrap_err();
        assert!(err.to_string().contains("long_short_mode"));
        assert!(ensure_net_mode(r#"{"code":"0","msg":"","data":[]}"#).is_err());
        assert!(ensure_net_mode(r#"{"code":"50113","msg":"Invalid Sign","data":[]}"#).is_err());
    }

    #[test]
    fn parses_funding_bills_page() {
        let text = r#"{"code":"0","msg":"","data":[
            {"billId":"200","balChg":"-0.31","ts":"1700028800000","instId":"BTC-USDT-SWAP","type":"8","subType":"173"},
            {"billId":"100","balChg":"0.12","ts":"1700000000000","instId":"BTC-USDT-SWAP","type":"8","subType":"174"}
        ]}"#;
        let (records, last) = parse_funding_bills(text, &Symbol::new("BTC", "USDT")).unwrap();
        assert_eq!(last.as_deref(), Some("100"));
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].income, Decimal::new(-31, 2));
        assert_eq!(records[0].tran_id, 1_700_028_800_000);
        assert_eq!(records[1].time_ms, 1_700_000_000_000);

        let (records, last) = parse_funding_bills(r#"{"code":"0","msg":"","data":[]}"#, &Symbol::new("BTC", "USDT")).unwrap();
        assert!(records.is_empty() && last.is_none());

        assert!(parse_funding_bills(r#"{"code":"50113","msg":"Invalid Sign","data":[]}"#, &Symbol::new("BTC", "USDT")).is_err());
    }

    #[test]
    fn formats_iso8601_timestamps() {
        assert_eq!(iso8601_ms(1_607_418_537_715), "2020-12-08T09:08:57.715Z");
        assert_eq!(iso8601_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601_ms(951_782_400_001), "2000-02-29T00:00:00.001Z");
    }

    #[test]
    fn signs_like_okx_docs() {
        // base64(HMAC-SHA256("secret", "msg"))，向量用 Python hmac 标准库算出。
        assert_eq!(okx_sign("secret", "msg"), "/k+cQY9oPwNPavkNHdW4asA1XdljMsWcx0WY0HNhB/Y=");
        assert_eq!(path_with_query("/api/v5/trade/order", &[("instId", "BTC-USDT-SWAP"), ("ordId", "1")]),
            "/api/v5/trade/order?instId=BTC-USDT-SWAP&ordId=1");
        assert_eq!(path_with_query("/api/v5/trade/order", &[]), "/api/v5/trade/order");
    }

    #[test]
    fn builds_market_order_body() {
        let symbol = Symbol::new("PEPE", "USDT");
        let body = build_market_order_body(&symbol, OrderSide::Sell, "2.50".parse().unwrap(), MarginMode::Isolated, Some("cf170000000000012345")).unwrap();
        assert_eq!(body["instId"], "PEPE-USDT-SWAP");
        assert_eq!(body["tdMode"], "isolated");
        assert_eq!(body["side"], "sell");
        assert_eq!(body["ordType"], "market");
        assert_eq!(body["sz"], "2.5");
        assert_eq!(body["clOrdId"], "cf170000000000012345");

        let body = build_market_order_body(&symbol, OrderSide::Buy, Decimal::ONE, MarginMode::Cross, None).unwrap();
        assert_eq!(body["tdMode"], "cross");
        assert!(body.get("clOrdId").is_none());
    }

    #[test]
    fn rejects_invalid_cl_ord_id_and_size() {
        let symbol = Symbol::new("PEPE", "USDT");
        assert!(build_market_order_body(&symbol, OrderSide::Buy, Decimal::ONE, MarginMode::Cross, Some("cf-1-2")).is_err());
        assert!(build_market_order_body(&symbol, OrderSide::Buy, Decimal::ONE, MarginMode::Cross, Some(&"a".repeat(33))).is_err());
        assert!(build_market_order_body(&symbol, OrderSide::Buy, Decimal::ZERO, MarginMode::Cross, None).is_err());
    }

    #[test]
    fn parses_place_order_ack_and_rejection() {
        let ok = r#"{"code":"0","msg":"","data":[{"clOrdId":"cf1","ordId":"312269865356374016","tag":"","sCode":"0","sMsg":""}]}"#;
        let result = parse_place_order_response(ok).unwrap();
        assert_eq!(result.order_id, "312269865356374016");
        assert_eq!(result.status, OrderStatus::New);

        let rejected = r#"{"code":"1","msg":"All operations failed","data":[{"clOrdId":"cf1","ordId":"","sCode":"51008","sMsg":"Order failed. Insufficient USDT margin in account"}]}"#;
        let err = parse_place_order_response(rejected).unwrap_err();
        assert!(err.to_string().contains("51008"), "{err}");

        let auth = r#"{"code":"50113","msg":"Invalid Sign","data":[]}"#;
        assert!(parse_place_order_response(auth).unwrap_err().to_string().contains("50113"));
    }

    #[test]
    fn parses_query_order() {
        let text = r#"{"code":"0","msg":"","data":[{"instId":"PEPE-USDT-SWAP","ordId":"1","clOrdId":"cf1","state":"filled","accFillSz":"3","avgPx":"0.000004036","fee":"-0.001"}]}"#;
        let result = parse_query_order_response(text).unwrap();
        assert_eq!(result.status, OrderStatus::Filled);
        assert_eq!(result.filled_qty, Decimal::from(3));
        assert_eq!(result.avg_price, Some("0.000004036".parse().unwrap()));

        let text = r#"{"code":"0","msg":"","data":[{"instId":"PEPE-USDT-SWAP","ordId":"1","state":"canceled","accFillSz":"0","avgPx":""}]}"#;
        let result = parse_query_order_response(text).unwrap();
        assert_eq!(result.status, OrderStatus::Expired);
        assert_eq!(result.avg_price, None);
    }

    #[test]
    fn sums_positions_by_side() {
        assert_eq!(parse_positions(r#"{"code":"0","msg":"","data":[]}"#).unwrap(), Decimal::ZERO);
        assert_eq!(parse_positions(r#"{"code":"0","msg":"","data":[{"pos":"-12","posSide":"net"}]}"#).unwrap(), Decimal::from(-12));
        assert_eq!(
            parse_positions(r#"{"code":"0","msg":"","data":[{"pos":"5","posSide":"long"},{"pos":"2","posSide":"short"}]}"#).unwrap(),
            Decimal::from(3)
        );
        assert!(parse_positions(r#"{"code":"50111","msg":"Invalid OK-ACCESS-KEY","data":[]}"#).is_err());
    }

    #[test]
    fn parses_private_ws_messages() {
        let venue = Venue::new("okx_futures");
        assert!(matches!(
            parse_private_message(r#"{"event":"login","code":"0","msg":"","connId":"a"}"#, &venue),
            PrivateMessage::LoggedIn
        ));
        assert!(matches!(
            parse_private_message(r#"{"event":"login","code":"60009","msg":"Login failed.","connId":"a"}"#, &venue),
            PrivateMessage::Error(_)
        ));
        assert!(matches!(
            parse_private_message(r#"{"event":"error","code":"60012","msg":"Invalid request","connId":"a"}"#, &venue),
            PrivateMessage::Error(_)
        ));
        assert!(matches!(
            parse_private_message(r#"{"event":"subscribe","arg":{"channel":"orders","instType":"SWAP"},"connId":"a"}"#, &venue),
            PrivateMessage::Subscribed
        ));
        assert!(matches!(parse_private_message("pong", &venue), PrivateMessage::Other));

        let push = r#"{"arg":{"channel":"orders","instType":"SWAP","uid":"1"},"data":[
            {"instType":"SWAP","instId":"PEPE-USDT-SWAP","ordId":"312269865356374016","clOrdId":"cf170000000000012345",
             "state":"partially_filled","accFillSz":"2","avgPx":"0.000004","fillSz":"2","fillFee":"-0.0001","fillFeeCcy":"USDT","fee":"-0.0003"}]}"#;
        let PrivateMessage::Orders(updates) = parse_private_message(push, &venue) else {
            panic!("expected orders");
        };
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.symbol, Some(Symbol::new("PEPE", "USDT")));
        assert_eq!(update.client_order_id.as_deref(), Some("cf170000000000012345"));
        assert_eq!(update.exchange_order_id.as_deref(), Some("312269865356374016"));
        assert_eq!(update.status, OrderStatus::PartiallyFilled);
        assert_eq!(update.filled_qty, Decimal::from(2));
        // 取本次的 fillFee 而不是累计 fee，扣费取反成正数成本。
        assert_eq!(update.fee, Some("0.0001".parse().unwrap()));
        assert_eq!(update.fee_asset.as_deref(), Some("USDT"));
    }

    #[test]
    fn ws_order_fee_handles_rebate_and_missing_fields() {
        let venue = Venue::new("okx_futures");
        let fee_of = |fields: &str| {
            let push = format!(
                r#"{{"arg":{{"channel":"orders","instType":"SWAP"}},"data":[{{"instId":"BTC-USDT-SWAP","ordId":"1","state":"filled","accFillSz":"1","avgPx":"100"{fields}}}]}}"#
            );
            let PrivateMessage::Orders(updates) = parse_private_message(&push, &venue) else {
                panic!("expected orders");
            };
            (updates[0].fee, updates[0].fee_asset.clone())
        };
        // maker 返佣：正的 fillFee 记成负成本。
        assert_eq!(fee_of(r#","fillFee":"0.02","fillFeeCcy":"USDT""#), (Some("-0.02".parse().unwrap()), Some("USDT".to_string())));
        assert_eq!(fee_of(r#","fillFee":"","fillFeeCcy":"""#), (None, None));
        assert_eq!(fee_of(r#","fillFee":"-0.02","fillFeeCcy":"""#), (None, None));
        assert_eq!(fee_of(""), (None, None));
    }

    #[test]
    fn login_message_has_signature() {
        let contract_sizes =
            Arc::new(ContractSizeCache::new(Arc::new(crate::order::contract_size::tests::FakeSpecs::new(&[]))));
        let stream = OkxFuturesPrivateOrderStream::new(
            Venue::new("okx_futures"),
            "k".into(),
            "s".into(),
            "p".into(),
            contract_sizes,
            None,
        );
        let msg = stream.login_message(1_538_054_050);
        assert_eq!(msg["op"], "login");
        assert_eq!(msg["args"][0]["timestamp"], "1538054050");
        assert_eq!(msg["args"][0]["sign"], okx_sign("s", "1538054050GET/users/self/verify"));
    }
}
