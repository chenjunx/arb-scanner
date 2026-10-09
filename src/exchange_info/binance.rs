use std::collections::HashMap;

use anyhow::Context;
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as base64_engine;
use ring::signature::Ed25519KeyPair;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::market_data::now_ms;
use crate::types::{Symbol, Venue};

use super::types::{
    FundingRateRecord, MarketPrecision, PerpContractSpec, PredictedFundingRate, QtyPrecision, SpotPerpPair, TradingFee,
};
use super::{ExchangeInfoProvider, FundingRateProvider};

const SPOT_MAINNET_HOST: &str = "https://api.binance.com";
const SPOT_TESTNET_HOST: &str = "https://testnet.binance.vision";
const FUTURES_MAINNET_HOST: &str = "https://fapi.binance.com";
const FUTURES_TESTNET_HOST: &str = "https://testnet.binancefuture.com";
const RECV_WINDOW_MS: u64 = 5_000;

/// 币安"基础信息"(现货 + USDT-M 永续合约)客户端：查询账户实际手续费率、
/// 列出可交易的 USDT 计价交易对。签名方式和 `order::binance`/`wallet::binance`
/// 一致，用 Ed25519，凭证也复用同一套环境变量(同一个 API Key 上勾选现货+合约
/// 权限即可，两个市场共用一把 key)。
pub struct BinanceExchangeInfoProvider {
    venue: Venue,
    api_key: String,
    key_pair: Ed25519KeyPair,
    spot_host: &'static str,
    futures_host: &'static str,
    http: reqwest::Client,
}

impl BinanceExchangeInfoProvider {
    pub fn new(venue: Venue, api_key: String, private_key_pem: &str, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let key_pair = load_ed25519_key(private_key_pem)?;
        let http = build_http_client(proxy)?;
        Ok(Self {
            venue,
            api_key,
            key_pair,
            spot_host: if testnet { SPOT_TESTNET_HOST } else { SPOT_MAINNET_HOST },
            futures_host: if testnet { FUTURES_TESTNET_HOST } else { FUTURES_MAINNET_HOST },
            http,
        })
    }

    /// 从环境变量读取凭证并构造实例:`BINANCE_API_KEY` +
    /// `BINANCE_API_SECRET`(完整 PEM 文本)，和 `order::binance`/`wallet::binance` 同一套。
    pub fn from_env(venue: Venue, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("BINANCE_API_KEY").context("BINANCE_API_KEY not set")?;
        let private_key_pem = std::env::var("BINANCE_API_SECRET").context("BINANCE_API_SECRET not set")?;
        Self::new(venue, api_key, &private_key_pem, testnet, proxy)
    }

    /// 对参数做签名并发起一次已签名请求(query string 里带 timestamp/recvWindow/signature)。
    async fn signed_request(
        &self,
        host: &str,
        path: &str,
        mut params: Vec<(String, String)>,
    ) -> anyhow::Result<String> {
        params.push(("timestamp".to_string(), now_ms().to_string()));
        params.push(("recvWindow".to_string(), RECV_WINDOW_MS.to_string()));
        let query = build_query_string(&params);
        let signature = sign_ed25519(&self.key_pair, &query);
        let url = format!("{host}{path}?{query}&signature={signature}");

        crate::ratelimit::throttle(host).await;
        let resp = self
            .http
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .context("binance exchange_info request failed")?;
        resp.text().await.context("failed to read binance exchange_info response body")
    }

    /// 不需要签名的公开接口请求，用于查询交易对列表。
    async fn public_request(&self, host: &str, path: &str, params: Vec<(String, String)>) -> anyhow::Result<String> {
        let query = build_query_string(&params);
        let url = if query.is_empty() {
            format!("{host}{path}")
        } else {
            format!("{host}{path}?{query}")
        };
        crate::ratelimit::throttle(host).await;
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("binance exchange_info public request failed")?;
        resp.text().await.context("failed to read binance exchange_info public response body")
    }

    fn binance_symbol(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }

    /// 维护币安现货/USDT 本位永续的可对冲映射：拉取两边当前可交易的 USDT
    /// 交易对列表，按 base 配对(处理永续侧的"合约乘数"前缀，见
    /// [`strip_contract_multiplier`])。这条换算规则是币安专属的，不放进
    /// `ExchangeInfoProvider` trait。
    pub async fn spot_perpetual_pairs(&self) -> anyhow::Result<Vec<SpotPerpPair>> {
        let (spot, perp) = tokio::try_join!(self.usdt_spot_symbols(), self.usdt_perpetual_symbols())?;
        Ok(build_spot_perp_pairs(&spot, &perp))
    }
}

#[async_trait]
impl ExchangeInfoProvider for BinanceExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn spot_trading_fee(&self, symbol: &Symbol) -> anyhow::Result<TradingFee> {
        let params = vec![("symbol".to_string(), Self::binance_symbol(symbol))];
        let text = self.signed_request(self.spot_host, "/sapi/v1/asset/tradeFee", params).await?;
        parse_spot_trading_fee(&text, symbol)
    }

    async fn perpetual_trading_fee(&self, symbol: &Symbol) -> anyhow::Result<TradingFee> {
        let params = vec![("symbol".to_string(), Self::binance_symbol(symbol))];
        let text = self
            .signed_request(self.futures_host, "/fapi/v1/commissionRate", params)
            .await?;
        parse_futures_trading_fee(&text)
    }

    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self.public_request(self.spot_host, "/api/v3/exchangeInfo", Vec::new()).await?;
        parse_usdt_spot_symbols(&text)
    }

    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self
            .public_request(self.futures_host, "/fapi/v1/exchangeInfo", Vec::new())
            .await?;
        parse_usdt_perpetual_symbols(&text)
    }

    async fn spot_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        let text = self.public_request(self.spot_host, "/api/v3/exchangeInfo", Vec::new()).await?;
        parse_spot_market_precisions(&text)
    }

    async fn perpetual_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        let text = self
            .public_request(self.futures_host, "/fapi/v1/exchangeInfo", Vec::new())
            .await?;
        parse_perpetual_market_precisions(&text)
    }

    /// 币安 U 本位永续的报价单位和数量单位一致，都由合约名的乘数前缀决定，
    /// 不需要查接口，见 [`binance_perp_contract_spec`]。
    async fn perpetual_contract_spec(&self, symbol: &Symbol) -> anyhow::Result<PerpContractSpec> {
        Ok(binance_perp_contract_spec(symbol))
    }
}

/// `/fapi/v1/fundingRate` 单次最多返回 1000 条。
const FUNDING_HISTORY_PAGE_LIMIT: usize = 1000;
/// `/fapi/v1/fundingRate` 和 `/fapi/v1/fundingInfo` 另有共享的 500 次/5 分钟/IP
/// 额度(和合约 host 的权重额度分开算，超了直接 403 封 IP)，单独一个令牌桶。
const FUNDING_RATE_LIMIT_KEY: &str = "binance:/fapi/v1/fundingRate";
/// `/fapi/v1/fundingInfo` 只列出结算周期/上下限被调整过的合约，没列出的就是默认 8h。
const DEFAULT_FUNDING_INTERVAL_HOURS: u32 = 8;

#[async_trait]
impl FundingRateProvider for BinanceExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `/fapi/v1/fundingRate` 按时间升序返回，满页时从最后一条之后继续翻。
    async fn funding_rate_history(
        &self,
        symbol: &Symbol,
        start_time_ms: u64,
        end_time_ms: Option<u64>,
    ) -> anyhow::Result<Vec<FundingRateRecord>> {
        let mut records = Vec::new();
        let mut cursor = start_time_ms;
        loop {
            let mut params = vec![
                ("symbol".to_string(), Self::binance_symbol(symbol)),
                ("startTime".to_string(), cursor.to_string()),
                ("limit".to_string(), FUNDING_HISTORY_PAGE_LIMIT.to_string()),
            ];
            if let Some(end) = end_time_ms {
                params.push(("endTime".to_string(), end.to_string()));
            }
            crate::ratelimit::throttle(FUNDING_RATE_LIMIT_KEY).await;
            let text = self.public_request(self.futures_host, "/fapi/v1/fundingRate", params).await?;
            let page = parse_funding_rate_history(&text, symbol)?;
            let page_len = page.len();
            let last_time = page.last().map(|r| r.funding_time_ms);
            records.extend(page);
            match last_time {
                Some(last) if page_len >= FUNDING_HISTORY_PAGE_LIMIT && last >= cursor => cursor = last + 1,
                _ => break,
            }
        }
        Ok(records)
    }

    /// 不传 `startTime`/`endTime` 时 `/fapi/v1/fundingRate` 返回最近 `limit` 条(升序)。
    async fn recent_funding_rates(&self, symbol: &Symbol, limit: usize) -> anyhow::Result<Vec<FundingRateRecord>> {
        let params = vec![
            ("symbol".to_string(), Self::binance_symbol(symbol)),
            ("limit".to_string(), limit.clamp(1, FUNDING_HISTORY_PAGE_LIMIT).to_string()),
        ];
        crate::ratelimit::throttle(FUNDING_RATE_LIMIT_KEY).await;
        let text = self.public_request(self.futures_host, "/fapi/v1/fundingRate", params).await?;
        parse_funding_rate_history(&text, symbol)
    }

    /// `/fapi/v1/premiumIndex` 不传 symbol 返回全部合约，`lastFundingRate` 就是
    /// 本期实时估算、将在 `nextFundingTime` 结算的费率；结算周期另查
    /// `/fapi/v1/fundingInfo`。
    async fn predicted_funding_rates(&self) -> anyhow::Result<Vec<PredictedFundingRate>> {
        let (premium_text, info_text) = tokio::try_join!(
            self.public_request(self.futures_host, "/fapi/v1/premiumIndex", Vec::new()),
            async {
                crate::ratelimit::throttle(FUNDING_RATE_LIMIT_KEY).await;
                self.public_request(self.futures_host, "/fapi/v1/fundingInfo", Vec::new()).await
            },
        )?;
        let intervals = parse_funding_intervals(&info_text)?;
        parse_predicted_funding_rates(&premium_text, &intervals)
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

/// 按插入顺序拼接 `k=v&k=v...`，签名必须覆盖和实际发送完全一致的 query string。
fn build_query_string(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// 解析 PKCS8 PEM 文本，交给 `Ed25519KeyPair::from_pkcs8_maybe_unchecked` 加载私钥。
/// 用 `_maybe_unchecked` 而不是 `from_pkcs8`：兼容 `openssl genpkey` 产出的不带
/// 内嵌公钥的 PKCS8 v1 格式，详见 `order::binance` 里的同名函数说明。
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

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    code: i64,
    msg: String,
}

#[derive(Debug, Deserialize)]
struct TradeFeeEntry {
    #[serde(rename = "makerCommission")]
    maker_commission: Decimal,
    #[serde(rename = "takerCommission")]
    taker_commission: Decimal,
}

/// 币安现货 `/sapi/v1/asset/tradeFee` 返回的手续费是小数比例(如 "0.001000"
/// 表示 0.1%)，换算成 bps 要乘以 10000。
fn parse_spot_trading_fee(text: &str, symbol: &Symbol) -> anyhow::Result<TradingFee> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let entries: Vec<TradeFeeEntry> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance tradeFee response, raw body: {text}"))?;
    let entry = entries
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("binance tradeFee returned no entry for {symbol}"))?;

    let bps_multiplier = Decimal::from(10_000);
    Ok(TradingFee {
        maker_bps: entry.maker_commission * bps_multiplier,
        taker_bps: entry.taker_commission * bps_multiplier,
    })
}

#[derive(Debug, Deserialize)]
struct CommissionRateResponse {
    #[serde(rename = "makerCommissionRate")]
    maker_commission_rate: Decimal,
    #[serde(rename = "takerCommissionRate")]
    taker_commission_rate: Decimal,
}

/// 币安 U 本位合约 `/fapi/v1/commissionRate` 返回的也是小数比例，换算规则
/// 和现货一致。
fn parse_futures_trading_fee(text: &str) -> anyhow::Result<TradingFee> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let resp: CommissionRateResponse = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance commissionRate response, raw body: {text}"))?;

    let bps_multiplier = Decimal::from(10_000);
    Ok(TradingFee {
        maker_bps: resp.maker_commission_rate * bps_multiplier,
        taker_bps: resp.taker_commission_rate * bps_multiplier,
    })
}

#[derive(Debug, Deserialize)]
struct SpotExchangeInfoResponse {
    symbols: Vec<SpotSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct SpotSymbolInfo {
    status: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(default)]
    filters: Vec<serde_json::Value>,
}

fn parse_usdt_spot_symbols(text: &str) -> anyhow::Result<Vec<Symbol>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let resp: SpotExchangeInfoResponse =
        serde_json::from_str(text).context("failed to parse binance spot exchangeInfo response")?;
    Ok(resp
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING" && s.quote_asset.eq_ignore_ascii_case("USDT"))
        .map(|s| Symbol::new(s.base_asset, s.quote_asset))
        .collect())
}

#[derive(Debug, Deserialize)]
struct FuturesExchangeInfoResponse {
    symbols: Vec<FuturesSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct FuturesSymbolInfo {
    status: String,
    #[serde(rename = "contractType")]
    contract_type: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(default)]
    filters: Vec<serde_json::Value>,
}

fn parse_usdt_perpetual_symbols(text: &str) -> anyhow::Result<Vec<Symbol>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let resp: FuturesExchangeInfoResponse =
        serde_json::from_str(text).context("failed to parse binance futures exchangeInfo response")?;
    Ok(resp
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING" && s.contract_type == "PERPETUAL" && s.quote_asset.eq_ignore_ascii_case("USDT"))
        .map(|s| Symbol::new(s.base_asset, s.quote_asset))
        .collect())
}

/// 从币安 `filters` 数组里按 `filterType` 取 `stepSize`/`minQty`。
fn extract_qty_precision(filters: &[serde_json::Value], filter_type: &str) -> Option<QtyPrecision> {
    let filter = filters.iter().find(|f| f.get("filterType").and_then(|v| v.as_str()) == Some(filter_type))?;
    let qty_step: Decimal = filter.get("stepSize")?.as_str()?.parse().ok()?;
    let min_qty: Decimal = filter.get("minQty")?.as_str()?.parse().ok()?;
    Some(QtyPrecision { qty_step, min_qty })
}

/// 从币安 `filters` 数组里取 `PRICE_FILTER.tickSize`。
fn extract_price_tick(filters: &[serde_json::Value]) -> Option<Decimal> {
    let filter = filters.iter().find(|f| f.get("filterType").and_then(|v| v.as_str()) == Some("PRICE_FILTER"))?;
    filter.get("tickSize")?.as_str()?.parse().ok()
}

/// 币安市价单按 `MARKET_LOT_SIZE` 校验精度、限价单按 `LOT_SIZE`，两者可能不同
/// (`MARKET_LOT_SIZE` 的 stepSize 通常更粗)。`MARKET_LOT_SIZE` 该 symbol 缺失时
/// 整体退回 `LOT_SIZE`，不混用两个 filter 的字段。
fn build_market_precision(symbol: Symbol, filters: &[serde_json::Value]) -> Option<MarketPrecision> {
    let limit = extract_qty_precision(filters, "LOT_SIZE")?;
    let market = extract_qty_precision(filters, "MARKET_LOT_SIZE").unwrap_or(limit);
    let price_tick = extract_price_tick(filters).unwrap_or(Decimal::ZERO);
    Some(MarketPrecision {
        symbol,
        market,
        limit,
        price_tick,
    })
}

fn parse_spot_market_precisions(text: &str) -> anyhow::Result<Vec<MarketPrecision>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance error {}: {}", err.code, err.msg);
    }
    let resp: SpotExchangeInfoResponse =
        serde_json::from_str(text).context("failed to parse binance spot exchangeInfo response")?;
    Ok(resp
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING" && s.quote_asset.eq_ignore_ascii_case("USDT"))
        .filter_map(|s| build_market_precision(Symbol::new(s.base_asset, s.quote_asset), &s.filters))
        .collect())
}

fn parse_perpetual_market_precisions(text: &str) -> anyhow::Result<Vec<MarketPrecision>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let resp: FuturesExchangeInfoResponse =
        serde_json::from_str(text).context("failed to parse binance futures exchangeInfo response")?;
    Ok(resp
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING" && s.contract_type == "PERPETUAL" && s.quote_asset.eq_ignore_ascii_case("USDT"))
        .filter_map(|s| build_market_precision(Symbol::new(s.base_asset, s.quote_asset), &s.filters))
        .collect())
}

#[derive(Debug, Deserialize)]
struct FundingRateEntry {
    #[serde(rename = "fundingTime")]
    funding_time: u64,
    #[serde(rename = "fundingRate")]
    funding_rate: Decimal,
}

fn parse_funding_rate_history(text: &str, symbol: &Symbol) -> anyhow::Result<Vec<FundingRateRecord>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let entries: Vec<FundingRateEntry> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance fundingRate response, raw body: {text}"))?;
    let mut records: Vec<FundingRateRecord> = entries
        .into_iter()
        .map(|e| FundingRateRecord { symbol: symbol.clone(), rate: e.funding_rate, funding_time_ms: e.funding_time })
        .collect();
    records.sort_by_key(|r| r.funding_time_ms);
    Ok(records)
}

#[derive(Debug, Deserialize)]
struct FundingInfoEntry {
    symbol: String,
    #[serde(rename = "fundingIntervalHours")]
    funding_interval_hours: u32,
}

fn parse_funding_intervals(text: &str) -> anyhow::Result<HashMap<String, u32>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let entries: Vec<FundingInfoEntry> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance fundingInfo response, raw body: {text}"))?;
    Ok(entries.into_iter().map(|e| (e.symbol, e.funding_interval_hours)).collect())
}

#[derive(Debug, Deserialize)]
struct PremiumIndexEntry {
    symbol: String,
    #[serde(rename = "lastFundingRate")]
    last_funding_rate: Decimal,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: u64,
}

/// `premiumIndex` 里混着 USDC 永续和交割合约(`BTCUSDT_261225`，
/// `nextFundingTime=0`)，只保留 `XXXUSDT` 形式、有下一次结算时间的永续。
fn parse_predicted_funding_rates(text: &str, intervals: &HashMap<String, u32>) -> anyhow::Result<Vec<PredictedFundingRate>> {
    if let Ok(err) = serde_json::from_str::<ErrorResponse>(text) {
        anyhow::bail!("binance futures error {}: {}", err.code, err.msg);
    }
    let entries: Vec<PremiumIndexEntry> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse binance premiumIndex response, raw body: {text}"))?;
    Ok(entries
        .into_iter()
        .filter(|e| e.next_funding_time > 0 && !e.symbol.contains('_'))
        .filter_map(|e| {
            let base = e.symbol.strip_suffix("USDT").filter(|b| !b.is_empty())?;
            let funding_interval_hours = intervals.get(&e.symbol).copied().unwrap_or(DEFAULT_FUNDING_INTERVAL_HOURS);
            Some(PredictedFundingRate {
                symbol: Symbol::new(base, "USDT"),
                rate: e.last_funding_rate,
                next_funding_time_ms: e.next_funding_time,
                funding_interval_hours,
            })
        })
        .collect())
}

/// 按 base 把现货和永续列表配成对：优先精确匹配(覆盖 "1INCH" 这类本身就以
/// 数字开头的真实 ticker，两边 base 完全一致，不会走到剥离前缀那一步)；精确
/// 匹配不上时，尝试剥离永续 base 开头的"合约乘数"前缀(见
/// [`strip_contract_multiplier`])再匹配一次。两边都没有 base 交集的永续
/// 品种直接跳过。
fn build_spot_perp_pairs(spot: &[Symbol], perp: &[Symbol]) -> Vec<SpotPerpPair> {
    let spot_by_base: HashMap<String, &Symbol> = spot.iter().map(|s| (s.base.to_ascii_uppercase(), s)).collect();

    let mut pairs: Vec<SpotPerpPair> = perp
        .iter()
        .filter_map(|p| {
            let perp_base = p.base.to_ascii_uppercase();
            if let Some(spot_symbol) = spot_by_base.get(&perp_base) {
                return Some(SpotPerpPair {
                    spot_symbol: (*spot_symbol).clone(),
                    perp_symbol: p.clone(),
                    contract_multiplier: 1,
                });
            }
            let (multiplier, core_base) = strip_contract_multiplier(&perp_base)?;
            let spot_symbol = spot_by_base.get(&core_base)?;
            Some(SpotPerpPair {
                spot_symbol: (*spot_symbol).clone(),
                perp_symbol: p.clone(),
                contract_multiplier: multiplier,
            })
        })
        .collect();

    pairs.sort_by(|a, b| a.spot_symbol.to_string().cmp(&b.spot_symbol.to_string()));
    pairs
}

/// 剥离币安永续合约 base 开头的"合约乘数"前缀，如 `"1000PEPE"` ->
/// `(1000, "PEPE")`。认两种前缀：
/// - "1" 后面跟若干个 "0"(10/100/1000/10000/1000000...)，如 `1000PEPEUSDT`/
///   `1000SHIBUSDT`/`1000000MOGUSDT`。要求至少 2 位数字，避免把单个数字开头的
///   真实 ticker(`1INCH`)误判成带乘数。
/// - "1M"(=1000000)，如 `1MBABYDOGEUSDT`。
///
/// 调用方应先按原 base 精确匹配，匹配不上再用这个函数剥离后重试。
pub fn strip_contract_multiplier(perp_base: &str) -> Option<(u64, String)> {
    if let Some(core) = perp_base.strip_prefix("1M").filter(|c| !c.is_empty() && !c.starts_with(|ch: char| ch.is_ascii_digit())) {
        return Some((1_000_000, core.to_string()));
    }
    let digit_len = perp_base.chars().take_while(|c| c.is_ascii_digit()).count();
    if digit_len < 2 {
        return None;
    }
    let (digits, core) = perp_base.split_at(digit_len);
    if core.is_empty() || !digits.starts_with('1') || !digits[1..].chars().all(|c| c == '0') {
        return None;
    }
    let multiplier: u64 = digits.parse().ok()?;
    Some((multiplier, core.to_string()))
}

/// `1000PEPEUSDT` 这类带乘数前缀的合约，报价和数量 1 都对应 1000 个币；
/// 没有前缀的合约两个单位都是 1。前缀规则见 [`strip_contract_multiplier`]——
/// 它只能按名字猜(真实 ticker 恰好以 `1000` 开头时会误判)，调用方应当再用
/// 实际价格做一次合理性检查兜底。
pub fn binance_perp_contract_spec(symbol: &Symbol) -> PerpContractSpec {
    let multiplier = strip_contract_multiplier(&symbol.base.to_ascii_uppercase())
        .map(|(multiplier, _)| Decimal::from(multiplier))
        .unwrap_or(Decimal::ONE);
    PerpContractSpec { price_unit: multiplier, qty_unit: multiplier }
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

        let payload = "symbol=BTCUSDT&timestamp=1700000000000";
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
            ("timestamp".to_string(), "123".to_string()),
        ];
        assert_eq!(build_query_string(&params), "symbol=BTCUSDT&timestamp=123");
    }

    #[test]
    fn parses_spot_trading_fee_response() {
        let text = r#"[{"symbol":"BTCUSDT","makerCommission":"0.001000","takerCommission":"0.001000"}]"#;
        let fee = parse_spot_trading_fee(text, &Symbol::new("BTC", "USDT")).expect("should parse");
        assert_eq!(fee.maker_bps, Decimal::from(10));
        assert_eq!(fee.taker_bps, Decimal::from(10));
    }

    #[test]
    fn parse_spot_trading_fee_surfaces_error_response() {
        let text = r#"{"code":-2014,"msg":"API-key format invalid."}"#;
        let err = parse_spot_trading_fee(text, &Symbol::new("BTC", "USDT")).unwrap_err();
        assert!(err.to_string().contains("-2014"));
    }

    #[test]
    fn parses_futures_trading_fee_response() {
        let text = r#"{"symbol":"BTCUSDT","makerCommissionRate":"0.0000200","takerCommissionRate":"0.0004000"}"#;
        let fee = parse_futures_trading_fee(text).expect("should parse");
        assert_eq!(fee.maker_bps, "0.2".parse().unwrap());
        assert_eq!(fee.taker_bps, Decimal::from(4));
    }

    #[test]
    fn parse_futures_trading_fee_surfaces_error_response() {
        let text = r#"{"code":-1121,"msg":"Invalid symbol."}"#;
        let err = parse_futures_trading_fee(text).unwrap_err();
        assert!(err.to_string().contains("-1121"));
    }

    #[test]
    fn parses_usdt_spot_symbols_filters_by_quote_and_status() {
        let text = r#"{
            "symbols": [
                {"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT"},
                {"symbol":"ETHBTC","status":"TRADING","baseAsset":"ETH","quoteAsset":"BTC"},
                {"symbol":"OLDUSDT","status":"BREAK","baseAsset":"OLD","quoteAsset":"USDT"}
            ]
        }"#;
        let symbols = parse_usdt_spot_symbols(text).expect("should parse");
        assert_eq!(symbols, vec![Symbol::new("BTC", "USDT")]);
    }

    #[test]
    fn parses_usdt_perpetual_symbols_filters_by_contract_type_quote_and_status() {
        let text = r#"{
            "symbols": [
                {"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDT"},
                {"symbol":"BTCUSD_PERP","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USD"},
                {"symbol":"BTCUSDT_240628","status":"TRADING","contractType":"CURRENT_QUARTER","baseAsset":"BTC","quoteAsset":"USDT"},
                {"symbol":"OLDUSDT","status":"SETTLING","contractType":"PERPETUAL","baseAsset":"OLD","quoteAsset":"USDT"}
            ]
        }"#;
        let symbols = parse_usdt_perpetual_symbols(text).expect("should parse");
        assert_eq!(symbols, vec![Symbol::new("BTC", "USDT")]);
    }

    #[test]
    fn parse_usdt_spot_symbols_surfaces_error_response() {
        let text = r#"{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}"#;
        let err = parse_usdt_spot_symbols(text).unwrap_err();
        assert!(err.to_string().contains("-2015"));
    }

    #[test]
    fn parse_spot_market_precisions_prefers_market_lot_size_over_lot_size() {
        // 市价单精度按 MARKET_LOT_SIZE 校验，即便 stepSize 比 LOT_SIZE 粗，
        // 也必须优先取它——否则会复现 -1111 Precision is over the maximum
        // defined for this asset。
        let text = r#"{
            "symbols": [
                {
                    "symbol": "APEUSDT",
                    "status": "TRADING",
                    "baseAsset": "APE",
                    "quoteAsset": "USDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                        {"filterType": "LOT_SIZE", "minQty": "1", "maxQty": "1000000", "stepSize": "1"},
                        {"filterType": "MARKET_LOT_SIZE", "minQty": "10", "maxQty": "500000", "stepSize": "10"}
                    ]
                }
            ]
        }"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert_eq!(precisions.len(), 1);
        let info = &precisions[0];
        assert_eq!(info.market.qty_step, "10".parse().unwrap());
        assert_eq!(info.market.min_qty, "10".parse().unwrap());
        assert_eq!(info.limit.qty_step, "1".parse().unwrap());
        assert_eq!(info.limit.min_qty, "1".parse().unwrap());
        assert_eq!(info.price_tick, "0.0001".parse().unwrap());
    }

    #[test]
    fn parse_spot_market_precisions_falls_back_to_lot_size_when_market_lot_size_missing() {
        let text = r#"{
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "filters": [
                        {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"}
                    ]
                }
            ]
        }"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert_eq!(precisions.len(), 1);
        assert_eq!(precisions[0].market, precisions[0].limit);
        assert_eq!(precisions[0].market.qty_step, "0.001".parse().unwrap());
    }

    #[test]
    fn parse_spot_market_precisions_filters_by_status_and_quote() {
        let text = r#"{
            "symbols": [
                {"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[{"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"}]},
                {"symbol":"ETHBTC","status":"TRADING","baseAsset":"ETH","quoteAsset":"BTC","filters":[{"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"}]},
                {"symbol":"OLDUSDT","status":"BREAK","baseAsset":"OLD","quoteAsset":"USDT","filters":[{"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"}]}
            ]
        }"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert_eq!(precisions.len(), 1);
        assert_eq!(precisions[0].symbol, Symbol::new("BTC", "USDT"));
    }

    #[test]
    fn parse_perpetual_market_precisions_prefers_market_lot_size() {
        let text = r#"{
            "symbols": [
                {
                    "symbol": "APEUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "baseAsset": "APE",
                    "quoteAsset": "USDT",
                    "filters": [
                        {"filterType": "LOT_SIZE", "minQty": "1", "maxQty": "1000000", "stepSize": "1"},
                        {"filterType": "MARKET_LOT_SIZE", "minQty": "10", "maxQty": "500000", "stepSize": "10"}
                    ]
                }
            ]
        }"#;
        let precisions = parse_perpetual_market_precisions(text).expect("should parse");
        assert_eq!(precisions.len(), 1);
        assert_eq!(precisions[0].market.qty_step, "10".parse().unwrap());
    }

    #[test]
    fn parse_spot_market_precisions_surfaces_error_response() {
        let text = r#"{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}"#;
        let err = parse_spot_market_precisions(text).unwrap_err();
        assert!(err.to_string().contains("-2015"));
    }

    #[test]
    fn parses_funding_rate_history_sorted_ascending() {
        let text = r#"[
            {"symbol":"BTCUSDT","fundingTime":1789689600003,"fundingRate":"0.00007769","markPrice":"76385.9","rateType":"Regular"},
            {"symbol":"BTCUSDT","fundingTime":1789660800000,"fundingRate":"-0.00008505","markPrice":"76755.7","rateType":"Regular"}
        ]"#;
        let symbol = Symbol::new("BTC", "USDT");
        let records = parse_funding_rate_history(text, &symbol).expect("should parse");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].funding_time_ms, 1789660800000);
        assert_eq!(records[0].rate, "-0.00008505".parse().unwrap());
        assert_eq!(records[1].symbol, symbol);
    }

    #[test]
    fn parse_funding_rate_history_surfaces_error_response() {
        let text = r#"{"code":-1121,"msg":"Invalid symbol."}"#;
        let err = parse_funding_rate_history(text, &Symbol::new("BTC", "USDT")).unwrap_err();
        assert!(err.to_string().contains("-1121"));
    }

    #[test]
    fn parses_predicted_funding_rates_filters_non_usdt_perpetuals_and_applies_intervals() {
        let premium = r#"[
            {"symbol":"BTCUSDT","markPrice":"77697.6","lastFundingRate":"0.00007217","nextFundingTime":1789718400000,"time":1789715993003},
            {"symbol":"LPTUSDT","markPrice":"5.1","lastFundingRate":"-0.0005","nextFundingTime":1789718400000,"time":1789715993003},
            {"symbol":"AAVEUSDC","markPrice":"300","lastFundingRate":"0.0001","nextFundingTime":1789718400000,"time":1789715993003},
            {"symbol":"BTCUSDT_261225","markPrice":"78800","lastFundingRate":"0.00000000","nextFundingTime":0,"time":1789715993003}
        ]"#;
        let info = r#"[{"symbol":"LPTUSDT","adjustedFundingRateCap":"0.02","adjustedFundingRateFloor":"-0.02","fundingIntervalHours":4,"disclaimer":false,"updateTime":1752854309429}]"#;
        let intervals = parse_funding_intervals(info).expect("should parse fundingInfo");
        let rates = parse_predicted_funding_rates(premium, &intervals).expect("should parse premiumIndex");
        assert_eq!(rates.len(), 2);
        assert_eq!(rates[0].symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(rates[0].rate, "0.00007217".parse().unwrap());
        assert_eq!(rates[0].next_funding_time_ms, 1789718400000);
        assert_eq!(rates[0].funding_interval_hours, 8);
        assert_eq!(rates[1].symbol, Symbol::new("LPT", "USDT"));
        assert_eq!(rates[1].funding_interval_hours, 4);
    }

    #[test]
    fn perp_contract_spec_uses_multiplier_prefix_for_both_units() {
        let pepe = binance_perp_contract_spec(&Symbol::new("1000PEPE", "USDT"));
        assert_eq!(pepe, PerpContractSpec { price_unit: Decimal::from(1000), qty_unit: Decimal::from(1000) });
        let btc = binance_perp_contract_spec(&Symbol::new("BTC", "USDT"));
        assert_eq!(btc, PerpContractSpec { price_unit: Decimal::ONE, qty_unit: Decimal::ONE });
        assert_eq!(binance_perp_contract_spec(&Symbol::new("1INCH", "USDT")).qty_unit, Decimal::ONE);
    }

    #[test]
    fn strip_contract_multiplier_recognizes_power_of_ten_prefixes() {
        assert_eq!(strip_contract_multiplier("1000PEPE"), Some((1000, "PEPE".to_string())));
        assert_eq!(strip_contract_multiplier("1000000BABYDOGE"), Some((1_000_000, "BABYDOGE".to_string())));
        assert_eq!(strip_contract_multiplier("1MBABYDOGE"), Some((1_000_000, "BABYDOGE".to_string())));
    }

    #[test]
    fn strip_contract_multiplier_rejects_non_power_of_ten_and_short_prefixes() {
        assert_eq!(strip_contract_multiplier("1INCH"), None);
        assert_eq!(strip_contract_multiplier("25BTC"), None);
        assert_eq!(strip_contract_multiplier("1000"), None);
        assert_eq!(strip_contract_multiplier("1M"), None);
    }

    #[test]
    fn build_spot_perp_pairs_matches_exact_base_with_multiplier_one() {
        let spot = vec![Symbol::new("BTC", "USDT"), Symbol::new("1INCH", "USDT")];
        let perp = vec![Symbol::new("BTC", "USDT"), Symbol::new("1INCH", "USDT")];
        let pairs = build_spot_perp_pairs(&spot, &perp);
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().all(|p| p.contract_multiplier == 1));
        assert!(pairs.iter().any(|p| p.spot_symbol == Symbol::new("1INCH", "USDT")));
    }

    #[test]
    fn build_spot_perp_pairs_matches_multiplier_prefixed_perp_symbols() {
        let spot = vec![Symbol::new("PEPE", "USDT")];
        let perp = vec![Symbol::new("1000PEPE", "USDT")];
        let pairs = build_spot_perp_pairs(&spot, &perp);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].spot_symbol, Symbol::new("PEPE", "USDT"));
        assert_eq!(pairs[0].perp_symbol, Symbol::new("1000PEPE", "USDT"));
        assert_eq!(pairs[0].contract_multiplier, 1000);
    }

    #[test]
    fn build_spot_perp_pairs_skips_perp_symbols_without_matching_spot() {
        let spot = vec![Symbol::new("BTC", "USDT")];
        let perp = vec![Symbol::new("ETH", "USDT"), Symbol::new("1000RATS", "USDT")];
        assert!(build_spot_perp_pairs(&spot, &perp).is_empty());
    }
}
