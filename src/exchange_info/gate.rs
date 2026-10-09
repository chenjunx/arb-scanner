use anyhow::Context;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::types::{Symbol, Venue};

use super::types::{
    FundingRateRecord, MarketPrecision, PerpContractSpec, PredictedFundingRate, QtyPrecision, TradingFee,
};
use super::{ExchangeInfoProvider, FundingRateProvider};

const HOST: &str = "https://api.gateio.ws";
const API_PREFIX: &str = "/api/v4";

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// Gate.io "基础信息"客户端：查询账户实际手续费率、列出可交易的 USDT
/// 计价现货交易对。签名方式和 `order::gate` 完全相同(REST v4 HMAC-SHA512)，
/// 但和 Kraken 系列的既有约定一样不共享这套签名/请求辅助函数，两个文件各自
/// 独立一份。
///
/// USDT 本位永续合约(`/futures/usdt/*`)的精度、合约乘数、资金费率都是公开数据，
/// 不需要签名。
pub struct GateExchangeInfoProvider {
    venue: Venue,
    api_key: String,
    api_secret: String,
    http: reqwest::Client,
}

impl GateExchangeInfoProvider {
    pub fn new(venue: Venue, api_key: String, api_secret: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self { venue, api_key, api_secret, http })
    }

    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let api_key = std::env::var("GATE_API_KEY").context("GATE_API_KEY not set")?;
        let api_secret = std::env::var("GATE_API_SECRET").context("GATE_API_SECRET not set")?;
        Self::new(venue, api_key, api_secret, proxy)
    }

    async fn signed_request(&self, path: &str) -> anyhow::Result<String> {
        let timestamp = now_secs().to_string();
        let full_path = format!("{API_PREFIX}{path}");
        let signature = gate_sign(&self.api_secret, "GET", &full_path, "", "", &timestamp);

        crate::ratelimit::throttle(HOST).await;
        let resp = self
            .http
            .get(format!("{HOST}{full_path}"))
            .header("KEY", &self.api_key)
            .header("Timestamp", &timestamp)
            .header("SIGN", &signature)
            .send()
            .await
            .context("gate exchange_info request failed")?;
        resp.text().await.context("failed to read gate exchange_info response body")
    }

    async fn public_request(&self, path: &str, query_params: &[(String, String)]) -> anyhow::Result<String> {
        let query_string = query_params.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
        let url = if query_string.is_empty() {
            format!("{HOST}{API_PREFIX}{path}")
        } else {
            format!("{HOST}{API_PREFIX}{path}?{query_string}")
        };
        crate::ratelimit::throttle(HOST).await;
        let resp = self.http.get(&url).send().await.context("gate exchange_info public request failed")?;
        resp.text().await.context("failed to read gate exchange_info public response body")
    }
}

#[async_trait]
impl ExchangeInfoProvider for GateExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `GET /wallet/fee` 返回账户当前(不区分 symbol 的)统一 maker/taker 费率，
    /// 忽略传入的 `symbol` 参数——和 Kraken 按 pair 查询不同，Gate 现货手续费是
    /// 账户级别的统一费率，不按交易对区分。
    async fn spot_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
        let text = self.signed_request("/wallet/fee").await?;
        parse_trading_fee(&text)
    }

    async fn perpetual_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
        anyhow::bail!("gate perpetual trading fee is not supported yet")
    }

    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self.public_request("/spot/currency_pairs", &[]).await?;
        parse_usdt_spot_symbols(&text)
    }

    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self.public_request("/futures/usdt/contracts", &[]).await?;
        Ok(parse_perpetual_market_precisions(&text)?.into_iter().map(|p| p.symbol).collect())
    }

    async fn spot_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        let text = self.public_request("/spot/currency_pairs", &[]).await?;
        parse_spot_market_precisions(&text)
    }

    async fn perpetual_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        let text = self.public_request("/futures/usdt/contracts", &[]).await?;
        parse_perpetual_market_precisions(&text)
    }

    async fn perpetual_contract_spec(&self, symbol: &Symbol) -> anyhow::Result<PerpContractSpec> {
        let text = self.public_request(&format!("/futures/usdt/contracts/{}", gate_contract(symbol)), &[]).await?;
        parse_perp_contract_spec(&text)
    }
}

/// `/futures/usdt/funding_rate` 单次最多返回 1000 条(`limit=1001` 会报
/// `INVALID_PARAM_VALUE`)。只传 `to` 时实际返回的是 `to` 往前 30 天的窗口
/// (1h 周期也才 720 条)，不会被这个上限截断。
const FUNDING_HISTORY_PAGE_LIMIT: usize = 1000;

#[async_trait]
impl FundingRateProvider for GateExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `/futures/usdt/funding_rate` 按时间倒序返回、时间单位是秒。这个接口的
    /// `from` 不能早于 180 天前(否则报 `from time exceeds 180-day limit`)，所以
    /// 不传 `from`，只用 `to` 从区间末尾往前一段一段翻(每段约 30 天)，直到越过
    /// `start_time_ms` 或翻到空页(合约上线前)，最后按毫秒边界过滤并升序排列。
    async fn funding_rate_history(
        &self,
        symbol: &Symbol,
        start_time_ms: u64,
        end_time_ms: Option<u64>,
    ) -> anyhow::Result<Vec<FundingRateRecord>> {
        let contract = gate_contract(symbol);
        let from_secs = start_time_ms / 1000;
        let mut to_secs = end_time_ms.map(|ms| ms / 1000);
        let mut records = Vec::new();
        loop {
            let mut query = vec![
                ("contract".to_string(), contract.clone()),
                ("limit".to_string(), FUNDING_HISTORY_PAGE_LIMIT.to_string()),
            ];
            if let Some(to) = to_secs {
                query.push(("to".to_string(), to.to_string()));
            }
            let text = self.public_request("/futures/usdt/funding_rate", &query).await?;
            let page = parse_funding_rate_history(&text, symbol)?;
            let oldest_secs = page.iter().map(|r| r.funding_time_ms / 1000).min();
            records.extend(page);
            match oldest_secs {
                // 要求 `to` 严格往前推进，防止接口行为变化时死循环。
                Some(oldest) if oldest > from_secs && to_secs.is_none_or(|to| oldest <= to) => to_secs = Some(oldest - 1),
                _ => break,
            }
        }
        records.retain(|r| r.funding_time_ms >= start_time_ms && end_time_ms.is_none_or(|end| r.funding_time_ms <= end));
        records.sort_by_key(|r| r.funding_time_ms);
        records.dedup_by_key(|r| r.funding_time_ms);
        Ok(records)
    }

    /// 不传 `from`/`to` 时 `/futures/usdt/funding_rate` 返回最近 `limit` 条(倒序)。
    async fn recent_funding_rates(&self, symbol: &Symbol, limit: usize) -> anyhow::Result<Vec<FundingRateRecord>> {
        let query = vec![
            ("contract".to_string(), gate_contract(symbol)),
            ("limit".to_string(), limit.clamp(1, FUNDING_HISTORY_PAGE_LIMIT).to_string()),
        ];
        let text = self.public_request("/futures/usdt/funding_rate", &query).await?;
        let mut records = parse_funding_rate_history(&text, symbol)?;
        records.sort_by_key(|r| r.funding_time_ms);
        Ok(records)
    }

    /// `/futures/usdt/contracts` 一次返回全部合约：`funding_rate` 是将在
    /// `funding_next_apply`(秒)结算的费率，`funding_interval` 是结算周期(秒)。
    /// `funding_rate_indicative` 已被 Gate 标记为废弃，和 `funding_rate` 同值，不用它。
    async fn predicted_funding_rates(&self) -> anyhow::Result<Vec<PredictedFundingRate>> {
        let text = self.public_request("/futures/usdt/contracts", &[]).await?;
        parse_predicted_funding_rates(&text)
    }
}

/// Gate 永续合约名是 `BASE_QUOTE`(如 `BTC_USDT`)。
fn gate_contract(symbol: &Symbol) -> String {
    format!("{}_{}", symbol.base, symbol.quote).to_ascii_uppercase()
}

fn build_http_client(proxy: Option<&str>) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy) = proxy {
        let proxy = reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build gate http client")
}

/// 和 `order::gate::gate_sign` 完全相同的算法，两个文件各自独立一份实现
/// (同 Kraken 系列的既有约定)。
fn gate_sign(secret: &str, method: &str, url_path: &str, query_string: &str, body: &str, timestamp: &str) -> String {
    let body_hash = hex_encode(ring::digest::digest(&ring::digest::SHA512, body.as_bytes()).as_ref());
    let payload = format!("{method}\n{url_path}\n{query_string}\n{body_hash}\n{timestamp}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA512, secret.as_bytes());
    let signature = ring::hmac::sign(&key, payload.as_bytes());
    hex_encode(signature.as_ref())
}

#[derive(Debug, Deserialize)]
struct GateErrorResponse {
    label: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct WalletFeeResponse {
    taker_fee: Decimal,
    maker_fee: Decimal,
}

/// `/wallet/fee` 返回的是小数(fraction)如 `"0.0020"` 表示 0.2%，换算成 bps
/// 要乘以 `10000`——和 `exchange_info::kraken::parse_trading_fee` 的 `×100`
/// 不同，因为 Kraken `TradeVolume` 返回的是百分数字符串(如 "0.2600" 表示
/// 0.26%)，两者小数点位置差了 2 位。
fn parse_trading_fee(text: &str) -> anyhow::Result<TradingFee> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let resp: WalletFeeResponse =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate wallet fee response, raw body: {text}"))?;
    let bps_multiplier = Decimal::from(10000);
    Ok(TradingFee { maker_bps: resp.maker_fee * bps_multiplier, taker_bps: resp.taker_fee * bps_multiplier })
}

#[derive(Debug, Deserialize)]
struct CurrencyPairEntry {
    #[serde(default)]
    base: Option<String>,
    #[serde(default)]
    quote: Option<String>,
    #[serde(default)]
    trade_status: Option<String>,
    #[serde(default)]
    amount_precision: Option<u32>,
    #[serde(default)]
    precision: Option<u32>,
    #[serde(default)]
    min_base_amount: Option<String>,
}

fn is_tradable_usdt_pair(entry: &CurrencyPairEntry) -> bool {
    entry.trade_status.as_deref() == Some("tradable")
        && entry.quote.as_deref().is_some_and(|q| q.eq_ignore_ascii_case("USDT"))
}

fn parse_usdt_spot_symbols(text: &str) -> anyhow::Result<Vec<Symbol>> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let pairs: Vec<CurrencyPairEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate currency_pairs response, raw body: {text}"))?;
    let mut symbols: Vec<Symbol> = pairs
        .into_iter()
        .filter(is_tradable_usdt_pair)
        .filter_map(|p| Some(Symbol::new(p.base?, "USDT")))
        .collect();
    symbols.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    symbols.dedup();
    Ok(symbols)
}

/// Gate 不区分市价/限价精度规则，`market == limit`。`amount_precision`/
/// `precision` 缺失时代表这个交易对没有可用的精度信息，跳过而不是拿一个
/// 猜测值凑数，和 `exchange_info::kraken::parse_spot_market_precisions`
/// 的既有约定一致。`min_base_amount` 缺失时按 0 处理。
fn parse_spot_market_precisions(text: &str) -> anyhow::Result<Vec<MarketPrecision>> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let pairs: Vec<CurrencyPairEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate currency_pairs response, raw body: {text}"))?;
    Ok(pairs
        .into_iter()
        .filter(is_tradable_usdt_pair)
        .filter_map(|p| {
            let base = p.base?;
            let amount_precision = p.amount_precision?;
            let qty_step = Decimal::new(1, amount_precision);
            let min_qty = p.min_base_amount.and_then(|v| v.parse().ok()).unwrap_or(Decimal::ZERO);
            let price_tick = p.precision.map(|d| Decimal::new(1, d)).unwrap_or(Decimal::ZERO);
            let qty_precision = QtyPrecision { qty_step, min_qty };
            Some(MarketPrecision { symbol: Symbol::new(base, "USDT"), market: qty_precision, limit: qty_precision, price_tick })
        })
        .collect())
}

#[derive(Debug, Deserialize)]
struct FundingRateEntry {
    t: u64,
    r: Decimal,
}

fn parse_funding_rate_history(text: &str, symbol: &Symbol) -> anyhow::Result<Vec<FundingRateRecord>> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let entries: Vec<FundingRateEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate funding_rate response, raw body: {text}"))?;
    Ok(entries
        .into_iter()
        .map(|e| FundingRateRecord { symbol: symbol.clone(), rate: e.r, funding_time_ms: e.t * 1000 })
        .collect())
}

#[derive(Debug, Deserialize)]
struct FuturesContractEntry {
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    funding_rate: Option<Decimal>,
    /// Gate 文档里标的是 double，按 f64 读再取整，避免带小数时反序列化失败。
    #[serde(default)]
    funding_next_apply: Option<f64>,
    #[serde(default)]
    funding_interval: Option<u64>,
}

/// Gate 合约接口里数值字段有的是字符串(`quanto_multiplier`/`order_price_round`)，
/// 有的是 JSON 数字(`order_size_min`)，统一按 `Value` 读再转 `Decimal`。
fn json_decimal(value: &serde_json::Value) -> Option<Decimal> {
    match value {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
struct FuturesContractSpecEntry {
    name: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    quanto_multiplier: serde_json::Value,
    #[serde(default)]
    order_size_min: serde_json::Value,
    #[serde(default)]
    order_price_round: serde_json::Value,
}

/// 下单量单位是"张"，统一按整数张下单：步进 1、最少 1 张(`order_size_min`
/// 更大时取它)。部分合约 `enable_decimal=true` 允许小数张，这里不用，避免
/// 下单接口对小数 size 的兼容性问题。价格 tick 取 `order_price_round`，缺失时
/// 记 0(不按 tick 取整)。
fn parse_perpetual_market_precisions(text: &str) -> anyhow::Result<Vec<MarketPrecision>> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let contracts: Vec<FuturesContractSpecEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate futures contracts response, raw body: {text}"))?;
    Ok(contracts
        .into_iter()
        .filter(|c| c.status.as_deref().is_none_or(|s| s == "trading"))
        .filter_map(|c| {
            let (base, quote) = c.name.split_once('_')?;
            if !quote.eq_ignore_ascii_case("USDT") || base.is_empty() {
                return None;
            }
            let min_qty = json_decimal(&c.order_size_min).unwrap_or(Decimal::ONE).max(Decimal::ONE).ceil();
            let price_tick = json_decimal(&c.order_price_round).unwrap_or(Decimal::ZERO);
            let qty_precision = QtyPrecision { qty_step: Decimal::ONE, min_qty };
            Some(MarketPrecision { symbol: Symbol::new(base, "USDT"), market: qty_precision, limit: qty_precision, price_tick })
        })
        .collect())
}

/// Gate 永续报价就是每币价格(`price_unit = 1`)，下单量单位是张，
/// 1 张 = `quanto_multiplier` 个币。
fn parse_perp_contract_spec(text: &str) -> anyhow::Result<PerpContractSpec> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let contract: FuturesContractSpecEntry =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate futures contract response, raw body: {text}"))?;
    let qty_unit = json_decimal(&contract.quanto_multiplier)
        .filter(|m| *m > Decimal::ZERO)
        .with_context(|| format!("gate contract {} has no valid quanto_multiplier", contract.name))?;
    Ok(PerpContractSpec { price_unit: Decimal::ONE, qty_unit })
}

/// 只保留 `status == "trading"` 的 `XXX_USDT` 合约；缺费率/结算时间/周期的条目
/// 跳过而不是拿猜测值凑数。
fn parse_predicted_funding_rates(text: &str) -> anyhow::Result<Vec<PredictedFundingRate>> {
    if let Ok(err) = serde_json::from_str::<GateErrorResponse>(text) {
        anyhow::bail!("gate error {}: {}", err.label, err.message);
    }
    let contracts: Vec<FuturesContractEntry> =
        serde_json::from_str(text).with_context(|| format!("failed to parse gate futures contracts response, raw body: {text}"))?;
    Ok(contracts
        .into_iter()
        .filter(|c| c.status.as_deref().is_none_or(|s| s == "trading"))
        .filter_map(|c| {
            let (base, quote) = c.name.split_once('_')?;
            if !quote.eq_ignore_ascii_case("USDT") || base.is_empty() {
                return None;
            }
            let next_apply_secs = c.funding_next_apply.filter(|t| *t > 0.0)?;
            let interval_secs = c.funding_interval.filter(|s| *s > 0)?;
            Some(PredictedFundingRate {
                symbol: Symbol::new(base, "USDT"),
                rate: c.funding_rate?,
                next_funding_time_ms: next_apply_secs as u64 * 1000,
                funding_interval_hours: (interval_secs / 3600) as u32,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_sign_changes_with_any_field() {
        let secret = "test-secret";
        let base = gate_sign(secret, "GET", "/api/v4/wallet/fee", "", "", "1700000000");

        assert_ne!(base, gate_sign(secret, "POST", "/api/v4/wallet/fee", "", "", "1700000000"));
        assert_ne!(base, gate_sign(secret, "GET", "/api/v4/spot/orders", "", "", "1700000000"));
        assert_ne!(base, gate_sign(secret, "GET", "/api/v4/wallet/fee", "a=b", "", "1700000000"));
        assert_ne!(base, gate_sign(secret, "GET", "/api/v4/wallet/fee", "", "", "1700000001"));
        assert_ne!(base, gate_sign("other-secret", "GET", "/api/v4/wallet/fee", "", "", "1700000000"));
    }

    #[test]
    fn parses_trading_fee_response() {
        let text = r#"{"user_id":123,"taker_fee":"0.0020","maker_fee":"0.0016"}"#;
        let fee = parse_trading_fee(text).expect("should parse");
        assert_eq!(fee.taker_bps, Decimal::from(20));
        assert_eq!(fee.maker_bps, Decimal::from(16));
    }

    #[test]
    fn parse_trading_fee_surfaces_error_response() {
        let text = r#"{"label":"INVALID_KEY","message":"invalid key provided"}"#;
        let err = parse_trading_fee(text).unwrap_err();
        assert!(err.to_string().contains("invalid key provided"));
    }

    #[test]
    fn parses_usdt_spot_symbols_filters_by_quote_and_status() {
        let text = r#"[
            {"id":"BTC_USDT","base":"BTC","quote":"USDT","trade_status":"tradable"},
            {"id":"BTC_USD","base":"BTC","quote":"USD","trade_status":"tradable"},
            {"id":"OLD_USDT","base":"OLD","quote":"USDT","trade_status":"untradable"}
        ]"#;
        let symbols = parse_usdt_spot_symbols(text).expect("should parse");
        assert_eq!(symbols, vec![Symbol::new("BTC", "USDT")]);
    }

    #[test]
    fn parse_usdt_spot_symbols_surfaces_error_response() {
        let text = r#"{"label":"INVALID_PARAM_VALUE","message":"bad request"}"#;
        let err = parse_usdt_spot_symbols(text).unwrap_err();
        assert!(err.to_string().contains("bad request"));
    }

    #[test]
    fn parse_spot_market_precisions_reads_amount_and_price_precision() {
        let text = r#"[
            {"id":"BTC_USDT","base":"BTC","quote":"USDT","trade_status":"tradable","amount_precision":6,"precision":2,"min_base_amount":"0.0001"}
        ]"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert_eq!(precisions.len(), 1);
        let info = &precisions[0];
        assert_eq!(info.symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(info.market, info.limit);
        assert_eq!(info.market.qty_step, Decimal::new(1, 6));
        assert_eq!(info.market.min_qty, "0.0001".parse().unwrap());
        assert_eq!(info.price_tick, Decimal::new(1, 2));
    }

    #[test]
    fn parse_spot_market_precisions_defaults_min_qty_to_zero_when_missing() {
        let text = r#"[
            {"id":"BTC_USDT","base":"BTC","quote":"USDT","trade_status":"tradable","amount_precision":6,"precision":2}
        ]"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert_eq!(precisions[0].market.min_qty, Decimal::ZERO);
    }

    #[test]
    fn parse_spot_market_precisions_skips_pairs_missing_amount_precision() {
        let text = r#"[
            {"id":"BTC_USDT","base":"BTC","quote":"USDT","trade_status":"tradable","precision":2}
        ]"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert!(precisions.is_empty());
    }

    #[test]
    fn parse_spot_market_precisions_skips_non_tradable_pairs() {
        let text = r#"[
            {"id":"BTC_USDT","base":"BTC","quote":"USDT","trade_status":"untradable","amount_precision":6,"precision":2}
        ]"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert!(precisions.is_empty());
    }

    #[test]
    fn gate_contract_uses_underscore_separator() {
        assert_eq!(gate_contract(&Symbol::new("btc", "usdt")), "BTC_USDT");
    }

    #[test]
    fn parses_funding_rate_history_converting_seconds_to_ms() {
        let text = r#"[{"r":"0.00002","t":1789689601},{"r":"-0.000049","t":1789660801}]"#;
        let symbol = Symbol::new("BTC", "USDT");
        let records = parse_funding_rate_history(text, &symbol).expect("should parse");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].funding_time_ms, 1789689601000);
        assert_eq!(records[1].rate, "-0.000049".parse().unwrap());
    }

    #[test]
    fn parse_funding_rate_history_surfaces_error_response() {
        let text = r#"{"label":"INVALID_PARAM_VALUE","message":"Invalid request parameter `limit` value: 1001"}"#;
        let err = parse_funding_rate_history(text, &Symbol::new("BTC", "USDT")).unwrap_err();
        assert!(err.to_string().contains("INVALID_PARAM_VALUE"));
    }

    #[test]
    fn parses_predicted_funding_rates_filters_status_and_quote() {
        let text = r#"[
            {"name":"BTC_USDT","status":"trading","funding_rate":"0.000057","funding_rate_indicative":"0.000057","funding_next_apply":1789718400,"funding_interval":28800},
            {"name":"LPT_USDT","status":"trading","funding_rate":"-0.0005","funding_next_apply":1789718400.0,"funding_interval":14400},
            {"name":"OLD_USDT","status":"delisted","funding_rate":"0.0001","funding_next_apply":1789718400,"funding_interval":28800},
            {"name":"BTC_USD","status":"trading","funding_rate":"0.0001","funding_next_apply":1789718400,"funding_interval":28800}
        ]"#;
        let rates = parse_predicted_funding_rates(text).expect("should parse");
        assert_eq!(rates.len(), 2);
        assert_eq!(rates[0].symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(rates[0].rate, "0.000057".parse().unwrap());
        assert_eq!(rates[0].next_funding_time_ms, 1789718400000);
        assert_eq!(rates[0].funding_interval_hours, 8);
        assert_eq!(rates[1].funding_interval_hours, 4);
    }

    #[test]
    fn parse_perpetual_market_precisions_uses_whole_contracts() {
        let text = r#"[
            {"name":"PEPE_USDT","status":"trading","quanto_multiplier":"10000000","order_size_min":0,"order_price_round":"0.000000001"},
            {"name":"BTC_USDT","status":"trading","quanto_multiplier":"0.0001","order_size_min":5,"order_price_round":"0.1"},
            {"name":"OLD_USDT","status":"delisted","quanto_multiplier":"1","order_size_min":1,"order_price_round":"0.1"}
        ]"#;
        let precisions = parse_perpetual_market_precisions(text).expect("should parse");
        assert_eq!(precisions.len(), 2);
        assert_eq!(precisions[0].symbol, Symbol::new("PEPE", "USDT"));
        assert_eq!(precisions[0].market.qty_step, Decimal::ONE);
        assert_eq!(precisions[0].market.min_qty, Decimal::ONE);
        assert_eq!(precisions[0].price_tick, "0.000000001".parse().unwrap());
        assert_eq!(precisions[1].market.min_qty, Decimal::from(5));
    }

    #[test]
    fn parses_perp_contract_spec_from_quanto_multiplier() {
        let text = r#"{"name":"PEPE_USDT","status":"trading","quanto_multiplier":"10000000","order_size_min":0}"#;
        let spec = parse_perp_contract_spec(text).expect("should parse");
        assert_eq!(spec, PerpContractSpec { price_unit: Decimal::ONE, qty_unit: Decimal::from(10_000_000) });
    }

    #[test]
    fn parse_perp_contract_spec_rejects_missing_multiplier_and_errors() {
        assert!(parse_perp_contract_spec(r#"{"name":"X_USDT"}"#).is_err());
        let err = parse_perp_contract_spec(r#"{"label":"CONTRACT_NOT_FOUND","message":"contract not found"}"#).unwrap_err();
        assert!(err.to_string().contains("CONTRACT_NOT_FOUND"));
    }

    #[test]
    fn parse_predicted_funding_rates_skips_entries_missing_funding_fields() {
        let text = r#"[{"name":"BTC_USDT","status":"trading","funding_rate":"0.000057"}]"#;
        assert!(parse_predicted_funding_rates(text).expect("should parse").is_empty());
    }
}
