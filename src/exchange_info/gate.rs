use anyhow::Context;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::types::{Symbol, Venue};

use super::ExchangeInfoProvider;
use super::types::{MarketPrecision, QtyPrecision, TradingFee};

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
        anyhow::bail!("gate perpetual trading fee is not supported: no perpetual contracts are wired up for gate")
    }

    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self.public_request("/spot/currency_pairs", &[]).await?;
        parse_usdt_spot_symbols(&text)
    }

    /// Gate 当前没有接入永续合约场景，返回空列表，和 Kraken
    /// `usdt_perpetual_symbols` 的既有约定一致，不是 bug。
    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        Ok(Vec::new())
    }

    async fn spot_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        let text = self.public_request("/spot/currency_pairs", &[]).await?;
        parse_spot_market_precisions(&text)
    }

    async fn perpetual_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        Ok(Vec::new())
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
}
