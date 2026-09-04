use anyhow::Context;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::market_data::now_ms;
use crate::types::{Symbol, Venue};

use super::ExchangeInfoProvider;
use super::types::{MarketPrecision, QtyPrecision, TradingFee};

const HOST: &str = "https://api.coinex.com";
const API_PREFIX: &str = "/v2";

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// CoinEx v2 "基础信息"客户端：查询账户实际交易手续费率、列出可交易的 USDT
/// 计价现货交易对。CoinEx 也有 USDT 本位永续合约，但当前项目只把 CoinEx
/// 接入成现货副交易所，不为它另外接入合约端点——`perpetual_trading_fee`
/// 直接报错、`usdt_perpetual_symbols`/`perpetual_market_precisions` 返回空
/// Vec，和 `exchange_info::gate` 对没有接入合约场景的既有约定一致，不是 bug。
pub struct CoinexExchangeInfoProvider {
    venue: Venue,
    access_id: String,
    secret_key: String,
    http: reqwest::Client,
}

impl CoinexExchangeInfoProvider {
    pub fn new(venue: Venue, access_id: String, secret_key: String, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self { venue, access_id, secret_key, http })
    }

    /// 从环境变量读取凭证并构造实例：`COINEX_ACCESS_ID` + `COINEX_SECRET_KEY`。
    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let access_id = std::env::var("COINEX_ACCESS_ID").context("COINEX_ACCESS_ID not set")?;
        let secret_key = std::env::var("COINEX_SECRET_KEY").context("COINEX_SECRET_KEY not set")?;
        Self::new(venue, access_id, secret_key, proxy)
    }

    /// CoinEx v2 私有接口签名请求：`GET`，query 参数直接拼进 `request_path`
    /// 参与签名，body 恒为空字符串。
    async fn private_get(&self, path: &str, query: &[(String, String)]) -> anyhow::Result<String> {
        let request_path = build_request_path(path, query);
        let timestamp = now_ms().to_string();
        let signature = coinex_sign(&self.secret_key, "GET", &request_path, "", &timestamp);

        crate::ratelimit::throttle(HOST).await;
        let resp = self
            .http
            .get(format!("{HOST}{request_path}"))
            .header("X-COINEX-KEY", &self.access_id)
            .header("X-COINEX-SIGN", signature)
            .header("X-COINEX-TIMESTAMP", &timestamp)
            .header("Content-Type", "application/json; charset=utf-8")
            .send()
            .await
            .context("coinex exchange_info private request failed")?;
        resp.text().await.context("failed to read coinex exchange_info private response body")
    }

    /// 不需要签名的公开接口请求，用于查询现货交易对列表。
    async fn public_request(&self, path: &str, query: &[(String, String)]) -> anyhow::Result<String> {
        let request_path = build_request_path(path, query);
        crate::ratelimit::throttle(HOST).await;
        let resp = self
            .http
            .get(format!("{HOST}{request_path}"))
            .send()
            .await
            .context("coinex exchange_info public request failed")?;
        resp.text().await.context("failed to read coinex exchange_info public response body")
    }

    fn coinex_market(symbol: &Symbol) -> String {
        format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
    }
}

#[async_trait]
impl ExchangeInfoProvider for CoinexExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `GET /v2/spot/market` 返回的 `maker_fee_rate`/`taker_fee_rate` 只是该
    /// 交易对的默认/基础费率，不是账户实际费率(会受 VIP/做市商折扣影响)，
    /// 账户实际费率要查 `GET /v2/account/trade-fee-rate`。
    async fn spot_trading_fee(&self, symbol: &Symbol) -> anyhow::Result<TradingFee> {
        let query = vec![
            ("market_type".to_string(), "SPOT".to_string()),
            ("market".to_string(), Self::coinex_market(symbol)),
        ];
        let text = self.private_get("/account/trade-fee-rate", &query).await?;
        parse_trading_fee(&text)
    }

    async fn perpetual_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
        anyhow::bail!("coinex perpetual trading fee is not supported: no perpetual contracts are wired up for coinex")
    }

    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let text = self.public_request("/spot/market", &[]).await?;
        parse_usdt_spot_symbols(&text)
    }

    /// CoinEx 当前没有接入永续合约场景，返回空列表，和
    /// `exchange_info::gate::usdt_perpetual_symbols` 的既有约定一致，不是 bug。
    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        Ok(Vec::new())
    }

    /// 复用 [`Self::usdt_spot_symbols`] 已经在打的同一个 `/spot/market` 端点。
    /// CoinEx 没有 MARKET_LOT_SIZE/LOT_SIZE 那种下单方式区分，`market`/`limit`
    /// 两份精度填相同值。
    async fn spot_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        let text = self.public_request("/spot/market", &[]).await?;
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
    builder.build().context("failed to build coinex http client")
}

/// `path` 相对 `/v2` 的路径(如 `/spot/market`)，`query` 按传入顺序拼接——
/// 调用方负责保证签名和实际发出请求用的是同一份 query 顺序。
fn build_request_path(path: &str, query: &[(String, String)]) -> String {
    let base = format!("{API_PREFIX}{path}");
    if query.is_empty() {
        return base;
    }
    let query_string = query.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    format!("{base}?{query_string}")
}

/// CoinEx v2 签名算法：
/// `signature = hex(HMAC_SHA256(secret_key, method + request_path + body + timestamp))`，
/// 全小写十六进制。`request_path` 包含 query string(如
/// `/v2/spot/market?market=BTCUSDT`)，`body` 是请求体原始 JSON 字符串(GET
/// 请求恒为空字符串)，`timestamp` 是毫秒级时间戳字符串，且要和
/// `X-COINEX-TIMESTAMP` 请求头里发送的值完全一致。
fn coinex_sign(secret: &str, method: &str, request_path: &str, body: &str, timestamp: &str) -> String {
    let payload = format!("{method}{request_path}{body}{timestamp}");
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let signature = ring::hmac::sign(&key, payload.as_bytes());
    hex_encode(signature.as_ref())
}

#[derive(Debug, Deserialize)]
struct CoinexEnvelope<T> {
    code: i64,
    #[serde(default)]
    data: Option<T>,
    #[serde(default)]
    message: String,
}

/// 解析 CoinEx v2 通用的 `{code, data, message}` 信封：`code != 0` 视为失败。
fn unwrap_data<T: DeserializeOwned>(text: &str) -> anyhow::Result<T> {
    let envelope: CoinexEnvelope<serde_json::Value> = serde_json::from_str(text)
        .with_context(|| format!("failed to parse coinex response envelope, raw body: {text}"))?;
    if envelope.code != 0 {
        anyhow::bail!("coinex error {}: {}", envelope.code, envelope.message);
    }
    let data = envelope.data.ok_or_else(|| anyhow::anyhow!("coinex response missing data"))?;
    serde_json::from_value(data.clone())
        .with_context(|| format!("failed to parse coinex data payload, raw data: {data}"))
}

#[derive(Debug, Deserialize)]
struct TradeFeeRateData {
    maker_rate: Decimal,
    taker_rate: Decimal,
}

/// `maker_rate`/`taker_rate` 是小数(fraction)，如 `"0.0020"` 表示 0.2%，
/// 换算成 bps 要乘以 `10000`。
fn parse_trading_fee(text: &str) -> anyhow::Result<TradingFee> {
    let data: TradeFeeRateData = unwrap_data(text)?;
    let bps_multiplier = Decimal::from(10000);
    Ok(TradingFee { maker_bps: data.maker_rate * bps_multiplier, taker_bps: data.taker_rate * bps_multiplier })
}

#[derive(Debug, Deserialize)]
struct MarketEntry {
    #[serde(default)]
    base_ccy: Option<String>,
    #[serde(default)]
    quote_ccy: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    base_ccy_precision: Option<u32>,
    #[serde(default)]
    quote_ccy_precision: Option<u32>,
    #[serde(default)]
    min_amount: Option<String>,
}

fn is_online_usdt_market(entry: &MarketEntry) -> bool {
    entry.status.as_deref() == Some("online") && entry.quote_ccy.as_deref().is_some_and(|q| q.eq_ignore_ascii_case("USDT"))
}

fn parse_usdt_spot_symbols(text: &str) -> anyhow::Result<Vec<Symbol>> {
    let markets: Vec<MarketEntry> = unwrap_data(text)?;
    let mut symbols: Vec<Symbol> = markets
        .into_iter()
        .filter(is_online_usdt_market)
        .filter_map(|m| Some(Symbol::new(m.base_ccy?, "USDT")))
        .collect();
    symbols.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    symbols.dedup();
    Ok(symbols)
}

/// CoinEx 不区分市价/限价精度规则，`market == limit`。`base_ccy_precision`/
/// `quote_ccy_precision` 缺失时代表这个交易对没有可用的精度信息，跳过而不是
/// 拿一个猜测值凑数，和 `exchange_info::kraken`/`exchange_info::gate` 的既有
/// 约定一致。`min_amount` 缺失时按 0 处理。
fn parse_spot_market_precisions(text: &str) -> anyhow::Result<Vec<MarketPrecision>> {
    let markets: Vec<MarketEntry> = unwrap_data(text)?;
    Ok(markets
        .into_iter()
        .filter(is_online_usdt_market)
        .filter_map(|m| {
            let base = m.base_ccy?;
            let base_precision = m.base_ccy_precision?;
            let quote_precision = m.quote_ccy_precision?;
            let qty_step = Decimal::new(1, base_precision);
            let min_qty = m.min_amount.and_then(|v| v.parse().ok()).unwrap_or(Decimal::ZERO);
            let price_tick = Decimal::new(1, quote_precision);
            let qty_precision = QtyPrecision { qty_step, min_qty };
            Some(MarketPrecision { symbol: Symbol::new(base, "USDT"), market: qty_precision, limit: qty_precision, price_tick })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coinex_sign_changes_with_any_field() {
        let secret = "test-secret";
        let base = coinex_sign(secret, "GET", "/v2/spot/market", "", "1700000000000");

        assert_ne!(base, coinex_sign(secret, "POST", "/v2/spot/market", "", "1700000000000"));
        assert_ne!(base, coinex_sign(secret, "GET", "/v2/account/trade-fee-rate", "", "1700000000000"));
        assert_ne!(base, coinex_sign(secret, "GET", "/v2/spot/market", "{\"a\":1}", "1700000000000"));
        assert_ne!(base, coinex_sign(secret, "GET", "/v2/spot/market", "", "1700000000001"));
        assert_ne!(base, coinex_sign("other-secret", "GET", "/v2/spot/market", "", "1700000000000"));
        // 64 个字符的小写十六进制(HMAC-SHA256 输出 32 字节)。
        assert_eq!(base.len(), 64);
        assert_eq!(base, base.to_ascii_lowercase());
    }

    #[test]
    fn builds_request_path_with_and_without_query() {
        assert_eq!(build_request_path("/spot/market", &[]), "/v2/spot/market");
        assert_eq!(
            build_request_path("/spot/market", &[("market".to_string(), "BTCUSDT".to_string())]),
            "/v2/spot/market?market=BTCUSDT"
        );
    }

    #[test]
    fn parses_trading_fee_response() {
        let text = r#"{"code":0,"data":{"market":"BTCUSDT","maker_rate":"0.0016","taker_rate":"0.002"},"message":"OK"}"#;
        let fee = parse_trading_fee(text).expect("should parse");
        assert_eq!(fee.maker_bps, Decimal::from(16));
        assert_eq!(fee.taker_bps, Decimal::from(20));
    }

    #[test]
    fn parse_trading_fee_surfaces_error_response() {
        let text = r#"{"code":3008,"data":{},"message":"require auth"}"#;
        let err = parse_trading_fee(text).unwrap_err();
        assert!(err.to_string().contains("require auth"));
    }

    #[test]
    fn parses_usdt_spot_symbols_filters_by_quote_and_status() {
        let text = r#"{"code":0,"data":[
            {"market":"BTCUSDT","base_ccy":"BTC","quote_ccy":"USDT","status":"online"},
            {"market":"BTCUSDC","base_ccy":"BTC","quote_ccy":"USDC","status":"online"},
            {"market":"OLDUSDT","base_ccy":"OLD","quote_ccy":"USDT","status":"delisted"}
        ],"message":"OK"}"#;
        let symbols = parse_usdt_spot_symbols(text).expect("should parse");
        assert_eq!(symbols, vec![Symbol::new("BTC", "USDT")]);
    }

    #[test]
    fn parse_usdt_spot_symbols_surfaces_error_response() {
        let text = r#"{"code":1,"data":null,"message":"internal error"}"#;
        let err = parse_usdt_spot_symbols(text).unwrap_err();
        assert!(err.to_string().contains("internal error"));
    }

    #[test]
    fn parse_spot_market_precisions_reads_base_and_quote_precision() {
        let text = r#"{"code":0,"data":[
            {"market":"BTCUSDT","base_ccy":"BTC","quote_ccy":"USDT","status":"online","base_ccy_precision":8,"quote_ccy_precision":2,"min_amount":"0.0001"}
        ],"message":"OK"}"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert_eq!(precisions.len(), 1);
        let info = &precisions[0];
        assert_eq!(info.symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(info.market, info.limit);
        assert_eq!(info.market.qty_step, Decimal::new(1, 8));
        assert_eq!(info.market.min_qty, "0.0001".parse().unwrap());
        assert_eq!(info.price_tick, Decimal::new(1, 2));
    }

    #[test]
    fn parse_spot_market_precisions_defaults_min_qty_to_zero_when_missing() {
        let text = r#"{"code":0,"data":[
            {"market":"BTCUSDT","base_ccy":"BTC","quote_ccy":"USDT","status":"online","base_ccy_precision":8,"quote_ccy_precision":2}
        ],"message":"OK"}"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert_eq!(precisions[0].market.min_qty, Decimal::ZERO);
    }

    #[test]
    fn parse_spot_market_precisions_skips_entries_missing_precision() {
        let text = r#"{"code":0,"data":[
            {"market":"BTCUSDT","base_ccy":"BTC","quote_ccy":"USDT","status":"online","quote_ccy_precision":2}
        ],"message":"OK"}"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert!(precisions.is_empty());
    }

    #[test]
    fn parse_spot_market_precisions_skips_non_online_markets() {
        let text = r#"{"code":0,"data":[
            {"market":"BTCUSDT","base_ccy":"BTC","quote_ccy":"USDT","status":"delisted","base_ccy_precision":8,"quote_ccy_precision":2}
        ],"message":"OK"}"#;
        let precisions = parse_spot_market_precisions(text).expect("should parse");
        assert!(precisions.is_empty());
    }
}
