use anyhow::Context;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::types::{Symbol, Venue};

use super::ExchangeInfoProvider;
use super::types::{MarketPrecision, QtyPrecision, TradingFee};

const HOST: &str = "https://api.nonkyc.io";
const API_PREFIX: &str = "/api/v2";

/// Nonkyc.io(`api.nonkyc.io`,自带 Swagger `openapi.json`)"基础信息"客户端:
/// 查询可交易的 USDT 计价现货交易对及下单精度,两者都打公开的
/// `GET /market/getlist`,不需要 API key。Nonkyc 的 REST API 通读全部端点/schema
/// 后确认没有任何查询账户实际 maker/taker 费率的端点——`spot_trading_fee` 因此
/// 只能报错,这是真实的 API 限制,不是本项目选择不实现。`scan::find_overlap`
/// 不调用 `spot_trading_fee`,不受影响;只有 `monitor` 子命令会用到,目前
/// `monitor --secondary nonkyc` 会让每个候选币在查费率阶段被跳过。Nonkyc 另有
/// 独立的 `perp.nonkyc.io` 永续合约产品,和 `exchange_info::coinex` 同样的既有
/// 约定,这次不接入,`perpetual_trading_fee` 报错、
/// `usdt_perpetual_symbols`/`perpetual_market_precisions` 返回空 Vec。
pub struct NonkycExchangeInfoProvider {
    venue: Venue,
    http: reqwest::Client,
}

impl NonkycExchangeInfoProvider {
    pub fn new(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let http = build_http_client(proxy)?;
        Ok(Self { venue, http })
    }

    /// Nonkyc 现货交易对列表/精度查询都是公开接口,不需要凭证,这里只是保持和
    /// 其它交易所工厂函数一致的 `from_env` 调用形式。
    pub fn from_env(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        Self::new(venue, proxy)
    }

    async fn public_request<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        let url = format!("{HOST}{API_PREFIX}{path}");
        crate::ratelimit::throttle(HOST).await;
        let resp = self.http.get(&url).send().await.context("nonkyc exchange_info public request failed")?;
        let text = resp.text().await.context("failed to read nonkyc exchange_info public response body")?;
        parse_data(&text)
    }
}

#[async_trait]
impl ExchangeInfoProvider for NonkycExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn spot_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
        anyhow::bail!("nonkyc spot trading fee is not supported: the Nonkyc REST API has no endpoint for it")
    }

    async fn perpetual_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
        anyhow::bail!("nonkyc perpetual trading fee is not supported: no perpetual contracts are wired up for nonkyc")
    }

    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        let markets: Vec<MarketEntry> = self.public_request("/market/getlist").await?;
        Ok(parse_usdt_spot_symbols(&markets))
    }

    /// Nonkyc 当前没有接入永续合约场景,返回空列表,和
    /// `exchange_info::coinex::usdt_perpetual_symbols` 的既有约定一致,不是 bug。
    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        Ok(Vec::new())
    }

    /// 复用 [`Self::usdt_spot_symbols`] 已经在打的同一个 `/market/getlist` 端点,
    /// 一次请求同时拿到交易对列表和精度。Nonkyc 没有 market/limit 下单方式的
    /// 精度区分,两份填相同值。
    async fn spot_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        let markets: Vec<MarketEntry> = self.public_request("/market/getlist").await?;
        Ok(parse_spot_market_precisions(&markets))
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
    builder.build().context("failed to build nonkyc http client")
}

#[derive(Debug, Deserialize)]
struct NonkycError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct NonkycErrorEnvelope {
    error: NonkycError,
}

/// Nonkyc 响应解析:成功时响应体直接就是数据本身(不像 CoinEx 包一层
/// `{code,data}` 信封);业务错误返回 `{"error":{code,message,...}}`;网关层
/// 错误(如未认证的 401)返回纯文本,不是 JSON。依次尝试三种情况,都不是的话把
/// 原始 body 内容整个报出来,方便定位。
fn parse_data<T: DeserializeOwned>(text: &str) -> anyhow::Result<T> {
    if let Ok(data) = serde_json::from_str::<T>(text) {
        return Ok(data);
    }
    if let Ok(envelope) = serde_json::from_str::<NonkycErrorEnvelope>(text) {
        anyhow::bail!("nonkyc error {}: {}", envelope.error.code, envelope.error.message);
    }
    anyhow::bail!("nonkyc request failed, raw response: {text}");
}

#[derive(Debug, Deserialize)]
struct MarketEntry {
    symbol: String,
    #[serde(rename = "isActive")]
    is_active: bool,
    #[serde(rename = "priceDecimals")]
    price_decimals: u32,
    #[serde(rename = "quantityDecimals")]
    quantity_decimals: u32,
    #[serde(rename = "minimumQuantity", default)]
    minimum_quantity: Decimal,
}

/// `symbol` 是 `"BASE/USDT"` 格式,和项目 `Symbol` 的 `Display` 天然一致,
/// 但这里仍手动 split 一次而不是直接 parse,因为要按 quote 精确过滤。
fn split_usdt_symbol(entry: &MarketEntry) -> Option<&str> {
    let (base, quote) = entry.symbol.split_once('/')?;
    quote.eq_ignore_ascii_case("USDT").then_some(base)
}

fn parse_usdt_spot_symbols(markets: &[MarketEntry]) -> Vec<Symbol> {
    let mut symbols: Vec<Symbol> = markets
        .iter()
        .filter(|m| m.is_active)
        .filter_map(|m| split_usdt_symbol(m).map(|base| Symbol::new(base.to_string(), "USDT")))
        .collect();
    symbols.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    symbols.dedup();
    symbols
}

fn parse_spot_market_precisions(markets: &[MarketEntry]) -> Vec<MarketPrecision> {
    markets
        .iter()
        .filter(|m| m.is_active)
        .filter_map(|m| {
            let base = split_usdt_symbol(m)?;
            let qty_precision = QtyPrecision { qty_step: Decimal::new(1, m.quantity_decimals), min_qty: m.minimum_quantity };
            Some(MarketPrecision {
                symbol: Symbol::new(base.to_string(), "USDT"),
                market: qty_precision,
                limit: qty_precision,
                price_tick: Decimal::new(1, m.price_decimals),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market(symbol: &str, is_active: bool, price_decimals: u32, quantity_decimals: u32, minimum_quantity: &str) -> MarketEntry {
        MarketEntry {
            symbol: symbol.to_string(),
            is_active,
            price_decimals,
            quantity_decimals,
            minimum_quantity: minimum_quantity.parse().unwrap(),
        }
    }

    #[test]
    fn parses_usdt_spot_symbols_filters_by_quote_and_active() {
        let markets = vec![
            market("BTC/USDT", true, 2, 8, "0.000015"),
            market("BTC/BTC3L", true, 2, 8, "0"),
            market("OLD/USDT", false, 2, 8, "0"),
        ];
        let symbols = parse_usdt_spot_symbols(&markets);
        assert_eq!(symbols, vec![Symbol::new("BTC", "USDT")]);
    }

    #[test]
    fn parse_spot_market_precisions_reads_decimals_and_min_qty() {
        let markets = vec![market("SHIB/USDT", true, 9, 0, "50000")];
        let precisions = parse_spot_market_precisions(&markets);
        assert_eq!(precisions.len(), 1);
        let info = &precisions[0];
        assert_eq!(info.symbol, Symbol::new("SHIB", "USDT"));
        assert_eq!(info.market, info.limit);
        assert_eq!(info.market.qty_step, Decimal::new(1, 0));
        assert_eq!(info.market.min_qty, "50000".parse().unwrap());
        assert_eq!(info.price_tick, Decimal::new(1, 9));
    }

    #[test]
    fn parse_spot_market_precisions_skips_non_active_and_non_usdt() {
        let markets = vec![market("BTC/USDC", true, 2, 8, "0"), market("ETH/USDT", false, 2, 8, "0")];
        assert!(parse_spot_market_precisions(&markets).is_empty());
    }

    #[test]
    fn parses_direct_data_response() {
        let text = r#"[{"symbol":"BTC/USDT","isActive":true,"priceDecimals":2,"quantityDecimals":8,"minimumQuantity":0.000015}]"#;
        let markets: Vec<MarketEntry> = parse_data(text).expect("should parse");
        assert_eq!(markets.len(), 1);
    }

    #[test]
    fn parse_data_surfaces_error_envelope() {
        let text = r#"{"error":{"code":2001,"message":"Market not found","description":"This is not an active market identifier"}}"#;
        let err = parse_data::<Vec<MarketEntry>>(text).unwrap_err();
        assert!(err.to_string().contains("Market not found"));
    }

    #[test]
    fn parse_data_surfaces_plain_text_error() {
        let err = parse_data::<Vec<MarketEntry>>("Not Authorized").unwrap_err();
        assert!(err.to_string().contains("Not Authorized"));
    }
}
