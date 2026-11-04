use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::types::{Symbol, Venue};

use super::binance::binance_perp_contract_spec;
use super::types::{
    FundingRateRecord, MarketPrecision, PerpContractSpec, PredictedFundingRate, QtyPrecision, TradingFee,
};
use super::{ExchangeInfoProvider, FundingRateProvider};

const HOST: &str = "https://api.bitget.com";
/// Bitget v2 合约接口里 USDT 本位永续的 `productType`。
pub const BITGET_USDT_FUTURES: &str = "USDT-FUTURES";

/// Bitget "基础信息"客户端，目前只实现 USDT 本位永续(`productType=USDT-FUTURES`)
/// 用得到的部分：合约列表、下单精度、合约单位。这些都是公开数据，不需要签名；
/// 现货和手续费查询暂不支持。
///
/// Bitget 永续合约名和币安一样是 `BASEUSDT`(如 `BTCUSDT`、`1000BONKUSDT`)，
/// 内部 `Symbol` 的 base 保留乘数前缀(`1000BONK/USDT`)。
pub struct BitgetExchangeInfoProvider {
    venue: Venue,
    http: reqwest::Client,
}

impl BitgetExchangeInfoProvider {
    pub fn new(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if let Some(proxy) = proxy {
            builder = builder.proxy(reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?);
        }
        let http = builder.build().context("failed to build bitget http client")?;
        Ok(Self { venue, http })
    }

    async fn public_get(&self, path: &str, query: &[(&str, String)]) -> anyhow::Result<String> {
        crate::ratelimit::throttle(HOST).await;
        let resp = self
            .http
            .get(format!("{HOST}{path}"))
            .query(query)
            .send()
            .await
            .with_context(|| format!("bitget request {path} failed"))?;
        resp.text().await.context("failed to read bitget response body")
    }
}

#[async_trait]
impl ExchangeInfoProvider for BitgetExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn spot_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
        anyhow::bail!("bitget spot trading fee is not supported yet")
    }

    async fn perpetual_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
        anyhow::bail!("bitget perpetual trading fee is not supported yet")
    }

    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        anyhow::bail!("bitget spot symbols are not supported yet")
    }

    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        Ok(self.perpetual_market_precisions().await?.into_iter().map(|p| p.symbol).collect())
    }

    async fn spot_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        anyhow::bail!("bitget spot market precisions are not supported yet")
    }

    async fn perpetual_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        let text = self
            .public_get("/api/v2/mix/market/contracts", &[("productType", BITGET_USDT_FUTURES.to_string())])
            .await?;
        parse_perpetual_market_precisions(&text)
    }

    /// Bitget 下单量单位是 base 币(合约名里的 base，含乘数前缀)，报价也是一个
    /// base 单位的价格，规则和币安一致，所以直接复用 [`binance_perp_contract_spec`]
    /// 按名字前缀推断，不需要查接口。
    async fn perpetual_contract_spec(&self, symbol: &Symbol) -> anyhow::Result<PerpContractSpec> {
        Ok(binance_perp_contract_spec(symbol))
    }
}

/// `/api/v2/mix/market/history-fund-rate` 的 `pageSize` 上限。
const FUNDING_HISTORY_PAGE_SIZE: usize = 100;
const DEFAULT_FUND_INTERVAL_HOURS: u32 = 8;

#[async_trait]
impl FundingRateProvider for BitgetExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `/api/v2/mix/market/history-fund-rate` 按时间倒序、按 `pageNo` 翻页，
    /// 没有时间范围参数：翻到最老一条越过 `start_time_ms` 或页不满为止，再按
    /// 毫秒边界过滤。
    async fn funding_rate_history(
        &self,
        symbol: &Symbol,
        start_time_ms: u64,
        end_time_ms: Option<u64>,
    ) -> anyhow::Result<Vec<FundingRateRecord>> {
        let mut records = Vec::new();
        let mut page_no = 1usize;
        loop {
            let page = self.funding_history_page(symbol, page_no, FUNDING_HISTORY_PAGE_SIZE).await?;
            let full = page.len() >= FUNDING_HISTORY_PAGE_SIZE;
            let oldest = page.iter().map(|r| r.funding_time_ms).min();
            records.extend(page);
            match oldest {
                Some(oldest) if full && oldest > start_time_ms => page_no += 1,
                _ => break,
            }
        }
        records.retain(|r| r.funding_time_ms >= start_time_ms && end_time_ms.is_none_or(|end| r.funding_time_ms <= end));
        records.sort_by_key(|r| r.funding_time_ms);
        records.dedup_by_key(|r| r.funding_time_ms);
        Ok(records)
    }

    async fn recent_funding_rates(&self, symbol: &Symbol, limit: usize) -> anyhow::Result<Vec<FundingRateRecord>> {
        self.funding_history_page(symbol, 1, limit.clamp(1, FUNDING_HISTORY_PAGE_SIZE)).await
    }

    /// `tickers` 一次返回全部合约的当前 `fundingRate`，但没有结算周期和下次结算
    /// 时间；周期取自 `contracts` 的 `fundInterval`(小时)，下次结算时间按周期对齐
    /// 到 UTC 整点边界推算(Bitget 结算点是 UTC 0/8/16 时这类对齐时刻)。
    async fn predicted_funding_rates(&self) -> anyhow::Result<Vec<PredictedFundingRate>> {
        let query = [("productType", BITGET_USDT_FUTURES.to_string())];
        let tickers = self.public_get("/api/v2/mix/market/tickers", &query).await?;
        let contracts = self.public_get("/api/v2/mix/market/contracts", &query).await?;
        parse_predicted_funding_rates(&tickers, &contracts, now_ms())
    }
}

impl BitgetExchangeInfoProvider {
    async fn funding_history_page(
        &self,
        symbol: &Symbol,
        page_no: usize,
        page_size: usize,
    ) -> anyhow::Result<Vec<FundingRateRecord>> {
        let query = [
            ("symbol", bitget_symbol(symbol)),
            ("productType", BITGET_USDT_FUTURES.to_string()),
            ("pageSize", page_size.to_string()),
            ("pageNo", page_no.to_string()),
        ];
        let text = self.public_get("/api/v2/mix/market/history-fund-rate", &query).await?;
        parse_funding_rate_history(&text, symbol)
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingHistoryEntry {
    #[serde(default)]
    funding_rate: String,
    #[serde(default)]
    funding_time: String,
}

/// 返回按时间升序。
fn parse_funding_rate_history(text: &str, symbol: &Symbol) -> anyhow::Result<Vec<FundingRateRecord>> {
    let entries: Vec<FundingHistoryEntry> = parse_bitget_data(text, "history-fund-rate")?;
    let mut records: Vec<FundingRateRecord> = entries
        .into_iter()
        .filter_map(|e| {
            Some(FundingRateRecord {
                symbol: symbol.clone(),
                rate: opt_decimal(&e.funding_rate)?,
                funding_time_ms: e.funding_time.parse().ok()?,
            })
        })
        .collect();
    records.sort_by_key(|r| r.funding_time_ms);
    Ok(records)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerEntry {
    symbol: String,
    #[serde(default)]
    funding_rate: String,
}

fn parse_predicted_funding_rates(
    tickers_text: &str,
    contracts_text: &str,
    now_ms: u64,
) -> anyhow::Result<Vec<PredictedFundingRate>> {
    let tickers: Vec<TickerEntry> = parse_bitget_data(tickers_text, "tickers")?;
    let contracts: Vec<ContractEntry> = parse_bitget_data(contracts_text, "contracts")?;
    let intervals: HashMap<String, u32> = contracts
        .into_iter()
        .filter(|c| c.symbol_status.is_empty() || c.symbol_status == "normal")
        .filter(|c| c.symbol_type.is_empty() || c.symbol_type == "perpetual")
        .filter(|c| c.quote_coin.eq_ignore_ascii_case("USDT"))
        .map(|c| {
            let hours = c.fund_interval.parse::<u32>().ok().filter(|h| *h > 0).unwrap_or(DEFAULT_FUND_INTERVAL_HOURS);
            (c.symbol.to_ascii_uppercase(), hours)
        })
        .collect();
    Ok(tickers
        .into_iter()
        .filter_map(|t| {
            let name = t.symbol.to_ascii_uppercase();
            let hours = *intervals.get(&name)?;
            let base = name.strip_suffix("USDT").filter(|b| !b.is_empty())?;
            let interval_ms = u64::from(hours) * 3_600_000;
            Some(PredictedFundingRate {
                symbol: Symbol::new(base, "USDT"),
                rate: opt_decimal(&t.funding_rate)?,
                next_funding_time_ms: (now_ms / interval_ms + 1) * interval_ms,
                funding_interval_hours: hours,
            })
        })
        .collect())
}

/// `BTC/USDT` → `BTCUSDT`。
pub fn bitget_symbol(symbol: &Symbol) -> String {
    format!("{}{}", symbol.base, symbol.quote).to_ascii_uppercase()
}

/// Bitget REST 统一外层：`code == "00000"` 才是成功。
#[derive(Debug, Deserialize)]
pub(crate) struct BitgetResponse<T> {
    code: String,
    #[serde(default)]
    msg: String,
    data: Option<T>,
}

pub(crate) fn parse_bitget_data<T: serde::de::DeserializeOwned>(text: &str, what: &str) -> anyhow::Result<T> {
    let resp: BitgetResponse<T> =
        serde_json::from_str(text).with_context(|| format!("failed to parse bitget {what} response, raw body: {text}"))?;
    if resp.code != "00000" {
        anyhow::bail!("bitget error {}: {}", resp.code, resp.msg);
    }
    resp.data.with_context(|| format!("bitget {what} response has no data, raw body: {text}"))
}

/// Bitget 数值字段都是字符串，缺失时可能是空串。
pub(crate) fn opt_decimal(s: &str) -> Option<Decimal> {
    if s.is_empty() { None } else { s.parse().ok() }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContractEntry {
    symbol: String,
    #[serde(default)]
    quote_coin: String,
    #[serde(default)]
    min_trade_num: String,
    #[serde(default)]
    size_multiplier: String,
    #[serde(default)]
    volume_place: String,
    #[serde(default)]
    price_place: String,
    #[serde(default)]
    price_end_step: String,
    #[serde(default)]
    symbol_status: String,
    #[serde(default)]
    symbol_type: String,
    #[serde(default)]
    fund_interval: String,
}

fn decimal_places_step(places: &str) -> Option<Decimal> {
    let places: u32 = places.parse().ok()?;
    Some(Decimal::new(1, places))
}

/// 步进 `sizeMultiplier`(缺失时按 `volumePlace` 位小数)，最少 `minTradeNum`；
/// 价格 tick = `priceEndStep × 10^-pricePlace`。只保留正常交易的永续。
/// `minTradeUSDT`(最小名义金额)不在精度缓存的表达范围内，没有校验。
fn parse_perpetual_market_precisions(text: &str) -> anyhow::Result<Vec<MarketPrecision>> {
    let contracts: Vec<ContractEntry> = parse_bitget_data(text, "contracts")?;
    Ok(contracts
        .into_iter()
        .filter(|c| c.symbol_status.is_empty() || c.symbol_status == "normal")
        .filter(|c| c.symbol_type.is_empty() || c.symbol_type == "perpetual")
        .filter_map(|c| {
            if !c.quote_coin.eq_ignore_ascii_case("USDT") {
                return None;
            }
            let base = c.symbol.to_ascii_uppercase().strip_suffix("USDT")?.to_string();
            if base.is_empty() {
                return None;
            }
            let qty_step = opt_decimal(&c.size_multiplier)
                .filter(|s| *s > Decimal::ZERO)
                .or_else(|| decimal_places_step(&c.volume_place))?;
            let min_qty = opt_decimal(&c.min_trade_num).unwrap_or(qty_step).max(qty_step);
            let price_tick = match (decimal_places_step(&c.price_place), opt_decimal(&c.price_end_step)) {
                (Some(place), Some(end)) if end > Decimal::ZERO => place * end,
                (Some(place), _) => place,
                _ => Decimal::ZERO,
            };
            let qty_precision = QtyPrecision { qty_step, min_qty };
            Some(MarketPrecision {
                symbol: Symbol::new(base, "USDT"),
                market: qty_precision,
                limit: qty_precision,
                price_tick,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn parses_normal_perpetual_precisions() {
        let text = r#"{"code":"00000","msg":"success","requestTime":1,"data":[
            {"symbol":"BTCUSDT","baseCoin":"BTC","quoteCoin":"USDT","minTradeNum":"0.0001","sizeMultiplier":"0.0001","volumePlace":"4","pricePlace":"1","priceEndStep":"1","symbolStatus":"normal","symbolType":"perpetual"},
            {"symbol":"1000BONKUSDT","baseCoin":"1000BONK","quoteCoin":"USDT","minTradeNum":"10","sizeMultiplier":"1","volumePlace":"0","pricePlace":"6","priceEndStep":"5","symbolStatus":"normal","symbolType":"perpetual"},
            {"symbol":"OLDUSDT","baseCoin":"OLD","quoteCoin":"USDT","minTradeNum":"1","sizeMultiplier":"1","volumePlace":"0","pricePlace":"2","priceEndStep":"1","symbolStatus":"off","symbolType":"perpetual"},
            {"symbol":"BTCUSDT0329","baseCoin":"BTC","quoteCoin":"USDT","minTradeNum":"0.001","sizeMultiplier":"0.001","volumePlace":"3","pricePlace":"1","priceEndStep":"1","symbolStatus":"normal","symbolType":"delivery"}
        ]}"#;
        let precisions = parse_perpetual_market_precisions(text).unwrap();
        assert_eq!(precisions.len(), 2);
        assert_eq!(precisions[0].symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(precisions[0].market, QtyPrecision { qty_step: d("0.0001"), min_qty: d("0.0001") });
        assert_eq!(precisions[0].price_tick, d("0.1"));
        assert_eq!(precisions[1].symbol, Symbol::new("1000BONK", "USDT"));
        assert_eq!(precisions[1].market, QtyPrecision { qty_step: d("1"), min_qty: d("10") });
        assert_eq!(precisions[1].price_tick, d("0.000005"));
    }

    #[test]
    fn parses_funding_history_ascending() {
        let text = r#"{"code":"00000","msg":"success","data":[
            {"symbol":"BTCUSDT","fundingRate":"0.0001","fundingTime":"1789689600000"},
            {"symbol":"BTCUSDT","fundingRate":"-0.0003","fundingTime":"1789660800000"}
        ]}"#;
        let records = parse_funding_rate_history(text, &Symbol::new("BTC", "USDT")).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!((records[0].funding_time_ms, records[0].rate), (1789660800000, d("-0.0003")));
        assert_eq!((records[1].funding_time_ms, records[1].rate), (1789689600000, d("0.0001")));
    }

    #[test]
    fn parses_predicted_rates_using_contract_interval_and_aligned_next_time() {
        let tickers = r#"{"code":"00000","msg":"success","data":[
            {"symbol":"BTCUSDT","fundingRate":"0.0001"},
            {"symbol":"LPTUSDT","fundingRate":"-0.0005"},
            {"symbol":"OLDUSDT","fundingRate":"0.0001"}
        ]}"#;
        let contracts = r#"{"code":"00000","msg":"success","data":[
            {"symbol":"BTCUSDT","quoteCoin":"USDT","fundInterval":"8","symbolStatus":"normal","symbolType":"perpetual"},
            {"symbol":"LPTUSDT","quoteCoin":"USDT","fundInterval":"4","symbolStatus":"normal","symbolType":"perpetual"},
            {"symbol":"OLDUSDT","quoteCoin":"USDT","fundInterval":"8","symbolStatus":"off","symbolType":"perpetual"}
        ]}"#;
        // 1789715993000 = 某日 07:19:53 UTC 附近，下一个 8h 边界 1789718400000，4h 边界同。
        let rates = parse_predicted_funding_rates(tickers, contracts, 1_789_715_993_000).unwrap();
        assert_eq!(rates.len(), 2);
        assert_eq!(rates[0].symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(rates[0].funding_interval_hours, 8);
        assert_eq!(rates[0].next_funding_time_ms % (8 * 3_600_000), 0);
        assert!(rates[0].next_funding_time_ms > 1_789_715_993_000);
        assert_eq!(rates[1].funding_interval_hours, 4);
        assert_eq!(rates[1].rate, d("-0.0005"));
    }

    #[test]
    fn surfaces_api_errors() {
        let err = parse_perpetual_market_precisions(r#"{"code":"40034","msg":"Parameter does not exist","data":null}"#)
            .unwrap_err();
        assert!(err.to_string().contains("40034"));
    }

    #[test]
    fn contract_spec_follows_multiplier_prefix() {
        assert_eq!(binance_perp_contract_spec(&Symbol::new("1000BONK", "USDT")).qty_unit, Decimal::from(1000));
        assert_eq!(bitget_symbol(&Symbol::new("1000BONK", "USDT")), "1000BONKUSDT");
    }
}
