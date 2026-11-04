use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::types::{Symbol, Venue};

use super::types::{
    FundingRateRecord, MarketPrecision, PerpContractSpec, PredictedFundingRate, QtyPrecision, TradingFee,
};
use super::{ExchangeInfoProvider, FundingRateProvider};

const HOST: &str = "https://www.okx.com";

/// OKX "基础信息"客户端，目前只实现 USDT 本位永续(`instType=SWAP`)用得到的
/// 部分：合约列表、下单精度、合约面值。这些都是公开数据，不需要签名；现货和
/// 手续费查询暂不支持。
///
/// OKX 永续合约名是 `BASE-USDT-SWAP`，内部 `Symbol` 是 `BASE/USDT`。
pub struct OkxExchangeInfoProvider {
    venue: Venue,
    http: reqwest::Client,
}

impl OkxExchangeInfoProvider {
    pub fn new(venue: Venue, proxy: Option<&str>) -> anyhow::Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if let Some(proxy) = proxy {
            builder = builder.proxy(reqwest::Proxy::all(format!("http://{proxy}")).context("invalid proxy address")?);
        }
        let http = builder.build().context("failed to build okx http client")?;
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
            .with_context(|| format!("okx request {path} failed"))?;
        resp.text().await.context("failed to read okx response body")
    }
}

#[async_trait]
impl ExchangeInfoProvider for OkxExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    async fn spot_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
        anyhow::bail!("okx spot trading fee is not supported yet")
    }

    async fn perpetual_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
        anyhow::bail!("okx perpetual trading fee is not supported yet")
    }

    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        anyhow::bail!("okx spot symbols are not supported yet")
    }

    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
        Ok(self.perpetual_market_precisions().await?.into_iter().map(|p| p.symbol).collect())
    }

    async fn spot_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        anyhow::bail!("okx spot market precisions are not supported yet")
    }

    async fn perpetual_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
        let text = self.public_get("/api/v5/public/instruments", &[("instType", "SWAP".to_string())]).await?;
        parse_perpetual_market_precisions(&text)
    }

    async fn perpetual_contract_spec(&self, symbol: &Symbol) -> anyhow::Result<PerpContractSpec> {
        let query = [("instType", "SWAP".to_string()), ("instId", okx_swap_inst_id(symbol))];
        let text = self.public_get("/api/v5/public/instruments", &query).await?;
        parse_perp_contract_spec(&text, symbol)
    }
}

/// `/api/v5/public/funding-rate-history` 单页上限(新文档写 400，旧文档 100，取小的)。
const FUNDING_HISTORY_PAGE_LIMIT: usize = 100;

#[async_trait]
impl FundingRateProvider for OkxExchangeInfoProvider {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    /// `/api/v5/public/funding-rate-history` 按时间倒序返回，`after` 取"早于该
    /// 时间戳"的记录，所以用最老一条的 `fundingTime` 往前翻，直到越过
    /// `start_time_ms` 或翻到空页。OKX 只保留最近约 3 个月的历史。
    async fn funding_rate_history(
        &self,
        symbol: &Symbol,
        start_time_ms: u64,
        end_time_ms: Option<u64>,
    ) -> anyhow::Result<Vec<FundingRateRecord>> {
        let inst_id = okx_swap_inst_id(symbol);
        let mut after = end_time_ms.map(|end| end + 1);
        let mut records = Vec::new();
        loop {
            let mut query = vec![("instId", inst_id.clone()), ("limit", FUNDING_HISTORY_PAGE_LIMIT.to_string())];
            if let Some(after) = after {
                query.push(("after", after.to_string()));
            }
            let text = self.public_get("/api/v5/public/funding-rate-history", &query).await?;
            let page = parse_funding_rate_history(&text, symbol)?;
            let Some(oldest) = page.iter().map(|r| r.funding_time_ms).min() else { break };
            records.extend(page);
            if oldest <= start_time_ms || after.is_some_and(|a| oldest >= a) {
                break;
            }
            after = Some(oldest);
        }
        records.retain(|r| r.funding_time_ms >= start_time_ms && end_time_ms.is_none_or(|end| r.funding_time_ms <= end));
        records.sort_by_key(|r| r.funding_time_ms);
        records.dedup_by_key(|r| r.funding_time_ms);
        Ok(records)
    }

    async fn recent_funding_rates(&self, symbol: &Symbol, limit: usize) -> anyhow::Result<Vec<FundingRateRecord>> {
        let query = [
            ("instId", okx_swap_inst_id(symbol)),
            ("limit", limit.clamp(1, FUNDING_HISTORY_PAGE_LIMIT).to_string()),
        ];
        let text = self.public_get("/api/v5/public/funding-rate-history", &query).await?;
        parse_funding_rate_history(&text, symbol)
    }

    /// `instId=ANY` 一次返回全部永续：`fundingRate` 是将在 `fundingTime` 结算的
    /// 费率，结算周期用 `nextFundingTime - fundingTime` 推出。
    async fn predicted_funding_rates(&self) -> anyhow::Result<Vec<PredictedFundingRate>> {
        let text = self.public_get("/api/v5/public/funding-rate", &[("instId", "ANY".to_string())]).await?;
        parse_predicted_funding_rates(&text)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingHistoryEntry {
    #[serde(default)]
    funding_rate: String,
    #[serde(default)]
    realized_rate: String,
    funding_time: String,
}

/// 取 `realizedRate`(实际结算值)；缺失时退回 `fundingRate`(结算前的预测值)。
/// 返回按时间升序。
fn parse_funding_rate_history(text: &str, symbol: &Symbol) -> anyhow::Result<Vec<FundingRateRecord>> {
    let entries: Vec<FundingHistoryEntry> = parse_okx_data(text, "funding-rate-history")?;
    let mut records: Vec<FundingRateRecord> = entries
        .into_iter()
        .filter_map(|e| {
            Some(FundingRateRecord {
                symbol: symbol.clone(),
                rate: opt_decimal(&e.realized_rate).or_else(|| opt_decimal(&e.funding_rate))?,
                funding_time_ms: e.funding_time.parse().ok()?,
            })
        })
        .collect();
    records.sort_by_key(|r| r.funding_time_ms);
    Ok(records)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRateEntry {
    inst_id: String,
    #[serde(default)]
    funding_rate: String,
    #[serde(default)]
    funding_time: String,
    #[serde(default)]
    next_funding_time: String,
}

fn parse_predicted_funding_rates(text: &str) -> anyhow::Result<Vec<PredictedFundingRate>> {
    let entries: Vec<FundingRateEntry> = parse_okx_data(text, "funding-rate")?;
    Ok(entries
        .into_iter()
        .filter_map(|e| {
            let symbol = parse_okx_swap_inst_id(&e.inst_id).filter(|s| s.quote.as_ref() == "USDT")?;
            let funding_time_ms: u64 = e.funding_time.parse().ok()?;
            let next_ms: u64 = e.next_funding_time.parse().ok()?;
            let interval_hours = (next_ms.checked_sub(funding_time_ms)? as f64 / 3_600_000.0).round() as u32;
            if interval_hours == 0 {
                return None;
            }
            Some(PredictedFundingRate {
                symbol,
                rate: opt_decimal(&e.funding_rate)?,
                next_funding_time_ms: funding_time_ms,
                funding_interval_hours: interval_hours,
            })
        })
        .collect())
}

/// `BTC/USDT` → `BTC-USDT-SWAP`。
pub fn okx_swap_inst_id(symbol: &Symbol) -> String {
    format!("{}-{}-SWAP", symbol.base, symbol.quote).to_ascii_uppercase()
}

/// `BTC-USDT-SWAP` → `BTC/USDT`；不是 `-SWAP` 结尾的返回 None。
pub fn parse_okx_swap_inst_id(inst_id: &str) -> Option<Symbol> {
    let (base, quote) = inst_id.strip_suffix("-SWAP")?.split_once('-')?;
    if base.is_empty() || quote.is_empty() || quote.contains('-') {
        return None;
    }
    Some(Symbol::new(base, quote))
}

/// OKX REST 统一外层：`code == "0"` 才是成功。
#[derive(Debug, Deserialize)]
pub(crate) struct OkxResponse<T> {
    pub(crate) code: String,
    #[serde(default)]
    pub(crate) msg: String,
    #[serde(default = "Vec::new")]
    pub(crate) data: Vec<T>,
}

pub(crate) fn parse_okx_data<T: serde::de::DeserializeOwned>(text: &str, what: &str) -> anyhow::Result<Vec<T>> {
    let resp: OkxResponse<T> =
        serde_json::from_str(text).with_context(|| format!("failed to parse okx {what} response, raw body: {text}"))?;
    if resp.code != "0" {
        anyhow::bail!("okx error {}: {}", resp.code, resp.msg);
    }
    Ok(resp.data)
}

/// OKX 数值字段都是字符串，缺失时是空串。
pub(crate) fn opt_decimal(s: &str) -> Option<Decimal> {
    if s.is_empty() { None } else { s.parse().ok() }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstrumentEntry {
    inst_id: String,
    #[serde(default)]
    ct_type: String,
    #[serde(default)]
    settle_ccy: String,
    #[serde(default)]
    ct_val: String,
    #[serde(default)]
    ct_mult: String,
    #[serde(default)]
    ct_val_ccy: String,
    #[serde(default)]
    lot_sz: String,
    #[serde(default)]
    min_sz: String,
    #[serde(default)]
    tick_sz: String,
    #[serde(default)]
    state: String,
}

impl InstrumentEntry {
    /// 只要正在交易的 USDT 本位线性永续。
    fn usdt_linear_symbol(&self) -> Option<Symbol> {
        if self.ct_type != "linear" || self.settle_ccy != "USDT" || (!self.state.is_empty() && self.state != "live") {
            return None;
        }
        parse_okx_swap_inst_id(&self.inst_id).filter(|s| s.quote.as_ref() == "USDT")
    }

    /// 1 张 = `ctVal × ctMult` 个 `ctValCcy`。
    fn coins_per_contract(&self) -> Option<Decimal> {
        let ct_val = opt_decimal(&self.ct_val)?;
        let ct_mult = opt_decimal(&self.ct_mult).unwrap_or(Decimal::ONE);
        Some(ct_val * ct_mult).filter(|v| *v > Decimal::ZERO)
    }
}

/// 下单量单位是张，步进 `lotSz`、最少 `minSz`(可以是小数张，如 BTC 的 0.01 张)。
fn parse_perpetual_market_precisions(text: &str) -> anyhow::Result<Vec<MarketPrecision>> {
    let instruments: Vec<InstrumentEntry> = parse_okx_data(text, "instruments")?;
    Ok(instruments
        .into_iter()
        .filter_map(|inst| {
            let symbol = inst.usdt_linear_symbol()?;
            let qty_step = opt_decimal(&inst.lot_sz)?;
            let min_qty = opt_decimal(&inst.min_sz).unwrap_or(qty_step).max(qty_step);
            let qty_precision = QtyPrecision { qty_step, min_qty };
            Some(MarketPrecision {
                symbol,
                market: qty_precision,
                limit: qty_precision,
                price_tick: opt_decimal(&inst.tick_sz).unwrap_or(Decimal::ZERO),
            })
        })
        .collect())
}

/// OKX 永续报价就是每币价格(`price_unit = 1`)；1 张 = `ctVal × ctMult` 个币。
/// 面值币种不是 base 币的(理论上 USDT 线性永续不会出现)直接报错，免得数量
/// 算错一个数量级还照常下单。
fn parse_perp_contract_spec(text: &str, symbol: &Symbol) -> anyhow::Result<PerpContractSpec> {
    let instruments: Vec<InstrumentEntry> = parse_okx_data(text, "instruments")?;
    let inst_id = okx_swap_inst_id(symbol);
    let inst = instruments
        .into_iter()
        .find(|i| i.inst_id == inst_id)
        .with_context(|| format!("okx instrument {inst_id} not found"))?;
    if !inst.ct_val_ccy.eq_ignore_ascii_case(&symbol.base) {
        anyhow::bail!("okx instrument {inst_id} has ctValCcy={} (expected {})", inst.ct_val_ccy, symbol.base);
    }
    let qty_unit = inst
        .coins_per_contract()
        .with_context(|| format!("okx instrument {inst_id} has no valid ctVal/ctMult"))?;
    Ok(PerpContractSpec { price_unit: Decimal::ONE, qty_unit })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    const INSTRUMENTS: &str = r#"{"code":"0","msg":"","data":[
        {"instId":"BTC-USDT-SWAP","instType":"SWAP","ctType":"linear","settleCcy":"USDT","ctVal":"0.01","ctMult":"1","ctValCcy":"BTC","lotSz":"0.01","minSz":"0.01","tickSz":"0.1","state":"live"},
        {"instId":"PEPE-USDT-SWAP","instType":"SWAP","ctType":"linear","settleCcy":"USDT","ctVal":"10000000","ctMult":"1","ctValCcy":"PEPE","lotSz":"0.1","minSz":"1","tickSz":"0.0000000001","state":"live"},
        {"instId":"BTC-USD-SWAP","instType":"SWAP","ctType":"inverse","settleCcy":"BTC","ctVal":"100","ctMult":"1","ctValCcy":"USD","lotSz":"1","minSz":"1","tickSz":"0.1","state":"live"},
        {"instId":"OLD-USDT-SWAP","instType":"SWAP","ctType":"linear","settleCcy":"USDT","ctVal":"1","ctMult":"1","ctValCcy":"OLD","lotSz":"1","minSz":"1","tickSz":"0.1","state":"suspend"}
    ]}"#;

    #[test]
    fn inst_id_round_trips() {
        let symbol = Symbol::new("PEPE", "USDT");
        assert_eq!(okx_swap_inst_id(&symbol), "PEPE-USDT-SWAP");
        assert_eq!(parse_okx_swap_inst_id("PEPE-USDT-SWAP"), Some(symbol));
        assert_eq!(parse_okx_swap_inst_id("PEPE-USDT"), None);
        assert_eq!(parse_okx_swap_inst_id("-USDT-SWAP"), None);
    }

    #[test]
    fn parses_live_usdt_linear_precisions() {
        let precisions = parse_perpetual_market_precisions(INSTRUMENTS).unwrap();
        assert_eq!(precisions.len(), 2);
        assert_eq!(precisions[0].symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(precisions[0].market, QtyPrecision { qty_step: d("0.01"), min_qty: d("0.01") });
        assert_eq!(precisions[0].price_tick, d("0.1"));
        assert_eq!(precisions[1].symbol, Symbol::new("PEPE", "USDT"));
        assert_eq!(precisions[1].market, QtyPrecision { qty_step: d("0.1"), min_qty: d("1") });
    }

    #[test]
    fn parses_contract_spec_from_ct_val() {
        let spec = parse_perp_contract_spec(INSTRUMENTS, &Symbol::new("PEPE", "USDT")).unwrap();
        assert_eq!(spec, PerpContractSpec { price_unit: Decimal::ONE, qty_unit: Decimal::from(10_000_000) });
        let spec = parse_perp_contract_spec(INSTRUMENTS, &Symbol::new("BTC", "USDT")).unwrap();
        assert_eq!(spec.qty_unit, d("0.01"));
        assert!(parse_perp_contract_spec(INSTRUMENTS, &Symbol::new("ETH", "USDT")).is_err());
    }

    #[test]
    fn parses_funding_history_ascending_preferring_realized_rate() {
        let text = r#"{"code":"0","msg":"","data":[
            {"instId":"BTC-USDT-SWAP","fundingRate":"0.0002","realizedRate":"0.0001","fundingTime":"1789689600000"},
            {"instId":"BTC-USDT-SWAP","fundingRate":"-0.0003","realizedRate":"","fundingTime":"1789660800000"}
        ]}"#;
        let records = parse_funding_rate_history(text, &Symbol::new("BTC", "USDT")).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!((records[0].funding_time_ms, records[0].rate), (1789660800000, d("-0.0003")));
        assert_eq!((records[1].funding_time_ms, records[1].rate), (1789689600000, d("0.0001")));
    }

    #[test]
    fn parses_predicted_rates_deriving_interval_and_filtering() {
        let text = r#"{"code":"0","msg":"","data":[
            {"instId":"BTC-USDT-SWAP","fundingRate":"0.0001","fundingTime":"1789718400000","nextFundingRate":"0.0002","nextFundingTime":"1789747200000"},
            {"instId":"LPT-USDT-SWAP","fundingRate":"-0.0005","fundingTime":"1789718400000","nextFundingRate":"","nextFundingTime":"1789732800000"},
            {"instId":"BTC-USD-SWAP","fundingRate":"0.0001","fundingTime":"1789718400000","nextFundingRate":"","nextFundingTime":"1789747200000"},
            {"instId":"BTC-USDT-250627","fundingRate":"","fundingTime":"","nextFundingRate":"","nextFundingTime":""}
        ]}"#;
        let rates = parse_predicted_funding_rates(text).unwrap();
        assert_eq!(rates.len(), 2);
        assert_eq!(rates[0].symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(rates[0].rate, d("0.0001"));
        assert_eq!(rates[0].next_funding_time_ms, 1789718400000);
        assert_eq!(rates[0].funding_interval_hours, 8);
        assert_eq!(rates[1].funding_interval_hours, 4);
    }

    #[test]
    fn surfaces_api_errors() {
        let err = parse_perpetual_market_precisions(r#"{"code":"51001","msg":"Instrument ID does not exist","data":[]}"#)
            .unwrap_err();
        assert!(err.to_string().contains("51001"));
    }
}
