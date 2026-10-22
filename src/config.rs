use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub venues: Vec<VenueConfig>,
    pub symbols: Vec<SymbolConfig>,
    #[serde(default)]
    pub triangular_paths: Vec<TriangularPathConfig>,
    #[serde(default = "default_min_profit_bps")]
    pub min_profit_bps: Decimal,
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,
    #[serde(default)]
    pub cross_exchange_execution: Option<CrossExchangeExecutionConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct VenueConfig {
    pub name: String,
    #[serde(default)]
    pub taker_fee_bps: Decimal,
    /// 挂单(maker)手续费率，默认不配置(None 时按 `taker_fee_bps` 保守回退,
    /// 见 `FeeSchedule::new`)。目前仅 `CrossExchangeStrategy` 的 Kraken 挂单
    /// 判断会用到，其余用途一律按 taker 计算。
    #[serde(default)]
    pub maker_fee_bps: Option<Decimal>,
    /// 行情数据源实现: "mock"(默认,随机游走假行情) | "binance_spot"(币安现货真实行情)
    /// | "kraken_spot"(Kraken 现货真实行情) | "coinex_spot"(CoinEx 现货真实行情)
    /// | "gate_spot"(Gate.io 现货真实行情)。
    #[serde(default = "default_source")]
    pub source: String,
    /// 仅当 source = "binance_spot" 时生效,是否连接币安测试网。
    #[serde(default)]
    pub testnet: bool,
    /// 手续费折扣乘数(如 "0.75" 表示打 7.5 折),默认 1(不打折)。用于
    /// `monitor` 子命令按折后价计算实际成本(如 BNB 抵扣手续费)。
    #[serde(default = "default_fee_discount")]
    pub fee_discount: Decimal,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SymbolConfig {
    pub base: String,
    pub quote: String,
    pub initial_mid: Decimal,
    #[serde(default = "default_volatility")]
    pub volatility: f64,
    #[serde(default = "default_spread")]
    pub spread: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TriangularLegConfig {
    pub base: String,
    pub quote: String,
    /// "buy" 表示用 quote 买入 base，"sell" 表示卖出 base 换回 quote。
    pub side: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TriangularPathConfig {
    pub venue: String,
    pub legs: [TriangularLegConfig; 3],
}

/// `CrossExchangeStrategy` 的下单执行配置。缺省(不写这一段)时策略退化为
/// 现状:只在 `on_quote` 里打日志,不下单。
#[derive(Debug, Deserialize, Clone)]
pub struct CrossExchangeExecutionConfig {
    pub enabled: bool,
    /// false=dry_run(默认),true 才会真实下单,与 `open --live` 的约定一致。
    #[serde(default)]
    pub live: bool,
    pub kraken_venue: String,
    pub binance_venue: String,
    #[serde(default = "default_ioc_wait_ms")]
    pub ioc_wait_timeout_ms: u64,
    /// key 是 asset(如 "BTC"),value 是允许的 |kraken持仓 - binance持仓| / 较大一边持仓
    /// 的比例上限(如 "0.2" 表示 20%)。两边当前持仓都为 0(还没交易过)时跳过该检查。
    /// 未在这里单独列出的资产，退回用 `default_asset_imbalance_ratio`(如果配了的话)。
    #[serde(default)]
    pub asset_imbalance_limits: HashMap<String, Decimal>,
    /// 没在 `asset_imbalance_limits` 里单独配置的资产，统一套用这个比例上限；
    /// 不写(默认 None)时那些资产完全不受这项检查约束。
    #[serde(default)]
    pub default_asset_imbalance_ratio: Option<Decimal>,
    /// live 下单时定期打印组合盈亏/仓位/订单报告的间隔(秒)，0 表示不打印。
    #[serde(default = "default_report_interval_secs")]
    pub report_interval_secs: u64,
}

fn default_ioc_wait_ms() -> u64 {
    5_000
}

fn default_report_interval_secs() -> u64 {
    300
}

fn default_source() -> String {
    "mock".to_string()
}

fn default_fee_discount() -> Decimal {
    Decimal::ONE
}

fn default_min_profit_bps() -> Decimal {
    Decimal::from(5)
}

fn default_tick_interval_ms() -> u64 {
    500
}

fn default_volatility() -> f64 {
    0.002
}

fn default_spread() -> f64 {
    0.001
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        toml::from_str(&text).context("failed to parse config toml")
    }
}

/// 只关心 `[[venues]]` 的宽松包装，用来在不要求 `symbols` 等字段存在的前提下
/// 从 `config.toml` 里单独读某个 venue 的配置(如 `fee_discount`)。字段之外的
/// 其余 TOML 内容会被 serde 自动忽略。
#[derive(Debug, Deserialize, Default)]
struct VenuesConfigFile {
    #[serde(default)]
    venues: Vec<VenueConfig>,
}

impl VenueConfig {
    /// 读取 `path` 里 `[[venues]]` 中名为 `venue_name`(大小写不敏感)的
    /// `fee_discount`。文件不存在、解析失败、或找不到该 venue 时，均回退到
    /// 默认值 1(不打折)，不返回 `Err`，和 [`ScanConfig::load_blacklist`] 同样
    /// 的兜底原则。
    pub fn load_fee_discount(path: impl AsRef<Path>, venue_name: &str) -> Decimal {
        let path = path.as_ref();
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(_) => return default_fee_discount(),
        };
        match toml::from_str::<VenuesConfigFile>(&text) {
            Ok(file) => file
                .venues
                .into_iter()
                .find(|v| v.name.eq_ignore_ascii_case(venue_name))
                .map(|v| v.fee_discount)
                .unwrap_or_else(default_fee_discount),
            Err(err) => {
                log::warn!(
                    "config: failed to parse '{}' for venue '{venue_name}' fee_discount, falling back to 1: {err:#}",
                    path.display()
                );
                default_fee_discount()
            }
        }
    }
}

/// `scan`/`monitor` 子命令的黑名单配置。和 [`AppConfig`] 独立，不要求
/// `venues`/`symbols` 等字段存在，因为这两个子命令本身不接入 `config.toml`
/// 驱动的默认主流程，只是顺带复用同一个配置文件里的 `[scan]` 段。
#[derive(Debug, Deserialize, Clone)]
pub struct ScanConfig {
    #[serde(default = "default_blacklist")]
    pub blacklist: Vec<String>,
}

impl Default for ScanConfig {
    fn default() -> Self {
        ScanConfig { blacklist: default_blacklist() }
    }
}

fn default_blacklist() -> Vec<String> {
    ["BTC", "ETH", "SOL", "USDC", "XRP"].into_iter().map(String::from).collect()
}

/// 只关心 `[scan]` 段的宽松包装，用来在不要求 `venues`/`symbols` 等字段存在的
/// 前提下从 `config.toml` 里单独读黑名单。字段之外的其余 TOML 内容
/// (`venues`/`symbols`/...) 会被 serde 自动忽略。
#[derive(Debug, Deserialize, Default)]
struct ScanConfigFile {
    #[serde(default)]
    scan: ScanConfig,
}

impl ScanConfig {
    /// 读取 `path` 里的 `[scan].blacklist`。文件不存在、无法解析、或没有
    /// `[scan]` 段/`blacklist` 字段时，均回退到默认黑名单
    /// (BTC/ETH/SOL/USDC/XRP)，不返回 `Err` —— 黑名单是 `scan`/`monitor` 的
    /// 安全兜底，不应该因为配置文件缺失就完全不生效。
    pub fn load_blacklist(path: impl AsRef<Path>) -> Vec<String> {
        let path = path.as_ref();
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(_) => return default_blacklist(),
        };
        match toml::from_str::<ScanConfigFile>(&text) {
            Ok(file) => file.scan.blacklist,
            Err(err) => {
                log::warn!(
                    "config: failed to parse '{}' for [scan] blacklist, falling back to defaults: {err:#}",
                    path.display()
                );
                default_blacklist()
            }
        }
    }
}

/// `cross-funding` 子命令的风控/执行参数(`config.toml` 的 `[cross_funding]` 段)。
/// 名义上限和敞口上限没有默认值，必须显式配置。
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CrossFundingConfig {
    /// 单行名义金额上限(USDT，币数 × 多腿每币价)，启动时和触发时各查一次。
    pub max_notional_usdt: Decimal,
    /// 已结束(开完/停止)各行未对冲敞口之和的上限(USDT)，超过就停掉所有行。
    pub max_unhedged_usdt: Decimal,
    /// 单腿成交滑点(相对触发时对手价)告警阈值，bp。
    #[serde(default = "default_cf_max_slippage_bp")]
    pub max_slippage_bp: f64,
    /// 启动时两边每币价格相差超过这个百分比就跳过这一行。
    #[serde(default = "default_cf_max_price_diff_pct")]
    pub max_price_diff_pct: Decimal,
    /// 下单后多少秒两腿还没全部成交就停止这一行。
    #[serde(default = "default_cf_fill_timeout_secs")]
    pub fill_timeout_secs: u64,
    /// 报价超过多少毫秒没更新就不参与决策/触发。
    #[serde(default = "default_cf_quote_max_age_ms")]
    pub quote_max_age_ms: u64,
    /// t0 时最新小时 K 线已收盘超过多少小时就不开仓。
    #[serde(default = "default_cf_max_bar_age_hours")]
    pub max_bar_age_hours: u64,
    /// σ ÷ 365 天 σ 中位数超过这个值就不开仓。
    #[serde(default = "default_cf_max_sigma_ratio")]
    pub max_sigma_ratio: f64,
    /// 这个文件存在时不决策、不下单，删掉恢复。
    #[serde(default = "default_cf_kill_switch_file")]
    pub kill_switch_file: String,
    /// 决策/结果记录(JSON Lines)。
    #[serde(default = "default_cf_record_file")]
    pub record_file: String,
}

fn default_cf_max_slippage_bp() -> f64 {
    10.0
}

fn default_cf_max_price_diff_pct() -> Decimal {
    Decimal::from(5)
}

fn default_cf_fill_timeout_secs() -> u64 {
    30
}

fn default_cf_quote_max_age_ms() -> u64 {
    5_000
}

fn default_cf_max_bar_age_hours() -> u64 {
    2
}

fn default_cf_max_sigma_ratio() -> f64 {
    3.0
}

fn default_cf_kill_switch_file() -> String {
    "cross_funding.stop".to_string()
}

fn default_cf_record_file() -> String {
    "logs/cross_funding_orders.jsonl".to_string()
}

/// 只关心 `[cross_funding]` 段的宽松包装。
#[derive(Debug, Deserialize)]
struct CrossFundingConfigFile {
    cross_funding: Option<CrossFundingConfig>,
}

impl CrossFundingConfig {
    /// 读取 `path` 里的 `[cross_funding]` 段。和黑名单不同，这里是真金白银的风控
    /// 上限：文件不存在、解析失败、缺这一段都直接报错，不回退默认值。
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("invalid [cross_funding] in {}", path.display()))
    }

    fn parse(text: &str) -> anyhow::Result<Self> {
        let file: CrossFundingConfigFile = toml::from_str(text).context("failed to parse config toml")?;
        file.cross_funding.ok_or_else(|| anyhow::anyhow!("missing [cross_funding] section"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_funding_config_requires_section_and_limits() {
        let config = CrossFundingConfig::parse(
            "[scan]\nblacklist = []\n[cross_funding]\nmax_notional_usdt = 200\nmax_unhedged_usdt = \"50\"\n",
        )
        .unwrap();
        assert_eq!(config.max_notional_usdt, Decimal::from(200));
        assert_eq!(config.max_unhedged_usdt, Decimal::from(50));
        assert_eq!(config.max_slippage_bp, 10.0);
        assert_eq!(config.fill_timeout_secs, 30);
        assert_eq!(config.max_bar_age_hours, 2);
        assert_eq!(config.max_sigma_ratio, 3.0);
        assert_eq!(config.record_file, "logs/cross_funding_orders.jsonl");

        assert!(CrossFundingConfig::parse("[scan]\nblacklist = []\n").is_err());
        assert!(CrossFundingConfig::parse("[cross_funding]\nmax_notional_usdt = 200\n").is_err());
        assert!(
            CrossFundingConfig::parse("[cross_funding]\nmax_notional_usdt = 1\nmax_unhedged_usdt = 1\ntypo = 1\n").is_err()
        );
    }
}
