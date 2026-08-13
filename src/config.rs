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
}

#[derive(Debug, Deserialize, Clone)]
pub struct VenueConfig {
    pub name: String,
    #[serde(default)]
    pub taker_fee_bps: Decimal,
    /// 行情数据源实现: "mock"(默认,随机游走假行情) | "binance_spot"(币安现货真实行情)
    /// | "kraken_spot"(Kraken 现货真实行情)。
    #[serde(default = "default_source")]
    pub source: String,
    /// 仅当 source = "binance_spot" 时生效,是否连接币安测试网。
    #[serde(default)]
    pub testnet: bool,
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

fn default_source() -> String {
    "mock".to_string()
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
