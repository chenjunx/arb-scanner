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
