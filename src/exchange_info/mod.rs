pub mod binance;
pub mod kraken;
pub mod types;

use async_trait::async_trait;

use crate::types::{Symbol, Venue};
use types::TradingFee;

/// 交易所"基础信息"扩展点：查询账户实际交易手续费率、列出当前可交易的
/// USDT 计价现货/USDT 本位永续合约交易对。和 `wallet::WalletProvider`、
/// `order::OrderProvider` 一样是按需调用的请求/响应接口，不接入 engine 主循环。
///
/// 链上提币手续费不在这个 trait 里重复暴露——已经由
/// `wallet::WalletProvider::asset_info` 提供(见 `wallet::types::ChainInfo::withdraw_fee`)，
/// 这里只覆盖 wallet 模块没有覆盖的信息。
#[async_trait]
pub trait ExchangeInfoProvider: Send + Sync {
    fn venue(&self) -> Venue;

    /// 查询某个现货交易对在当前账户下的实际 maker/taker 手续费率。
    async fn spot_trading_fee(&self, symbol: &Symbol) -> anyhow::Result<TradingFee>;

    /// 查询某个 USDT 本位永续合约在当前账户下的实际 maker/taker 手续费率。
    async fn perpetual_trading_fee(&self, symbol: &Symbol) -> anyhow::Result<TradingFee>;

    /// 列出该交易所全部以 USDT 计价、当前可交易的现货交易对。
    async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>>;

    /// 列出该交易所全部 USDT 本位、当前可交易的永续合约交易对。
    async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>>;
}
