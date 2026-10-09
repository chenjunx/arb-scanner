pub mod binance;
pub mod bitget;
pub mod cache;
pub mod coinex;
pub mod gate;
pub mod kraken;
pub mod nonkyc;
pub mod okx;
pub mod types;

use std::collections::HashMap;

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};

use crate::types::{Symbol, Venue};
use types::{FundingRateRecord, MarketPrecision, PerpContractSpec, PredictedFundingRate, TradingFee};

pub use cache::PrecisionCache;

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

    /// 一次性拉取全部现货 USDT 交易对的下单精度(市价/限价步进、最小下单量、
    /// 价格 tick)，供 [`PrecisionCache`] 在启动时加载进内存。
    async fn spot_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>>;

    /// 同上，USDT 本位永续合约。Kraken 当前没有可下单的 USDT 永续(参照
    /// `usdt_perpetual_symbols` 的既有约定)，返回空 Vec，不是 bug。
    async fn perpetual_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>>;

    /// 查询某个 USDT 本位永续合约的计价/下单单位，见 [`PerpContractSpec`]。
    /// 默认实现是报错，没有永续合约的交易所不需要实现。
    async fn perpetual_contract_spec(&self, symbol: &Symbol) -> anyhow::Result<PerpContractSpec> {
        anyhow::bail!("perpetual contract spec not supported for venue {} ({symbol})", self.venue())
    }
}

/// USDT 本位永续合约的公开资金费率查询：历史已结算费率 + 下一期预测费率。
/// 和 `accounting::FundingFeeProvider`(本账户实际收付的资金费流水，需要签名)
/// 不是一回事——这里是市场公开数据，用于扫描/选币/开平仓判断。
///
/// 单独成一个 trait 而不是并进 [`ExchangeInfoProvider`]：没有永续合约的交易所
/// (Kraken/Nonkyc)不需要实现它。有永续的交易所由各自的 `XxxExchangeInfoProvider`
/// 实现，复用同一个 http client。
#[async_trait]
pub trait FundingRateProvider: Send + Sync {
    fn venue(&self) -> Venue;

    /// 拉取该永续合约 `funding_time` 落在 `[start_time_ms, end_time_ms]` 内的
    /// 历史结算费率，按时间升序返回。`end_time_ms` 为 `None` 表示到当前为止。
    /// 实现内部负责翻页，调用方拿到的是完整区间。
    async fn funding_rate_history(
        &self,
        symbol: &Symbol,
        start_time_ms: u64,
        end_time_ms: Option<u64>,
    ) -> anyhow::Result<Vec<FundingRateRecord>>;

    /// 拉取该永续合约最近 `limit` 次已结算费率(一次请求，不翻页)，按时间升序
    /// 返回。`limit` 超过交易所单页上限时按上限截断，见各实现。
    async fn recent_funding_rates(&self, symbol: &Symbol, limit: usize) -> anyhow::Result<Vec<FundingRateRecord>>;

    /// 批量版 [`Self::recent_funding_rates`]：逐个合约并发
    /// [`FUNDING_HISTORY_BATCH_CONCURRENCY`] 个调用，实际请求速率仍由
    /// `ratelimit::throttle` 控制。单个合约失败不影响其他合约，结果里对应的是 `Err`。
    async fn recent_funding_rates_batch(
        &self,
        symbols: &[Symbol],
        limit: usize,
    ) -> HashMap<Symbol, anyhow::Result<Vec<FundingRateRecord>>> {
        // 按值传 Symbol 进 async 块：借用 `&Symbol` 会撞上 async_trait 要求 Send
        // 时的高阶生命周期推断问题(`FnOnce` is not general enough)。
        stream::iter(symbols.to_vec())
            .map(|symbol| async move {
                let result = self.recent_funding_rates(&symbol, limit).await;
                (symbol, result)
            })
            .buffer_unordered(FUNDING_HISTORY_BATCH_CONCURRENCY)
            .collect()
            .await
    }

    /// 一次性拉取全部 USDT 本位永续合约的下一期预测费率。
    async fn predicted_funding_rates(&self) -> anyhow::Result<Vec<PredictedFundingRate>>;
}

/// [`FundingRateProvider::recent_funding_rates_batch`] 的并发度。
pub const FUNDING_HISTORY_BATCH_CONCURRENCY: usize = 8;
