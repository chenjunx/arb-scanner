use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::types::{Symbol, Venue};

/// 单条资金费结算记录，跨交易所统一形状。
#[derive(Debug, Clone, PartialEq)]
pub struct FundingIncomeRecord {
    pub symbol: Symbol,
    /// 正=收到资金费，负=支付资金费。
    pub income: Decimal,
    pub time_ms: u64,
    /// 交易所侧的流水 ID，保证同一 (venue, symbol) 内单调递增，用于
    /// `accounting::FundingCursorStore` 去重。
    pub tran_id: i64,
}

/// 资金费查询接口：每个交易所的期货 provider 实现一份，
/// `FundingFeeTracker` 只依赖这个 trait，不关心具体交易所的签名/REST 细节。
#[async_trait]
pub trait FundingFeeProvider: Send + Sync {
    fn venue(&self) -> Venue;

    /// 拉取该 symbol 从 `start_time_ms`(含)起的资金费流水，按 `tran_id` 升序
    /// 返回。`start_time_ms` 为 `None` 时由具体实现决定默认回溯窗口。
    async fn funding_income(&self, symbol: &Symbol, start_time_ms: Option<u64>) -> anyhow::Result<Vec<FundingIncomeRecord>>;
}
