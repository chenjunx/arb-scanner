pub mod cross_exchange;
pub mod triangular;

use rust_decimal::Decimal;

use crate::engine::MarketView;
use crate::types::{MarketEvent, Symbol, Venue};

/// 某个 venue 的手续费配置，用于在计算套利收益时扣除成本。
#[derive(Debug, Clone, Copy)]
pub struct FeeSchedule {
    pub taker_bps: Decimal,
}

impl FeeSchedule {
    pub fn new(taker_bps: impl Into<Decimal>) -> Self {
        Self {
            taker_bps: taker_bps.into(),
        }
    }

    /// 买入时实际付出的价格 = ask * buy_multiplier（手续费推高实际成本）。
    pub fn buy_multiplier(&self) -> Decimal {
        Decimal::ONE + self.taker_bps / Decimal::from(10_000)
    }

    /// 卖出时实际收到的价格 = bid * sell_multiplier（手续费压低实际收益）。
    pub fn sell_multiplier(&self) -> Decimal {
        Decimal::ONE - self.taker_bps / Decimal::from(10_000)
    }
}

#[derive(Debug, Clone)]
pub enum OpportunityKind {
    CrossExchange {
        symbol: Symbol,
        buy_venue: Venue,
        sell_venue: Venue,
    },
    Triangular {
        venue: Venue,
        legs: [Symbol; 3],
    },
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub strategy: &'static str,
    pub kind: OpportunityKind,
    pub expected_profit_bps: Decimal,
    pub detail: String,
    pub ts_ms: u64,
}

/// 套利策略扩展点：每次行情缓存更新后被引擎调用一次，
/// 基于当前快照判断是否存在套利机会。
pub trait Strategy: Send + Sync {
    fn name(&self) -> &str;
    fn on_update(&self, view: &MarketView, changed: &MarketEvent) -> Vec<Opportunity>;
}
