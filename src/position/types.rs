use rust_decimal::Decimal;

use crate::types::{Symbol, Venue};

/// 单个 (venue, symbol) 上的净仓位快照。
#[derive(Debug, Clone, PartialEq)]
pub struct VenuePosition {
    pub venue: Venue,
    pub symbol: Symbol,
    /// 净数量，base 币种单位。正=净多头，负=净空头(含合约空头)。
    pub net_qty: Decimal,
    /// 当前净仓位的加权平均建仓价；net_qty 为 0 时是 None。
    pub avg_price: Option<Decimal>,
    pub updated_at_ms: u64,
}

impl VenuePosition {
    pub fn flat(venue: Venue, symbol: Symbol) -> Self {
        Self {
            venue,
            symbol,
            net_qty: Decimal::ZERO,
            avg_price: None,
            updated_at_ms: 0,
        }
    }
}

/// `PositionManager::on_filled` 的返回值：把这次调用在 `PositionStore` 原子更新
/// 内部算出的已实现盈亏带给调用方，`PositionManager` 本身不存储/累计它。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FillOutcome {
    pub realized_pnl: Decimal,
}

/// 按 base 资产跨 venue/产品聚合后的全局敞口。
#[derive(Debug, Clone)]
pub struct AssetExposure {
    pub asset: String,
    /// 所有相关 venue+symbol 净仓位之和；接近 0 视为已对冲。
    pub net_qty: Decimal,
    /// 参与聚合的明细，用于按 venue 拆解出"该平多少"。
    pub venues: Vec<VenuePosition>,
}
