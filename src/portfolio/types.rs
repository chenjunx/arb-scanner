use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::types::{Symbol, Venue};

/// 单个 (venue, symbol) 的已实现盈亏/手续费累计。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VenuePnl {
    pub venue: Venue,
    pub symbol: Symbol,
    pub realized_pnl: Decimal,
    pub trade_count: u64,
    pub updated_at_ms: u64,
}

impl VenuePnl {
    pub fn flat(venue: Venue, symbol: Symbol) -> Self {
        Self {
            venue,
            symbol,
            realized_pnl: Decimal::ZERO,
            trade_count: 0,
            updated_at_ms: 0,
        }
    }
}

/// 按 base 资产聚合的已实现盈亏汇总，含浮动盈亏拼接。
#[derive(Debug, Clone, PartialEq)]
pub struct AssetPnlSummary {
    pub asset: String,
    pub realized_pnl: Decimal,
    /// 来自 `asset_valuation()` 的浮动盈亏；缺行情时为 None，不当 0 处理。
    pub unrealized_pnl: Option<Decimal>,
    /// `realized_pnl + unrealized_pnl.unwrap_or(0)`；缺行情时仍然给出这个值
    /// (只是不含浮动部分)，并靠 unrealized_pnl=None 提示调用方"这不是全量"。
    pub net_pnl: Decimal,
}

/// 单个 (venue, symbol) 的 mark-to-market 估值快照。
#[derive(Debug, Clone, PartialEq)]
pub struct VenuePositionValuation {
    pub venue: Venue,
    pub symbol: Symbol,
    pub net_qty: Decimal,
    pub avg_price: Option<Decimal>,
    pub mark_price: Option<Decimal>,
    pub market_value: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
}

/// 按 base 资产聚合的估值。
#[derive(Debug, Clone, PartialEq)]
pub struct AssetValuation {
    pub asset: String,
    pub net_qty: Decimal,
    /// 只有当参与聚合的 venue 全部拿到了 mark price 才是 Some，避免"部分 venue
    /// 缺价"时的市值被悄悄少算却看起来像是完整数字。
    pub market_value: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub venues: Vec<VenuePositionValuation>,
}
