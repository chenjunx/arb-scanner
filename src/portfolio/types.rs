use rust_decimal::Decimal;

use crate::types::{Symbol, Venue};

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
    /// 直接来自 `PositionManager::VenuePosition.realized_pnl`：成交平仓 +
    /// 手续费 + 资金费的完整已实现盈亏，`PortfolioManager` 不再自己维护账本。
    pub realized_pnl: Decimal,
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
