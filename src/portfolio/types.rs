use rust_decimal::Decimal;

use crate::types::{Symbol, Venue};

/// 单个 (venue, symbol) 的已实现盈亏/手续费累计。
#[derive(Debug, Clone, PartialEq)]
pub struct VenuePnl {
    pub venue: Venue,
    pub symbol: Symbol,
    pub realized_pnl: Decimal,
    pub fees_paid: Decimal,
    /// 只要 fees_paid 里累加过一次 `FeeConfig` 估算值(而非交易所真实手续费)就
    /// 置 true，提示调用方这个累计数不是全部来自交易所真实返还值，且一旦置
    /// true 不会被后续的真实值覆盖回 false。
    pub fee_is_estimated: bool,
    pub trade_count: u64,
    pub updated_at_ms: u64,
}

impl VenuePnl {
    pub fn flat(venue: Venue, symbol: Symbol) -> Self {
        Self {
            venue,
            symbol,
            realized_pnl: Decimal::ZERO,
            fees_paid: Decimal::ZERO,
            fee_is_estimated: false,
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
    pub fees_paid: Decimal,
    /// 来自 `asset_valuation()` 的浮动盈亏；缺行情时为 None，不当 0 处理。
    pub unrealized_pnl: Option<Decimal>,
    /// `realized_pnl - fees_paid + unrealized_pnl.unwrap_or(0)`；缺行情时仍然
    /// 给出这个值(只是不含浮动部分)，并靠 unrealized_pnl=None 提示调用方
    /// "这不是全量"。
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

/// 单个 venue 的手续费估算参数：`fee = filled_qty_delta.abs() * fill_price *
/// taker_fee_bps / 10000 * fee_discount`，只在拿不到交易所真实手续费时使用。
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct FeeConfig {
    pub taker_fee_bps: Decimal,
    pub fee_discount: Decimal,
}
