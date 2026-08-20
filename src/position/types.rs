use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::{Symbol, Venue};

/// 单个 (venue, symbol) 上的净仓位快照。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VenuePosition {
    pub venue: Venue,
    pub symbol: Symbol,
    /// 净数量，base 币种单位。正=净多头，负=净空头(含合约空头)。
    pub net_qty: Decimal,
    /// 当前净仓位的加权平均建仓价；net_qty 为 0 时是 None。
    pub avg_price: Option<Decimal>,
    /// 按币种汇总的累计手续费。键为币种符号（如 "USDT"、"BNB"），
    /// 值为该币种的累计手续费金额。`#[serde(default)]` 兼容这个字段引入
    /// 之前写入的旧 Redis 记录——否则反序列化失败会导致
    /// `RedisPositionStore::update` 把已有仓位当成不存在，静默清空
    /// net_qty/avg_price 重新计算，是真实的仓位状态丢失风险。
    #[serde(default)]
    pub total_fees: HashMap<String, Decimal>,
    /// 累计已实现盈亏：每次 `PositionManager::on_filled` 在减仓/穿零反向时
    /// 算出的那笔盈亏都会加进来(同方向加仓/从 0 建仓恒为 0，不影响这个值)，
    /// 以及每次 `PositionManager::apply_adjustment`（资金费结算、手续费换算成
    /// USDT 后冲减盈亏、人工修正等非成交事件）加进来的调整量。
    /// `on_filled` 那部分和 `FillOutcome::realized_pnl` 用的是同一次计算，这里
    /// 只是把逐笔结果就地累加、随仓位持久化，`PortfolioManager`/`PnlStore` 里
    /// 独立维护的那份累计值不受影响，两者应当始终相等。`#[serde(default)]`
    /// 兼容这个字段引入之前写入的旧 Redis 记录。
    #[serde(default)]
    pub realized_pnl: Decimal,
    pub updated_at_ms: u64,
}

impl VenuePosition {
    pub fn flat(venue: Venue, symbol: Symbol) -> Self {
        Self {
            venue,
            symbol,
            net_qty: Decimal::ZERO,
            avg_price: None,
            total_fees: HashMap::new(),
            realized_pnl: Decimal::ZERO,
            updated_at_ms: 0,
        }
    }
}

/// `PositionManager::on_filled` 的返回值：把这次调用在 `PositionStore` 原子更新
/// 内部算出的已实现盈亏带给调用方，`PositionManager` 本身不存储/累计它。
#[derive(Debug, Clone, PartialEq)]
pub struct FillOutcome {
    pub realized_pnl: Decimal,
    /// 本次成交的手续费（金额，币种）
    pub fee: Option<(Decimal, String)>,
    /// 本次成交的手续费换算成 USDT 的等值，只反映 `on_filled` 调用那一刻
    /// **同步**能解出来的值(稳定币直通/复用成交价)；需要异步查价的情形
    /// (如 BNB/KFEE)这里是 `None`，稍后由后台任务通过
    /// `PositionManager::apply_adjustment` 冲减进 `realized_pnl`，不会体现在
    /// 这个一次性返回值里。
    pub fee_usdt: Option<Decimal>,
}

/// `PositionManager::apply_adjustment` 的调整来源。不落进 `VenuePosition`
/// 状态里——避免把每笔调整都存成一个永远增长的流水数组；而是随每次调用一起
/// 写进独立的 `AdjustmentRecord` 审计日志（见 `adjustment_log` 模块）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum AdjustmentReason {
    /// 永续合约资金费结算
    Funding,
    /// 手续费换算成 USDT 后冲减已实现盈亏
    FeeUsdt,
    /// 人工修正
    Manual,
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
