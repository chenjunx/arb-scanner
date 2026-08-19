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
    /// 已成功换算成 USDT 计价的手续费累计（见 `pricing::FeeUsdtConverter`）。
    /// 只是"已换算成功部分"的运行总和，`fees_usdt_incomplete` 为 true 时
    /// 提示这个数可能偏低。`#[serde(default)]` 兼容旧的、没有这个字段的
    /// Redis 记录。
    #[serde(default)]
    pub total_fees_usdt: Decimal,
    /// 曾经有手续费未能(或尚未)换算成 USDT——REST 查价失败，或换算任务
    /// 在完成前进程崩溃。不区分"进行中"和"失败"，只表示"别信
    /// total_fees_usdt 是全量"。
    #[serde(default)]
    pub fees_usdt_incomplete: bool,
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
            total_fees_usdt: Decimal::ZERO,
            fees_usdt_incomplete: false,
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
    /// `PositionManager::apply_fee_usdt` 补齐，不会体现在这个一次性返回值里。
    pub fee_usdt: Option<Decimal>,
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
