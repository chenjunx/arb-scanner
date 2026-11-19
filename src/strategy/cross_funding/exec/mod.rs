//! 开仓执行算法：信号层(`cross_funding`)决定"现在开 N 币"，执行层决定怎么下单、
//! 怎么跟踪成交、什么时候算完成或失败。
//!
//! 算法只返回 [`ExecAction`]，不直接发单：发单统一由策略走 `Strategy::submit_order`
//! 这类 trait 方法。模拟盘下策略自己按盘口把 `Market` 动作模拟成成交事件再喂回算法，
//! 所以实盘和模拟盘共用同一套算法逻辑。

mod taker;

use std::fmt;

use rust_decimal::Decimal;

use crate::exchange_info::types::PerpContractSpec;
use crate::market_data::now_ms;
use crate::market_data::orderbook::OrderBookLevels;
use crate::order::types::OrderSide;

pub(super) use taker::TakerTaker;

use super::{LegRole, LegState, per_coin};

/// 一次决策用的两腿盘口。
#[derive(Clone, Copy)]
pub(super) struct BookPair<'a> {
    pub long: &'a OrderBookLevels,
    pub short: &'a OrderBookLevels,
    pub now_ms: u64,
    /// 盘口超过这么久就不能用。
    pub max_age_ms: u64,
}

/// 按当前盘口估算的开仓成本，信号层用 `exec_bp` 判断 Target 是否达标。
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct ExecEstimate {
    pub long_px_per_coin: Decimal,
    pub short_px_per_coin: Decimal,
    /// 预估成交价差(bp)，口径和 t0 的 `baseline_bp` 一致。
    pub exec_bp: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum ExecAction {
    /// 市价单，`qty` 是合约 base 单位。
    Market { leg: LegRole, side: OrderSide, qty: Decimal, client_order_id: String },
}

/// 策略从订单事件里翻译出来喂给算法的事件。
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ExecEvent {
    Filled { leg: LegRole, filled_qty: Decimal, avg_price: Decimal },
    /// 订单终态失败(拒单/撤单/失效)。`partial` 是权威状态里已成交的部分
    /// `(filled_qty, avg_price)`。
    Failed { leg: LegRole, kind: String, reason: String, partial: Option<(Decimal, Decimal)> },
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum ExecStatus {
    Running,
    /// 两腿成交都已写进 `LegState::filled`。
    Done,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct ExecStep {
    pub actions: Vec<ExecAction>,
    pub status: ExecStatus,
}

impl ExecStep {
    pub fn running(actions: Vec<ExecAction>) -> Self {
        Self { actions, status: ExecStatus::Running }
    }

    pub fn idle() -> Self {
        Self::running(Vec::new())
    }

    pub fn done() -> Self {
        Self { actions: Vec::new(), status: ExecStatus::Done }
    }

    pub fn failed(reason: impl Into<String>) -> Self {
        Self { actions: Vec::new(), status: ExecStatus::Failed(reason.into()) }
    }
}

/// 一行的开仓执行算法。每行一个实例，状态随这一行走。
pub(super) trait ExecAlgo: fmt::Debug + Send {
    /// 写进复盘记录的成交方式(`liquidity` 字段)。
    fn liquidity(&self) -> &'static str;

    fn clone_box(&self) -> Box<dyn ExecAlgo>;

    /// 按当前盘口估算开仓价差；盘口过期、深度不够等拿不到时返回 None。
    fn estimate(&self, books: &BookPair<'_>, long: &LegState, short: &LegState) -> Option<ExecEstimate>;

    /// 信号层触发后开始执行。
    fn start(&mut self, long: &mut LegState, short: &mut LegState, now_ms: u64) -> ExecStep;

    fn on_event(&mut self, long: &mut LegState, short: &mut LegState, event: ExecEvent, now_ms: u64) -> ExecStep;

    /// 每帧行情都会调用一次，用来处理超时之类和订单事件无关的推进。
    fn on_tick(&mut self, long: &mut LegState, short: &mut LegState, now_ms: u64) -> ExecStep;
}

impl Clone for Box<dyn ExecAlgo> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// 按市价单方向扫盘口，返回覆盖完整下单量后的每币 VWAP。盘口数量以交易所原生单位
/// 表示，先用合约面值换成内部合约 base 单位。
pub(super) fn market_vwap(levels: &[(Decimal, Decimal)], order_qty: Decimal, spec: &PerpContractSpec) -> Option<Decimal> {
    let contract_size = spec.contract_size();
    if order_qty <= Decimal::ZERO || contract_size <= Decimal::ZERO {
        return None;
    }

    let mut remaining = order_qty;
    let mut notional = Decimal::ZERO;
    for (price, native_qty) in levels {
        if *price <= Decimal::ZERO || *native_qty <= Decimal::ZERO {
            continue;
        }
        let level_qty = *native_qty * contract_size;
        let filled_qty = level_qty.min(remaining);
        notional += per_coin(*price, spec) * filled_qty;
        remaining -= filled_qty;
        if remaining.is_zero() {
            return Some(notional / order_qty);
        }
    }
    None
}

pub(super) fn new_client_order_id() -> String {
    // 刻意不用 `Strategy::generate_client_order_id`(带完整策略名)：Gate `text`
    // 前缀后最多 28 字节、OKX `clOrdId` 只能是 1~32 位字母数字，短的纯字母数字
    // id 才能在各所原样回传、按 client_order_id 关联。
    format!("cf{}{:05}", now_ms(), rand::random::<u32>() % 100_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_order_id_fits_gate_text_limit() {
        let id = new_client_order_id();
        assert!(id.starts_with("cf"));
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric()), "{id}");
        assert!(id.len() <= 28, "{id}");
    }
}
