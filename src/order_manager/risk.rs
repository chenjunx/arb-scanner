use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rust_decimal::Decimal;

use crate::order::types::OrderAmount;
use crate::position::{FillOutcome, PositionManager};
use crate::types::{Symbol, Venue};

use super::types::RiskCheckResult;

/// 单个 venue+symbol 的风控限额配置
#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// 单笔订单最大数量 (base 或 quote，取决于订单本身的 OrderAmount 类型)
    pub max_order_amount: Decimal,
    /// 该 venue+symbol 上允许的最大持仓敞口 (净头寸的绝对值上限)
    pub max_position: Decimal,
    /// 滑动窗口内允许的最大订单数 (简单计数，不做真正的时间窗口，见 `check` 说明)
    pub max_orders_per_window: u32,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_order_amount: Decimal::MAX,
            max_position: Decimal::MAX,
            max_orders_per_window: u32::MAX,
        }
    }
}

/// 风控引擎：在订单提交交易所前做前置校验。当前实现是同步的内存态检查，
/// 覆盖三类最基础的风控:单笔限额、持仓限额、下单频率。
///
/// 持仓状态不再由 `RiskEngine` 自己存储，而是委托给 `PositionManager`
/// (见 `docs/position_manager_design.md`)，避免出现两份会漂移的持仓状态；
/// `RiskEngine` 只保留和风控本身相关的限额配置和下单计数。
pub struct RiskEngine {
    limits: HashMap<(Venue, Symbol), RiskLimits>,
    default_limits: RiskLimits,
    position_manager: Arc<PositionManager>,
    /// 每个 (venue, symbol) 已提交的订单计数，用于频率限制。
    /// 生产环境应替换成真正的滑动时间窗口，这里先用简单计数占位。
    order_counts: Mutex<HashMap<(Venue, Symbol), u32>>,
}

impl RiskEngine {
    pub fn new(limits: HashMap<(Venue, Symbol), RiskLimits>, position_manager: Arc<PositionManager>) -> Self {
        Self {
            limits,
            default_limits: RiskLimits::default(),
            position_manager,
            order_counts: Mutex::new(HashMap::new()),
        }
    }

    fn limits_for(&self, venue: &Venue, symbol: &Symbol) -> &RiskLimits {
        self.limits
            .get(&(venue.clone(), symbol.clone()))
            .unwrap_or(&self.default_limits)
    }

    /// 对一个待提交的订单做风控检查。检查通过后立即预占用下单计数，
    /// 避免同一批并发订单绕过限额 (乐观预占用，被交易所拒绝或者取消时需要调用
    /// `release` 回滚)。
    pub fn check(&self, venue: &Venue, symbol: &Symbol, side: crate::order::types::OrderSide, amount: &OrderAmount) -> RiskCheckResult {
        let limits = self.limits_for(venue, symbol);

        let order_qty = amount.value();
        if order_qty > limits.max_order_amount {
            return RiskCheckResult::Rejected {
                reason: format!(
                    "order amount {order_qty} exceeds max_order_amount {} for {venue}/{symbol}",
                    limits.max_order_amount
                ),
            };
        }

        let key = (venue.clone(), symbol.clone());
        {
            let counts = self.order_counts.lock().unwrap();
            let current = counts.get(&key).copied().unwrap_or(0);
            if current >= limits.max_orders_per_window {
                return RiskCheckResult::Rejected {
                    reason: format!(
                        "order count {current} reached max_orders_per_window {} for {venue}/{symbol}",
                        limits.max_orders_per_window
                    ),
                };
            }
        }

        {
            let current_position = self.position_manager.position(venue, symbol);
            let delta = match side {
                crate::order::types::OrderSide::Buy => order_qty,
                crate::order::types::OrderSide::Sell => -order_qty,
            };
            let projected = current_position + delta;
            if projected.abs() > limits.max_position {
                return RiskCheckResult::Rejected {
                    reason: format!(
                        "projected position {projected} would exceed max_position {} for {venue}/{symbol}",
                        limits.max_position
                    ),
                };
            }
        }

        // 通过检查后预占用下单计数，防止并发订单绕过限额。
        self.order_counts.lock().unwrap().entry(key.clone()).and_modify(|c| *c += 1).or_insert(1);
        RiskCheckResult::Approved
    }

    /// 订单最终成交后调用，用实际成交数量(增量)和成交价更新 `PositionManager`
    /// 里的持仓状态 (替代 `check` 时的预估值，因为市价单实际成交量可能和请求量
    /// 有偏差)。`fill_price` 拿不到时只更新数量，均价不变，见
    /// `PositionManager::on_filled`。原样转发 `FillOutcome`，供调用方喂给
    /// `PortfolioManager::record_fill` 记账已实现盈亏。
    pub fn on_filled(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        side: crate::order::types::OrderSide,
        filled_qty: Decimal,
        fill_price: Option<Decimal>,
        ts_ms: u64,
    ) -> FillOutcome {
        self.position_manager.on_filled(venue, symbol, side, filled_qty, fill_price, ts_ms)
    }

    /// 订单被交易所拒绝或从未真正下单成功时调用，释放 `check` 阶段预占用的
    /// 下单计数，避免额度被白白占用。
    pub fn release(&self, venue: &Venue, symbol: &Symbol) {
        let key = (venue.clone(), symbol.clone());
        if let Some(count) = self.order_counts.lock().unwrap().get_mut(&key) {
            *count = count.saturating_sub(1);
        }
    }

    /// 查询某个 venue+symbol 当前的净持仓，供上层展示/调试使用。
    pub fn position(&self, venue: &Venue, symbol: &Symbol) -> Decimal {
        self.position_manager.position(venue, symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::types::OrderSide;
    use crate::position::InMemoryPositionStore;

    fn venue() -> Venue {
        Venue::new("binance")
    }
    fn symbol() -> Symbol {
        Symbol::new("BTC", "USDT")
    }

    fn position_manager() -> Arc<PositionManager> {
        Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())))
    }

    #[test]
    fn approves_order_within_limits() {
        let mut limits = HashMap::new();
        limits.insert(
            (venue(), symbol()),
            RiskLimits {
                max_order_amount: Decimal::new(1, 0),
                max_position: Decimal::new(10, 0),
                max_orders_per_window: 10,
            },
        );
        let engine = RiskEngine::new(limits, position_manager());
        let result = engine.check(&venue(), &symbol(), OrderSide::Buy, &OrderAmount::Base(Decimal::new(5, 1)));
        assert!(matches!(result, RiskCheckResult::Approved));
    }

    #[test]
    fn rejects_order_exceeding_max_amount() {
        let mut limits = HashMap::new();
        limits.insert(
            (venue(), symbol()),
            RiskLimits {
                max_order_amount: Decimal::new(1, 0),
                max_position: Decimal::new(10, 0),
                max_orders_per_window: 10,
            },
        );
        let engine = RiskEngine::new(limits, position_manager());
        let result = engine.check(&venue(), &symbol(), OrderSide::Buy, &OrderAmount::Base(Decimal::new(2, 0)));
        assert!(matches!(result, RiskCheckResult::Rejected { .. }));
    }

    #[test]
    fn rejects_order_exceeding_position_limit() {
        let mut limits = HashMap::new();
        limits.insert(
            (venue(), symbol()),
            RiskLimits {
                max_order_amount: Decimal::new(100, 0),
                max_position: Decimal::new(1, 0),
                max_orders_per_window: 100,
            },
        );
        let engine = RiskEngine::new(limits, position_manager());
        engine.on_filled(&venue(), &symbol(), OrderSide::Buy, Decimal::new(9, 1), Some(Decimal::new(50000, 0)), 1);
        let result = engine.check(&venue(), &symbol(), OrderSide::Buy, &OrderAmount::Base(Decimal::new(5, 1)));
        assert!(matches!(result, RiskCheckResult::Rejected { .. }));
    }

    #[test]
    fn rejects_when_order_count_window_exhausted() {
        let mut limits = HashMap::new();
        limits.insert(
            (venue(), symbol()),
            RiskLimits {
                max_order_amount: Decimal::new(100, 0),
                max_position: Decimal::new(100, 0),
                max_orders_per_window: 1,
            },
        );
        let engine = RiskEngine::new(limits, position_manager());
        let first = engine.check(&venue(), &symbol(), OrderSide::Buy, &OrderAmount::Base(Decimal::ONE));
        assert!(matches!(first, RiskCheckResult::Approved));
        let second = engine.check(&venue(), &symbol(), OrderSide::Buy, &OrderAmount::Base(Decimal::ONE));
        assert!(matches!(second, RiskCheckResult::Rejected { .. }));
    }

    #[test]
    fn release_frees_up_order_count_slot() {
        let mut limits = HashMap::new();
        limits.insert(
            (venue(), symbol()),
            RiskLimits {
                max_order_amount: Decimal::new(100, 0),
                max_position: Decimal::new(100, 0),
                max_orders_per_window: 1,
            },
        );
        let engine = RiskEngine::new(limits, position_manager());
        assert!(matches!(
            engine.check(&venue(), &symbol(), OrderSide::Buy, &OrderAmount::Base(Decimal::ONE)),
            RiskCheckResult::Approved
        ));
        engine.release(&venue(), &symbol());
        assert!(matches!(
            engine.check(&venue(), &symbol(), OrderSide::Buy, &OrderAmount::Base(Decimal::ONE)),
            RiskCheckResult::Approved
        ));
    }

    #[test]
    fn on_filled_delegates_to_shared_position_manager() {
        let pm = position_manager();
        let mut limits = HashMap::new();
        limits.insert(
            (venue(), symbol()),
            RiskLimits::default(),
        );
        let engine = RiskEngine::new(limits, pm.clone());

        engine.on_filled(&venue(), &symbol(), OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);

        // 通过共享的 PositionManager 也能看到同一份持仓状态，证明 RiskEngine
        // 没有维护自己的一份影子状态。
        assert_eq!(pm.position(&venue(), &symbol()), Decimal::ONE);
        assert_eq!(engine.position(&venue(), &symbol()), Decimal::ONE);
    }
}
