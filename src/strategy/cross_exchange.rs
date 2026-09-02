use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{error, info, warn};
use rust_decimal::Decimal;

use crate::exchange_info::PrecisionCache;
use crate::exchange_info::types::PrecisionKind;
use crate::market_data::link_health::LinkHealthMonitor;
use crate::market_data::now_ms;
use crate::order::types::{OrderAmount, OrderSide, OrderStatus};
use crate::order_manager::OrderManager;
use crate::order_manager::types::{OrderEvent, OrderId};
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{FeeSchedule, Opportunity, OpportunityKind, Strategy};

/// 跨交易所下单执行所需的全部依赖打包。`kraken_venue`/`binance_venue` 是
/// `on_quote` 里用来匹配价差机会的行情 venue 名；`kraken_trade_venue`/
/// `binance_trade_venue` 是实际下单用的 venue 名——两者通常相同，但
/// `run_monitor_command` 里 Kraken 的行情 venue 是 "kraken"、交易 venue 是
/// "kraken_spot"，这里显式区分以便将来复用。
pub struct CrossExecutionConfig {
    pub kraken_venue: Venue,
    pub kraken_trade_venue: Venue,
    pub binance_venue: Venue,
    pub binance_trade_venue: Venue,
    pub kraken_precision: Arc<PrecisionCache>,
    pub binance_precision: Arc<PrecisionCache>,
    pub order_manager: Arc<OrderManager>,
    /// 启动时预算好的每个 symbol 的下单量：两边交易所最小下单量里较大的那个。
    pub order_qty_by_symbol: HashMap<Symbol, Decimal>,
    pub ioc_wait_timeout: Duration,
    pub hedge_wait_timeout: Duration,
}

/// 跨交易所同交易对价差套利：在多个 venue 上监控同一批 symbol，
/// 若某 venue 的卖一价（扣费后）低于另一 venue 的买一价（扣费后），则存在套利空间。
pub struct CrossExchangeStrategy {
    symbols: Vec<Symbol>,
    fees: HashMap<Venue, FeeSchedule>,
    min_profit_bps: Decimal,
    health: Arc<LinkHealthMonitor>,
    latest: Mutex<HashMap<Symbol, HashMap<Venue, Quote>>>,
    bus: Arc<TopicBus>,
    execution: Option<Arc<CrossExecutionConfig>>,
    /// 已挂出、还在等终态的 kraken GTC 挂单：key 是 client_order_id。
    /// `on_order_event` 收到订单事件后按 order_id 反查出 client_order_id，
    /// 在这张表里找到对应条目才说明这是我们自己在等的 kraken 挂单
    /// （而不是币安对冲单自己的事件），随后取出成交量去下对冲单。
    /// 同一时刻每个 symbol 最多一条记录——`submit_kraken_maker_order` 下单前
    /// 会扫描这张表，同一 symbol 已有在途挂单时跳过；`evaluate_kraken_resting_order`
    /// 靠这张表判断该 symbol 当前是"无挂单"/"挂单仍然有效"/"挂单需要撤销"。
    pending_kraken_orders: Arc<Mutex<HashMap<String, PendingKrakenLeg>>>,
    /// 已发出、还在等成交结果的 binance 对冲单：key 是 client_order_id。
    /// 与 `pending_kraken_orders` 同一套模式——`on_order_event` 按
    /// client_order_id 对表，找到即说明这是我们自己在等的对冲单终态，
    /// 不需要再像旧版那样临时订阅事件流等一笔。
    pending_binance_orders: Arc<Mutex<HashMap<String, PendingBinanceLeg>>>,
}

/// `pending_kraken_orders` 表里的一条登记：足够 `on_order_event` 拿去下对冲单
/// （symbol 决定精度/下单量四舍五入，kraken_side 翻转后就是对冲单方向）。
/// `price` 是精度取整后实际挂到交易所的价格，供 `evaluate_kraken_resting_order`
/// 判断"是否还在最优盘口"。
struct PendingKrakenLeg {
    symbol: Symbol,
    kraken_side: OrderSide,
    price: Decimal,
    /// 已经对这笔单发出过撤单请求、正在等待终态(Cancelled/Filled)确认。
    /// 用于防止同一笔单被重复撤单——撤单不经过 `RiskService` 限流，没有这个
    /// 标记的话每次 `on_quote` 都会再发一次撤单请求。
    cancel_requested: bool,
}

/// `pending_binance_orders` 表里的一条登记：`on_order_event` 收到币安对冲单
/// 终态后，靠这些信息记日志（kraken 侧的 order_id/成交量是为了让失衡告警能
/// 定位到具体是哪一笔 kraken 探路单）。
struct PendingBinanceLeg {
    symbol: Symbol,
    kraken_order_id: OrderId,
    kraken_filled_qty: Decimal,
}

impl CrossExchangeStrategy {
    pub fn new(
        symbols: Vec<Symbol>,
        fees: HashMap<Venue, FeeSchedule>,
        min_profit_bps: Decimal,
        health: Arc<LinkHealthMonitor>,
        bus: Arc<TopicBus>,
    ) -> Self {
        Self {
            symbols,
            fees,
            min_profit_bps,
            health,
            latest: Mutex::new(HashMap::new()),
            bus,
            execution: None,
            pending_kraken_orders: Arc::new(Mutex::new(HashMap::new())),
            pending_binance_orders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 注入下单执行配置，激活"价差超过阈值时自动下单"。未调用时策略保持
    /// 现状的纯打日志行为，不影响未接入执行能力的调用点（如
    /// `run_monitor_command`）。
    pub fn with_execution(mut self, execution: Arc<CrossExecutionConfig>) -> Self {
        self.execution = Some(execution);
        self
    }

    /// `subscriptions()` 就是从 `fees` 派生出来的，所以这里查不到只可能是调用方
    /// 传入的行情不是自己订阅范围内的——修 bug 而不是兜底掩盖它。
    fn fee_for(&self, venue: &Venue) -> FeeSchedule {
        self.fees
            .get(venue)
            .copied()
            .expect("CrossExchangeStrategy received a quote for a venue outside its subscriptions")
    }
}

/// 给定买/卖两侧的价格和各自的成本/收益乘数，返回扣费后的价差(基点)。买价
/// <= 0 时返回 `None`(报价还没来)。乘数由调用方按 taker/maker 场景传入
/// (`FeeSchedule::buy_multiplier`/`sell_multiplier` 或
/// `maker_buy_multiplier`/`maker_sell_multiplier`)，本函数不关心费率来源。
pub fn compute_profit_bps(
    buy_ask: Decimal,
    buy_cost_multiplier: Decimal,
    sell_bid: Decimal,
    sell_proceeds_multiplier: Decimal,
) -> Option<Decimal> {
    if buy_ask <= Decimal::ZERO {
        return None;
    }
    let buy_cost = buy_ask * buy_cost_multiplier;
    let sell_proceeds = sell_bid * sell_proceeds_multiplier;
    Some((sell_proceeds - buy_cost) / buy_cost * Decimal::from(10_000))
}

fn log_opportunity(opportunity: &Opportunity) {
    let OpportunityKind::CrossExchange {
        symbol,
        buy_venue,
        sell_venue,
    } = &opportunity.kind
    else {
        return;
    };
    info!(
        "[{}] {} buy={} sell={} profit_bps={} detail={}",
        opportunity.strategy, symbol, buy_venue, sell_venue, opportunity.expected_profit_bps, opportunity.detail
    );
}

impl Strategy for CrossExchangeStrategy {
    fn name(&self) -> &str {
        "cross_exchange"
    }

    fn subscriptions(&self) -> Vec<Topic> {
        self.fees
            .keys()
            .flat_map(|venue| {
                self.symbols
                    .iter()
                    .map(move |symbol| Topic::quote(venue.clone(), symbol.clone()))
            })
            .collect()
    }

    fn bus(&self) -> &Arc<TopicBus> {
        &self.bus
    }

    fn on_quote(&self, topic: &Topic, quote: &Quote) {
        let (venue, symbol) = match topic {
            Topic::Quote { venue, symbol } => (venue, symbol),
            _ => return,
        };
        let mut latest = self.latest.lock().unwrap();
        let symbol_quotes = latest.entry(symbol.clone()).or_default();
        symbol_quotes.insert(venue.clone(), *quote);

        let mut found = Vec::new();
        for (buy_venue, buy_quote) in symbol_quotes.iter() {
            if buy_quote.ask <= Decimal::ZERO {
                continue;
            }
            let buy_fee = self.fee_for(buy_venue);

            for (sell_venue, sell_quote) in symbol_quotes.iter() {
                if buy_venue == sell_venue {
                    continue;
                }
                let sell_fee = self.fee_for(sell_venue);

                if !self.health.is_healthy(buy_venue) || !self.health.is_healthy(sell_venue) {
                    continue;
                }

                let Some(profit_bps) = compute_profit_bps(buy_quote.ask, buy_fee.buy_multiplier(), sell_quote.bid, sell_fee.sell_multiplier()) else {
                    continue;
                };

                if profit_bps < self.min_profit_bps {
                    continue;
                }

                let buy_cost = buy_quote.ask * buy_fee.buy_multiplier();
                let sell_proceeds = sell_quote.bid * sell_fee.sell_multiplier();
                found.push(Opportunity {
                    strategy: "cross_exchange",
                    kind: OpportunityKind::CrossExchange {
                        symbol: symbol.clone(),
                        buy_venue: buy_venue.clone(),
                        sell_venue: sell_venue.clone(),
                    },
                    expected_profit_bps: profit_bps,
                    detail: format!(
                        "buy {symbol} on {buy_venue} @ {buy_ask} (cost {buy_cost}), sell on {sell_venue} @ {sell_bid} (proceeds {sell_proceeds})",
                        buy_ask = buy_quote.ask,
                        sell_bid = sell_quote.bid,
                    ),
                    ts_ms: quote.ts_ms,
                });
            }
        }

        // Kraken 挂单判断需要 kraken/binance 两个 venue 各自最新的 quote，
        // 克隆出来后再释放 `latest` 的锁——`evaluate_kraken_resting_order`
        // 内部还要拿 `pending_kraken_orders` 的锁、可能调用
        // `submit_limit_order`/`cancel_order`，不能嵌套持有 `latest` 的锁。
        let execution_quotes = self.execution.as_ref().and_then(|execution| {
            let kraken_quote = symbol_quotes.get(&execution.kraken_venue).copied()?;
            let binance_quote = symbol_quotes.get(&execution.binance_venue).copied()?;
            Some((kraken_quote, binance_quote))
        });
        drop(latest);

        for opportunity in &found {
            log_opportunity(opportunity);
        }

        if let (Some(execution), Some((kraken_quote, binance_quote))) = (&self.execution, execution_quotes) {
            self.evaluate_kraken_resting_order(execution, symbol, kraken_quote, binance_quote);
        }
    }

    /// 订单事件回调：kraken 挂单、binance 对冲单都靠这里驱动，两张登记表
    /// （`pending_kraken_orders`/`pending_binance_orders`）按 client_order_id
    /// 互斥，不属于自己的事件直接忽略。`PartiallyFilled` 不在终态集合里——
    /// kraken 挂单部分成交后仍然挂在盘口上，不能当终态处理（会漏掉后续继续
    /// 成交的部分）；binance 对冲单是市价单，`PartiallyFilled` 同样只是过程态。
    fn on_order_event(&self, event: &OrderEvent) {
        let Some(execution) = self.execution.clone() else { return };

        if !matches!(
            event,
            OrderEvent::Filled { .. } | OrderEvent::Cancelled { .. } | OrderEvent::RejectedByExchange { .. } | OrderEvent::RejectedByRisk { .. }
        ) {
            return;
        }

        let Some(client_order_id) = event.client_order_id() else { return };

        if self.pending_kraken_orders.lock().unwrap().contains_key(client_order_id) {
            self.handle_kraken_leg_event(&execution, client_order_id, event);
            return;
        }

        if let Some(leg) = self.pending_binance_orders.lock().unwrap().remove(client_order_id) {
            self.handle_binance_leg_event(leg, event);
        }
    }
}

impl CrossExchangeStrategy {
    /// 每次收到新报价后，重新评估 kraken/binance 这一对 quote 是否应该
    /// 挂单/撤单/维持现状。撤单条件用 OR：不再是最优盘口、或扣费后价差不再
    /// 满足 `min_profit_bps`，任一满足就撤单（比 AND 更保守，换来更高的
    /// 撤单频率）。
    fn evaluate_kraken_resting_order(&self, execution: &Arc<CrossExecutionConfig>, symbol: &Symbol, kraken_quote: Quote, binance_quote: Quote) {
        if !self.health.is_healthy(&execution.kraken_venue) || !self.health.is_healthy(&execution.binance_venue) {
            return;
        }

        let kraken_fee = self.fee_for(&execution.kraken_venue);
        let binance_fee = self.fee_for(&execution.binance_venue);

        // kraken 挂在买一吃 binance 卖出对冲、kraken 挂在卖一吃 binance 买入
        // 对冲，两个方向分别算一次挂单侧收益；正常行情下至多一个方向达标。
        let buy_kraken_bps = compute_profit_bps(kraken_quote.ask, kraken_fee.maker_buy_multiplier(), binance_quote.bid, binance_fee.sell_multiplier());
        let sell_kraken_bps = compute_profit_bps(binance_quote.ask, binance_fee.buy_multiplier(), kraken_quote.bid, kraken_fee.maker_sell_multiplier());

        let raw_target = match buy_kraken_bps {
            Some(bps) if bps >= self.min_profit_bps => Some((OrderSide::Buy, kraken_quote.ask)),
            _ => match sell_kraken_bps {
                Some(bps) if bps >= self.min_profit_bps => Some((OrderSide::Sell, kraken_quote.bid)),
                _ => None,
            },
        };

        let target = match raw_target {
            Some((side, ref_price)) => match execution.kraken_precision.round_price(symbol, ref_price) {
                Ok(price) => Some((side, price)),
                Err(err) => {
                    error!("cross_exchange: failed to round kraken resting price for symbol={symbol}: {err:#}");
                    None
                }
            },
            None => None,
        };

        let existing = {
            let pending = self.pending_kraken_orders.lock().unwrap();
            pending
                .iter()
                .find(|(_, leg)| &leg.symbol == symbol)
                .map(|(id, leg)| (id.clone(), leg.kraken_side, leg.price, leg.cancel_requested))
        };

        match existing {
            None => {
                if let Some((side, price)) = target {
                    self.submit_kraken_maker_order(execution, symbol.clone(), side, price);
                }
            }
            Some((client_order_id, existing_side, existing_price, cancel_requested)) => {
                if cancel_requested || target == Some((existing_side, existing_price)) {
                    return;
                }
                if let Some(leg) = self.pending_kraken_orders.lock().unwrap().get_mut(&client_order_id) {
                    leg.cancel_requested = true;
                }
                info!(
                    "cross_exchange: kraken resting order client_order_id={client_order_id} symbol={symbol} side={existing_side:?} price={existing_price} \
                     no longer at best price or profit below threshold, cancelling"
                );
                self.cancel_order(execution.kraken_trade_venue.clone(), client_order_id, None, None);
            }
        }
    }

    /// kraken 挂单命中终态后的处理。`RejectedByExchange` 分支是核心正确性
    /// 设计：该事件同时可能是"下单被拒"或"撤单尝试失败(订单可能仍存活)"，
    /// 必须查 `order_manager.get_order` 的权威状态才能确认——只有订单当前
    /// 状态真的是终态时才移除记录/对冲，否则保留记录、重置 `cancel_requested`
    /// 允许下次 `on_quote` 重试撤单，避免后续真正的 Filled/Cancelled 事件到达
    /// 时表里已经找不到记录，漏掉对冲、造成孤儿仓位。
    fn handle_kraken_leg_event(&self, execution: &Arc<CrossExecutionConfig>, client_order_id: &str, event: &OrderEvent) {
        let order_id = event.order_id();

        match event {
            OrderEvent::Filled { filled_qty, avg_price, .. } | OrderEvent::Cancelled { filled_qty, avg_price, .. } => {
                if let Some(leg) = self.pending_kraken_orders.lock().unwrap().remove(client_order_id) {
                    self.settle_kraken_leg(execution, leg, order_id.clone(), *filled_qty, *avg_price);
                }
            }
            OrderEvent::RejectedByRisk { reason, .. } => {
                if let Some(leg) = self.pending_kraken_orders.lock().unwrap().remove(client_order_id) {
                    warn!("cross_exchange: kraken resting order_id={order_id} symbol={} rejected by risk: {reason}", leg.symbol);
                }
            }
            OrderEvent::RejectedByExchange { reason, .. } => {
                let Some(order) = execution.order_manager.get_order(order_id) else {
                    warn!("cross_exchange: kraken order_id={order_id} not found in order manager after rejected_by_exchange (reason={reason}), dropping tracking");
                    self.pending_kraken_orders.lock().unwrap().remove(client_order_id);
                    return;
                };

                let is_terminal = matches!(
                    order.status,
                    OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Expired
                );

                if is_terminal {
                    if let Some(leg) = self.pending_kraken_orders.lock().unwrap().remove(client_order_id) {
                        self.settle_kraken_leg(execution, leg, order_id.clone(), order.filled_qty, order.avg_price.unwrap_or(Decimal::ZERO));
                    }
                } else {
                    if let Some(leg) = self.pending_kraken_orders.lock().unwrap().get_mut(client_order_id) {
                        leg.cancel_requested = false;
                    }
                    error!(
                        "cross_exchange: kraken order_id={order_id} cancel attempt rejected by exchange (reason={reason}) but order status is still \
                         {:?}, keeping tracking and will retry cancel on next quote",
                        order.status
                    );
                }
            }
            _ => unreachable!("filtered to terminal variants above"),
        }
    }

    /// `handle_kraken_leg_event` 确认拿到权威终态后的收尾：成交量够了就同步
    /// 下 binance 对冲单，否则记日志说明机会消失、无需对冲。
    fn settle_kraken_leg(&self, execution: &Arc<CrossExecutionConfig>, leg: PendingKrakenLeg, order_id: OrderId, filled_qty: Decimal, avg_price: Decimal) {
        if filled_qty <= Decimal::ZERO {
            info!("cross_exchange: kraken resting order_id={order_id} symbol={} filled_qty=0, opportunity vanished, skip hedge", leg.symbol);
            return;
        }

        info!(
            "cross_exchange: kraken resting order_id={order_id} symbol={} filled_qty={filled_qty} avg_price={avg_price}, hedging on binance",
            leg.symbol
        );

        self.submit_binance_hedge(execution, leg.symbol, leg.kraken_side, order_id, filled_qty);
    }

    /// binance 对冲单命中终态后的处理：只负责记日志——同一 symbol 同一时刻
    /// 最多一笔在途 kraken 挂单（见 `pending_kraken_orders` 上的注释），
    /// binance 对冲单的并发量上限交给 `RiskService` 的
    /// `max_position`/`max_orders_per_window` 兜底。
    fn handle_binance_leg_event(&self, leg: PendingBinanceLeg, event: &OrderEvent) {
        let hedge_order_id = event.order_id();
        match event {
            OrderEvent::Filled { filled_qty, avg_price, .. } => {
                info!(
                    "cross_exchange: binance hedge order_id={hedge_order_id} symbol={} filled_qty={filled_qty} avg_price={avg_price}",
                    leg.symbol
                );
            }
            OrderEvent::Cancelled { filled_qty, avg_price, .. } => {
                error!(
                    "cross_exchange: kraken order_id={} filled_qty={} but binance hedge order_id={hedge_order_id} was cancelled (filled_qty={filled_qty} avg_price={avg_price}), \
                     position may be imbalanced and needs manual check",
                    leg.kraken_order_id, leg.kraken_filled_qty
                );
            }
            OrderEvent::RejectedByRisk { reason, .. } => {
                error!(
                    "cross_exchange: kraken order_id={} filled_qty={} but binance hedge order_id={hedge_order_id} rejected by risk: {reason}, \
                     position is now imbalanced and needs manual check",
                    leg.kraken_order_id, leg.kraken_filled_qty
                );
            }
            OrderEvent::RejectedByExchange { reason, .. } => {
                error!(
                    "cross_exchange: kraken order_id={} filled_qty={} but binance hedge order_id={hedge_order_id} rejected by exchange: {reason}, \
                     position is now imbalanced and needs manual check",
                    leg.kraken_order_id, leg.kraken_filled_qty
                );
            }
            _ => unreachable!("on_order_event only forwards Filled/Cancelled/RejectedByRisk/RejectedByExchange here"),
        }
    }

    /// 挂 kraken GTC 限价单：检查同 symbol 是否已有在途挂单、登记进
    /// `pending_kraken_orders`、发布下单请求，全程同步操作，不需要
    /// `.await`，所以不用 spawn task。`price` 由调用方（`evaluate_kraken_resting_order`
    /// 或测试）传入，已经过精度取整。
    fn submit_kraken_maker_order(&self, execution: &Arc<CrossExecutionConfig>, symbol: Symbol, kraken_side: OrderSide, price: Decimal) {
        let Some(&qty) = execution.order_qty_by_symbol.get(&symbol) else {
            warn!("cross_exchange: no preloaded order qty for symbol={symbol}, skip");
            return;
        };

        let mut pending = self.pending_kraken_orders.lock().unwrap();
        let already_in_flight = pending.values().any(|leg| leg.symbol == symbol);
        if already_in_flight {
            info!("cross_exchange: kraken resting order symbol={symbol} already has an in-flight order, skip");
            return;
        }

        let client_order_id = generate_client_order_id("kraken");
        pending.insert(
            client_order_id.clone(),
            PendingKrakenLeg {
                symbol: symbol.clone(),
                kraken_side,
                price,
                cancel_requested: false,
            },
        );
        drop(pending);

        self.submit_limit_order(execution.kraken_trade_venue.clone(), symbol, kraken_side, qty, price, Some(client_order_id), None, None);
    }

    /// kraken 挂单成交后的对冲：在 Binance Spot 下市价单对冲。跟
    /// `submit_kraken_maker_order` 是同一套模式——算精度取整量、登记进
    /// `pending_binance_orders`、发布下单请求，全程同步不需要 `.await`，
    /// 不用 spawn task。任何一步失败只记日志、不重试/不回滚——与
    /// `manual.rs` 现有的失败处理哲学一致，失衡了需要人工介入。
    fn submit_binance_hedge(
        &self,
        execution: &Arc<CrossExecutionConfig>,
        symbol: Symbol,
        kraken_side: OrderSide,
        kraken_order_id: OrderId,
        kraken_filled_qty: Decimal,
    ) {
        let hedge_qty = match execution.binance_precision.round_qty(&symbol, PrecisionKind::Market, kraken_filled_qty) {
            Ok(q) => q,
            Err(err) => {
                error!(
                    "cross_exchange: kraken order_id={kraken_order_id} filled_qty={kraken_filled_qty} for symbol={symbol} cannot be rounded to a valid binance market qty ({err:#}), \
                     position is now imbalanced and needs manual hedge"
                );
                return;
            }
        };

        let hedge_side = match kraken_side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };

        let hedge_client_order_id = generate_client_order_id("binance");
        self.pending_binance_orders.lock().unwrap().insert(
            hedge_client_order_id.clone(),
            PendingBinanceLeg {
                symbol: symbol.clone(),
                kraken_order_id,
                kraken_filled_qty,
            },
        );

        self.submit_order(
            execution.binance_trade_venue.clone(),
            symbol,
            hedge_side,
            OrderAmount::Base(hedge_qty),
            Some(hedge_client_order_id),
            None,
            None,
        );
    }
}

/// kraken 的 `cl_ord_id` 只接受 32 位 UUID 或 ≤18 个 ASCII 字符的自由文本，
/// 超出格式会被直接拒单（`EGeneral:Invalid arguments:cl_ord_id`）——之前带
/// `{strategy_name}-{leg}-` 前缀的版本超长，所以这里舍弃可读前缀，只留 leg
/// 标记 + 十六进制时间戳 + 随机数，保证两条腿在各自 pending 期内不重复即可。
fn generate_client_order_id(leg: &str) -> String {
    let leg_tag = match leg {
        "kraken" => "k",
        "binance" => "b",
        other => other,
    };
    format!("x{leg_tag}{:x}{:03x}", now_ms() & 0xFFFF_FFFF, rand::random::<u16>() & 0xFFF)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures_util::StreamExt;

    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::exchange_info::types::{MarketPrecision, QtyPrecision};
    use crate::order::OrderProvider;
    use crate::order::types::{LimitOrderRequest, MarketOrderRequest, OrderResult, OrderStatus};
    use crate::order_manager::types::Order;
    use crate::order_manager::{ExchangeAdapter, ExchangeOrderUpdate, ExecutionService, InMemoryOrderIdAllocator, InMemoryOrderStore, RiskLimits, RiskService};
    use crate::position::{InMemoryPositionStore, PositionManager};

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote {
            bid: bid.parse().unwrap(),
            bid_size: Decimal::ONE,
            ask: ask.parse().unwrap(),
            ask_size: Decimal::ONE,
            ts_ms: 1,
        }
    }

    fn fees_for(venues: &[&Venue]) -> HashMap<Venue, FeeSchedule> {
        venues.iter().map(|v| ((*v).clone(), FeeSchedule::new(0))).collect()
    }

    #[test]
    fn subscriptions_only_cover_configured_venues_and_symbols() {
        let watched = Symbol::new("BTC", "USDT");
        let venue_a = Venue::new("a");
        let strategy = CrossExchangeStrategy::new(
            vec![watched.clone()],
            fees_for(&[&venue_a]),
            Decimal::ZERO,
            Arc::new(LinkHealthMonitor::always_healthy()),
            Arc::new(TopicBus::new()),
        );

        let subs = strategy.subscriptions();
        assert_eq!(subs, vec![Topic::quote(venue_a, watched)]);
    }

    #[test]
    fn detects_cross_exchange_opportunity_above_threshold() {
        let symbol = Symbol::new("BTC", "USDT");
        let venue_a = Venue::new("a");
        let venue_b = Venue::new("b");
        let strategy = CrossExchangeStrategy::new(
            vec![symbol.clone()],
            fees_for(&[&venue_a, &venue_b]),
            Decimal::from(1),
            Arc::new(LinkHealthMonitor::always_healthy()),
            Arc::new(TopicBus::new()),
        );

        strategy.on_quote(&Topic::quote(venue_a.clone(), symbol.clone()), &quote("100.0", "100.5"));
        strategy.on_quote(&Topic::quote(venue_b.clone(), symbol.clone()), &quote("102.0", "102.5"));

        let latest = strategy.latest.lock().unwrap();
        let symbol_quotes = &latest[&symbol];
        let profit_bps = compute_profit_bps(
            symbol_quotes[&venue_a].ask,
            strategy.fee_for(&venue_a).buy_multiplier(),
            symbol_quotes[&venue_b].bid,
            strategy.fee_for(&venue_b).sell_multiplier(),
        )
        .unwrap();
        assert!(profit_bps > Decimal::from(1));
    }

    #[test]
    fn no_opportunity_when_spread_below_threshold() {
        let symbol = Symbol::new("BTC", "USDT");
        let venue_a = Venue::new("a");
        let venue_b = Venue::new("b");
        let strategy = CrossExchangeStrategy::new(
            vec![symbol.clone()],
            fees_for(&[&venue_a, &venue_b]),
            Decimal::from(50),
            Arc::new(LinkHealthMonitor::always_healthy()),
            Arc::new(TopicBus::new()),
        );

        strategy.on_quote(&Topic::quote(venue_a.clone(), symbol.clone()), &quote("100.0", "100.1"));
        strategy.on_quote(&Topic::quote(venue_b.clone(), symbol.clone()), &quote("100.05", "100.15"));

        let latest = strategy.latest.lock().unwrap();
        let symbol_quotes = &latest[&symbol];
        let profit_bps = compute_profit_bps(
            symbol_quotes[&venue_a].ask,
            strategy.fee_for(&venue_a).buy_multiplier(),
            symbol_quotes[&venue_b].bid,
            strategy.fee_for(&venue_b).sell_multiplier(),
        )
        .unwrap();
        assert!(profit_bps < Decimal::from(50));
    }

    // ---- try_execute 执行链路测试 ----
    //
    // 假 provider 的下单响应固定是 status=New/filled_qty=0（和真实交易所一致：
    // REST 响应不代表成交状态），成交由测试通过 `push_exchange_update` 模拟
    // 私有 WS 推送驱动，与 `manual.rs` 测试里的 `drive_fill` 是同一个模式。

    fn btc_usdt() -> Symbol {
        Symbol::new("BTC", "USDT")
    }

    fn precision_cache(symbol: &Symbol, qty_step: &str, min_qty: &str, price_tick: &str) -> PrecisionCache {
        let precision = QtyPrecision {
            qty_step: qty_step.parse().unwrap(),
            min_qty: min_qty.parse().unwrap(),
        };
        PrecisionCache::from_precisions(vec![MarketPrecision {
            symbol: symbol.clone(),
            market: precision,
            limit: precision,
            price_tick: price_tick.parse().unwrap(),
        }])
    }

    /// 记录每次真实下单/撤单调用，下单响应固定 status=New、filled_qty=0——
    /// 成交必须由测试驱动 WS 推送模拟（`push_exchange_update`），和真实交易所
    /// 一致。撤单是否报错由 `cancel_should_fail` 控制，用来模拟"撤单请求本身
    /// 失败，但订单可能仍然存活在交易所"的场景。
    #[derive(Clone)]
    struct FakeProviderHandles {
        market_calls: Arc<Mutex<Vec<(OrderSide, Decimal)>>>,
        limit_calls: Arc<Mutex<Vec<(OrderSide, Decimal, Decimal)>>>,
        cancel_calls: Arc<Mutex<Vec<String>>>,
        cancel_should_fail: Arc<AtomicBool>,
    }

    struct FakeExchangeProvider {
        venue: Venue,
        handles: FakeProviderHandles,
    }

    fn fake_provider(venue: Venue) -> (Arc<dyn OrderProvider>, FakeProviderHandles) {
        let handles = FakeProviderHandles {
            market_calls: Arc::new(Mutex::new(Vec::new())),
            limit_calls: Arc::new(Mutex::new(Vec::new())),
            cancel_calls: Arc::new(Mutex::new(Vec::new())),
            cancel_should_fail: Arc::new(AtomicBool::new(false)),
        };
        let provider: Arc<dyn OrderProvider> = Arc::new(FakeExchangeProvider { venue, handles: handles.clone() });
        (provider, handles)
    }

    #[async_trait]
    impl OrderProvider for FakeExchangeProvider {
        fn venue(&self) -> Venue {
            self.venue.clone()
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            let OrderAmount::Base(qty) = req.amount else {
                unreachable!("hedge leg only uses OrderAmount::Base")
            };
            self.handles.market_calls.lock().unwrap().push((req.side, qty));
            Ok(OrderResult {
                order_id: format!("{}-{}", self.venue, req.symbol),
                status: OrderStatus::New,
                filled_qty: Decimal::ZERO,
                avg_price: None,
                fee: None,
                fee_asset: None,
            })
        }
        async fn place_limit_order_raw(&self, req: &LimitOrderRequest) -> anyhow::Result<OrderResult> {
            self.handles.limit_calls.lock().unwrap().push((req.side, req.quantity, req.price));
            Ok(OrderResult {
                order_id: format!("{}-{}", self.venue, req.symbol),
                status: OrderStatus::New,
                filled_qty: Decimal::ZERO,
                avg_price: None,
                fee: None,
                fee_asset: None,
            })
        }
        async fn cancel_order(&self, _symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<()> {
            self.handles.cancel_calls.lock().unwrap().push(exchange_order_id.to_string());
            if self.handles.cancel_should_fail.load(Ordering::SeqCst) {
                anyhow::bail!("simulated exchange cancel failure");
            }
            Ok(())
        }
    }

    /// 内存版全套依赖：`TopicBus` + `RiskService` + `ExecutionService` +
    /// `OrderManager`，和 `manual.rs::setup_live_env` 是同一个模式。
    struct TestEnv {
        bus: Arc<TopicBus>,
        order_manager: Arc<OrderManager>,
        _risk_handle: tokio::task::JoinHandle<()>,
        _execution_handle: tokio::task::JoinHandle<()>,
        _cancel_handle: tokio::task::JoinHandle<()>,
    }

    /// 建好环境后先等一小会儿：`RiskService`/`ExecutionService` 的后台任务
    /// 通过 broadcast channel 订阅 `TopicBus`，如果调用方在它们真正跑到
    /// `subscribe()` 之前就发布订单请求，这条消息会直接丢失（broadcast 语义，
    /// 不缓冲订阅前的消息）——和 `manual.rs::setup_live_env` 调用点里的
    /// `tokio::time::sleep(10ms)` 是同一个原因。
    async fn setup_env(providers: Vec<Arc<dyn OrderProvider>>, symbol: Symbol) -> TestEnv {
        let mut risk_limits = HashMap::new();
        let mut adapters = HashMap::new();
        for provider in &providers {
            let venue = provider.venue();
            risk_limits.insert(
                (venue.clone(), symbol.clone()),
                RiskLimits {
                    max_order_amount: Decimal::MAX,
                    max_position: Decimal::MAX,
                    max_orders_per_window: 100,
                },
            );
            adapters.insert(venue.clone(), Arc::new(ExchangeAdapter::new(venue, provider.clone())));
        }

        let bus = Arc::new(TopicBus::new());
        let position_manager = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let order_store = Arc::new(InMemoryOrderStore::new());
        let order_id_allocator = Arc::new(InMemoryOrderIdAllocator::new());

        let risk_service = Arc::new(RiskService::new(
            bus.clone(),
            order_id_allocator,
            order_store.clone(),
            risk_limits,
            position_manager.clone(),
        ));
        let execution_service = Arc::new(ExecutionService::new(bus.clone(), adapters, order_store.clone()));
        let order_manager = Arc::new(OrderManager::new(bus.clone(), position_manager.clone(), order_store, None));

        let risk_handle = risk_service.clone().start();
        let execution_handle = execution_service.clone().start();
        let cancel_handle = execution_service.clone().start_cancel_listener();
        tokio::time::sleep(Duration::from_millis(10)).await;

        TestEnv {
            bus,
            order_manager,
            _risk_handle: risk_handle,
            _execution_handle: execution_handle,
            _cancel_handle: cancel_handle,
        }
    }

    /// 轮询直到某个 order_id 在 order_manager 里的记录已经拿到
    /// `exchange_order_id`——撤单需要这个字段（`ExecutionService::handle_cancel_request`
    /// 没有它会直接拒绝），测试驱动撤单前必须先等它落地。
    async fn poll_until_exchange_order_id(order_manager: &OrderManager, order_id: &OrderId) -> Order {
        for _ in 0..500 {
            if let Some(order) = order_manager.get_order(order_id) {
                if order.exchange_order_id.is_some() {
                    return order;
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("order_id={order_id} never got an exchange_order_id");
    }

    /// 轮询直到某个条件满足——用于等待 `OrderEvent` 经由 bus 异步派发到
    /// `on_order_event` 之后，策略内部状态（如 `pending_kraken_orders`）发生
    /// 预期的变化。
    async fn poll_until(mut cond: impl FnMut() -> bool, what: &str) {
        for _ in 0..500 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("condition not met within timeout: {what}");
    }

    /// 轮询直到 order_manager 里出现 client_order_id 带指定前缀的订单——
    /// try_execute 内部随机生成 client_order_id，测试没法提前知道完整值，
    /// 只知道 "xk"/"xb"（kraken/binance 两条腿）前缀。
    async fn poll_order_by_prefix(order_manager: &OrderManager, prefix: &str) -> Order {
        for _ in 0..500 {
            if let Some(order) = order_manager
                .all_orders()
                .into_iter()
                .find(|o| o.request.client_order_id().map(|id| id.starts_with(prefix)).unwrap_or(false))
            {
                return order;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("no order with client_order_id prefix={prefix} was ever submitted to the order manager");
    }

    /// 模拟交易所私有 WS 推送一次订单状态更新。
    async fn push_exchange_update(
        order_manager: &OrderManager,
        venue: &Venue,
        order: &Order,
        status: OrderStatus,
        filled_qty: Decimal,
        avg_price: Decimal,
    ) {
        let symbol = order.request.as_trade().unwrap().symbol.clone();
        order_manager
            .handle_exchange_update(ExchangeOrderUpdate {
                venue: venue.clone(),
                symbol: Some(symbol),
                client_order_id: order.request.client_order_id().map(|s| s.to_string()),
                exchange_order_id: order.exchange_order_id.clone(),
                status,
                filled_qty,
                avg_price: Some(avg_price),
                fee: None,
                fee_asset: None,
                ts_ms: 1,
            })
            .await;
    }

    /// 构造带 `execution` 的 `CrossExchangeStrategy`，并 spawn 一个模拟
    /// `ArbitrageEngine` 的常驻任务：订阅该策略名下的 `OrderEvent`，收到就转发
    /// 给 `on_order_event`——和生产环境里 `engine.rs::ArbitrageEngine::run`
    /// 实际做的事一样。测试驱动 kraken/binance 成交靠已有的
    /// `poll_order_by_prefix` + `push_exchange_update`，它们本身带轮询重试，
    /// 天然兼容 on_order_event 是异步派发这件事,不需要额外 sleep。
    fn build_strategy(env: &TestEnv, symbol: &Symbol, execution: Arc<CrossExecutionConfig>) -> Arc<CrossExchangeStrategy> {
        let strategy = Arc::new(
            CrossExchangeStrategy::new(
                vec![symbol.clone()],
                HashMap::new(),
                Decimal::ZERO,
                Arc::new(LinkHealthMonitor::always_healthy()),
                env.bus.clone(),
            )
            .with_execution(execution),
        );

        let dispatcher_strategy = strategy.clone();
        let mut events = env.bus.subscribe::<OrderEvent>(Topic::order_event("cross_exchange"));
        tokio::spawn(async move {
            while let Some((_, event)) = events.next().await {
                dispatcher_strategy.on_order_event(&event);
            }
        });

        strategy
    }

    fn test_execution_config(
        env: &TestEnv,
        symbol: &Symbol,
        kraken_venue: Venue,
        binance_venue: Venue,
        qty: Decimal,
    ) -> Arc<CrossExecutionConfig> {
        Arc::new(CrossExecutionConfig {
            kraken_venue: kraken_venue.clone(),
            kraken_trade_venue: kraken_venue,
            binance_venue: binance_venue.clone(),
            binance_trade_venue: binance_venue,
            kraken_precision: Arc::new(precision_cache(symbol, "0.001", "0.001", "0.01")),
            binance_precision: Arc::new(precision_cache(symbol, "0.001", "0.001", "0.01")),
            order_manager: env.order_manager.clone(),
            order_qty_by_symbol: HashMap::from([(symbol.clone(), qty)]),
            ioc_wait_timeout: Duration::from_millis(500),
            hedge_wait_timeout: Duration::from_millis(500),
        })
    }

    #[tokio::test]
    async fn kraken_gtc_full_fill_triggers_binance_hedge() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let (kraken_provider, kraken_handles) = fake_provider(kraken_venue.clone());
        let (binance_provider, binance_handles) = fake_provider(binance_venue.clone());

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue.clone(), qty);
        let strategy = build_strategy(&env, &symbol, execution.clone());

        let order_manager = env.order_manager.clone();
        let driver = tokio::spawn(async move {
            let kraken_order = poll_order_by_prefix(&order_manager, "xk").await;
            push_exchange_update(&order_manager, &kraken_venue, &kraken_order, OrderStatus::Filled, qty, Decimal::from(100)).await;

            let binance_order = poll_order_by_prefix(&order_manager, "xb").await;
            push_exchange_update(&order_manager, &binance_venue, &binance_order, OrderStatus::Filled, qty, Decimal::from(100)).await;
        });

        strategy.submit_kraken_maker_order(&execution, symbol, OrderSide::Buy, Decimal::from(100));
        driver.await.unwrap();

        assert_eq!(kraken_handles.limit_calls.lock().unwrap().len(), 1);
        let hedge_calls = binance_handles.market_calls.lock().unwrap();
        assert_eq!(hedge_calls.len(), 1, "kraken 完全成交后应该触发一次 binance 对冲");
        assert_eq!(hedge_calls[0], (OrderSide::Sell, qty), "kraken 买入后应该在 binance 卖出对冲，数量按实际成交量");
    }

    #[tokio::test]
    async fn kraken_gtc_cancelled_zero_fill_skips_hedge() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let (kraken_provider, kraken_handles) = fake_provider(kraken_venue.clone());
        let (binance_provider, binance_handles) = fake_provider(binance_venue.clone());

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue, qty);
        let strategy = build_strategy(&env, &symbol, execution.clone());

        let order_manager = env.order_manager.clone();
        let driver = tokio::spawn(async move {
            let kraken_order = poll_order_by_prefix(&order_manager, "xk").await;
            // 挂单还没成交就被撤销：机会消失，撤单确认后 filled_qty=0。
            push_exchange_update(&order_manager, &kraken_venue, &kraken_order, OrderStatus::Cancelled, Decimal::ZERO, Decimal::ZERO).await;
        });

        strategy.submit_kraken_maker_order(&execution, symbol, OrderSide::Buy, Decimal::from(100));
        driver.await.unwrap();

        assert_eq!(kraken_handles.limit_calls.lock().unwrap().len(), 1);
        assert!(binance_handles.market_calls.lock().unwrap().is_empty(), "撤单时 0 成交不应该下对冲单");
    }

    #[tokio::test]
    async fn kraken_gtc_cancelled_partial_fill_hedges_actual_filled_qty() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let (kraken_provider, kraken_handles) = fake_provider(kraken_venue.clone());
        let (binance_provider, binance_handles) = fake_provider(binance_venue.clone());

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue.clone(), qty);
        let strategy = build_strategy(&env, &symbol, execution.clone());

        let partial_fill = Decimal::new(6, 1); // 0.6

        let order_manager = env.order_manager.clone();
        let driver = tokio::spawn(async move {
            let kraken_order = poll_order_by_prefix(&order_manager, "xk").await;
            // 部分成交后撤单：Cancelled 事件自带准确的累计成交量。
            push_exchange_update(&order_manager, &kraken_venue, &kraken_order, OrderStatus::Cancelled, partial_fill, Decimal::from(100)).await;

            let binance_order = poll_order_by_prefix(&order_manager, "xb").await;
            push_exchange_update(&order_manager, &binance_venue, &binance_order, OrderStatus::Filled, partial_fill, Decimal::from(100)).await;
        });

        strategy.submit_kraken_maker_order(&execution, symbol, OrderSide::Sell, Decimal::from(100));
        driver.await.unwrap();

        assert_eq!(kraken_handles.limit_calls.lock().unwrap().len(), 1);
        let hedge_calls = binance_handles.market_calls.lock().unwrap();
        assert_eq!(hedge_calls.len(), 1, "部分成交后撤单也应该按实际成交量触发一次对冲");
        assert_eq!(hedge_calls[0], (OrderSide::Buy, partial_fill), "kraken 卖出部分成交后应该在 binance 买入对冲，数量是实际成交的 0.6 而不是下单量 1");
    }

    #[tokio::test]
    async fn kraken_gtc_partially_filled_event_is_not_terminal_then_later_fill_hedges_full_qty() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let (kraken_provider, kraken_handles) = fake_provider(kraken_venue.clone());
        let (binance_provider, binance_handles) = fake_provider(binance_venue.clone());

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue.clone(), qty);
        let strategy = build_strategy(&env, &symbol, execution.clone());

        strategy.submit_kraken_maker_order(&execution, symbol.clone(), OrderSide::Buy, Decimal::from(100));
        let kraken_order = poll_order_by_prefix(&env.order_manager, "xk").await;
        let client_order_id = kraken_order.request.client_order_id().unwrap().to_string();

        let partial_fill = Decimal::new(4, 1); // 0.4
        push_exchange_update(&env.order_manager, &kraken_venue, &kraken_order, OrderStatus::PartiallyFilled, partial_fill, Decimal::from(100)).await;

        // 给 PartiallyFilled 事件的异步派发留出时间落地，确认它没有被当成
        // 终态处理——挂单记录还在、没有提前触发对冲。
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(binance_handles.market_calls.lock().unwrap().is_empty(), "PartiallyFilled 不应该触发对冲");
        assert!(
            strategy.pending_kraken_orders.lock().unwrap().contains_key(&client_order_id),
            "PartiallyFilled 不应该把挂单记录从跟踪表里移除"
        );

        let order_manager = env.order_manager.clone();
        let driver = tokio::spawn(async move {
            push_exchange_update(&order_manager, &kraken_venue, &kraken_order, OrderStatus::Filled, qty, Decimal::from(100)).await;

            let binance_order = poll_order_by_prefix(&order_manager, "xb").await;
            push_exchange_update(&order_manager, &binance_venue, &binance_order, OrderStatus::Filled, qty, Decimal::from(100)).await;
        });
        driver.await.unwrap();

        assert_eq!(kraken_handles.limit_calls.lock().unwrap().len(), 1);
        let hedge_calls = binance_handles.market_calls.lock().unwrap();
        assert_eq!(hedge_calls.len(), 1, "最终 Filled 到达后应该按累计成交量触发一次对冲");
        assert_eq!(hedge_calls[0], (OrderSide::Sell, qty), "kraken 买入后应该在 binance 卖出对冲，数量是累计成交量而不是中途的部分成交量");
    }

    #[tokio::test]
    async fn kraken_gtc_rejected_by_exchange_cancel_attempt_retried_when_order_still_alive() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let (kraken_provider, kraken_handles) = fake_provider(kraken_venue.clone());
        let (binance_provider, binance_handles) = fake_provider(binance_venue.clone());

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue.clone(), qty);
        let strategy = build_strategy(&env, &symbol, execution.clone());

        strategy.submit_kraken_maker_order(&execution, symbol.clone(), OrderSide::Buy, Decimal::from(100));
        let kraken_order = poll_order_by_prefix(&env.order_manager, "xk").await;
        let client_order_id = kraken_order.request.client_order_id().unwrap().to_string();
        poll_until_exchange_order_id(&env.order_manager, &kraken_order.order_id).await;

        // 模拟 `evaluate_kraken_resting_order` 已经判定需要撤单、标记了
        // cancel_requested=true，接下来撤单请求本身失败(交易所报错)，但订单
        // 在交易所侧其实还活着(status 仍是 New，没有任何 WS 终态推送)。
        strategy.pending_kraken_orders.lock().unwrap().get_mut(&client_order_id).unwrap().cancel_requested = true;
        kraken_handles.cancel_should_fail.store(true, Ordering::SeqCst);
        strategy.cancel_order(execution.kraken_trade_venue.clone(), client_order_id.clone(), None, None);

        poll_until(
            || {
                strategy
                    .pending_kraken_orders
                    .lock()
                    .unwrap()
                    .get(&client_order_id)
                    .map(|leg| !leg.cancel_requested)
                    .unwrap_or(false)
            },
            "cancel_requested reset to false after a failed cancel attempt on a still-live order",
        )
        .await;
        assert!(
            strategy.pending_kraken_orders.lock().unwrap().contains_key(&client_order_id),
            "撤单尝试失败但订单还活着时不应该把记录从跟踪表里移除"
        );

        // 证明重置后确实能重试并最终正确对冲：撤单不再失败，driver 推送真正
        // 的 Filled 终态。
        kraken_handles.cancel_should_fail.store(false, Ordering::SeqCst);
        let order_manager = env.order_manager.clone();
        let driver = tokio::spawn(async move {
            push_exchange_update(&order_manager, &kraken_venue, &kraken_order, OrderStatus::Filled, qty, Decimal::from(100)).await;

            let binance_order = poll_order_by_prefix(&order_manager, "xb").await;
            push_exchange_update(&order_manager, &binance_venue, &binance_order, OrderStatus::Filled, qty, Decimal::from(100)).await;
        });
        driver.await.unwrap();

        let hedge_calls = binance_handles.market_calls.lock().unwrap();
        assert_eq!(hedge_calls.len(), 1, "撤单失败重试后，最终的真实成交仍然应该正确触发对冲");
        assert_eq!(hedge_calls[0], (OrderSide::Sell, qty));
        assert_eq!(kraken_handles.cancel_calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn kraken_gtc_rejected_by_exchange_but_order_already_terminal_settles_using_authoritative_status() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let (kraken_provider, kraken_handles) = fake_provider(kraken_venue.clone());
        let (binance_provider, binance_handles) = fake_provider(binance_venue.clone());

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue.clone(), qty);
        // 这个用例需要精确控制 `on_order_event` 的调用时机，构造一个"撤单失败
        // 的 RejectedByExchange 比真正的终态事件先被处理"的乱序场景，所以不用
        // `build_strategy`（它会自动把 bus 上的 OrderEvent 转发给
        // `on_order_event`），改成手动直接调用。
        let strategy = Arc::new(
            CrossExchangeStrategy::new(
                vec![symbol.clone()],
                HashMap::new(),
                Decimal::ZERO,
                Arc::new(LinkHealthMonitor::always_healthy()),
                env.bus.clone(),
            )
            .with_execution(execution.clone()),
        );

        strategy.submit_kraken_maker_order(&execution, symbol.clone(), OrderSide::Buy, Decimal::from(100));
        let kraken_order = poll_order_by_prefix(&env.order_manager, "xk").await;
        let client_order_id = kraken_order.request.client_order_id().unwrap().to_string();

        // 订单在 order_manager 里已经真正到达终态 Filled（模拟 WS 推送先落地；
        // 这里没有 dispatcher 订阅 bus，所以 handle_exchange_update 内部发布的
        // 真实 Filled 事件不会被自动转发，不会干扰下面手动构造的乱序场景）。
        push_exchange_update(&env.order_manager, &kraken_venue, &kraken_order, OrderStatus::Filled, qty, Decimal::from(100)).await;

        // 模拟撤单请求的响应比 WS 推送更晚才回来、但先被派发到策略。
        let rejected = OrderEvent::RejectedByExchange {
            order_id: kraken_order.order_id.clone(),
            client_order_id: Some(client_order_id.clone()),
            reason: "cancel_order exchange error: too late, already filled".to_string(),
        };
        strategy.on_order_event(&rejected);

        // `submit_binance_hedge` 内部走 `self.submit_order` -> bus.publish ->
        // RiskService -> ExecutionService -> provider，是异步链路，`on_order_event`
        // 本身同步返回不代表对冲单已经落地，需要轮询等待。
        poll_until(
            || !binance_handles.market_calls.lock().unwrap().is_empty(),
            "binance hedge order reaching the fake provider",
        )
        .await;

        assert_eq!(kraken_handles.limit_calls.lock().unwrap().len(), 1);
        let hedge_calls = binance_handles.market_calls.lock().unwrap();
        assert_eq!(hedge_calls.len(), 1, "RejectedByExchange 到达时若订单其实已经是真正终态 Filled，应该按权威状态对冲");
        assert_eq!(hedge_calls[0], (OrderSide::Sell, qty));
        assert!(
            !strategy.pending_kraken_orders.lock().unwrap().contains_key(&client_order_id),
            "确认真实终态后应该移除跟踪记录"
        );
    }

    #[tokio::test]
    async fn submit_kraken_maker_order_skips_duplicate_in_flight_order_for_same_symbol() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let (kraken_provider, kraken_handles) = fake_provider(kraken_venue.clone());
        let (binance_provider, _binance_handles) = fake_provider(binance_venue.clone());

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue, qty);
        let strategy = build_strategy(&env, &symbol, execution.clone());

        // 第一笔挂单发出后还没拿到终态（不驱动任何 fill/cancel 事件），此时同
        // symbol 的后续调用——即使价格不同——也应该被跳过，不产生新的下单
        // 请求。这是这次改动里最关键的行为变化：旧版按 symbol+side+price 去重
        // （允许同 symbol 不同价位并发探路单），新版按 symbol 去重，因为同一
        // 时刻每个 symbol 只应该有一笔挂单在途（cancel-and-replace 语义）。
        strategy.submit_kraken_maker_order(&execution, symbol.clone(), OrderSide::Buy, Decimal::from(100));
        poll_order_by_prefix(&env.order_manager, "xk").await;

        strategy.submit_kraken_maker_order(&execution, symbol.clone(), OrderSide::Buy, Decimal::from(101));
        // 给第二次调用（如果它错误地真的下单了）留出时间落地。
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            kraken_handles.limit_calls.lock().unwrap().len(),
            1,
            "同一 symbol 已有在途挂单时不应该被重复下单，即使价格不同"
        );
    }

    /// 记录"价格决策"（`submit_kraken_maker_order` 被调用，等价于 `on_quote`
    /// 判定出套利机会那一刻）到 `OrderProvider::place_limit_order_raw` 被调用
    /// （生产环境里这一步就是真实 HTTP 请求发出前）之间的纯内部调度耗时：
    /// bus.publish -> RiskService -> bus.publish -> ExecutionService -> adapter.submit。
    /// 不连接任何真实交易所，只测代码路径本身的开销。
    #[tokio::test]
    async fn measures_internal_dispatch_latency_from_price_decision_to_provider() {
        use std::time::Instant;
        use tokio::sync::mpsc;

        let _ = env_logger::builder().filter_level(log::LevelFilter::Debug).is_test(true).try_init();

        struct TimestampingProvider {
            venue: Venue,
            tx: mpsc::UnboundedSender<Instant>,
        }

        #[async_trait]
        impl OrderProvider for TimestampingProvider {
            fn venue(&self) -> Venue {
                self.venue.clone()
            }
            async fn place_market_order_raw(&self, _req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
                unreachable!("benchmark only drives the kraken leg")
            }
            async fn place_limit_order_raw(&self, req: &LimitOrderRequest) -> anyhow::Result<OrderResult> {
                let _ = self.tx.send(Instant::now());
                Ok(OrderResult {
                    order_id: format!("{}-{}", self.venue, req.symbol),
                    status: OrderStatus::New,
                    filled_qty: Decimal::ZERO,
                    avg_price: None,
                    fee: None,
                    fee_asset: None,
                })
            }
        }

        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let (tx, mut rx) = mpsc::unbounded_channel();
        let kraken_provider: Arc<dyn OrderProvider> = Arc::new(TimestampingProvider { venue: kraken_venue.clone(), tx });
        let (binance_provider, _binance_handles) = fake_provider(binance_venue.clone());

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue, binance_venue, qty);
        let strategy = build_strategy(&env, &symbol, execution.clone());

        // `setup_env` 固定给每个 (venue, symbol) 配 `max_orders_per_window: 100`
        // 的风控限额，超过就会被 `RiskService` 拒单而不会走到 provider，所以这里
        // 迭代次数留在限额以内。
        const ITERATIONS: usize = 80;
        let mut samples = Vec::with_capacity(ITERATIONS);
        for i in 0..ITERATIONS {
            // 新设计按 symbol 去重（同一时刻每个 symbol 只应该有一笔挂单在
            // 途），这个基准只跑 `submit_kraken_maker_order`、从不驱动任何
            // 成交/撤单事件，挂单永远不会从 `pending_kraken_orders` 里自然
            // 移除，所以每次迭代手动清空一下，模拟"上一笔已经终结"，让每次
            // 调用都能真正走到 provider。
            strategy.pending_kraken_orders.lock().unwrap().clear();
            let price = Decimal::from(100) + Decimal::new(i as i64, 2);
            let start = Instant::now();
            strategy.submit_kraken_maker_order(&execution, symbol.clone(), OrderSide::Buy, price);
            let received_at = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("provider was not called within timeout")
                .expect("provider channel closed unexpectedly");
            samples.push(received_at.duration_since(start));
        }

        samples.sort();
        let sum: Duration = samples.iter().sum();
        let mean = sum / samples.len() as u32;
        let min = samples[0];
        let p50 = samples[samples.len() / 2];
        let p95 = samples[samples.len() * 95 / 100];
        let max = *samples.last().unwrap();

        println!(
            "internal dispatch latency (price decision -> OrderProvider::place_limit_order_raw), n={}: min={min:?} p50={p50:?} mean={mean:?} p95={p95:?} max={max:?}",
            samples.len(),
        );
    }
}
