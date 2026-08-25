use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use log::{error, info, warn};
use rust_decimal::Decimal;

use crate::exchange_info::PrecisionCache;
use crate::exchange_info::types::PrecisionKind;
use crate::market_data::link_health::LinkHealthMonitor;
use crate::market_data::now_ms;
use crate::order::types::{OrderAmount, OrderSide};
use crate::order_manager::OrderManager;
use crate::order_manager::types::{AnyOrderRequest, OrderEvent, OrderId, OrderKind, OrderRequest};
use crate::topic::{BoxTopicStream, Topic, TopicBus};
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
    pub ioc_price_slippage_bps: Decimal,
    pub ioc_wait_timeout: Duration,
    pub hedge_wait_timeout: Duration,
    pub bus: Arc<TopicBus>,
    pub strategy_name: String,
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
    /// 正在执行下单的 symbol 集合，防止同一 symbol 在上一次尝试完成前被
    /// 重复触发。用 `Arc` 而不是普通 `Mutex` 字段，因为清除标记的动作发生在
    /// `tokio::spawn` 出去的异步任务里，不持有 `&self`。
    in_flight: Arc<Mutex<HashSet<Symbol>>>,
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
            in_flight: Arc::new(Mutex::new(HashSet::new())),
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

/// 给定买/卖两侧的价格和各自手续费，返回扣费后的价差(基点)。买价 <= 0 时返回
/// `None`(报价还没来)。供 `on_quote` 使用。
pub fn compute_profit_bps(
    buy_ask: Decimal,
    buy_fee: FeeSchedule,
    sell_bid: Decimal,
    sell_fee: FeeSchedule,
) -> Option<Decimal> {
    if buy_ask <= Decimal::ZERO {
        return None;
    }
    let buy_cost = buy_ask * buy_fee.buy_multiplier();
    let sell_proceeds = sell_bid * sell_fee.sell_multiplier();
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
        // symbol、Kraken 侧应下的 side、Kraken 侧下单参考价（对侧最优价）。
        // 一次 on_quote 最多触发一次执行，命中第一个符合 kraken/binance 配对
        // 的机会即可——正常行情下这一对 venue 只会在一个方向上出现有效价差。
        let mut to_execute: Option<(Symbol, OrderSide, Decimal)> = None;
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

                let Some(profit_bps) = compute_profit_bps(buy_quote.ask, buy_fee, sell_quote.bid, sell_fee) else {
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

                if to_execute.is_none() {
                    if let Some(execution) = &self.execution {
                        if buy_venue == &execution.kraken_venue && sell_venue == &execution.binance_venue {
                            to_execute = Some((symbol.clone(), OrderSide::Buy, buy_quote.ask));
                        } else if buy_venue == &execution.binance_venue && sell_venue == &execution.kraken_venue {
                            to_execute = Some((symbol.clone(), OrderSide::Sell, sell_quote.bid));
                        }
                    }
                }
            }
        }
        drop(latest);

        for opportunity in &found {
            log_opportunity(opportunity);
        }

        if let (Some(execution), Some((symbol, kraken_side, kraken_ref_price))) = (&self.execution, to_execute) {
            let mut in_flight = self.in_flight.lock().unwrap();
            if !in_flight.contains(&symbol) {
                in_flight.insert(symbol.clone());
                drop(in_flight);

                let execution = execution.clone();
                let in_flight_handle = self.in_flight.clone();
                let symbol_for_task = symbol.clone();
                tokio::spawn(async move {
                    try_execute(execution, symbol_for_task.clone(), kraken_side, kraken_ref_price).await;
                    in_flight_handle.lock().unwrap().remove(&symbol_for_task);
                });
            }
        }
    }
}

/// 价差探路+对冲的核心执行任务：先在 Kraken 下限价 IOC 单探路，成交后立刻
/// 在 Binance Spot 下市价单对冲。不持有 `&CrossExchangeStrategy`，只需要
/// `CrossExecutionConfig`（可以在 `tokio::spawn` 出去的 `'static` 任务里安全
/// 使用）。任何一步失败只记日志、不重试/不回滚——与 `manual.rs` 现有的失败
/// 处理哲学一致，失衡了需要人工介入。
async fn try_execute(execution: Arc<CrossExecutionConfig>, symbol: Symbol, kraken_side: OrderSide, kraken_ref_price: Decimal) {
    let Some(&qty) = execution.order_qty_by_symbol.get(&symbol) else {
        warn!("cross_exchange: no preloaded order qty for symbol={symbol}, skip");
        return;
    };

    let slippage = execution.ioc_price_slippage_bps / Decimal::from(10_000);
    let raw_price = match kraken_side {
        OrderSide::Buy => kraken_ref_price * (Decimal::ONE + slippage),
        OrderSide::Sell => kraken_ref_price * (Decimal::ONE - slippage),
    };
    let price = match execution.kraken_precision.round_price(&symbol, raw_price) {
        Ok(p) => p,
        Err(err) => {
            error!("cross_exchange: failed to round kraken IOC price for symbol={symbol}: {err:#}");
            return;
        }
    };

    let client_order_id = generate_client_order_id(&execution.strategy_name, "kraken");
    let mut event_stream = execution.bus.subscribe::<OrderEvent>(Topic::order_event(&execution.strategy_name));

    let request = OrderRequest {
        strategy_id: execution.strategy_name.clone(),
        venue: execution.kraken_trade_venue.clone(),
        symbol: symbol.clone(),
        side: kraken_side,
        amount: OrderAmount::Base(qty),
        order_kind: OrderKind::LimitIoc { price },
        client_order_id: Some(client_order_id.clone()),
        group_id: None,
        metadata: None,
        order_id: None,
    };
    execution.bus.publish(Topic::order_submit(), AnyOrderRequest::Trade(request));

    let order_id = match resolve_order_id(&execution.order_manager, &client_order_id).await {
        Ok(id) => id,
        Err(err) => {
            error!("cross_exchange: failed to resolve kraken IOC order_id client_order_id={client_order_id}: {err:#}");
            return;
        }
    };

    let (filled_qty, avg_price) = match tokio::time::timeout(
        execution.ioc_wait_timeout,
        wait_for_ioc_terminal(&mut event_stream, &order_id, &execution.order_manager),
    )
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(err)) => {
            warn!("cross_exchange: kraken IOC order_id={order_id} symbol={symbol} not executed: {err:#}");
            return;
        }
        Err(_) => {
            error!(
                "cross_exchange: kraken IOC order_id={order_id} symbol={symbol} did not reach terminal state within {:?}, needs manual check",
                execution.ioc_wait_timeout
            );
            return;
        }
    };

    if filled_qty <= Decimal::ZERO {
        info!("cross_exchange: kraken IOC order_id={order_id} symbol={symbol} filled_qty=0, opportunity vanished, skip hedge");
        return;
    }

    info!("cross_exchange: kraken IOC order_id={order_id} symbol={symbol} filled_qty={filled_qty} avg_price={avg_price}, hedging on binance");

    let hedge_qty = match execution.binance_precision.round_qty(&symbol, PrecisionKind::Market, filled_qty) {
        Ok(q) => q,
        Err(err) => {
            error!(
                "cross_exchange: kraken order_id={order_id} filled_qty={filled_qty} for symbol={symbol} cannot be rounded to a valid binance market qty ({err:#}), \
                 position is now imbalanced and needs manual hedge"
            );
            return;
        }
    };

    let hedge_side = match kraken_side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    };

    let hedge_client_order_id = generate_client_order_id(&execution.strategy_name, "binance");
    let mut hedge_event_stream = execution.bus.subscribe::<OrderEvent>(Topic::order_event(&execution.strategy_name));

    let hedge_request = OrderRequest {
        strategy_id: execution.strategy_name.clone(),
        venue: execution.binance_trade_venue.clone(),
        symbol: symbol.clone(),
        side: hedge_side,
        amount: OrderAmount::Base(hedge_qty),
        order_kind: OrderKind::Market,
        client_order_id: Some(hedge_client_order_id.clone()),
        group_id: None,
        metadata: None,
        order_id: None,
    };
    execution.bus.publish(Topic::order_submit(), AnyOrderRequest::Trade(hedge_request));

    let hedge_order_id = match resolve_order_id(&execution.order_manager, &hedge_client_order_id).await {
        Ok(id) => id,
        Err(err) => {
            error!(
                "cross_exchange: kraken order_id={order_id} filled_qty={filled_qty} but failed to resolve binance hedge order_id (client_order_id={hedge_client_order_id}): {err:#}, \
                 position is now imbalanced and needs manual check"
            );
            return;
        }
    };

    match tokio::time::timeout(execution.hedge_wait_timeout, wait_for_order_event(&mut hedge_event_stream, &hedge_order_id)).await {
        Ok(Ok((_, hedge_filled_qty, hedge_avg_price))) => {
            info!("cross_exchange: binance hedge order_id={hedge_order_id} symbol={symbol} filled_qty={hedge_filled_qty} avg_price={hedge_avg_price}");
        }
        Ok(Err(err)) => {
            error!(
                "cross_exchange: kraken order_id={order_id} filled_qty={filled_qty} but binance hedge order_id={hedge_order_id} failed: {err:#}, \
                 position is now imbalanced and needs manual check"
            );
        }
        Err(_) => {
            error!(
                "cross_exchange: kraken order_id={order_id} filled_qty={filled_qty} but binance hedge order_id={hedge_order_id} did not reach terminal state within {:?}, \
                 position is now imbalanced and needs manual check",
                execution.hedge_wait_timeout
            );
        }
    }
}

fn generate_client_order_id(strategy_name: &str, leg: &str) -> String {
    format!("{strategy_name}-{leg}-{}-{:05}", now_ms(), rand::random::<u32>() % 100000)
}

const RESOLVE_ORDER_ID_TIMEOUT: Duration = Duration::from_secs(5);
const RESOLVE_ORDER_ID_POLL_INTERVAL: Duration = Duration::from_millis(2);

/// 轮询反查 client_order_id 对应的 order_id，逻辑抄自
/// `manual.rs::resolve_order_id`（那边是私有方法，不能跨模块复用）。
async fn resolve_order_id(order_manager: &OrderManager, client_order_id: &str) -> anyhow::Result<OrderId> {
    tokio::time::timeout(RESOLVE_ORDER_ID_TIMEOUT, async {
        loop {
            if let Some(order) = order_manager.find_by_client_order_id(client_order_id) {
                return order.order_id;
            }
            tokio::time::sleep(RESOLVE_ORDER_ID_POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow::anyhow!("client_order_id={client_order_id} was not submitted to the order manager within {RESOLVE_ORDER_ID_TIMEOUT:?}")
    })
}

/// Kraken IOC 单的终态等待：`Filled`/`PartiallyFilled` 直接返回其
/// filled_qty/avg_price；`RejectedByExchange`（对应 IOC 未完全成交部分被
/// Expired）时不当成错误，改为读 `OrderManager::get_order` 拿当前
/// filled_qty/avg_price 作为终态——因为仓位记账在这个事件发布之前已经正确
/// 写入了，只是事件本身不携带 filled_qty。`RejectedByRisk` 说明订单从未到
/// 交易所，仍然是真错误。
async fn wait_for_ioc_terminal(
    event_stream: &mut BoxTopicStream<OrderEvent>,
    target: &OrderId,
    order_manager: &OrderManager,
) -> anyhow::Result<(Decimal, Decimal)> {
    loop {
        match event_stream.next().await {
            Some((_, OrderEvent::Filled { order_id, filled_qty, avg_price })) if &order_id == target => {
                return Ok((filled_qty, avg_price));
            }
            Some((_, OrderEvent::PartiallyFilled { order_id, filled_qty, avg_price })) if &order_id == target => {
                return Ok((filled_qty, avg_price));
            }
            Some((_, OrderEvent::RejectedByExchange { order_id, reason })) if &order_id == target => {
                let order = order_manager.get_order(&order_id).ok_or_else(|| {
                    anyhow::anyhow!("order {order_id} disappeared from order manager after RejectedByExchange ({reason})")
                })?;
                return Ok((order.filled_qty, order.avg_price.unwrap_or(Decimal::ZERO)));
            }
            Some((_, OrderEvent::RejectedByRisk { order_id, reason })) if &order_id == target => {
                anyhow::bail!("order {order_id} rejected by risk: {reason}");
            }
            Some(_) => continue,
            None => anyhow::bail!("order event stream closed while waiting for IOC order {target} to reach terminal state"),
        }
    }
}

/// 与 `manual.rs::wait_for_order_event` 相同的模式（那边是私有函数，不能跨
/// 模块复用），用于等待 Binance 对冲市价单成交。
async fn wait_for_order_event(
    event_stream: &mut BoxTopicStream<OrderEvent>,
    target: &OrderId,
) -> anyhow::Result<(OrderId, Decimal, Decimal)> {
    loop {
        match event_stream.next().await {
            Some((_, OrderEvent::Filled { order_id, filled_qty, avg_price })) if &order_id == target => {
                return Ok((order_id, filled_qty, avg_price));
            }
            Some((_, OrderEvent::RejectedByRisk { order_id, reason })) if &order_id == target => {
                anyhow::bail!("order {order_id} rejected by risk: {reason}");
            }
            Some((_, OrderEvent::RejectedByExchange { order_id, reason })) if &order_id == target => {
                anyhow::bail!("order {order_id} rejected by exchange: {reason}");
            }
            Some(_) => continue,
            None => anyhow::bail!("order event stream closed while waiting for order {target} to fill"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    use crate::exchange_info::types::{MarketPrecision, QtyPrecision};
    use crate::order::OrderProvider;
    use crate::order::types::{LimitIocOrderRequest, MarketOrderRequest, OrderResult, OrderStatus};
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
            strategy.fee_for(&venue_a),
            symbol_quotes[&venue_b].bid,
            strategy.fee_for(&venue_b),
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
            strategy.fee_for(&venue_a),
            symbol_quotes[&venue_b].bid,
            strategy.fee_for(&venue_b),
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

    /// 记录每次真实下单调用（市价/限价IOC分开记），响应固定 status=New、
    /// filled_qty=0——成交必须由测试驱动 WS 推送模拟，和真实交易所一致。
    struct FakeExchangeProvider {
        venue: Venue,
        market_calls: Arc<Mutex<Vec<(OrderSide, Decimal)>>>,
        limit_ioc_calls: Arc<Mutex<Vec<(OrderSide, Decimal, Decimal)>>>,
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
            self.market_calls.lock().unwrap().push((req.side, qty));
            Ok(OrderResult {
                order_id: format!("{}-{}", self.venue, req.symbol),
                status: OrderStatus::New,
                filled_qty: Decimal::ZERO,
                avg_price: None,
                fee: None,
                fee_asset: None,
            })
        }
        async fn place_limit_ioc_order_raw(&self, req: &LimitIocOrderRequest) -> anyhow::Result<OrderResult> {
            self.limit_ioc_calls.lock().unwrap().push((req.side, req.quantity, req.price));
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

    /// 内存版全套依赖：`TopicBus` + `RiskService` + `ExecutionService` +
    /// `OrderManager`，和 `manual.rs::setup_live_env` 是同一个模式。
    struct TestEnv {
        bus: Arc<TopicBus>,
        order_manager: Arc<OrderManager>,
        _risk_handle: tokio::task::JoinHandle<()>,
        _execution_handle: tokio::task::JoinHandle<()>,
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
        tokio::time::sleep(Duration::from_millis(10)).await;

        TestEnv {
            bus,
            order_manager,
            _risk_handle: risk_handle,
            _execution_handle: execution_handle,
        }
    }

    /// 轮询直到 order_manager 里出现 client_order_id 带指定前缀的订单——
    /// try_execute 内部随机生成 client_order_id，测试没法提前知道完整值，
    /// 只知道 "{strategy_name}-kraken-"/"{strategy_name}-binance-" 前缀。
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
                symbol,
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
            ioc_price_slippage_bps: Decimal::from(10),
            ioc_wait_timeout: Duration::from_millis(500),
            hedge_wait_timeout: Duration::from_millis(500),
            bus: env.bus.clone(),
            strategy_name: "cross_exchange".to_string(),
        })
    }

    #[tokio::test]
    async fn kraken_ioc_full_fill_triggers_binance_hedge() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let kraken_limit_calls = Arc::new(Mutex::new(Vec::new()));
        let kraken_provider: Arc<dyn OrderProvider> = Arc::new(FakeExchangeProvider {
            venue: kraken_venue.clone(),
            market_calls: Arc::new(Mutex::new(Vec::new())),
            limit_ioc_calls: kraken_limit_calls.clone(),
        });
        let binance_market_calls = Arc::new(Mutex::new(Vec::new()));
        let binance_provider: Arc<dyn OrderProvider> = Arc::new(FakeExchangeProvider {
            venue: binance_venue.clone(),
            market_calls: binance_market_calls.clone(),
            limit_ioc_calls: Arc::new(Mutex::new(Vec::new())),
        });

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue.clone(), qty);

        let order_manager = env.order_manager.clone();
        let driver = tokio::spawn(async move {
            let kraken_order = poll_order_by_prefix(&order_manager, "cross_exchange-kraken-").await;
            push_exchange_update(&order_manager, &kraken_venue, &kraken_order, OrderStatus::Filled, qty, Decimal::from(100)).await;

            let binance_order = poll_order_by_prefix(&order_manager, "cross_exchange-binance-").await;
            push_exchange_update(&order_manager, &binance_venue, &binance_order, OrderStatus::Filled, qty, Decimal::from(100)).await;
        });

        try_execute(execution, symbol, OrderSide::Buy, Decimal::from(100)).await;
        driver.await.unwrap();

        assert_eq!(kraken_limit_calls.lock().unwrap().len(), 1);
        let hedge_calls = binance_market_calls.lock().unwrap();
        assert_eq!(hedge_calls.len(), 1, "kraken 完全成交后应该触发一次 binance 对冲");
        assert_eq!(hedge_calls[0], (OrderSide::Sell, qty), "kraken 买入后应该在 binance 卖出对冲，数量按实际成交量");
    }

    #[tokio::test]
    async fn kraken_ioc_zero_fill_skips_hedge() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let kraken_limit_calls = Arc::new(Mutex::new(Vec::new()));
        let kraken_provider: Arc<dyn OrderProvider> = Arc::new(FakeExchangeProvider {
            venue: kraken_venue.clone(),
            market_calls: Arc::new(Mutex::new(Vec::new())),
            limit_ioc_calls: kraken_limit_calls.clone(),
        });
        let binance_market_calls = Arc::new(Mutex::new(Vec::new()));
        let binance_provider: Arc<dyn OrderProvider> = Arc::new(FakeExchangeProvider {
            venue: binance_venue.clone(),
            market_calls: binance_market_calls.clone(),
            limit_ioc_calls: Arc::new(Mutex::new(Vec::new())),
        });

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue, qty);

        let order_manager = env.order_manager.clone();
        let driver = tokio::spawn(async move {
            let kraken_order = poll_order_by_prefix(&order_manager, "cross_exchange-kraken-").await;
            // IOC 完全没有成交：交易所推送 Expired，filled_qty=0。
            push_exchange_update(&order_manager, &kraken_venue, &kraken_order, OrderStatus::Expired, Decimal::ZERO, Decimal::ZERO).await;
        });

        try_execute(execution, symbol, OrderSide::Buy, Decimal::from(100)).await;
        driver.await.unwrap();

        assert_eq!(kraken_limit_calls.lock().unwrap().len(), 1);
        assert!(binance_market_calls.lock().unwrap().is_empty(), "IOC 0 成交时不应该下对冲单");
    }

    #[tokio::test]
    async fn kraken_ioc_partial_fill_via_expired_hedges_actual_filled_qty() {
        let symbol = btc_usdt();
        let kraken_venue = Venue::new("kraken_spot");
        let binance_venue = Venue::new("binance_spot");

        let kraken_limit_calls = Arc::new(Mutex::new(Vec::new()));
        let kraken_provider: Arc<dyn OrderProvider> = Arc::new(FakeExchangeProvider {
            venue: kraken_venue.clone(),
            market_calls: Arc::new(Mutex::new(Vec::new())),
            limit_ioc_calls: kraken_limit_calls.clone(),
        });
        let binance_market_calls = Arc::new(Mutex::new(Vec::new()));
        let binance_provider: Arc<dyn OrderProvider> = Arc::new(FakeExchangeProvider {
            venue: binance_venue.clone(),
            market_calls: binance_market_calls.clone(),
            limit_ioc_calls: Arc::new(Mutex::new(Vec::new())),
        });

        let env = setup_env(vec![kraken_provider, binance_provider], symbol.clone()).await;
        let qty = Decimal::ONE;
        let execution = test_execution_config(&env, &symbol, kraken_venue.clone(), binance_venue.clone(), qty);

        let partial_fill = Decimal::new(6, 1); // 0.6

        let order_manager = env.order_manager.clone();
        let driver = tokio::spawn(async move {
            let kraken_order = poll_order_by_prefix(&order_manager, "cross_exchange-kraken-").await;
            // IOC 部分成交后剩余部分过期：仓位记账里 filled_qty 已经正确写入
            // 0.6，但事件本身是不带 filled_qty 的 RejectedByExchange。
            push_exchange_update(&order_manager, &kraken_venue, &kraken_order, OrderStatus::Expired, partial_fill, Decimal::from(100)).await;

            let binance_order = poll_order_by_prefix(&order_manager, "cross_exchange-binance-").await;
            push_exchange_update(&order_manager, &binance_venue, &binance_order, OrderStatus::Filled, partial_fill, Decimal::from(100)).await;
        });

        try_execute(execution, symbol, OrderSide::Sell, Decimal::from(100)).await;
        driver.await.unwrap();

        assert_eq!(kraken_limit_calls.lock().unwrap().len(), 1);
        let hedge_calls = binance_market_calls.lock().unwrap();
        assert_eq!(hedge_calls.len(), 1, "部分成交也应该按实际成交量触发一次对冲");
        assert_eq!(hedge_calls[0], (OrderSide::Buy, partial_fill), "kraken 卖出部分成交后应该在 binance 买入对冲，数量是实际成交的 0.6 而不是下单量 1");
    }
}
