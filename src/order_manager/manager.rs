use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use log::{info, warn};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};

use crate::order::types::OrderStatus;
use crate::portfolio::PortfolioManager;

use super::execution::ExecutionEngine;
use super::risk::RiskEngine;
use super::store::OrderStore;
use super::stream::ExchangeOrderUpdate;
use super::types::{Order, OrderEvent, OrderId, OrderRequest, OrderResponse, RiskCheckResult};

/// 订单管理器：统筹订单生命周期，负责分配订单ID、调用风控引擎、
/// 委托执行引擎下单、维护订单状态、发布事件通知策略。
pub struct OrderManager {
    risk_engine: Arc<RiskEngine>,
    execution_engine: Arc<ExecutionEngine>,
    portfolio: Arc<PortfolioManager>,
    /// 全局订单计数器，用于生成唯一订单ID
    next_order_seq: AtomicU64,
    /// 订单状态存储，key=OrderId。用 `Arc` 包装以便后台执行任务共享同一份存储。
    orders: Arc<Mutex<HashMap<OrderId, Order>>>,
    /// 下单时透传给交易所的 client_order_id -> 内部 OrderId，用于交易所私有 WS
    /// 推送订单更新时反查订单（见 `handle_exchange_update`）。
    client_order_index: Arc<Mutex<HashMap<String, OrderId>>>,
    /// 交易所自己的订单号 -> 内部 OrderId，REST 下单响应拿到 exchange_order_id
    /// 后写入，作为 client_order_id 关联失败时的兜底。
    exchange_order_index: Arc<Mutex<HashMap<String, OrderId>>>,
    /// 事件发布通道，策略通过订阅这个通道接收订单事件
    event_tx: mpsc::Sender<OrderEvent>,
    /// 订单历史持久化，每次 `orders` map 更新后同步 upsert 一份。
    order_store: Arc<dyn OrderStore>,
}

impl OrderManager {
    pub fn new(
        risk_engine: Arc<RiskEngine>,
        execution_engine: Arc<ExecutionEngine>,
        portfolio: Arc<PortfolioManager>,
        event_tx: mpsc::Sender<OrderEvent>,
        order_store: Arc<dyn OrderStore>,
    ) -> Self {
        Self {
            risk_engine,
            execution_engine,
            portfolio,
            next_order_seq: AtomicU64::new(1),
            orders: Arc::new(Mutex::new(HashMap::new())),
            client_order_index: Arc::new(Mutex::new(HashMap::new())),
            exchange_order_index: Arc::new(Mutex::new(HashMap::new())),
            event_tx,
            order_store,
        }
    }

    /// 策略提交订单的入口。返回立即生成的订单ID和用于接收提交结果的 oneshot 通道
    /// (风控通过并成功发送到交易所，或被风控/交易所拒绝；不代表已经成交——成交
    /// 结果只由交易所私有 WS 通过 `handle_exchange_update` 驱动，见该方法说明)。
    pub async fn submit_order(&self, mut request: OrderRequest) -> OrderResponse {
        let order_id = self.generate_order_id();
        let (result_tx, result_rx) = oneshot::channel();

        // 确定透传给交易所的 client_order_id：策略指定了就用策略的，否则用
        // order_id 本身(同时满足 Binance newClientOrderId 和 Kraken cl_ord_id
        // 的长度限制)。记入索引，供后续 WS 推送反查订单。
        let client_order_id = request
            .client_order_id
            .clone()
            .unwrap_or_else(|| order_id.to_string());
        request.client_order_id = Some(client_order_id.clone());
        self.client_order_index
            .lock()
            .unwrap()
            .insert(client_order_id, order_id.clone());

        info!(
            "OrderManager: submit order_id={} strategy={} venue={} symbol={} side={:?} amount={:?}",
            order_id, request.strategy_name, request.venue, request.symbol, request.side, request.amount
        );

        // 创建内部订单对象
        let order = Order {
            order_id: order_id.clone(),
            request: request.clone(),
            status: OrderStatus::New,
            filled_qty: rust_decimal::Decimal::ZERO,
            avg_price: None,
            exchange_order_id: None,
            created_at_ms: current_timestamp_ms(),
            updated_at_ms: current_timestamp_ms(),
            reject_reason: None,
        };

        // 存储订单状态
        self.orders.lock().unwrap().insert(order_id.clone(), order.clone());
        self.order_store.upsert(order.clone());

        // 发送 Submitted 事件
        let _ = self.event_tx.send(OrderEvent::Submitted {
            order_id: order_id.clone(),
        }).await;

        // 在后台任务中执行风控检查和订单提交
        let risk_engine = self.risk_engine.clone();
        let execution_engine = self.execution_engine.clone();
        let orders = self.orders.clone();
        let exchange_order_index = self.exchange_order_index.clone();
        let event_tx = self.event_tx.clone();
        let order_store = self.order_store.clone();

        tokio::spawn(async move {
            let result = Self::process_order(
                order,
                risk_engine,
                execution_engine,
                orders,
                exchange_order_index,
                event_tx,
                order_store,
            ).await;
            let _ = result_tx.send(result);
        });

        OrderResponse {
            order_id,
            result_rx,
        }
    }

    /// 后台任务：执行风控检查 -> 执行引擎提交 -> 更新状态
    async fn process_order(
        order: Order,
        risk_engine: Arc<RiskEngine>,
        execution_engine: Arc<ExecutionEngine>,
        orders: Arc<Mutex<HashMap<OrderId, Order>>>,
        exchange_order_index: Arc<Mutex<HashMap<String, OrderId>>>,
        event_tx: mpsc::Sender<OrderEvent>,
        order_store: Arc<dyn OrderStore>,
    ) -> Result<Order, String> {
        // 1. 风控检查
        let risk_result = risk_engine.check(
            &order.request.venue,
            &order.request.symbol,
            order.request.side,
            &order.request.amount,
        );

        match risk_result {
            RiskCheckResult::Rejected { reason } => {
                info!("OrderManager: order_id={} rejected by risk: {}", order.order_id, reason);

                let mut updated_order = order;
                updated_order.status = OrderStatus::Rejected;
                updated_order.reject_reason = Some(reason.clone());
                updated_order.updated_at_ms = current_timestamp_ms();

                // 更新存储
                orders.lock().unwrap().insert(updated_order.order_id.clone(), updated_order.clone());
                order_store.upsert(updated_order.clone());

                // 发送拒绝事件
                let _ = event_tx.send(OrderEvent::RejectedByRisk {
                    order_id: updated_order.order_id.clone(),
                    reason: reason.clone(),
                }).await;

                return Err(reason);
            }
            RiskCheckResult::Approved => {
                info!("OrderManager: order_id={} passed risk check", order.order_id);
            }
        }

        // 2. 提交到执行引擎。注意：这里只处理"提交是否被交易所立即拒绝"，
        // 不会再把 REST 响应当成成交结果——两个交易所的成交都统一由交易所私有
        // WS 通过 `handle_exchange_update` 驱动，见该方法说明。
        let executed_order = execution_engine.execute(order).await;

        if executed_order.status == OrderStatus::Rejected {
            // 订单被拒绝，释放风控预占用的额度
            risk_engine.release(&executed_order.request.venue, &executed_order.request.symbol);
        }

        // 3. 记录 exchange_order_id -> OrderId，供 WS 推送在 client_order_id
        // 关联失败时兜底反查（例如推送先于 REST 响应到达时，client_order_id 已
        // 经能关联上，这里只是让后续以 exchange_order_id 为键的推送也能查到）。
        if let Some(exchange_order_id) = &executed_order.exchange_order_id {
            exchange_order_index
                .lock()
                .unwrap()
                .entry(exchange_order_id.clone())
                .or_insert_with(|| executed_order.order_id.clone());
        }

        // 4. 更新订单状态存储
        orders.lock().unwrap().insert(executed_order.order_id.clone(), executed_order.clone());
        order_store.upsert(executed_order.clone());

        if executed_order.status == OrderStatus::Rejected {
            Err(executed_order.reject_reason.unwrap_or_else(|| "unknown rejection".to_string()))
        } else {
            Ok(executed_order)
        }
    }

    /// 消费交易所私有 WS 推送的一条订单更新，是订单成交状态的唯一权威来源
    /// (REST 下单响应只负责"提交是否被立即拒绝"，见 `process_order`)。
    ///
    /// 关联顺序：先按 `client_order_id` 查，查不到再退化按 `exchange_order_id`
    /// 查(应对推送先于 REST 响应到达、或推送没回显 client_order_id 的情况)。
    /// 关联不上或对应订单未知则丢弃并打日志，不会 panic。
    ///
    /// 幂等/防倒退：`filled_qty` 比已存储的还小的推送视为过期重放直接丢弃；
    /// 已经是终态(Filled/Rejected/Expired)且这次推送没有带来新信息的重复推送
    /// 也会被忽略。只有成交量增加的部分才会计入风控持仓(`RiskEngine::on_filled`
    /// 按增量记账，累计值不能直接传)。
    pub async fn handle_exchange_update(&self, update: ExchangeOrderUpdate) {
        let Some(order_id) = self.resolve_order_id(&update) else {
            warn!(
                "OrderManager: exchange update from venue={} could not be correlated to any order (client_order_id={:?}, exchange_order_id={:?})",
                update.venue, update.client_order_id, update.exchange_order_id
            );
            return;
        };

        // 补录 exchange_order_id -> OrderId，方便后续没有回显 client_order_id
        // 的推送也能关联上。
        if let Some(exchange_order_id) = &update.exchange_order_id {
            self.exchange_order_index
                .lock()
                .unwrap()
                .entry(exchange_order_id.clone())
                .or_insert_with(|| order_id.clone());
        }

        let applied = {
            let mut orders = self.orders.lock().unwrap();
            let Some(order) = orders.get_mut(&order_id) else {
                warn!("OrderManager: exchange update for unknown order_id={order_id}");
                return;
            };

            if update.filled_qty < order.filled_qty {
                warn!(
                    "OrderManager: ignoring stale exchange update for order_id={order_id}: update filled_qty {} < stored {}",
                    update.filled_qty, order.filled_qty
                );
                return;
            }
            let already_terminal = matches!(
                order.status,
                OrderStatus::Filled | OrderStatus::Rejected | OrderStatus::Expired
            );
            if already_terminal && update.filled_qty == order.filled_qty && update.status == order.status {
                return;
            }

            let fill_delta = update.filled_qty - order.filled_qty;

            order.status = update.status;
            order.filled_qty = update.filled_qty;
            if update.avg_price.is_some() {
                order.avg_price = update.avg_price;
            }
            if let Some(exchange_order_id) = &update.exchange_order_id {
                order.exchange_order_id = Some(exchange_order_id.clone());
            }
            order.updated_at_ms = update.ts_ms;

            (
                order.request.venue.clone(),
                order.request.symbol.clone(),
                order.request.side,
                fill_delta,
                order.status,
                order.filled_qty,
                order.avg_price,
                order.clone(),
            )
        };

        let (venue, symbol, side, fill_delta, status, filled_qty, avg_price, updated_order) = applied;
        self.order_store.upsert(updated_order);

        if fill_delta > Decimal::ZERO {
            let outcome = self.risk_engine.on_filled(&venue, &symbol, side, fill_delta, avg_price, update.ts_ms);
            self.portfolio.record_fill(
                &venue,
                &symbol,
                fill_delta,
                avg_price,
                update.fee,
                outcome.realized_pnl,
                update.ts_ms,
            );
        }

        let event = match status {
            OrderStatus::PartiallyFilled => Some(OrderEvent::PartiallyFilled {
                order_id: order_id.clone(),
                filled_qty,
                avg_price: avg_price.unwrap_or(Decimal::ZERO),
            }),
            OrderStatus::Filled => Some(OrderEvent::Filled {
                order_id: order_id.clone(),
                filled_qty,
                avg_price: avg_price.unwrap_or(Decimal::ZERO),
            }),
            OrderStatus::Rejected | OrderStatus::Expired => Some(OrderEvent::RejectedByExchange {
                order_id: order_id.clone(),
                reason: format!("exchange order stream reported status={status:?}"),
            }),
            OrderStatus::New => None,
        };

        if let Some(event) = event {
            let _ = self.event_tx.send(event).await;
        }
    }

    fn resolve_order_id(&self, update: &ExchangeOrderUpdate) -> Option<OrderId> {
        if let Some(client_order_id) = &update.client_order_id {
            if let Some(order_id) = self.client_order_index.lock().unwrap().get(client_order_id).cloned() {
                return Some(order_id);
            }
        }
        if let Some(exchange_order_id) = &update.exchange_order_id {
            if let Some(order_id) = self.exchange_order_index.lock().unwrap().get(exchange_order_id).cloned() {
                return Some(order_id);
            }
        }
        None
    }

    /// 查询订单状态
    pub fn get_order(&self, order_id: &OrderId) -> Option<Order> {
        self.orders.lock().unwrap().get(order_id).cloned()
    }

    /// 获取所有订单（用于监控和调试）
    pub fn all_orders(&self) -> Vec<Order> {
        self.orders.lock().unwrap().values().cloned().collect()
    }

    /// 生成全局唯一的订单ID
    fn generate_order_id(&self) -> OrderId {
        let seq = self.next_order_seq.fetch_add(1, Ordering::SeqCst);
        OrderId::new(format!("ORD-{:012}", seq))
    }
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::types::{OrderAmount, OrderResult, OrderSide, OrderStatus};
    use crate::order::OrderProvider;
    use crate::order_manager::execution::ExchangeAdapter;
    use crate::order_manager::risk::RiskLimits;
    use crate::order_manager::store::InMemoryOrderStore;
    use crate::portfolio::{InMemoryPnlStore, PortfolioManager};
    use crate::position::{InMemoryPositionStore, PositionManager};
    use crate::types::{Symbol, Venue};
    use async_trait::async_trait;
    use dashmap::DashMap;
    use rust_decimal::Decimal;
    use std::sync::atomic::AtomicBool;

    struct FakeOrderProvider {
        venue: Venue,
        should_fail: AtomicBool,
    }

    #[async_trait]
    impl OrderProvider for FakeOrderProvider {
        fn venue(&self) -> Venue {
            self.venue.clone()
        }

        async fn place_market_order_raw(
            &self,
            req: &crate::order::types::MarketOrderRequest,
        ) -> anyhow::Result<OrderResult> {
            if self.should_fail.load(Ordering::SeqCst) {
                anyhow::bail!("simulated exchange failure");
            }
            let qty = match req.amount {
                OrderAmount::Base(q) => q,
                OrderAmount::Quote(q) => q / Decimal::new(50000, 0), // assume BTC price 50k
            };
            Ok(OrderResult {
                order_id: "exchange-123".to_string(),
                status: OrderStatus::Filled,
                filled_qty: qty,
                avg_price: Some(Decimal::new(50000, 0)),
                fee: None,
                fee_asset: None,
            })
        }
    }

    fn setup_manager() -> (Arc<OrderManager>, mpsc::Receiver<OrderEvent>, Arc<RiskEngine>) {
        let venue = Venue::new("test-venue");
        let symbol = Symbol::new("BTC", "USDT");

        let mut risk_limits = HashMap::new();
        risk_limits.insert(
            (venue.clone(), symbol.clone()),
            RiskLimits {
                max_order_amount: Decimal::new(10, 0),
                max_position: Decimal::new(100, 0),
                max_orders_per_window: 10,
            },
        );

        let position_manager = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let risk_engine = Arc::new(RiskEngine::new(risk_limits, position_manager.clone()));
        let portfolio = Arc::new(PortfolioManager::new(
            position_manager,
            Arc::new(InMemoryPnlStore::new()),
            Arc::new(DashMap::new()),
            HashMap::new(),
        ));

        let (event_tx, event_rx) = mpsc::channel(100);

        let provider = Arc::new(FakeOrderProvider {
            venue: venue.clone(),
            should_fail: AtomicBool::new(false),
        });
        let adapter = Arc::new(ExchangeAdapter::new(venue.clone(), provider));
        let mut adapters = HashMap::new();
        adapters.insert(venue.clone(), adapter);

        let execution_engine = Arc::new(ExecutionEngine::new(adapters, event_tx.clone()));
        let order_store = Arc::new(InMemoryOrderStore::new());
        let manager = Arc::new(OrderManager::new(risk_engine.clone(), execution_engine, portfolio, event_tx, order_store));

        (manager, event_rx, risk_engine)
    }

    fn sample_request() -> OrderRequest {
        OrderRequest {
            strategy_name: "test-strategy".to_string(),
            venue: Venue::new("test-venue"),
            symbol: Symbol::new("BTC", "USDT"),
            side: OrderSide::Buy,
            amount: OrderAmount::Base(Decimal::ONE),
            client_order_id: None,
            group_id: None,
            metadata: None,
        }
    }

    #[tokio::test]
    async fn submit_order_stays_new_until_ws_confirms() {
        let (manager, mut event_rx, _risk_engine) = setup_manager();

        let response = manager.submit_order(sample_request()).await;
        let order_id = response.order_id.clone();
        let result = response.result_rx.await.unwrap();

        // REST 提交路径不再是成交结果的来源：即使 FakeOrderProvider 同步返回了
        // Filled，订单在这里应该仍是 New/未成交，等交易所私有 WS 推送确认。
        assert!(result.is_ok());
        let order = result.unwrap();
        assert_eq!(order.status, OrderStatus::New);
        assert_eq!(order.filled_qty, Decimal::ZERO);

        let events: Vec<OrderEvent> = vec![event_rx.recv().await.unwrap(), event_rx.recv().await.unwrap()];
        assert!(matches!(events[0], OrderEvent::Submitted { .. }));
        assert!(matches!(events[1], OrderEvent::Accepted { .. }));

        assert_eq!(manager.get_order(&order_id).unwrap().status, OrderStatus::New);
    }

    #[tokio::test]
    async fn ws_updates_drive_fill_events_and_incremental_risk_credit() {
        let (manager, mut event_rx, risk_engine) = setup_manager();

        let response = manager.submit_order(sample_request()).await;
        let order_id = response.order_id.clone();
        let order = response.result_rx.await.unwrap().unwrap();
        let client_order_id = order.request.client_order_id.clone().unwrap();

        // 消费掉 Submitted/Accepted
        event_rx.recv().await.unwrap();
        event_rx.recv().await.unwrap();

        let venue = Venue::new("test-venue");
        let symbol = Symbol::new("BTC", "USDT");

        manager
            .handle_exchange_update(ExchangeOrderUpdate {
                venue: venue.clone(),
                client_order_id: Some(client_order_id.clone()),
                exchange_order_id: Some("exchange-123".to_string()),
                status: OrderStatus::PartiallyFilled,
                filled_qty: Decimal::new(4, 1), // 0.4
                avg_price: Some(Decimal::new(50000, 0)),
                fee: None,
                fee_asset: None,
                ts_ms: 1,
            })
            .await;

        manager
            .handle_exchange_update(ExchangeOrderUpdate {
                venue,
                client_order_id: Some(client_order_id),
                exchange_order_id: Some("exchange-123".to_string()),
                status: OrderStatus::Filled,
                filled_qty: Decimal::ONE,
                avg_price: Some(Decimal::new(50000, 0)),
                fee: None,
                fee_asset: None,
                ts_ms: 2,
            })
            .await;

        let updated = manager.get_order(&order_id).unwrap();
        assert_eq!(updated.status, OrderStatus::Filled);
        assert_eq!(updated.filled_qty, Decimal::ONE);
        // 两次推送的 filled_qty 都是累计值(0.4 然后 1.0)，风控只应按增量记账，
        // 不能把 0.4 + 1.0 都算上。
        assert_eq!(risk_engine.position(&Venue::new("test-venue"), &symbol), Decimal::ONE);

        let partial = event_rx.recv().await.unwrap();
        let filled = event_rx.recv().await.unwrap();
        assert!(matches!(partial, OrderEvent::PartiallyFilled { .. }));
        assert!(matches!(filled, OrderEvent::Filled { .. }));
    }

    #[tokio::test]
    async fn stale_or_regressed_update_is_ignored() {
        let (manager, mut event_rx, risk_engine) = setup_manager();

        let response = manager.submit_order(sample_request()).await;
        let order_id = response.order_id.clone();
        let order = response.result_rx.await.unwrap().unwrap();
        let client_order_id = order.request.client_order_id.clone().unwrap();
        event_rx.recv().await.unwrap();
        event_rx.recv().await.unwrap();

        let venue = Venue::new("test-venue");
        let symbol = Symbol::new("BTC", "USDT");

        manager
            .handle_exchange_update(ExchangeOrderUpdate {
                venue: venue.clone(),
                client_order_id: Some(client_order_id.clone()),
                exchange_order_id: Some("exchange-123".to_string()),
                status: OrderStatus::Filled,
                filled_qty: Decimal::ONE,
                avg_price: Some(Decimal::new(50000, 0)),
                fee: None,
                fee_asset: None,
                ts_ms: 1,
            })
            .await;
        event_rx.recv().await.unwrap(); // Filled

        // 过期重放/倒退的推送(比如乱序到达的旧快照)必须被忽略，不能覆盖已有状态、
        // 不能重复记账、也不能重复发事件。
        manager
            .handle_exchange_update(ExchangeOrderUpdate {
                venue,
                client_order_id: Some(client_order_id),
                exchange_order_id: Some("exchange-123".to_string()),
                status: OrderStatus::PartiallyFilled,
                filled_qty: Decimal::new(5, 1),
                avg_price: Some(Decimal::new(50000, 0)),
                fee: None,
                fee_asset: None,
                ts_ms: 2,
            })
            .await;

        let updated = manager.get_order(&order_id).unwrap();
        assert_eq!(updated.status, OrderStatus::Filled);
        assert_eq!(updated.filled_qty, Decimal::ONE);
        assert_eq!(risk_engine.position(&Venue::new("test-venue"), &symbol), Decimal::ONE);
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn update_for_unknown_order_is_dropped_without_panic() {
        let (manager, mut event_rx, _risk_engine) = setup_manager();

        manager
            .handle_exchange_update(ExchangeOrderUpdate {
                venue: Venue::new("test-venue"),
                client_order_id: Some("does-not-exist".to_string()),
                exchange_order_id: None,
                status: OrderStatus::Filled,
                filled_qty: Decimal::ONE,
                avg_price: None,
                fee: None,
                fee_asset: None,
                ts_ms: 1,
            })
            .await;

        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn generates_unique_order_ids() {
        let (manager, _event_rx, _risk_engine) = setup_manager();

        let resp1 = manager.submit_order(sample_request()).await;
        let resp2 = manager.submit_order(sample_request()).await;

        assert_ne!(resp1.order_id, resp2.order_id);
    }
}
