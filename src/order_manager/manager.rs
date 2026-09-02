use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use log::{info, warn};
use rust_decimal::Decimal;

use crate::order::types::OrderStatus;
use crate::position::{AdjustmentReason, PositionManager};
use crate::pricing::FeeUsdtConverter;
use crate::topic::{Topic, TopicBus};

use super::store::{OrderStore, OrderUpdateOutcome};
use super::stream::ExchangeOrderUpdate;
use super::types::{Order, OrderEvent, OrderId, OrderKind};

/// 订单管理器（重构后）：只负责处理交易所 WS 推送的订单更新，
/// 是订单成交状态的唯一权威来源。订单初始创建由 RiskService 写入 Redis，
/// ExecutionService REST 下单后也直接更新 Redis 写入 exchange_order_id，
/// OrderManager 只负责：WS 更新 → 读 Redis → 更新 → 写回 Redis → 更新仓位/账本 → 发布事件。
/// 仓位是成交的直接结果，因此由 OrderManager 直接写入 `PositionManager`——
/// `RiskService` 只在下单前读仓位做限额检查，不再经手写入，避免读写分家。
pub struct OrderManager {
    bus: Arc<TopicBus>,
    position_manager: Arc<PositionManager>,
    /// client_order_id → OrderId 索引，按需从 Redis 懒加载
    client_order_index: Arc<Mutex<HashMap<String, OrderId>>>,
    /// exchange_order_id → OrderId 索引，按需从 Redis 懒加载
    exchange_order_index: Arc<Mutex<HashMap<String, OrderId>>>,
    /// 订单历史持久化（Redis 或内存）
    order_store: Arc<dyn OrderStore>,
    /// 手续费统一换算为 USDT 计价的服务；没有活跃 provider 注册表的低频场景
    /// （如 `reconcile-order --confirm` 人工回补）传 `None`，不接入实时换算。
    fee_converter: Option<Arc<FeeUsdtConverter>>,
}

impl OrderManager {
    pub fn new(
        bus: Arc<TopicBus>,
        position_manager: Arc<PositionManager>,
        order_store: Arc<dyn OrderStore>,
        fee_converter: Option<Arc<FeeUsdtConverter>>,
    ) -> Self {
        Self {
            bus,
            position_manager,
            client_order_index: Arc::new(Mutex::new(HashMap::new())),
            exchange_order_index: Arc::new(Mutex::new(HashMap::new())),
            order_store,
            fee_converter,
        }
    }

    /// 消费交易所私有 WS 推送的一条订单更新，是订单成交状态的唯一权威来源。
    ///
    /// 流程：
    /// 1. 通过 client_order_id / exchange_order_id 从索引查 OrderId
    /// 2. 如果索引没有，尝试从 Redis 加载订单并建立索引
    /// 3. 从 Redis 读取订单当前状态
    /// 4. 应用更新（幂等、防倒退）
    /// 5. 写回 Redis
    /// 6. 更新风控持仓 + 账本
    /// 7. 发布 OrderEvent
    pub async fn handle_exchange_update(&self, update: ExchangeOrderUpdate) {
        let order_id = match self.resolve_order_id(&update) {
            Some(id) => id,
            None => {
                warn!(
                    "OrderManager: exchange update from venue={} could not be correlated to any order (client_order_id={:?}, exchange_order_id={:?})",
                    update.venue, update.client_order_id, update.exchange_order_id
                );
                return;
            }
        };

        // Transfer 订单没有 WS 回流，handle_exchange_update 不会被调用
        if self.order_store.get(&order_id).map(|o| o.request.as_transfer().is_some()).unwrap_or(false) {
            return;
        }

        // 补录 exchange_order_id 索引
        if let Some(exchange_order_id) = &update.exchange_order_id {
            self.exchange_order_index
                .lock()
                .unwrap()
                .entry(exchange_order_id.clone())
                .or_insert_with(|| order_id.clone());
        }

        // 幂等检查(防倒退/重复)和字段更新放在同一次 `OrderStore::update` 调用
        // 里原子完成，避免像 get()+upsert() 那样两步之间被 ExecutionService
        // 并发写 exchange_order_id 的操作插入，导致互相覆盖丢失更新。
        let mut fill_delta = Decimal::ZERO;
        let new_filled_qty = update.filled_qty;
        let new_status = update.status;
        let new_avg_price = update.avg_price;
        let new_exchange_order_id = &update.exchange_order_id;
        let new_ts_ms = update.ts_ms;
        let outcome = self.order_store.update(
            &order_id,
            Box::new(|order| {
                if new_filled_qty < order.filled_qty {
                    warn!(
                        "OrderManager: ignoring stale exchange update for order_id={order_id}: update filled_qty {} < stored {}",
                        new_filled_qty, order.filled_qty
                    );
                    return false;
                }
                // IOC 单未完全成交就终结 → Expired(自动失效)；GTC 限价单
                // (Limit)同样收到"未完全成交就终结"的推送，只可能是主动撤单
                // → 重新解释成 Cancelled。所有交易所的 map_status 统一吐
                // Expired，判定依据只看这一处的 order_kind，不碰交易所解析代码。
                let effective_new_status = if new_status == OrderStatus::Expired {
                    match order.request.as_trade().map(|trade| &trade.order_kind) {
                        Some(OrderKind::LimitIoc { .. }) | None => OrderStatus::Expired,
                        Some(_) => OrderStatus::Cancelled,
                    }
                } else {
                    new_status
                };
                let already_terminal = matches!(
                    order.status,
                    OrderStatus::Filled | OrderStatus::Rejected | OrderStatus::Expired | OrderStatus::Cancelled
                );
                if already_terminal && new_filled_qty == order.filled_qty && effective_new_status == order.status {
                    return false;
                }

                fill_delta = new_filled_qty - order.filled_qty;
                order.status = effective_new_status;
                order.filled_qty = new_filled_qty;
                if new_avg_price.is_some() {
                    order.avg_price = new_avg_price;
                }
                if let Some(exchange_order_id) = new_exchange_order_id {
                    order.exchange_order_id = Some(exchange_order_id.clone());
                }
                order.updated_at_ms = new_ts_ms;
                true
            }),
        );

        let order = match outcome {
            OrderUpdateOutcome::NotFound => {
                warn!("OrderManager: exchange update for unknown order_id={order_id} (not found in Redis)");
                return;
            }
            OrderUpdateOutcome::Skipped => return,
            OrderUpdateOutcome::Applied(order) => order,
        };

        let trade = match order.request.as_trade() {
            Some(r) => r,
            None => return, // 不会发生：Transfer 订单在前面已经 early return，走 confirm_transfer 而非这条路径
        };
        let venue = trade.venue.clone();
        let symbol = trade.symbol.clone();
        let side = trade.side;
        let strategy_id = trade.strategy_id.clone();
        let status = order.status;
        let filled_qty = order.filled_qty;
        let avg_price = order.avg_price;

        // 更新持仓 + 账本
        if fill_delta > Decimal::ZERO {
            let fee_usdt_sync = match (update.fee, update.fee_asset.as_deref(), &self.fee_converter) {
                (Some(amount), Some(asset), Some(converter)) => {
                    converter.try_resolve_sync(&symbol, amount, asset, avg_price)
                }
                _ => None,
            };

            self.position_manager.on_filled(
                &venue,
                &symbol,
                side,
                fill_delta,
                avg_price,
                update.fee,
                update.fee_asset.clone(),
                fee_usdt_sync,
                update.ts_ms,
            );

            // 同步解不出来但确实有手续费 → 后台异步查价，不阻塞下面的事件发布
            if fee_usdt_sync.is_none() {
                if let (Some(amount), Some(asset), Some(converter)) =
                    (update.fee, update.fee_asset.clone(), self.fee_converter.clone())
                {
                    let venue = venue.clone();
                    let symbol = symbol.clone();
                    let position_manager = self.position_manager.clone();
                    let ts_ms = update.ts_ms;
                    tokio::spawn(async move {
                        if let Some(usdt) = converter.query_async(&venue, &asset, amount).await {
                            // 手续费是成本，冲减已实现盈亏用负数
                            position_manager.apply_adjustment(&venue, &symbol, -usdt, AdjustmentReason::FeeUsdt, ts_ms);
                        }
                    });
                }
            }
        }

        // 发布事件
        let client_order_id = trade.client_order_id.clone();
        let event = match status {
            OrderStatus::PartiallyFilled => Some(OrderEvent::PartiallyFilled {
                order_id: order_id.clone(),
                client_order_id,
                filled_qty,
                avg_price: avg_price.unwrap_or(Decimal::ZERO),
            }),
            OrderStatus::Filled => Some(OrderEvent::Filled {
                order_id: order_id.clone(),
                client_order_id,
                filled_qty,
                avg_price: avg_price.unwrap_or(Decimal::ZERO),
            }),
            OrderStatus::Rejected | OrderStatus::Expired => Some(OrderEvent::RejectedByExchange {
                order_id: order_id.clone(),
                client_order_id,
                reason: format!("exchange order stream reported status={status:?}"),
            }),
            OrderStatus::Cancelled => Some(OrderEvent::Cancelled {
                order_id: order_id.clone(),
                client_order_id,
                filled_qty,
                avg_price: avg_price.unwrap_or(Decimal::ZERO),
            }),
            OrderStatus::New | OrderStatus::Transferred | OrderStatus::DepositConfirmed => None,
        };

        if let Some(event) = event {
            self.bus.publish(Topic::order_event(&strategy_id), event);
        }
    }

    /// `TransferMonitor` 探测到目的地余额变动、认定到账确认后调用的唯一入口。
    /// 把 `Transferred → DepositConfirmed` 的状态推进、事件发布、仓位修正
    /// （按实际到账量修正乐观记账、清掉 pending 标记）收在同一个方法里完成，
    /// 和 `handle_exchange_update` 对交易单的处理是同一种形状，只是触发源不同
    /// （trade 是交易所 WS 推送，transfer 是 `TransferMonitor` 探测到的余额变动）。
    pub fn confirm_transfer(&self, order_id: &OrderId, actual_delta: Decimal, ts_ms: u64) {
        let outcome = self.order_store.update(
            order_id,
            Box::new(|order| {
                if order.status == OrderStatus::Transferred {
                    order.status = OrderStatus::DepositConfirmed;
                    order.updated_at_ms = ts_ms;
                    true
                } else {
                    false
                }
            }),
        );

        let order = match outcome {
            OrderUpdateOutcome::NotFound => {
                warn!("OrderManager: order_id={order_id} not found when confirming transfer deposit");
                return;
            }
            OrderUpdateOutcome::Skipped => {
                warn!(
                    "OrderManager: order_id={order_id} skipped confirming transfer deposit (status was not Transferred)"
                );
                return;
            }
            OrderUpdateOutcome::Applied(order) => order,
        };

        let transfer = match order.request.as_transfer() {
            Some(t) => t,
            None => {
                warn!("OrderManager: order_id={order_id} confirm_transfer called on a non-transfer order");
                return;
            }
        };
        let to_venue = transfer.to_venue.clone();
        let symbol = transfer.symbol.clone();
        let requested_qty = transfer.amount;
        let strategy_id = transfer.strategy_id.clone();
        let client_order_id = transfer.client_order_id.clone();
        let asset = symbol.base.as_ref().to_string();

        self.bus.publish(
            Topic::order_event(&strategy_id),
            OrderEvent::TransferConfirmed {
                order_id: order_id.clone(),
                client_order_id,
                to_venue: to_venue.clone(),
                asset,
                actual_delta,
            },
        );

        let adjustment = self.position_manager.settle_transfer_in(&to_venue, &symbol, requested_qty, actual_delta, ts_ms);
        info!(
            "OrderManager: order_id={order_id} transfer deposit confirmed to_venue={to_venue} symbol={symbol} requested={requested_qty} actual={actual_delta} realized_pnl_adjustment={adjustment}"
        );
    }

    fn resolve_order_id(&self, update: &ExchangeOrderUpdate) -> Option<OrderId> {
        // 先查内存索引
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

        // 内存索引没有，尝试从 Redis 扫描构建索引（慢路径，仅在重启后首次 WS 推送时触发）
        self.rebuild_index_from_redis(update)
    }

    fn rebuild_index_from_redis(&self, update: &ExchangeOrderUpdate) -> Option<OrderId> {
        // 从 Redis 加载所有订单，匹配 client_order_id 或 exchange_order_id
        let all_orders = self.order_store.all();
        for order in all_orders {
            // 构建索引
            if let Some(cid) = order.request.client_order_id() {
                self.client_order_index.lock().unwrap().insert(cid.to_string(), order.order_id.clone());
            }
            if let Some(eid) = &order.exchange_order_id {
                self.exchange_order_index.lock().unwrap().insert(eid.clone(), order.order_id.clone());
            }

            // 匹配当前 update
            if let Some(cid) = &update.client_order_id {
                if order.request.client_order_id() == Some(cid.as_str()) {
                    return Some(order.order_id.clone());
                }
            }
            if let Some(eid) = &update.exchange_order_id {
                if order.exchange_order_id.as_ref() == Some(eid) {
                    return Some(order.order_id.clone());
                }
            }
        }
        None
    }

    /// 查询订单状态（从 Redis 读取）
    pub fn get_order(&self, order_id: &OrderId) -> Option<Order> {
        self.order_store.get(order_id)
    }

    /// 按 client_order_id 反查订单（先查内存索引，查不到从 Redis 扫描）
    pub fn find_by_client_order_id(&self, client_order_id: &str) -> Option<Order> {
        if let Some(order_id) = self.client_order_index.lock().unwrap().get(client_order_id).cloned() {
            return self.order_store.get(&order_id);
        }

        // 内存索引没有，从 Redis 扫描
        let all_orders = self.order_store.all();
        for order in all_orders {
            if order.request.client_order_id() == Some(client_order_id) {
                self.client_order_index.lock().unwrap().insert(client_order_id.to_string(), order.order_id.clone());
                return Some(order);
            }
        }
        None
    }

    /// 把一笔从 Redis 读出来的历史订单加载到内存索引，仅用于 reconcile-order 命令
    pub fn seed_order(&self, order: Order) {
        if let Some(cid) = order.request.client_order_id() {
            self.client_order_index.lock().unwrap().insert(cid.to_string(), order.order_id.clone());
        }
        if let Some(exchange_order_id) = &order.exchange_order_id {
            self.exchange_order_index.lock().unwrap().insert(exchange_order_id.clone(), order.order_id.clone());
        }
    }

    pub fn all_orders(&self) -> Vec<Order> {
        self.order_store.all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    use crate::order::types::OrderSide;
    use crate::order_manager::store::InMemoryOrderStore;
    use crate::order_manager::types::{AnyOrderRequest, OrderRequest, TransferRequest};
    use crate::order_manager::stream::ExchangeOrderUpdate;
    use crate::position::InMemoryPositionStore;
    use crate::types::{Symbol, Venue};

    fn trade_order(order_id: &str, order_kind: OrderKind, client_order_id: &str) -> Order {
        Order {
            order_id: OrderId::new(order_id),
            request: AnyOrderRequest::Trade(OrderRequest {
                strategy_id: "test-strategy".to_string(),
                venue: Venue::new("kraken_spot"),
                symbol: Symbol::new("BTC", "USDT"),
                side: OrderSide::Buy,
                amount: crate::order::types::OrderAmount::Base(Decimal::ONE),
                order_kind,
                client_order_id: Some(client_order_id.to_string()),
                group_id: None,
                metadata: None,
                order_id: Some(OrderId::new(order_id)),
            }),
            status: OrderStatus::New,
            filled_qty: Decimal::ZERO,
            avg_price: None,
            exchange_order_id: Some(format!("EX-{order_id}")),
            created_at_ms: 1,
            updated_at_ms: 1,
            reject_reason: None,
        }
    }

    fn transfer_order(order_id: &str, status: OrderStatus) -> Order {
        Order {
            order_id: OrderId::new(order_id),
            request: AnyOrderRequest::Transfer(TransferRequest {
                strategy_id: "test-strategy".to_string(),
                from_venue: Venue::new("okx_spot"),
                to_venue: Venue::new("binance_spot"),
                symbol: Symbol::new("BTC", "USDT"),
                amount: Decimal::ONE,
                network: None,
                dry_run: false,
                client_order_id: None,
                group_id: None,
                metadata: None,
                order_id: Some(OrderId::new(order_id)),
            }),
            status,
            filled_qty: Decimal::ONE,
            avg_price: None,
            exchange_order_id: None,
            created_at_ms: 1,
            updated_at_ms: 1,
            reject_reason: None,
        }
    }

    fn manager_with_order(order: Order) -> OrderManager {
        let bus = Arc::new(TopicBus::new());
        let position_manager = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let order_store: Arc<dyn OrderStore> = Arc::new(InMemoryOrderStore::new());
        order_store.upsert(order);
        OrderManager::new(bus, position_manager, order_store, None)
    }

    #[tokio::test]
    async fn confirm_transfer_advances_status_publishes_event_and_settles_position() {
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");
        let order_id = OrderId::new("ORD-1");
        let om = manager_with_order(transfer_order("ORD-1", OrderStatus::Transferred));

        // 模拟 ExecutionService::handle_transfer 在提币受理时已经做过的乐观记账 + pending 标记
        om.position_manager.on_filled(
            &venue,
            &symbol,
            OrderSide::Buy,
            Decimal::ONE,
            Some(Decimal::new(50000, 0)),
            None,
            None,
            None,
            1,
        );
        om.position_manager.mark_transfer_pending(&venue, &symbol, Decimal::ONE, 1);

        let mut events = om.bus.subscribe::<OrderEvent>(Topic::order_event("test-strategy"));

        om.confirm_transfer(&order_id, Decimal::new(98, 2), 2);

        let (_, event) = events.next().await.unwrap();
        match event {
            OrderEvent::TransferConfirmed { order_id: id, to_venue, asset, actual_delta, .. } => {
                assert_eq!(id, order_id);
                assert_eq!(to_venue, venue);
                assert_eq!(asset, "BTC");
                assert_eq!(actual_delta, Decimal::new(98, 2));
            }
            other => panic!("unexpected event: {other:?}"),
        }

        let stored = om.order_store.get(&order_id).unwrap();
        assert_eq!(stored.status, OrderStatus::DepositConfirmed);

        let pos = om.position_manager.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::new(98, 2));
        assert_eq!(pos.pending_qty, Decimal::ZERO);
    }

    #[tokio::test]
    async fn confirm_transfer_skips_when_status_is_not_transferred() {
        let order_id = OrderId::new("ORD-2");
        let om = manager_with_order(transfer_order("ORD-2", OrderStatus::DepositConfirmed));

        om.confirm_transfer(&order_id, Decimal::ONE, 2);

        // 状态不是 Transferred(已经被确认过/竞态)：不应重复推进状态或修正仓位
        let stored = om.order_store.get(&order_id).unwrap();
        assert_eq!(stored.status, OrderStatus::DepositConfirmed);
        assert_eq!(stored.updated_at_ms, 1);
    }

    fn expired_update(client_order_id: &str, exchange_order_id: &str) -> ExchangeOrderUpdate {
        ExchangeOrderUpdate {
            venue: Venue::new("kraken_spot"),
            symbol: None,
            client_order_id: Some(client_order_id.to_string()),
            exchange_order_id: Some(exchange_order_id.to_string()),
            status: OrderStatus::Expired,
            filled_qty: Decimal::ZERO,
            avg_price: None,
            fee: None,
            fee_asset: None,
            ts_ms: 2,
        }
    }

    #[tokio::test]
    async fn limit_ioc_expired_wire_status_stays_expired_and_publishes_rejected() {
        let order_id = OrderId::new("ORD-IOC");
        let om = manager_with_order(trade_order(
            "ORD-IOC",
            OrderKind::LimitIoc { price: Decimal::new(50000, 0) },
            "cid-ioc",
        ));

        let mut events = om.bus.subscribe::<OrderEvent>(Topic::order_event("test-strategy"));
        om.handle_exchange_update(expired_update("cid-ioc", "EX-ORD-IOC")).await;

        let stored = om.order_store.get(&order_id).unwrap();
        assert_eq!(stored.status, OrderStatus::Expired);

        let (_, event) = events.next().await.unwrap();
        match event {
            OrderEvent::RejectedByExchange { order_id: id, .. } => assert_eq!(id, order_id),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn gtc_limit_expired_wire_status_becomes_cancelled_and_publishes_cancelled() {
        let order_id = OrderId::new("ORD-GTC");
        let om = manager_with_order(trade_order(
            "ORD-GTC",
            OrderKind::Limit { price: Decimal::new(50000, 0) },
            "cid-gtc",
        ));

        let mut events = om.bus.subscribe::<OrderEvent>(Topic::order_event("test-strategy"));
        om.handle_exchange_update(expired_update("cid-gtc", "EX-ORD-GTC")).await;

        let stored = om.order_store.get(&order_id).unwrap();
        assert_eq!(stored.status, OrderStatus::Cancelled);

        let (_, event) = events.next().await.unwrap();
        match event {
            OrderEvent::Cancelled { order_id: id, .. } => assert_eq!(id, order_id),
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
