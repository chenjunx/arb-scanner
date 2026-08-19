use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use log::warn;
use rust_decimal::Decimal;

use crate::order::types::OrderStatus;
use crate::portfolio::PortfolioManager;
use crate::position::PositionManager;
use crate::pricing::FeeUsdtConverter;
use crate::topic::{Topic, TopicBus};

use super::store::{OrderStore, OrderUpdateOutcome};
use super::stream::ExchangeOrderUpdate;
use super::types::{Order, OrderEvent, OrderId};

/// 订单管理器（重构后）：只负责处理交易所 WS 推送的订单更新，
/// 是订单成交状态的唯一权威来源。订单初始创建由 RiskService 写入 Redis，
/// ExecutionService REST 下单后也直接更新 Redis 写入 exchange_order_id，
/// OrderManager 只负责：WS 更新 → 读 Redis → 更新 → 写回 Redis → 更新仓位/账本 → 发布事件。
/// 仓位是成交的直接结果，因此由 OrderManager 直接写入 `PositionManager`——
/// `RiskService` 只在下单前读仓位做限额检查，不再经手写入，避免读写分家。
pub struct OrderManager {
    bus: Arc<TopicBus>,
    position_manager: Arc<PositionManager>,
    portfolio: Arc<PortfolioManager>,
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
        portfolio: Arc<PortfolioManager>,
        order_store: Arc<dyn OrderStore>,
        fee_converter: Option<Arc<FeeUsdtConverter>>,
    ) -> Self {
        Self {
            bus,
            position_manager,
            portfolio,
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
                let already_terminal = matches!(
                    order.status,
                    OrderStatus::Filled | OrderStatus::Rejected | OrderStatus::Expired
                );
                if already_terminal && new_filled_qty == order.filled_qty && new_status == order.status {
                    return false;
                }

                fill_delta = new_filled_qty - order.filled_qty;
                order.status = new_status;
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

        let venue = order.request.venue.clone();
        let symbol = order.request.symbol.clone();
        let side = order.request.side;
        let strategy_id = order.request.strategy_id.clone();
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

            let outcome = self.position_manager.on_filled(
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
            self.portfolio.record_fill(
                &venue,
                &symbol,
                fill_delta,
                avg_price,
                update.fee,
                fee_usdt_sync,
                outcome.realized_pnl,
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
                    let portfolio = self.portfolio.clone();
                    tokio::spawn(async move {
                        match converter.query_async(&venue, &asset, amount).await {
                            Some(usdt) => {
                                position_manager.apply_fee_usdt(&venue, &symbol, usdt);
                                portfolio.apply_fee_usdt(&venue, &symbol, usdt);
                            }
                            None => {
                                position_manager.mark_fee_usdt_incomplete(&venue, &symbol);
                                portfolio.mark_fee_usdt_incomplete(&venue, &symbol);
                            }
                        }
                    });
                }
            }
        }

        // 发布事件
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
            self.bus.publish(Topic::order_event(&strategy_id), event);
        }
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
            if let Some(cid) = &order.request.client_order_id {
                self.client_order_index.lock().unwrap().insert(cid.clone(), order.order_id.clone());
            }
            if let Some(eid) = &order.exchange_order_id {
                self.exchange_order_index.lock().unwrap().insert(eid.clone(), order.order_id.clone());
            }

            // 匹配当前 update
            if let Some(cid) = &update.client_order_id {
                if order.request.client_order_id.as_ref() == Some(cid) {
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
            if order.request.client_order_id.as_deref() == Some(client_order_id) {
                // 顺便建立索引
                self.client_order_index.lock().unwrap().insert(client_order_id.to_string(), order.order_id.clone());
                return Some(order);
            }
        }
        None
    }

    /// 把一笔从 Redis 读出来的历史订单加载到内存索引，仅用于 reconcile-order 命令
    pub fn seed_order(&self, order: Order) {
        if let Some(client_order_id) = &order.request.client_order_id {
            self.client_order_index
                .lock()
                .unwrap()
                .insert(client_order_id.clone(), order.order_id.clone());
        }
        if let Some(exchange_order_id) = &order.exchange_order_id {
            self.exchange_order_index
                .lock()
                .unwrap()
                .insert(exchange_order_id.clone(), order.order_id.clone());
        }
    }

    pub fn all_orders(&self) -> Vec<Order> {
        self.order_store.all()
    }
}
