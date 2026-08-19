use std::collections::HashMap;
use std::sync::Mutex;

use super::types::{Order, OrderId};

/// `OrderStore::update` 的结果：区分"订单不存在"和"订单存在但 `f` 判断不需要
/// 写回"（比如过期/重复的交易所更新），调用方据此决定是否要继续发布事件、
/// 记账等下游动作。
pub enum OrderUpdateOutcome {
    NotFound,
    Skipped,
    Applied(Order),
}

/// 订单历史持久化接口。和 `position::PositionStore`/`portfolio::PnlStore` 同一套
/// 设计语言。`upsert` 用于整体覆盖写入(创建新订单、或调用方已经拿到最新状态
/// 时)；`update` 用于需要原子读改写的场景——`ExecutionService`(REST 下单成功
/// 后写 `exchange_order_id`)和 `OrderManager`(WS 推送驱动的成交状态)可能
/// 并发更新同一个 order_id，如果各自 `get()` 再 `upsert()`，两次读之间可能被
/// 对方的写入插入，导致后写的覆盖掉先写的字段(丢失更新)。`update` 把整个
/// 读改写过程放在实现内部的同一把锁下完成，`f` 返回 `false` 表示不需要写回。
pub trait OrderStore: Send + Sync {
    fn all(&self) -> Vec<Order>;
    fn get(&self, order_id: &OrderId) -> Option<Order>;
    fn upsert(&self, order: Order);
    fn update(&self, order_id: &OrderId, f: Box<dyn FnOnce(&mut Order) -> bool + Send + '_>) -> OrderUpdateOutcome;
}

/// 纯内存实现，重启即丢。
#[derive(Default)]
pub struct InMemoryOrderStore {
    orders: Mutex<HashMap<OrderId, Order>>,
}

impl InMemoryOrderStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OrderStore for InMemoryOrderStore {
    fn all(&self) -> Vec<Order> {
        self.orders.lock().unwrap().values().cloned().collect()
    }

    fn get(&self, order_id: &OrderId) -> Option<Order> {
        self.orders.lock().unwrap().get(order_id).cloned()
    }

    fn upsert(&self, order: Order) {
        self.orders.lock().unwrap().insert(order.order_id.clone(), order);
    }

    fn update(&self, order_id: &OrderId, f: Box<dyn FnOnce(&mut Order) -> bool + Send + '_>) -> OrderUpdateOutcome {
        let mut orders = self.orders.lock().unwrap();
        match orders.get_mut(order_id) {
            None => OrderUpdateOutcome::NotFound,
            Some(order) => {
                if f(order) {
                    OrderUpdateOutcome::Applied(order.clone())
                } else {
                    OrderUpdateOutcome::Skipped
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::types::{OrderAmount, OrderSide, OrderStatus};
    use crate::order_manager::types::OrderRequest;
    use crate::types::{Symbol, Venue};
    use rust_decimal::Decimal;

    fn sample_order(order_id: &str) -> Order {
        Order {
            order_id: OrderId::new(order_id),
            request: OrderRequest {
                strategy_id: "test".to_string(),
                venue: Venue::new("binance_spot"),
                symbol: Symbol::new("BTC", "USDT"),
                side: OrderSide::Buy,
                amount: OrderAmount::Base(Decimal::ONE),
                client_order_id: None,
                group_id: None,
                metadata: None,
                order_id: None,
            },
            status: OrderStatus::New,
            filled_qty: Decimal::ZERO,
            avg_price: None,
            exchange_order_id: None,
            created_at_ms: 1,
            updated_at_ms: 1,
            reject_reason: None,
        }
    }

    #[test]
    fn upsert_overwrites_existing_entry() {
        let store = InMemoryOrderStore::new();
        let order_id = OrderId::new("ORD-1");

        store.upsert(sample_order("ORD-1"));
        let mut filled = sample_order("ORD-1");
        filled.status = OrderStatus::Filled;
        filled.filled_qty = Decimal::ONE;
        store.upsert(filled);

        let stored = store.get(&order_id).unwrap();
        assert_eq!(stored.status, OrderStatus::Filled);
        assert_eq!(stored.filled_qty, Decimal::ONE);
        assert_eq!(store.all().len(), 1);
    }
}
