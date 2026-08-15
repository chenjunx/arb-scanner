use std::collections::HashMap;
use std::sync::Mutex;

use super::types::{Order, OrderId};

/// 订单历史持久化接口。和 `position::PositionStore`/`portfolio::PnlStore` 同一套
/// 设计语言，但用 `upsert` 而不是原子读改写的 `update`——`Order` 在生命周期内
/// (New -> PartiallyFilled -> Filled)会被反复整体覆盖写入，不需要基于旧值计算
/// 新值，直接覆盖即可。
pub trait OrderStore: Send + Sync {
    fn all(&self) -> Vec<Order>;
    fn get(&self, order_id: &OrderId) -> Option<Order>;
    fn upsert(&self, order: Order);
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
                strategy_name: "test".to_string(),
                venue: Venue::new("binance_spot"),
                symbol: Symbol::new("BTC", "USDT"),
                side: OrderSide::Buy,
                amount: OrderAmount::Base(Decimal::ONE),
                client_order_id: None,
                group_id: None,
                metadata: None,
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
