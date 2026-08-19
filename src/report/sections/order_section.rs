use std::sync::Arc;

use crate::order::types::{OrderAmount, OrderStatus};
use crate::order_manager::OrderStore;
use crate::report::section::ReportSection;

const MAX_OPEN_ORDERS_LISTED: usize = 20;

/// 订单状态汇总计数 + 当前挂单（New/PartiallyFilled）明细。直接依赖
/// `OrderStore`（而非 `OrderManager`）——`OrderManager::all_orders()` 只读它
/// 自己进程内维护的内存状态，不会回读 `order_store`；`RedisOrderStore::all()`
/// 才是真正跨进程可见的实时数据源。
pub struct OrderSection {
    order_store: Arc<dyn OrderStore>,
}

impl OrderSection {
    pub fn new(order_store: Arc<dyn OrderStore>) -> Self {
        Self { order_store }
    }
}

impl ReportSection for OrderSection {
    fn title(&self) -> &str {
        "订单概览"
    }

    fn render(&self) -> String {
        let orders = self.order_store.all();
        if orders.is_empty() {
            return "(暂无订单记录)".to_string();
        }

        let count = |status: OrderStatus| orders.iter().filter(|o| o.status == status).count();
        let summary = format!(
            "共 {} 笔订单: New={} PartiallyFilled={} Filled={} Rejected={} Expired={}",
            orders.len(),
            count(OrderStatus::New),
            count(OrderStatus::PartiallyFilled),
            count(OrderStatus::Filled),
            count(OrderStatus::Rejected),
            count(OrderStatus::Expired),
        );

        let mut open_orders: Vec<_> = orders
            .into_iter()
            .filter(|o| matches!(o.status, OrderStatus::New | OrderStatus::PartiallyFilled))
            .collect();
        open_orders.sort_by_key(|o| std::cmp::Reverse(o.created_at_ms));

        if open_orders.is_empty() {
            return format!("{summary}\n(当前无挂单)");
        }

        let total_open = open_orders.len();
        let shown = open_orders.into_iter().take(MAX_OPEN_ORDERS_LISTED);
        let mut lines = vec![summary];
        if total_open > MAX_OPEN_ORDERS_LISTED {
            lines.push(format!("当前挂单(仅显示最近 {MAX_OPEN_ORDERS_LISTED} 条，共 {total_open} 条):"));
        } else {
            lines.push("当前挂单:".to_string());
        }
        for order in shown {
            // filled_qty 始终是成交的 base 数量；下单量 amount 可能是 base 也可能是
            // quote(按金额下单)，两者单位不一致时不能直接拼成一个分数，否则会把
            // quote 金额误读成 base 数量(例如"以 10 USDT 下单"显示成 filled=0/10)。
            let base = &order.request.symbol.base;
            let filled_vs_target = match order.request.amount {
                OrderAmount::Base(target) => format!("filled={} {base}/{} {base}", order.filled_qty, target),
                OrderAmount::Quote(target) => {
                    let quote = &order.request.symbol.quote;
                    format!("filled={} {base} target={} {quote}", order.filled_qty, target)
                }
            };
            lines.push(format!(
                "  {} {} {} {:?} side={:?} {}",
                order.order_id, order.request.venue, order.request.symbol, order.status, order.request.side, filled_vs_target,
            ));
        }
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::order::types::{OrderAmount, OrderSide};
    use crate::order_manager::store::InMemoryOrderStore;
    use crate::order_manager::types::{Order, OrderId, OrderRequest};
    use crate::types::{Symbol, Venue};

    fn order(id: &str, status: OrderStatus, created_at_ms: u64) -> Order {
        Order {
            order_id: OrderId::new(id),
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
            status,
            filled_qty: Decimal::ZERO,
            avg_price: None,
            exchange_order_id: None,
            created_at_ms,
            updated_at_ms: created_at_ms,
            reject_reason: None,
        }
    }

    #[test]
    fn renders_placeholder_when_no_orders() {
        let store: Arc<dyn OrderStore> = Arc::new(InMemoryOrderStore::new());
        let section = OrderSection::new(store);
        assert_eq!(section.render(), "(暂无订单记录)");
    }

    #[test]
    fn summarizes_counts_and_lists_open_orders_newest_first() {
        let store = InMemoryOrderStore::new();
        store.upsert(order("ORD-1", OrderStatus::Filled, 1));
        store.upsert(order("ORD-2", OrderStatus::New, 2));
        store.upsert(order("ORD-3", OrderStatus::PartiallyFilled, 3));
        let store: Arc<dyn OrderStore> = Arc::new(store);

        let section = OrderSection::new(store);
        let body = section.render();
        assert!(body.contains("共 3 笔订单: New=1 PartiallyFilled=1 Filled=1 Rejected=0 Expired=0"), "body was: {body}");
        let ord2_pos = body.find("ORD-2").unwrap();
        let ord3_pos = body.find("ORD-3").unwrap();
        assert!(ord3_pos < ord2_pos, "ORD-3(更新) 应该排在 ORD-2 前面: {body}");
        assert!(!body.contains("ORD-1"), "已成交订单不应出现在挂单列表: {body}");
    }
}
