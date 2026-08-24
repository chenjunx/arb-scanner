use std::sync::Arc;

use futures_util::StreamExt;
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::accounting::balance_stream::BalanceUpdate;
use crate::order::types::OrderStatus;
use crate::order_manager::store::OrderStore;
use crate::order_manager::OrderManager;
use crate::topic::{Topic, TopicBus};
use crate::types::Venue;

const TOLERANCE: Decimal = Decimal::from_parts(5, 0, 0, false, 2); // 0.05

/// 划转到账监控器：订阅 `Topic::BalanceUpdate`，宽松匹配状态为 `Transferred` 的
/// Transfer 订单。当余额增量与划转量之差在 5% 以内时，认为到账，把这件事交给
/// `OrderManager::confirm_transfer` 完成"状态推进 → 发布完成事件 → 按实际到账量
/// 修正仓位"这一整套动作——本服务只做纯探测/匹配，不直接改 order_store，也不碰
/// `PositionManager`。
pub struct TransferMonitor {
    bus: Arc<TopicBus>,
    order_store: Arc<dyn OrderStore>,
    order_manager: Arc<OrderManager>,
    venues: Vec<Venue>,
}

impl TransferMonitor {
    pub fn new(
        bus: Arc<TopicBus>,
        order_store: Arc<dyn OrderStore>,
        order_manager: Arc<OrderManager>,
        venues: Vec<Venue>,
    ) -> Self {
        Self { bus, order_store, order_manager, venues }
    }

    /// 为每个 venue 启动一个订阅任务，返回所有任务句柄。
    pub fn start(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        self.venues
            .iter()
            .cloned()
            .map(|venue| {
                let monitor = self.clone();
                tokio::spawn(async move {
                    let mut stream = monitor.bus.subscribe::<BalanceUpdate>(Topic::balance_update(venue));
                    while let Some((_topic, update)) = stream.next().await {
                        monitor.handle_balance_update(update).await;
                    }
                })
            })
            .collect()
    }

    async fn handle_balance_update(&self, update: BalanceUpdate) {
        if update.delta <= Decimal::ZERO {
            return;
        }

        let mut candidates: Vec<_> = self
            .order_store
            .all()
            .into_iter()
            .filter(|order| {
                let transfer = match order.request.as_transfer() {
                    Some(t) => t,
                    None => return false,
                };
                order.status == OrderStatus::Transferred
                    && transfer.to_venue == update.venue
                    && transfer.symbol.base.as_ref() == update.asset.as_str()
            })
            .collect();
        // 按创建时间升序排列，确定性地优先匹配最早提交的划转单——避免
        // `order_store.all()` 顺序未定义时把仓位修正记到错误的订单上。
        candidates.sort_by_key(|order| order.created_at_ms);

        for order in candidates {
            let transfer = order.request.as_transfer().expect("filtered above");

            let deviation = (update.delta - transfer.amount).abs();
            let within_tolerance = if transfer.amount.is_zero() {
                update.delta.is_zero()
            } else {
                deviation / transfer.amount <= TOLERANCE
            };

            if !within_tolerance {
                continue;
            }

            self.order_manager.confirm_transfer(&order.order_id, update.delta, update.ts_ms);

            // 一条余额变动事件只确认最早一笔匹配的划转（避免金额巧合时重复确认）
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::order_manager::store::InMemoryOrderStore;
    use crate::order_manager::types::{AnyOrderRequest, Order, OrderId, TransferRequest};
    use crate::order_manager::OrderManager;
    use crate::position::{InMemoryPositionStore, PositionManager};
    use crate::types::Symbol;

    fn transfer_order(order_id: &str, created_at_ms: u64, to_venue: Venue, symbol: Symbol, amount: Decimal) -> Order {
        Order {
            order_id: OrderId::new(order_id),
            request: AnyOrderRequest::Transfer(TransferRequest {
                strategy_id: "test-strategy".to_string(),
                from_venue: Venue::new("okx_spot"),
                to_venue,
                symbol,
                amount,
                network: None,
                dry_run: false,
                client_order_id: None,
                group_id: None,
                metadata: None,
                order_id: Some(OrderId::new(order_id)),
            }),
            status: OrderStatus::Transferred,
            filled_qty: amount,
            avg_price: None,
            exchange_order_id: None,
            created_at_ms,
            updated_at_ms: created_at_ms,
            reject_reason: None,
        }
    }

    #[tokio::test]
    async fn confirms_earliest_created_order_among_matching_candidates() {
        let bus = Arc::new(TopicBus::new());
        let order_store: Arc<dyn OrderStore> = Arc::new(InMemoryOrderStore::new());
        let position_manager = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let order_manager = Arc::new(OrderManager::new(bus.clone(), position_manager, order_store.clone(), None));

        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        // 乱序插入：先插入 created_at_ms 更晚的订单，再插入更早的，验证匹配不依赖插入顺序
        order_store.upsert(transfer_order("ORD-LATE", 200, venue.clone(), symbol.clone(), Decimal::ONE));
        order_store.upsert(transfer_order("ORD-EARLY", 100, venue.clone(), symbol.clone(), Decimal::ONE));

        let monitor = Arc::new(TransferMonitor::new(bus.clone(), order_store.clone(), order_manager, vec![venue.clone()]));

        monitor
            .handle_balance_update(BalanceUpdate {
                venue: venue.clone(),
                asset: "BTC".to_string(),
                delta: Decimal::ONE,
                ts_ms: 300,
            })
            .await;

        let early = order_store.get(&OrderId::new("ORD-EARLY")).unwrap();
        let late = order_store.get(&OrderId::new("ORD-LATE")).unwrap();
        assert_eq!(early.status, OrderStatus::DepositConfirmed);
        assert_eq!(late.status, OrderStatus::Transferred);
    }
}
