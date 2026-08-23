use std::sync::Arc;

use futures_util::StreamExt;
use log::{info, warn};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::order::types::OrderStatus;
use crate::topic::{Topic, TopicBus};
use crate::types::Venue;
use crate::wallet::balance_stream::BalanceUpdate;

use super::store::OrderStore;
use super::types::{OrderEvent, OrderId};

const TOLERANCE: Decimal = Decimal::from_parts(5, 0, 0, false, 2); // 0.05

/// 划转到账监控器：订阅 `Topic::BalanceUpdate`，宽松匹配状态为 `Filled` 的
/// Transfer 订单。当余额增量与划转量之差在 5% 以内时，认为到账，更新订单状态
/// 为 `DepositConfirmed` 并发布 `OrderEvent::TransferConfirmed`。
///
/// 位置更新已在提币成功时乐观完成，本服务只做到账确认通知。
pub struct TransferMonitor {
    bus: Arc<TopicBus>,
    order_store: Arc<dyn OrderStore>,
    venues: Vec<Venue>,
}

impl TransferMonitor {
    pub fn new(bus: Arc<TopicBus>, order_store: Arc<dyn OrderStore>, venues: Vec<Venue>) -> Self {
        Self { bus, order_store, venues }
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

        let orders = self.order_store.all();
        for order in orders {
            let transfer = match order.request.as_transfer() {
                Some(t) => t,
                None => continue,
            };

            if order.status != OrderStatus::Filled {
                continue;
            }
            if transfer.to_venue != update.venue {
                continue;
            }
            if transfer.symbol.base.as_ref() != update.asset.as_str() {
                continue;
            }

            let deviation = (update.delta - transfer.amount).abs();
            let within_tolerance = if transfer.amount.is_zero() {
                update.delta.is_zero()
            } else {
                deviation / transfer.amount <= TOLERANCE
            };

            if !within_tolerance {
                continue;
            }

            let order_id = order.order_id.clone();
            let strategy_id = transfer.strategy_id.clone();
            let to_venue = transfer.to_venue.clone();
            let asset = update.asset.clone();
            let actual_delta = update.delta;

            self.confirm_order(&order_id);
            info!(
                "TransferMonitor: deposit confirmed order_id={order_id} to_venue={to_venue} \
                 asset={asset} actual_delta={actual_delta} expected={}",
                transfer.amount
            );

            self.bus.publish(
                Topic::order_event(&strategy_id),
                OrderEvent::TransferConfirmed { order_id, to_venue, asset, actual_delta },
            );

            // 一条余额变动事件只确认最早一笔匹配的划转（避免金额巧合时重复确认）
            return;
        }
    }

    fn confirm_order(&self, order_id: &OrderId) {
        match self.order_store.update(
            order_id,
            Box::new(|order| {
                if order.status == OrderStatus::Filled {
                    order.status = OrderStatus::DepositConfirmed;
                    true
                } else {
                    false
                }
            }),
        ) {
            super::store::OrderUpdateOutcome::NotFound => {
                warn!("TransferMonitor: order_id={order_id} not found when confirming deposit");
            }
            super::store::OrderUpdateOutcome::Skipped => {
                warn!("TransferMonitor: order_id={order_id} skipped (status already changed)");
            }
            super::store::OrderUpdateOutcome::Applied(_) => {}
        }
    }
}
