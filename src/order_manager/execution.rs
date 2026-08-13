use std::collections::HashMap;
use std::sync::Arc;

use log::{error, info};
use tokio::sync::mpsc;

use crate::order::OrderProvider;
use crate::order::types::{MarketOrderRequest, OrderResult};
use crate::types::Venue;

use super::types::{Order, OrderEvent};

/// 交易所适配器：持有某个 venue 的 OrderProvider，
/// 负责把内部订单转换成交易所请求并实际下单。
pub struct ExchangeAdapter {
    venue: Venue,
    provider: Arc<dyn OrderProvider>,
}

impl ExchangeAdapter {
    pub fn new(venue: Venue, provider: Arc<dyn OrderProvider>) -> Self {
        Self { venue, provider }
    }

    pub fn venue(&self) -> &Venue {
        &self.venue
    }

    /// 提交订单到交易所，返回交易所的原始响应。
    pub async fn submit(&self, order: &Order) -> anyhow::Result<OrderResult> {
        let req = MarketOrderRequest {
            symbol: order.request.symbol.clone(),
            side: order.request.side,
            amount: order.request.amount,
            client_order_id: order.request.client_order_id.clone(),
            dry_run: false,
        };
        self.provider.place_market_order(req).await
    }
}

/// 执行引擎：根据订单的目标 venue 路由到对应的交易所适配器，
/// 并将交易所返回的成交事件发送给事件总线。
pub struct ExecutionEngine {
    adapters: HashMap<Venue, Arc<ExchangeAdapter>>,
    event_tx: mpsc::Sender<OrderEvent>,
}

impl ExecutionEngine {
    pub fn new(
        adapters: HashMap<Venue, Arc<ExchangeAdapter>>,
        event_tx: mpsc::Sender<OrderEvent>,
    ) -> Self {
        Self { adapters, event_tx }
    }

    /// 执行订单：找到目标 venue 的适配器，提交订单，发送 Accepted 事件。
    /// 只有交易所在下单当下就同步拒绝(`OrderStatus::Rejected`)时才在这里发
    /// `RejectedByExchange` 事件——`Filled`/`PartiallyFilled` 不再由 REST 响应
    /// 驱动，两个交易所的成交都统一由交易所私有 WS 通过
    /// `OrderManager::handle_exchange_update` 确认，避免两条路径竞态写同一个
    /// `Order`(也因为 Kraken 的 `AddOrder` 本来就不会同步带回成交结果)。
    pub async fn execute(&self, mut order: Order) -> Order {
        let adapter = match self.adapters.get(&order.request.venue) {
            Some(a) => a,
            None => {
                let reason = format!("no adapter registered for venue {}", order.request.venue);
                error!("ExecutionEngine: order_id={} {reason}", order.order_id);
                order.status = crate::order::types::OrderStatus::Rejected;
                order.reject_reason = Some(reason.clone());
                let _ = self.event_tx.send(OrderEvent::RejectedByExchange {
                    order_id: order.order_id.clone(),
                    reason,
                }).await;
                return order;
            }
        };

        info!(
            "ExecutionEngine: submitting order_id={} to venue={} symbol={} side={:?} amount={:?}",
            order.order_id, adapter.venue(), order.request.symbol, order.request.side, order.request.amount
        );

        // 通知已接受（发送到交易所）
        let _ = self.event_tx.send(OrderEvent::Accepted {
            order_id: order.order_id.clone(),
        }).await;

        match adapter.submit(&order).await {
            Ok(result) => {
                info!(
                    "ExecutionEngine: order_id={} exchange_order_id={} status={:?} filled_qty={}",
                    order.order_id, result.order_id, result.status, result.filled_qty
                );

                order.exchange_order_id = Some(result.order_id);
                order.updated_at_ms = current_timestamp_ms();

                if result.status == crate::order::types::OrderStatus::Rejected {
                    let reason = format!("exchange rejected: status={:?}", result.status);
                    order.status = result.status;
                    order.reject_reason = Some(reason.clone());
                    let _ = self.event_tx.send(OrderEvent::RejectedByExchange {
                        order_id: order.order_id.clone(),
                        reason,
                    }).await;
                }

                order
            }
            Err(err) => {
                let reason = format!("exchange error: {err:#}");
                error!("ExecutionEngine: order_id={} {reason}", order.order_id);
                order.status = crate::order::types::OrderStatus::Rejected;
                order.reject_reason = Some(reason.clone());
                order.updated_at_ms = current_timestamp_ms();
                let _ = self.event_tx.send(OrderEvent::RejectedByExchange {
                    order_id: order.order_id.clone(),
                    reason,
                }).await;
                order
            }
        }
    }
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
