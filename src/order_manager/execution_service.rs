use std::collections::HashMap;
use std::sync::Arc;

use futures_util::StreamExt;
use log::{error, info, warn};
use tokio::task::JoinHandle;

use crate::order::OrderProvider;
use crate::order::types::{MarketOrderRequest, OrderResult, OrderStatus};
use crate::topic::{Topic, TopicBus};
use crate::types::Venue;

use super::store::OrderStore;
use super::types::{Order, OrderEvent, OrderRequest};

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

/// 执行服务：订阅 `Topic::OrderExecute`，根据 venue 路由到对应的
/// `ExchangeAdapter` 下单，REST 成功后立即更新 Redis 写入 `exchange_order_id`，
/// 然后将结果（Accepted 或 RejectedByExchange）发布到 `Topic::OrderEvent{strategy_id}`。
///
/// 只有交易所在下单当下就同步拒绝时才发 `RejectedByExchange` 事件——
/// `Filled`/`PartiallyFilled` 由交易所私有 WS 通过
/// `OrderManager::handle_exchange_update` 驱动。
pub struct ExecutionService {
    bus: Arc<TopicBus>,
    adapters: HashMap<Venue, Arc<ExchangeAdapter>>,
    order_store: Arc<dyn OrderStore>,
}

impl ExecutionService {
    pub fn new(
        bus: Arc<TopicBus>,
        adapters: HashMap<Venue, Arc<ExchangeAdapter>>,
        order_store: Arc<dyn OrderStore>,
    ) -> Self {
        Self { bus, adapters, order_store }
    }

    /// 启动服务，订阅 `Topic::OrderExecute` 并处理每个订单请求
    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut stream = self.bus.subscribe::<OrderRequest>(Topic::order_execute());
            while let Some((_topic, request)) = stream.next().await {
                self.handle_order_request(request).await;
            }
        })
    }

    async fn handle_order_request(&self, request: OrderRequest) {
        // order_id 必须由 RiskService 提前填入
        let Some(order_id) = request.order_id.clone() else {
            error!(
                "ExecutionService: received OrderRequest without order_id for strategy={} venue={} symbol={}",
                request.strategy_id, request.venue, request.symbol
            );
            return;
        };

        let adapter = match self.adapters.get(&request.venue) {
            Some(a) => a,
            None => {
                let reason = format!("no adapter registered for venue {}", request.venue);
                error!(
                    "ExecutionService: order_id={} strategy={} venue={} symbol={} {reason}",
                    order_id, request.strategy_id, request.venue, request.symbol
                );
                self.publish_rejected(&request, order_id.clone(), reason);
                return;
            }
        };

        info!(
            "ExecutionService: order_id={} submitting strategy={} venue={} symbol={} side={:?} amount={:?} client_order_id={:?}",
            order_id, request.strategy_id, adapter.venue(), request.symbol, request.side, request.amount, request.client_order_id
        );

        // 构造临时 Order 用于调用 adapter.submit()（adapter 需要 Order 结构）
        let temp_order = super::types::Order {
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

        match adapter.submit(&temp_order).await {
            Ok(result) => {
                info!(
                    "ExecutionService: order_id={} strategy={} venue={} symbol={} exchange_order_id={} status={:?} filled_qty={}",
                    order_id, request.strategy_id, request.venue, request.symbol, result.order_id, result.status, result.filled_qty
                );

                if result.status == OrderStatus::Rejected {
                    let reason = format!("exchange rejected: status={:?}", result.status);
                    self.publish_rejected(&request, order_id, reason);
                } else {
                    // REST 成功，原子更新 Redis 写入 exchange_order_id。用
                    // `update` 而不是 get+upsert，避免和并发到达的 WS 成交
                    // 更新(OrderManager::handle_exchange_update)互相覆盖对方
                    // 刚写入的字段(丢失更新)。
                    let exchange_order_id = result.order_id.clone();
                    let updated_at_ms = current_timestamp_ms();
                    if matches!(
                        self.order_store.update(
                            &order_id,
                            Box::new(move |order| {
                                order.exchange_order_id = Some(exchange_order_id);
                                order.updated_at_ms = updated_at_ms;
                                true
                            }),
                        ),
                        super::store::OrderUpdateOutcome::NotFound
                    ) {
                        warn!(
                            "ExecutionService: order_id={order_id} not found in store when writing back exchange_order_id"
                        );
                    }

                    // 发送 Accepted 事件
                    let event = OrderEvent::Accepted { order_id };
                    self.bus.publish(
                        Topic::order_event(&request.strategy_id),
                        event,
                    );
                }
            }
            Err(err) => {
                let reason = format!("exchange error: {err:#}");
                error!(
                    "ExecutionService: order_id={} strategy={} venue={} symbol={} {reason}",
                    order_id, request.strategy_id, request.venue, request.symbol
                );
                self.publish_rejected(&request, order_id, reason);
            }
        }
    }

    fn publish_rejected(&self, request: &OrderRequest, order_id: super::types::OrderId, reason: String) {
        let event = OrderEvent::RejectedByExchange { order_id, reason };
        self.bus.publish(
            Topic::order_event(&request.strategy_id),
            event,
        );
    }
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
