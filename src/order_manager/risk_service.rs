use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use log::{debug, info, warn};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::market_data::now_ms;
use crate::order::types::OrderStatus;
use crate::position::PositionManager;
use crate::topic::{Topic, TopicBus};
use crate::types::{Symbol, Venue};

use super::id_allocator::OrderIdAllocator;
use super::store::OrderStore;
use super::types::{AnyOrderRequest, Order, OrderEvent, OrderId, OrderRequest, RiskCheckResult};

/// 单个 venue+symbol 的风控限额配置
#[derive(Debug, Clone)]
pub struct RiskLimits {
    pub max_order_amount: Decimal,
    pub max_position: Decimal,
    pub max_orders_per_window: u32,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_order_amount: Decimal::MAX,
            max_position: Decimal::MAX,
            max_orders_per_window: u32::MAX,
        }
    }
}

/// 风控服务：订阅 `Topic::OrderSubmit`，为每个订单请求分配 OrderId、
/// 执行风控检查，通过则先写入 Redis，再发布到 `Topic::OrderExecute`，
/// 否则发布 `OrderEvent::RejectedByRisk`。
/// 划转单（`AnyOrderRequest::Transfer`）跳过交易风控直接通过。
pub struct RiskService {
    bus: Arc<TopicBus>,
    order_id_allocator: Arc<dyn OrderIdAllocator>,
    order_store: Arc<dyn OrderStore>,
    limits: HashMap<(Venue, Symbol), RiskLimits>,
    default_limits: RiskLimits,
    position_manager: Arc<PositionManager>,
    order_counts: Mutex<HashMap<(Venue, Symbol), u32>>,
}

impl RiskService {
    pub fn new(
        bus: Arc<TopicBus>,
        order_id_allocator: Arc<dyn OrderIdAllocator>,
        order_store: Arc<dyn OrderStore>,
        limits: HashMap<(Venue, Symbol), RiskLimits>,
        position_manager: Arc<PositionManager>,
    ) -> Self {
        Self {
            bus,
            order_id_allocator,
            order_store,
            limits,
            default_limits: RiskLimits::default(),
            position_manager,
            order_counts: Mutex::new(HashMap::new()),
        }
    }

    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut stream = self.bus.subscribe::<AnyOrderRequest>(Topic::order_submit());
            while let Some((_topic, request)) = stream.next().await {
                self.handle_order_request(request).await;
            }
        })
    }

    async fn handle_order_request(&self, mut request: AnyOrderRequest) {
        let order_id = self.generate_order_id();

        debug!(
            "RiskService: allocated order_id={} client_order_id={:?} strategy={}",
            order_id,
            request.client_order_id(),
            request.strategy_id(),
        );

        let check_result = match &request {
            AnyOrderRequest::Trade(r) => self.check_trade(r),
            AnyOrderRequest::Transfer(r) => {
                info!(
                    "RiskService: order_id={} transfer from={} to={} symbol={} amount={}",
                    order_id, r.from_venue, r.to_venue, r.symbol, r.amount
                );
                RiskCheckResult::Approved
            }
        };

        match check_result {
            RiskCheckResult::Approved => {
                let order = Order {
                    order_id: order_id.clone(),
                    request: request.clone(),
                    status: OrderStatus::New,
                    filled_qty: Decimal::ZERO,
                    avg_price: None,
                    exchange_order_id: None,
                    created_at_ms: now_ms(),
                    updated_at_ms: now_ms(),
                    reject_reason: None,
                };
                self.order_store.upsert(order);

                request.set_order_id(order_id);
                self.bus.publish(Topic::order_execute(), request);
            }
            RiskCheckResult::Rejected { reason } => {
                warn!("RiskService: order_id={} rejected: {reason}", order_id);
                let strategy_id = request.strategy_id().to_string();
                let event = OrderEvent::RejectedByRisk { order_id, reason };
                self.bus.publish(Topic::order_event(&strategy_id), event);
            }
        }
    }

    fn generate_order_id(&self) -> OrderId {
        let ts = current_timestamp_ms();
        match self.order_id_allocator.next() {
            Some(seq) => OrderId::new(format!("ORD-{ts}-{seq:06}")),
            None => {
                let rand_seq = rand::random::<u32>() % 100000;
                OrderId::new(format!("ORD-{ts}-R{rand_seq:05}"))
            }
        }
    }

    fn limits_for(&self, venue: &Venue, symbol: &Symbol) -> &RiskLimits {
        self.limits
            .get(&(venue.clone(), symbol.clone()))
            .unwrap_or(&self.default_limits)
    }

    fn check_trade(&self, request: &OrderRequest) -> RiskCheckResult {
        let venue = &request.venue;
        let symbol = &request.symbol;
        let limits = self.limits_for(venue, symbol);

        let order_qty = request.amount.value();
        if order_qty > limits.max_order_amount {
            return RiskCheckResult::Rejected {
                reason: format!(
                    "order amount {order_qty} exceeds max_order_amount {} for {venue}/{symbol}",
                    limits.max_order_amount
                ),
            };
        }

        let key = (venue.clone(), symbol.clone());
        {
            let counts = self.order_counts.lock().unwrap();
            let current = counts.get(&key).copied().unwrap_or(0);
            if current >= limits.max_orders_per_window {
                return RiskCheckResult::Rejected {
                    reason: format!(
                        "order count {current} reached max_orders_per_window {} for {venue}/{symbol}",
                        limits.max_orders_per_window
                    ),
                };
            }
        }

        {
            let current_position = self.position_manager.position(venue, symbol);
            let delta = match request.side {
                crate::order::types::OrderSide::Buy => order_qty,
                crate::order::types::OrderSide::Sell => -order_qty,
            };
            let projected = current_position + delta;
            if projected.abs() > limits.max_position {
                return RiskCheckResult::Rejected {
                    reason: format!(
                        "projected position {projected} would exceed max_position {} for {venue}/{symbol}",
                        limits.max_position
                    ),
                };
            }
        }

        self.order_counts.lock().unwrap().entry(key).and_modify(|c| *c += 1).or_insert(1);
        RiskCheckResult::Approved
    }

    pub fn release(&self, venue: &Venue, symbol: &Symbol) {
        let key = (venue.clone(), symbol.clone());
        if let Some(count) = self.order_counts.lock().unwrap().get_mut(&key) {
            *count = count.saturating_sub(1);
        }
    }
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
