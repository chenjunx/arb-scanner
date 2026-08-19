use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use log::{debug, warn};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::market_data::now_ms;
use crate::order::types::{OrderAmount, OrderStatus};
use crate::position::PositionManager;
use crate::topic::{Topic, TopicBus};
use crate::types::{Symbol, Venue};

use super::id_allocator::OrderIdAllocator;
use super::store::OrderStore;
use super::types::{Order, OrderEvent, OrderId, OrderRequest, RiskCheckResult};

/// 单个 venue+symbol 的风控限额配置
#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// 单笔订单最大数量 (base 或 quote，取决于订单本身的 OrderAmount 类型)
    pub max_order_amount: Decimal,
    /// 该 venue+symbol 上允许的最大持仓敞口 (净头寸的绝对值上限)
    pub max_position: Decimal,
    /// 滑动窗口内允许的最大订单数 (简单计数，不做真正的时间窗口，见 `check` 说明)
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
/// 执行风控检查，通过则**先写入 Redis**，再发布到 `Topic::OrderExecute`，
/// 否则发布 `OrderEvent::RejectedByRisk` 到 `Topic::OrderEvent{strategy_id}`。
/// `client_order_id` 由调用方（`Strategy::submit_order` 或手动开仓流程）负责
/// 生成，这里只信任并透传，不再兜底生成。
///
/// 风控逻辑直接内嵌在这里，包括：单笔限额、持仓限额、下单频率三类检查。
/// 持仓限额检查只读 `PositionManager`，不写；成交后的仓位写入由
/// `OrderManager` 直接完成，见 `docs/position_manager_design.md`。
pub struct RiskService {
    bus: Arc<TopicBus>,
    order_id_allocator: Arc<dyn OrderIdAllocator>,
    order_store: Arc<dyn OrderStore>,
    limits: HashMap<(Venue, Symbol), RiskLimits>,
    default_limits: RiskLimits,
    position_manager: Arc<PositionManager>,
    /// 每个 (venue, symbol) 已提交的订单计数，用于频率限制。
    /// 生产环境应替换成真正的滑动时间窗口，这里先用简单计数占位。
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

    /// 启动服务，订阅 `Topic::OrderSubmit` 并处理每个订单请求
    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut stream = self.bus.subscribe::<OrderRequest>(Topic::order_submit());
            while let Some((_topic, request)) = stream.next().await {
                self.handle_order_request(request).await;
            }
        })
    }

    async fn handle_order_request(&self, mut request: OrderRequest) {
        // 1. 分配 OrderId (generate_order_id 逻辑内联到这里)
        let order_id = self.generate_order_id();

        debug!(
            "RiskService: allocated order_id={} client_order_id={:?} for strategy={} venue={} symbol={} side={:?} amount={:?}",
            order_id, request.client_order_id, request.strategy_id, request.venue, request.symbol, request.side, request.amount
        );

        // 2. 风控检查
        let check_result = self.check(&request.venue, &request.symbol, request.side, &request.amount);
        match check_result {
            RiskCheckResult::Approved => {
                // 3. 风控通过，先写入 Redis (status=New, exchange_order_id=None)
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

                debug!(
                    "RiskService: order_id={} passed risk check and written to Redis, publishing to Topic::OrderExecute",
                    order_id
                );

                // 4. 把 order_id 写入 request，发布到 Topic::OrderExecute
                request.order_id = Some(order_id);
                self.bus.publish(Topic::order_execute(), request);
            }
            RiskCheckResult::Rejected { reason } => {
                warn!(
                    "RiskService: order_id={} rejected by risk: {reason}",
                    order_id
                );
                self.publish_rejected(&request, order_id, reason);
            }
        }
    }

    fn publish_rejected(&self, request: &OrderRequest, order_id: OrderId, reason: String) {
        let event = OrderEvent::RejectedByRisk { order_id, reason };
        self.bus.publish(
            Topic::order_event(&request.strategy_id),
            event,
        );
    }

    /// 生成全局唯一的订单ID。ID 里始终带生成时的毫秒时间戳——即使 Redis 的
    /// `order_id_allocator` 计数器因为重启/未持久化/被清空而从头计数，新订单
    /// 的时间戳也和历史订单不同，不会撞上 `RedisOrderStore` 里已有的 Hash
    /// field 而覆盖历史记录（这才是真正要防的碰撞，而不是序号本身单调）。
    /// 序号只用来消歧同一毫秒内提交的多笔订单：优先用 `order_id_allocator`
    /// （跨进程共享，通常是 Redis INCR）；分配失败（如 Redis 断连）时退化为
    /// 本地计数器。
    fn generate_order_id(&self) -> OrderId {
        let ts = current_timestamp_ms();
        match self.order_id_allocator.next() {
            Some(seq) => OrderId::new(format!("ORD-{ts}-{seq:06}")),
            None => {
                // 没有跨服务的 fallback_order_seq，直接用随机数避免碰撞
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

    /// 对一个待提交的订单做风控检查。检查通过后立即预占用下单计数，
    /// 避免同一批并发订单绕过限额 (乐观预占用，被交易所拒绝或者取消时需要调用
    /// `release` 回滚)。
    fn check(&self, venue: &Venue, symbol: &Symbol, side: crate::order::types::OrderSide, amount: &OrderAmount) -> RiskCheckResult {
        let limits = self.limits_for(venue, symbol);

        let order_qty = amount.value();
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
            let delta = match side {
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

        // 通过检查后预占用下单计数，防止并发订单绕过限额。
        self.order_counts.lock().unwrap().entry(key.clone()).and_modify(|c| *c += 1).or_insert(1);
        RiskCheckResult::Approved
    }

    /// 订单被交易所拒绝或从未真正下单成功时调用，释放 `check` 阶段预占用的
    /// 下单计数，避免额度被白白占用。
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
