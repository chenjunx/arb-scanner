use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::StreamExt;
use log::{debug, info, warn};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::market_data::link_health::LinkHealthMonitor;
use crate::market_data::now_ms;
use crate::order::types::{OrderAmount, OrderStatus};
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

/// 按资产配置的"两 venue 持仓差"限额：`venue_a`/`venue_b` 各自的
/// `PositionManager::available_position` 差值绝对值超过 `max_diff` 时拒单。
/// 用于跨所策略防止币不断从一边流向另一边、直到一边被掏空。
#[derive(Debug, Clone)]
pub struct AssetImbalanceLimit {
    pub venue_a: Venue,
    pub venue_b: Venue,
    pub max_diff: Decimal,
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
    asset_imbalance_limits: HashMap<String, AssetImbalanceLimit>,
    link_health: Option<Arc<LinkHealthMonitor>>,
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
            asset_imbalance_limits: HashMap::new(),
            link_health: None,
        }
    }

    /// 注入按资产的跨 venue 持仓差限额（key 是 asset，如 "BTC"）。
    /// 未调用时该表为空，`check_trade` 里的失衡检查直接跳过，不影响
    /// 现有 `open`/`rotate`/`close`/`transfer` 调用点的行为。
    pub fn with_asset_imbalance_limits(mut self, limits: HashMap<String, AssetImbalanceLimit>) -> Self {
        self.asset_imbalance_limits = limits;
        self
    }

    /// 注入链路健康监控，`check_trade` 里会先查下单 venue 是否健康，
    /// 不健康直接拒单——避免在探针心跳都收不到的链路上盲目下单。
    /// 未调用时该项为 None，健康检查直接跳过，不影响现有调用点的行为。
    pub fn with_link_health(mut self, link_health: Arc<LinkHealthMonitor>) -> Self {
        self.link_health = Some(link_health);
        self
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
                let client_order_id = request.client_order_id().map(str::to_string);
                let event = OrderEvent::RejectedByRisk { order_id, client_order_id, reason };
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

        if let Some(link_health) = &self.link_health {
            if !link_health.is_healthy(venue) {
                return RiskCheckResult::Rejected {
                    reason: format!("venue {venue} link is unhealthy, rejecting order"),
                };
            }
        }

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
            let current_position = self.position_manager.available_position(venue, symbol);
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

        if let OrderAmount::Base(qty) = request.amount {
            let asset = request.symbol.base.to_string();
            if let Some(limit) = self.asset_imbalance_limits.get(&asset) {
                if venue == &limit.venue_a || venue == &limit.venue_b {
                    let qty_a = self.position_manager.available_position(&limit.venue_a, symbol);
                    let qty_b = self.position_manager.available_position(&limit.venue_b, symbol);
                    let delta = match request.side {
                        crate::order::types::OrderSide::Buy => qty,
                        crate::order::types::OrderSide::Sell => -qty,
                    };
                    let (proj_a, proj_b) = if venue == &limit.venue_a {
                        (qty_a + delta, qty_b)
                    } else {
                        (qty_a, qty_b + delta)
                    };
                    if (proj_a - proj_b).abs() > limit.max_diff {
                        return RiskCheckResult::Rejected {
                            reason: format!(
                                "projected {asset} holdings {}={proj_a} vs {}={proj_b} would exceed max_diff {}",
                                limit.venue_a, limit.venue_b, limit.max_diff
                            ),
                        };
                    }
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::types::OrderSide;
    use crate::order_manager::id_allocator::InMemoryOrderIdAllocator;
    use crate::order_manager::store::InMemoryOrderStore;
    use crate::order_manager::types::OrderKind;
    use crate::position::store::InMemoryPositionStore;
    use std::collections::HashMap as StdHashMap;

    fn risk_service(position_manager: Arc<PositionManager>, imbalance_limits: HashMap<String, AssetImbalanceLimit>) -> RiskService {
        RiskService::new(
            Arc::new(TopicBus::new()),
            Arc::new(InMemoryOrderIdAllocator::new()),
            Arc::new(InMemoryOrderStore::new()),
            StdHashMap::new(),
            position_manager,
        )
        .with_asset_imbalance_limits(imbalance_limits)
    }

    fn trade_request(venue: Venue, symbol: Symbol, side: OrderSide, qty: Decimal) -> OrderRequest {
        OrderRequest {
            strategy_id: "test".to_string(),
            venue,
            symbol,
            side,
            amount: OrderAmount::Base(qty),
            order_kind: OrderKind::Market,
            client_order_id: None,
            group_id: None,
            metadata: None,
            order_id: None,
        }
    }

    fn seed_position(pm: &PositionManager, venue: &Venue, symbol: &Symbol, qty: Decimal) {
        pm.on_filled(venue, symbol, OrderSide::Buy, qty, None, None, None, None, 0);
    }

    #[test]
    fn imbalance_check_rejects_order_that_widens_the_gap_beyond_max_diff() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let symbol = Symbol::new("BTC", "USDT");
        let kraken = Venue::new("kraken_spot");
        let binance = Venue::new("binance_spot");
        seed_position(&pm, &kraken, &symbol, Decimal::from(50));
        seed_position(&pm, &binance, &symbol, Decimal::from(50));

        let mut limits = HashMap::new();
        limits.insert(
            "BTC".to_string(),
            AssetImbalanceLimit { venue_a: kraken.clone(), venue_b: binance.clone(), max_diff: Decimal::from(20) },
        );
        let service = risk_service(pm, limits);

        // 从 kraken 卖出 30 个 BTC：kraken 变成 20，binance 仍是 50，差值 30 > max_diff 20，应被拒绝。
        let request = trade_request(kraken, symbol, OrderSide::Sell, Decimal::from(30));
        match service.check_trade(&request) {
            RiskCheckResult::Rejected { reason } => assert!(reason.contains("max_diff")),
            RiskCheckResult::Approved => panic!("expected rejection due to asset imbalance"),
        }
    }

    #[test]
    fn imbalance_check_is_skipped_when_asset_has_no_configured_limit() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let symbol = Symbol::new("ETH", "USDT");
        let kraken = Venue::new("kraken_spot");
        let binance = Venue::new("binance_spot");
        seed_position(&pm, &kraken, &symbol, Decimal::from(100));
        seed_position(&pm, &binance, &symbol, Decimal::ZERO);

        // 只给 BTC 配置了限额，ETH 没有配置，应该完全不受影响。
        let mut limits = HashMap::new();
        limits.insert(
            "BTC".to_string(),
            AssetImbalanceLimit { venue_a: kraken.clone(), venue_b: binance.clone(), max_diff: Decimal::from(10) },
        );
        let service = risk_service(pm, limits);

        let request = trade_request(kraken, symbol, OrderSide::Sell, Decimal::from(50));
        assert!(matches!(service.check_trade(&request), RiskCheckResult::Approved));
    }

    #[test]
    fn imbalance_check_allows_order_that_brings_the_gap_back_within_max_diff() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let symbol = Symbol::new("BTC", "USDT");
        let kraken = Venue::new("kraken_spot");
        let binance = Venue::new("binance_spot");
        // 已经失衡：kraken 20 / binance 80，差值 60，已经超过下面配置的 max_diff 20。
        seed_position(&pm, &kraken, &symbol, Decimal::from(20));
        seed_position(&pm, &binance, &symbol, Decimal::from(80));

        let mut limits = HashMap::new();
        limits.insert(
            "BTC".to_string(),
            AssetImbalanceLimit { venue_a: kraken.clone(), venue_b: binance.clone(), max_diff: Decimal::from(20) },
        );
        let service = risk_service(pm, limits);

        // 在 binance 卖出 45 个：binance 变成 35，kraken 仍是 20，差值收敛到 15（<= max_diff 20），应放行——
        // 检查看的是"下单之后"的投影差值，不是"是否比之前更小"，所以收敛单必须真的把差值拉回限额内才会放行。
        let request = trade_request(binance, symbol, OrderSide::Sell, Decimal::from(45));
        assert!(matches!(service.check_trade(&request), RiskCheckResult::Approved));
    }
}
