use std::collections::HashMap;
use std::sync::Arc;

use futures_util::StreamExt;
use log::{error, info, warn};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::order::{OrderProvider, order_outcome_unknown};
use crate::order::types::{LimitIocOrderRequest, LimitOrderRequest, MarketOrderRequest, OrderAmount, OrderResult, OrderStatus};
use crate::position::PositionManager;
use crate::topic::{Topic, TopicBus};
use crate::types::{Symbol, Venue};
use crate::wallet::WalletProvider;
use crate::wallet::transfer::{TransferParams, transfer_asset};

use super::store::OrderStore;
use super::types::{AnyOrderRequest, CancelRequest, Order, OrderEvent, OrderKind, OrderRequest};

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

    pub async fn submit(&self, order: &Order) -> anyhow::Result<OrderResult> {
        let trade = order.request.as_trade().expect("ExchangeAdapter::submit called on non-trade order");
        match &trade.order_kind {
            OrderKind::Market => {
                let req = MarketOrderRequest {
                    symbol: trade.symbol.clone(),
                    side: trade.side,
                    amount: trade.amount,
                    client_order_id: trade.client_order_id.clone(),
                    dry_run: false,
                };
                self.provider.place_market_order(req).await
            }
            OrderKind::LimitIoc { price } => {
                let OrderAmount::Base(quantity) = trade.amount else {
                    anyhow::bail!("LimitIoc order requires OrderAmount::Base, got {:?}", trade.amount);
                };
                let req = LimitIocOrderRequest {
                    symbol: trade.symbol.clone(),
                    side: trade.side,
                    quantity,
                    price: *price,
                    client_order_id: trade.client_order_id.clone(),
                    dry_run: false,
                };
                self.provider.place_limit_ioc_order(req).await
            }
            OrderKind::Limit { price } => {
                let OrderAmount::Base(quantity) = trade.amount else {
                    anyhow::bail!("Limit order requires OrderAmount::Base, got {:?}", trade.amount);
                };
                let req = LimitOrderRequest {
                    symbol: trade.symbol.clone(),
                    side: trade.side,
                    quantity,
                    price: *price,
                    client_order_id: trade.client_order_id.clone(),
                    dry_run: false,
                };
                self.provider.place_limit_order(req).await
            }
        }
    }

    pub async fn cancel(&self, symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<()> {
        self.provider.cancel_order(symbol, exchange_order_id).await
    }
}

/// 执行服务：订阅 `Topic::OrderExecute`，按订单类型路由。
/// - `Trade`：路由到对应 `ExchangeAdapter` 下单
/// - `Transfer`：调用源 venue 的 `WalletProvider::withdraw`，
///   成功后调 `PositionManager::on_filled`(双边) + `mark_transfer_pending`
///   乐观更新仓位，发布 `OrderEvent::Transferred`；到账确认后的仓位修正见
///   `OrderManager::confirm_transfer`
pub struct ExecutionService {
    bus: Arc<TopicBus>,
    adapters: HashMap<Venue, Arc<ExchangeAdapter>>,
    wallet_providers: HashMap<Venue, Arc<dyn WalletProvider>>,
    position_manager: Option<Arc<PositionManager>>,
    order_store: Arc<dyn OrderStore>,
}

impl ExecutionService {
    pub fn new(
        bus: Arc<TopicBus>,
        adapters: HashMap<Venue, Arc<ExchangeAdapter>>,
        order_store: Arc<dyn OrderStore>,
    ) -> Self {
        Self { bus, adapters, wallet_providers: HashMap::new(), position_manager: None, order_store }
    }

    /// 注入钱包提供者，启用划转单执行能力
    pub fn with_wallet_providers(
        mut self,
        providers: HashMap<Venue, Arc<dyn WalletProvider>>,
        position_manager: Arc<PositionManager>,
    ) -> Self {
        self.wallet_providers = providers;
        self.position_manager = Some(position_manager);
        self
    }

    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut stream = self.bus.subscribe::<AnyOrderRequest>(Topic::order_execute());
            while let Some((_topic, request)) = stream.next().await {
                self.handle_order_request(request).await;
            }
        })
    }

    /// 独立订阅 `Topic::order_cancel()`：撤单和下单语义完全独立，不合并进
    /// `start()` 的同一个 select 循环。
    pub fn start_cancel_listener(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut stream = self.bus.subscribe::<CancelRequest>(Topic::order_cancel());
            while let Some((_topic, request)) = stream.next().await {
                self.handle_cancel_request(request).await;
            }
        })
    }

    async fn handle_cancel_request(&self, request: CancelRequest) {
        let Some(order) = self.order_store.find_by_client_order_id(&request.client_order_id) else {
            warn!(
                "ExecutionService: cancel_order strategy={} client_order_id={} not found",
                request.strategy_id, request.client_order_id
            );
            return;
        };

        let already_terminal = matches!(
            order.status,
            OrderStatus::Filled
                | OrderStatus::Rejected
                | OrderStatus::Expired
                | OrderStatus::Cancelled
                | OrderStatus::Transferred
                | OrderStatus::DepositConfirmed
        );
        let Some(exchange_order_id) = order.exchange_order_id.clone() else {
            warn!(
                "ExecutionService: cancel_order order_id={} client_order_id={} has no exchange_order_id yet",
                order.order_id, request.client_order_id
            );
            self.publish_rejected(
                &request.strategy_id,
                order.order_id,
                Some(request.client_order_id),
                "cancel_order: order has no exchange_order_id yet".to_string(),
            );
            return;
        };
        if already_terminal {
            warn!(
                "ExecutionService: cancel_order order_id={} client_order_id={} already terminal status={:?}",
                order.order_id, request.client_order_id, order.status
            );
            self.publish_rejected(
                &request.strategy_id,
                order.order_id,
                Some(request.client_order_id),
                format!("cancel_order: order already terminal status={:?}", order.status),
            );
            return;
        }

        let Some(trade) = order.request.as_trade() else {
            warn!(
                "ExecutionService: cancel_order order_id={} client_order_id={} is not a trade order",
                order.order_id, request.client_order_id
            );
            return;
        };

        let adapter = match self.adapters.get(&trade.venue) {
            Some(a) => a,
            None => {
                let reason = format!("no adapter registered for venue {}", trade.venue);
                error!("ExecutionService: cancel_order order_id={} {reason}", order.order_id);
                self.publish_rejected(&request.strategy_id, order.order_id, Some(request.client_order_id), reason);
                return;
            }
        };

        info!(
            "ExecutionService: order_id={} cancel_order venue={} symbol={} exchange_order_id={exchange_order_id}",
            order.order_id, adapter.venue(), trade.symbol
        );

        // 撤单请求本身成功/失败只代表"指令被接受"，不代表最终状态确认——
        // 真正的终态由交易所私有 WS 推送 → OrderManager::handle_exchange_update
        // 落地，这里不主动发布 OrderEvent::Cancelled，避免和 WS 权威来源冲突。
        if let Err(err) = adapter.cancel(&trade.symbol, &exchange_order_id).await {
            let reason = format!("cancel_order exchange error: {err:#}");
            error!("ExecutionService: order_id={} {reason}", order.order_id);
            self.publish_rejected(&request.strategy_id, order.order_id, Some(request.client_order_id), reason);
        }
    }

    async fn handle_order_request(&self, request: AnyOrderRequest) {
        let Some(order_id) = request.order_id().cloned() else {
            error!(
                "ExecutionService: received request without order_id strategy={}",
                request.strategy_id()
            );
            return;
        };

        match request {
            AnyOrderRequest::Trade(r) => self.handle_trade(order_id, r).await,
            AnyOrderRequest::Transfer(r) => {
                let r = crate::order_manager::types::TransferRequest { order_id: Some(order_id.clone()), ..r };
                self.handle_transfer(r).await;
            }
        }
    }

    async fn handle_trade(&self, order_id: super::types::OrderId, request: OrderRequest) {
        let adapter = match self.adapters.get(&request.venue) {
            Some(a) => a,
            None => {
                let reason = format!("no adapter registered for venue {}", request.venue);
                error!("ExecutionService: order_id={order_id} {reason}");
                self.mark_rejected(&order_id, &reason);
                self.publish_rejected(&request.strategy_id, order_id, request.client_order_id.clone(), reason);
                return;
            }
        };

        info!(
            "ExecutionService: order_id={order_id} submitting trade venue={} symbol={} side={:?} amount={:?}",
            adapter.venue(), request.symbol, request.side, request.amount
        );

        let temp_order = Order {
            order_id: order_id.clone(),
            request: AnyOrderRequest::Trade(request.clone()),
            status: OrderStatus::New,
            filled_qty: Decimal::ZERO,
            avg_price: None,
            exchange_order_id: None,
            created_at_ms: current_timestamp_ms(),
            updated_at_ms: current_timestamp_ms(),
            reject_reason: None,
        };

        match adapter.submit(&temp_order).await {
            Ok(result) => {
                info!(
                    "ExecutionService: order_id={order_id} exchange_order_id={} status={:?} filled_qty={}",
                    result.order_id, result.status, result.filled_qty
                );

                if result.status == OrderStatus::Rejected {
                    let reason = format!("exchange rejected: status={:?}", result.status);
                    self.mark_rejected(&order_id, &reason);
                    self.publish_rejected(&request.strategy_id, order_id, request.client_order_id.clone(), reason);
                } else {
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
                        warn!("ExecutionService: order_id={order_id} not found when writing back exchange_order_id");
                    }

                    self.bus.publish(
                        Topic::order_event(&request.strategy_id),
                        OrderEvent::Accepted { order_id, client_order_id: request.client_order_id.clone() },
                    );
                }
            }
            Err(err) => {
                let reason = format!("exchange error: {err:#}");
                error!("ExecutionService: order_id={order_id} {reason}");
                // 结果未知(请求可能已被受理)时保持 New，交给后续 WS 推送/人工核对定终态；
                // 否则交易所上不存在这笔单，永远不会有推送，必须在这里落 Rejected。
                if order_outcome_unknown(&err) {
                    warn!("ExecutionService: order_id={order_id} outcome unknown, keeping status New until exchange confirms");
                } else {
                    self.mark_rejected(&order_id, &reason);
                }
                self.publish_rejected(&request.strategy_id, order_id, request.client_order_id.clone(), reason);
            }
        }
    }

    async fn handle_transfer(&self, request: crate::order_manager::types::TransferRequest) {
        let order_id = request.order_id.clone().expect("transfer request missing order_id");
        let strategy_id = request.strategy_id.clone();

        let from_wallet = match self.wallet_providers.get(&request.from_venue) {
            Some(w) => w.clone(),
            None => {
                let reason = format!("no wallet provider registered for from_venue={}", request.from_venue);
                error!("ExecutionService: order_id={order_id} {reason}");
                self.publish_rejected(&strategy_id, order_id, request.client_order_id.clone(), reason);
                return;
            }
        };

        let to_wallet = match self.wallet_providers.get(&request.to_venue) {
            Some(w) => w.clone(),
            None => {
                let reason = format!("no wallet provider registered for to_venue={}", request.to_venue);
                error!("ExecutionService: order_id={order_id} {reason}");
                self.publish_rejected(&strategy_id, order_id, request.client_order_id.clone(), reason);
                return;
            }
        };

        info!(
            "ExecutionService: order_id={order_id} transfer from={} to={} symbol={} amount={} dry_run={}",
            request.from_venue, request.to_venue, request.symbol, request.amount, request.dry_run
        );

        let asset = request.symbol.base.as_ref();
        let params = TransferParams {
            asset: asset.to_string(),
            amount: request.amount,
            network: request.network.clone(),
            dry_run: request.dry_run,
        };

        match transfer_asset(from_wallet.as_ref(), to_wallet.as_ref(), params).await {
            Ok((qty, withdraw_result)) => {
                info!(
                    "ExecutionService: order_id={order_id} transfer withdraw_id={} qty={qty}",
                    withdraw_result.id
                );

                // 更新订单记录：写入 withdraw_id + 状态置 Transferred(提币已提交，
                // 尚未到账确认；到账确认由 TransferMonitor 探测到后经
                // OrderManager::confirm_transfer 推进到 DepositConfirmed)
                let withdraw_id = withdraw_result.id.clone();
                let updated_at_ms = current_timestamp_ms();
                self.order_store.update(
                    &order_id,
                    Box::new(move |order| {
                        order.exchange_order_id = Some(withdraw_id);
                        order.status = OrderStatus::Transferred;
                        order.filled_qty = qty;
                        order.updated_at_ms = updated_at_ms;
                        true
                    }),
                );

                // 更新双边仓位：来源端 Sell（无价格 → 不计盈亏），目标端 Buy 以来源均价建仓
                if let Some(pm) = &self.position_manager {
                    let ts_ms = current_timestamp_ms();
                    let source_avg = pm
                        .venue_position(&request.from_venue, &request.symbol)
                        .and_then(|p| p.avg_price);
                    pm.on_filled(
                        &request.from_venue,
                        &request.symbol,
                        crate::order::types::OrderSide::Sell,
                        qty,
                        None,
                        None,
                        None,
                        None,
                        ts_ms,
                    );
                    pm.on_filled(
                        &request.to_venue,
                        &request.symbol,
                        crate::order::types::OrderSide::Buy,
                        qty,
                        source_avg,
                        None,
                        None,
                        None,
                        ts_ms,
                    );
                    // 目的地这笔数量已计入 net_qty，但在到账确认前不能当可用
                    // 敞口用——标记为 pending，等 OrderManager::confirm_transfer
                    // 在到账确认时通过 settle_transfer_in 清掉。
                    pm.mark_transfer_pending(&request.to_venue, &request.symbol, qty, ts_ms);
                }

                // 通知策略
                self.bus.publish(
                    Topic::order_event(&strategy_id),
                    OrderEvent::Transferred {
                        order_id,
                        client_order_id: request.client_order_id.clone(),
                        from_venue: request.from_venue,
                        to_venue: request.to_venue,
                        qty,
                        withdraw_id: withdraw_result.id,
                    },
                );
            }
            Err(err) => {
                let reason = format!("transfer error: {err:#}");
                error!("ExecutionService: order_id={order_id} {reason}");
                self.publish_rejected(&strategy_id, order_id, request.client_order_id.clone(), reason);
            }
        }
    }

    /// 下单阶段就被拒的订单落 `Rejected` + 原因。只改仍是 `New` 的订单：WS 推送
    /// 可能先一步把它推进到别的状态，那是更权威的结果，不能覆盖。
    fn mark_rejected(&self, order_id: &super::types::OrderId, reason: &str) {
        let reason = reason.to_string();
        let updated_at_ms = current_timestamp_ms();
        let outcome = self.order_store.update(
            order_id,
            Box::new(move |order| {
                if order.status != OrderStatus::New {
                    return false;
                }
                order.status = OrderStatus::Rejected;
                order.reject_reason = Some(reason);
                order.updated_at_ms = updated_at_ms;
                true
            }),
        );
        if matches!(outcome, super::store::OrderUpdateOutcome::NotFound) {
            warn!("ExecutionService: order_id={order_id} not found when marking rejected");
        }
    }

    fn publish_rejected(
        &self,
        strategy_id: &str,
        order_id: super::types::OrderId,
        client_order_id: Option<String>,
        reason: String,
    ) {
        let event = OrderEvent::RejectedByExchange { order_id, client_order_id, reason };
        self.bus.publish(Topic::order_event(strategy_id), event);
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
    use async_trait::async_trait;

    use super::*;
    use crate::order::OrderOutcomeUnknown;
    use crate::order::types::OrderSide;
    use crate::order_manager::store::InMemoryOrderStore;
    use crate::order_manager::types::OrderId;

    /// 下单总是返回 `make_err` 造出的错误。
    struct FailingProvider {
        make_err: fn() -> anyhow::Error,
    }

    #[async_trait]
    impl OrderProvider for FailingProvider {
        fn venue(&self) -> Venue {
            Venue::new("bitget_futures")
        }

        async fn place_market_order_raw(&self, _req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            Err((self.make_err)())
        }
    }

    fn request() -> OrderRequest {
        OrderRequest {
            strategy_id: "test".to_string(),
            venue: Venue::new("bitget_futures"),
            symbol: Symbol::new("FUTU", "USDT"),
            side: OrderSide::Buy,
            amount: OrderAmount::Base(Decimal::ONE),
            order_kind: OrderKind::Market,
            client_order_id: Some("cf1".to_string()),
            group_id: None,
            metadata: None,
            order_id: None,
        }
    }

    async fn submit_with(make_err: fn() -> anyhow::Error) -> Order {
        let store = Arc::new(InMemoryOrderStore::new());
        let order_id = OrderId::new("ORD-1");
        store.upsert(Order {
            order_id: order_id.clone(),
            request: AnyOrderRequest::Trade(request()),
            status: OrderStatus::New,
            filled_qty: Decimal::ZERO,
            avg_price: None,
            exchange_order_id: None,
            created_at_ms: 1,
            updated_at_ms: 1,
            reject_reason: None,
        });
        let venue = Venue::new("bitget_futures");
        let adapter = Arc::new(ExchangeAdapter::new(venue.clone(), Arc::new(FailingProvider { make_err })));
        let service = ExecutionService::new(Arc::new(TopicBus::new()), HashMap::from([(venue, adapter)]), store.clone());
        service.handle_trade(order_id.clone(), request()).await;
        store.get(&order_id).unwrap()
    }

    #[tokio::test]
    async fn explicit_exchange_rejection_is_persisted_as_rejected() {
        let order = submit_with(|| anyhow::anyhow!("bitget error 40774: unilateral position")).await;
        assert_eq!(order.status, OrderStatus::Rejected);
        assert!(order.reject_reason.unwrap().contains("40774"));
    }

    #[tokio::test]
    async fn unknown_outcome_keeps_order_new() {
        let order = submit_with(|| anyhow::Error::new(OrderOutcomeUnknown("ws closed".to_string())).context("submit")).await;
        assert_eq!(order.status, OrderStatus::New);
        assert!(order.reject_reason.is_none());
    }
}
