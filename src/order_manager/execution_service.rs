use std::collections::HashMap;
use std::sync::Arc;

use futures_util::StreamExt;
use log::{error, info, warn};
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::order::OrderProvider;
use crate::order::types::{MarketOrderRequest, OrderResult, OrderStatus};
use crate::position::PositionManager;
use crate::topic::{Topic, TopicBus};
use crate::types::Venue;
use crate::wallet::WalletProvider;
use crate::wallet::transfer::{TransferParams, transfer_asset};

use super::store::OrderStore;
use super::types::{AnyOrderRequest, Order, OrderEvent, OrderRequest};

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
        let req = MarketOrderRequest {
            symbol: trade.symbol.clone(),
            side: trade.side,
            amount: trade.amount,
            client_order_id: trade.client_order_id.clone(),
            dry_run: false,
        };
        self.provider.place_market_order(req).await
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
                self.publish_rejected(&request.strategy_id, order_id, reason);
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
                    self.publish_rejected(&request.strategy_id, order_id, reason);
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
                        OrderEvent::Accepted { order_id },
                    );
                }
            }
            Err(err) => {
                let reason = format!("exchange error: {err:#}");
                error!("ExecutionService: order_id={order_id} {reason}");
                self.publish_rejected(&request.strategy_id, order_id, reason);
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
                self.publish_rejected(&strategy_id, order_id, reason);
                return;
            }
        };

        let to_wallet = match self.wallet_providers.get(&request.to_venue) {
            Some(w) => w.clone(),
            None => {
                let reason = format!("no wallet provider registered for to_venue={}", request.to_venue);
                error!("ExecutionService: order_id={order_id} {reason}");
                self.publish_rejected(&strategy_id, order_id, reason);
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
                self.publish_rejected(&strategy_id, order_id, reason);
            }
        }
    }

    fn publish_rejected(&self, strategy_id: &str, order_id: super::types::OrderId, reason: String) {
        let event = OrderEvent::RejectedByExchange { order_id, reason };
        self.bus.publish(Topic::order_event(strategy_id), event);
    }
}

fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
