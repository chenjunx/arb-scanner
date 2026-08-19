use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures_util::StreamExt;
use rust_decimal::Decimal;

use crate::exchange_info::types::PrecisionKind;
use crate::exchange_info::PrecisionCache;
use crate::market_data::now_ms;
use crate::order::types::{MarketOrderRequest, OrderAmount, OrderResult, OrderSide};
use crate::order::OrderProvider;
use crate::order_manager::stream::ExchangeOrderUpdate;
use crate::order_manager::types::{OrderEvent, OrderId};
use crate::order_manager::OrderManager;
use crate::topic::{BoxTopicStream, Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::Strategy;

/// 解析 client_order_id 反查 order_id 的超时/轮询间隔：提交是异步任务写入
/// 索引的，需要轮询等它落地，见 `resolve_order_id`。
const RESOLVE_ORDER_ID_TIMEOUT: Duration = Duration::from_secs(5);
const RESOLVE_ORDER_ID_POLL_INTERVAL: Duration = Duration::from_millis(2);
/// `wait_for_fill` 超时后 REST 核对触发的写回事件，再等一次的超时时间。
const RECONCILE_REWAIT: Duration = Duration::from_secs(5);

/// 手动触发的下单策略：开对冲仓位、库存轮转、平仓，全部统一走
/// `submit_order() -> bus -> RiskService -> ExecutionService -> OrderManager`
/// 这条链路，保证成交后仓位/账本都能正确落账。不订阅任何行情，靠 CLI 命令
/// 触发。钱包划转不属于这个策略，见 `wallet::transfer`。
pub struct ManualStrategy {
    bus: Arc<TopicBus>,
    order_manager: Arc<OrderManager>,
}

impl ManualStrategy {
    pub fn new(bus: Arc<TopicBus>, order_manager: Arc<OrderManager>) -> Self {
        Self { bus, order_manager }
    }
}

impl Strategy for ManualStrategy {
    fn name(&self) -> &str {
        "manual"
    }

    fn subscriptions(&self) -> Vec<Topic> {
        Vec::new()
    }

    fn on_quote(&self, _topic: &Topic, _quote: &Quote) {}

    fn bus(&self) -> &Arc<TopicBus> {
        &self.bus
    }
}

/// 开仓参数：现货按 USDT 金额买入，合约等量做空对冲。
#[derive(Debug, Clone)]
pub struct OpenPositionParams {
    pub symbol: Symbol,
    pub quote_amount: Decimal,
    pub client_order_id_prefix: Option<String>,
    pub dry_run: bool,
    /// 等待 [`OrderManager`] 通过交易所私有 WS 确认成交的超时时间；只在
    /// live 路径里使用。
    pub fill_timeout: Duration,
}

/// 开仓结果汇总。`dry_run=true` 时只有 `spot_order` 有值，后续步骤未执行。
#[derive(Debug, Clone)]
pub struct OpenPositionReport {
    pub spot_order: OrderResult,
    pub futures_order: Option<OrderResult>,
    /// 交易所返回的原始订单号（区别于 `spot_order.order_id`——live 路径下
    /// 后者是 `OrderManager` 内部生成的 `ORD-xxx`，需要这个字段核对交易所后台）。
    pub spot_exchange_order_id: Option<String>,
    pub futures_exchange_order_id: Option<String>,
    pub note: Option<String>,
}

/// dry_run 路径：只调用现货 `place_market_order(dry_run=true)` 做参数校验和
/// 模拟，完全不接触 `OrderManager`/风控/Redis。
pub async fn open_hedged_position_dry_run(
    spot: &dyn OrderProvider,
    params: OpenPositionParams,
) -> anyhow::Result<OpenPositionReport> {
    let spot_order = spot
        .place_market_order(MarketOrderRequest {
            symbol: params.symbol.clone(),
            side: OrderSide::Buy,
            amount: OrderAmount::Quote(params.quote_amount),
            client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-spot")),
            dry_run: true,
        })
        .await?;
    log::info!("open_hedged_position_dry_run: spot buy result = {:?}", spot_order);

    Ok(OpenPositionReport {
        spot_order,
        futures_order: None,
        spot_exchange_order_id: None,
        futures_exchange_order_id: None,
        note: Some(
            "dry_run=true：仅校验并模拟了现货买入这一步；合约对冲数量依赖真实成交量，dry-run 下不做模拟。"
                .to_string(),
        ),
    })
}

/// 库存轮转参数：在 `sell_provider` 卖出、`buy_provider` 买入同等数量的同一
/// 资产，两条腿并发发起。数量统一按基础币指定，因为 Kraken 市价单只支持按
/// 基础币数量下单。
#[derive(Debug, Clone)]
pub struct RotateInventoryParams {
    pub symbol: Symbol,
    pub qty: Decimal,
    pub client_order_id_prefix: Option<String>,
    pub dry_run: bool,
    pub fill_timeout: Duration,
}

/// 库存轮转结果：两条腿都成功才会返回。
#[derive(Debug, Clone)]
pub struct RotateInventoryReport {
    pub sell_order: OrderResult,
    pub buy_order: OrderResult,
}

/// 平仓参数：三条腿相互独立，每条腿的数量都可选——`None` 表示不平这条腿。
#[derive(Debug, Clone)]
pub struct ClosePositionParams {
    pub symbol: Symbol,
    pub binance_spot_qty: Option<Decimal>,
    pub kraken_spot_qty: Option<Decimal>,
    pub futures_qty: Option<Decimal>,
    pub client_order_id_prefix: Option<String>,
    pub dry_run: bool,
    pub fill_timeout: Duration,
}

/// 平仓结果：只有三条 `Result` 都成功才会返回，未请求的腿对应字段为 `None`。
#[derive(Debug, Clone)]
pub struct ClosePositionReport {
    pub binance_spot_order: Option<OrderResult>,
    pub kraken_spot_order: Option<OrderResult>,
    pub futures_order: Option<OrderResult>,
}

impl ManualStrategy {
    /// live 路径：现货买入、合约对冲两条腿都通过 `submit_order` 发布到
    /// `Topic::OrderSubmit`，由 `RiskService -> ExecutionService -> WS 成交
    /// 确认` 这条流水线处理，然后等待各自的成交事件——这样
    /// `OrderManager::handle_exchange_update` 才会被触发，仓位/盈亏才会真正
    /// 落进 `PositionManager`/`PortfolioManager`。任何一步失败都直接 `?` 向
    /// 上传播，不做自动回滚——半吊子仓位需要人工介入。
    pub async fn open_hedged_position_live(
        &self,
        spot: &dyn OrderProvider,
        futures: &dyn OrderProvider,
        futures_precision: &PrecisionCache,
        params: OpenPositionParams,
    ) -> anyhow::Result<OpenPositionReport> {
        let spot_client_order_id = params
            .client_order_id_prefix
            .as_ref()
            .map(|p| format!("{p}-spot"))
            .unwrap_or_else(|| format!("spot-{}", now_ms()));

        let (spot_order_id, filled_qty, spot_avg_price) = self
            .submit_and_wait_for_fill(
                spot.venue(),
                params.symbol.clone(),
                OrderSide::Buy,
                OrderAmount::Quote(params.quote_amount),
                spot_client_order_id,
                params.fill_timeout,
                spot,
            )
            .await?;
        if filled_qty <= Decimal::ZERO {
            anyhow::bail!("spot buy filled_qty is zero, aborting hedge");
        }
        log::info!("open_hedged_position_live: spot buy filled qty={filled_qty} avg_price={spot_avg_price}");
        let spot_order_after_fill = self
            .order_manager
            .get_order(&spot_order_id)
            .context("spot order disappeared from order manager after fill confirmation")?;
        let spot_exchange_order_id = spot_order_after_fill.exchange_order_id.clone();
        let spot_result = OrderResult {
            order_id: spot_order_id.to_string(),
            status: spot_order_after_fill.status,
            filled_qty,
            avg_price: Some(spot_avg_price),
            fee: None,
            fee_asset: None,
        };

        let futures_qty = futures_precision
            .round_qty(&params.symbol, PrecisionKind::Market, filled_qty)
            .context("failed to round futures hedge quantity to exchange precision")?;
        if futures_qty != filled_qty {
            log::warn!(
                "open_hedged_position_live: futures qty_step rounding, spot filled {filled_qty}, hedging {futures_qty}, residual {}",
                filled_qty - futures_qty
            );
        }

        let futures_client_order_id = params
            .client_order_id_prefix
            .as_ref()
            .map(|p| format!("{p}-futures"))
            .unwrap_or_else(|| format!("futures-{}", now_ms()));

        let (futures_order_id, futures_filled_qty, futures_avg_price) = self
            .submit_and_wait_for_fill(
                futures.venue(),
                params.symbol.clone(),
                OrderSide::Sell,
                OrderAmount::Base(futures_qty),
                futures_client_order_id,
                params.fill_timeout,
                futures,
            )
            .await?;
        log::info!(
            "open_hedged_position_live: futures hedge filled qty={futures_filled_qty} avg_price={futures_avg_price}"
        );
        let futures_order_after_fill = self
            .order_manager
            .get_order(&futures_order_id)
            .context("futures order disappeared from order manager after fill confirmation")?;
        let futures_exchange_order_id = futures_order_after_fill.exchange_order_id.clone();
        let futures_result = OrderResult {
            order_id: futures_order_id.to_string(),
            status: futures_order_after_fill.status,
            filled_qty: futures_filled_qty,
            avg_price: Some(futures_avg_price),
            fee: None,
            fee_asset: None,
        };

        Ok(OpenPositionReport {
            spot_order: spot_result,
            futures_order: Some(futures_result),
            spot_exchange_order_id,
            futures_exchange_order_id,
            note: None,
        })
    }

    /// 并发向 `sell_provider` 发一笔卖单、向 `buy_provider` 发一笔等量买单。
    /// `dry_run=true` 时完全不接触 `OrderManager`/风控/Redis，直接调用
    /// `provider.place_market_order(dry_run=true)`；否则两条腿都走 bus/
    /// OrderManager 流水线并发下单+等待成交。两条腿互相独立，不做自动回滚：
    /// 如果一条腿失败、另一条已经成交，会留下单边仓位，返回的错误里会带上
    /// 已成交那一条腿的完整订单信息，需要人工介入对账。
    pub async fn rotate_inventory(
        &self,
        sell_provider: &dyn OrderProvider,
        buy_provider: &dyn OrderProvider,
        params: RotateInventoryParams,
    ) -> anyhow::Result<RotateInventoryReport> {
        if params.dry_run {
            let sell_req = MarketOrderRequest {
                symbol: params.symbol.clone(),
                side: OrderSide::Sell,
                amount: OrderAmount::Base(params.qty),
                client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-sell")),
                dry_run: true,
            };
            let buy_req = MarketOrderRequest {
                symbol: params.symbol.clone(),
                side: OrderSide::Buy,
                amount: OrderAmount::Base(params.qty),
                client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-buy")),
                dry_run: true,
            };
            let (sell_order, buy_order) = tokio::join!(
                sell_provider.place_market_order(sell_req),
                buy_provider.place_market_order(buy_req),
            );
            return Ok(RotateInventoryReport {
                sell_order: sell_order?,
                buy_order: buy_order?,
            });
        }

        let sell_client_order_id = params
            .client_order_id_prefix
            .as_ref()
            .map(|p| format!("{p}-sell"))
            .unwrap_or_else(|| format!("rotate-sell-{}", now_ms()));
        let buy_client_order_id = params
            .client_order_id_prefix
            .as_ref()
            .map(|p| format!("{p}-buy"))
            .unwrap_or_else(|| format!("rotate-buy-{}", now_ms()));

        let (sell_result, buy_result) = tokio::join!(
            self.submit_and_wait_for_fill(
                sell_provider.venue(),
                params.symbol.clone(),
                OrderSide::Sell,
                OrderAmount::Base(params.qty),
                sell_client_order_id,
                params.fill_timeout,
                sell_provider,
            ),
            self.submit_and_wait_for_fill(
                buy_provider.venue(),
                params.symbol.clone(),
                OrderSide::Buy,
                OrderAmount::Base(params.qty),
                buy_client_order_id,
                params.fill_timeout,
                buy_provider,
            ),
        );

        match (sell_result, buy_result) {
            (Ok(sell_fill), Ok(buy_fill)) => {
                let sell_order = self.build_order_result(sell_fill)?;
                let buy_order = self.build_order_result(buy_fill)?;
                log::info!("rotate_inventory: sell={:?} buy={:?}", sell_order, buy_order);
                Ok(RotateInventoryReport { sell_order, buy_order })
            }
            (Err(sell_err), Ok(buy_fill)) => {
                let buy_order = self.build_order_result(buy_fill)?;
                log::error!(
                    "rotate_inventory: sell leg failed ({sell_err}), buy leg already filled = {:?} -- manual reconciliation needed",
                    buy_order
                );
                Err(sell_err.context(format!(
                    "rotate_inventory: sell leg failed but buy leg already filled (buy_order={buy_order:?}), manual reconciliation needed"
                )))
            }
            (Ok(sell_fill), Err(buy_err)) => {
                let sell_order = self.build_order_result(sell_fill)?;
                log::error!(
                    "rotate_inventory: buy leg failed ({buy_err}), sell leg already filled = {:?} -- manual reconciliation needed",
                    sell_order
                );
                Err(buy_err.context(format!(
                    "rotate_inventory: buy leg failed but sell leg already filled (sell_order={sell_order:?}), manual reconciliation needed"
                )))
            }
            (Err(sell_err), Err(buy_err)) => {
                log::error!("rotate_inventory: both legs failed, sell_err={sell_err}, buy_err={buy_err}");
                Err(anyhow::anyhow!(
                    "rotate_inventory: both legs failed; sell leg error: {sell_err}; buy leg error: {buy_err}"
                ))
            }
        }
    }

    /// 并发平掉币安现货、Kraken 现货、币安合约三条腿：现货两条腿是卖出，合约
    /// 腿是买回。三条腿互相独立、不做自动回滚：任意一条腿失败，其它已经成交
    /// 的腿不会被撤销，返回的错误里会带上三条腿各自的成交/跳过/失败情况。
    pub async fn close_hedged_position(
        &self,
        binance_spot: Option<&dyn OrderProvider>,
        kraken_spot: Option<&dyn OrderProvider>,
        binance_futures: Option<&dyn OrderProvider>,
        params: ClosePositionParams,
    ) -> anyhow::Result<ClosePositionReport> {
        for (leg, qty) in [
            ("binance_spot", params.binance_spot_qty),
            ("kraken_spot", params.kraken_spot_qty),
            ("futures", params.futures_qty),
        ] {
            if let Some(q) = qty {
                if q <= Decimal::ZERO {
                    anyhow::bail!(
                        "close_hedged_position: {leg} qty must be positive, got {q} (omit the flag to skip this leg)"
                    );
                }
            }
        }
        if params.binance_spot_qty.is_none() && params.kraken_spot_qty.is_none() && params.futures_qty.is_none() {
            anyhow::bail!(
                "close_hedged_position: at least one of binance_spot_qty/kraken_spot_qty/futures_qty must be provided"
            );
        }

        if params.dry_run {
            let (binance_spot_result, kraken_spot_result, futures_result) = tokio::join!(
                close_leg_dry_run(
                    binance_spot,
                    &params.symbol,
                    params.binance_spot_qty,
                    OrderSide::Sell,
                    params.client_order_id_prefix.as_ref().map(|p| format!("{p}-close-binance-spot")),
                    "binance_spot",
                ),
                close_leg_dry_run(
                    kraken_spot,
                    &params.symbol,
                    params.kraken_spot_qty,
                    OrderSide::Sell,
                    params.client_order_id_prefix.as_ref().map(|p| format!("{p}-close-kraken-spot")),
                    "kraken_spot",
                ),
                close_leg_dry_run(
                    binance_futures,
                    &params.symbol,
                    params.futures_qty,
                    OrderSide::Buy,
                    params.client_order_id_prefix.as_ref().map(|p| format!("{p}-close-futures")),
                    "futures",
                ),
            );
            return match (binance_spot_result, kraken_spot_result, futures_result) {
                (Ok(binance_spot_order), Ok(kraken_spot_order), Ok(futures_order)) => Ok(ClosePositionReport {
                    binance_spot_order,
                    kraken_spot_order,
                    futures_order,
                }),
                (binance_spot_result, kraken_spot_result, futures_result) => {
                    let msg = format!(
                        "close_hedged_position: not all legs succeeded, manual reconciliation needed -- {}; {}; {}",
                        describe_leg_result("binance_spot", &binance_spot_result),
                        describe_leg_result("kraken_spot", &kraken_spot_result),
                        describe_leg_result("futures", &futures_result),
                    );
                    log::error!("{msg}");
                    Err(anyhow::anyhow!(msg))
                }
            };
        }

        let (binance_spot_result, kraken_spot_result, futures_result) = tokio::join!(
            self.close_leg_live(
                binance_spot,
                &params.symbol,
                params.binance_spot_qty,
                OrderSide::Sell,
                params.client_order_id_prefix.as_ref().map(|p| format!("{p}-close-binance-spot")),
                params.fill_timeout,
                "binance_spot",
            ),
            self.close_leg_live(
                kraken_spot,
                &params.symbol,
                params.kraken_spot_qty,
                OrderSide::Sell,
                params.client_order_id_prefix.as_ref().map(|p| format!("{p}-close-kraken-spot")),
                params.fill_timeout,
                "kraken_spot",
            ),
            self.close_leg_live(
                binance_futures,
                &params.symbol,
                params.futures_qty,
                OrderSide::Buy,
                params.client_order_id_prefix.as_ref().map(|p| format!("{p}-close-futures")),
                params.fill_timeout,
                "futures",
            ),
        );

        match (binance_spot_result, kraken_spot_result, futures_result) {
            (Ok(binance_spot_order), Ok(kraken_spot_order), Ok(futures_order)) => {
                log::info!(
                    "close_hedged_position: binance_spot={:?} kraken_spot={:?} futures={:?}",
                    binance_spot_order,
                    kraken_spot_order,
                    futures_order
                );
                Ok(ClosePositionReport {
                    binance_spot_order,
                    kraken_spot_order,
                    futures_order,
                })
            }
            (binance_spot_result, kraken_spot_result, futures_result) => {
                let msg = format!(
                    "close_hedged_position: not all legs succeeded, manual reconciliation needed -- {}; {}; {}",
                    describe_leg_result("binance_spot", &binance_spot_result),
                    describe_leg_result("kraken_spot", &kraken_spot_result),
                    describe_leg_result("futures", &futures_result),
                );
                log::error!("{msg}");
                Err(anyhow::anyhow!(msg))
            }
        }
    }

    async fn close_leg_live(
        &self,
        provider: Option<&dyn OrderProvider>,
        symbol: &Symbol,
        qty: Option<Decimal>,
        side: OrderSide,
        client_order_id: Option<String>,
        timeout: Duration,
        leg_name: &str,
    ) -> anyhow::Result<Option<OrderResult>> {
        let Some(qty) = qty else {
            return Ok(None);
        };
        let Some(provider) = provider else {
            anyhow::bail!(
                "close_hedged_position: {leg_name} qty {qty} was provided but no provider is configured for this leg"
            );
        };
        let client_order_id = client_order_id.unwrap_or_else(|| format!("close-{leg_name}-{}", now_ms()));
        let fill = self
            .submit_and_wait_for_fill(provider.venue(), symbol.clone(), side, OrderAmount::Base(qty), client_order_id, timeout, provider)
            .await?;
        Ok(Some(self.build_order_result(fill)?))
    }

    /// 订阅一份独立的 `OrderEvent` 流、提交订单、反查 order_id、再在这份独立
    /// 的事件流里按 order_id 过滤等待——每条腿各等各的，可以安全并发（同一
    /// `strategy_id` 下多条腿共用同一个 broadcast sender，如果不按 order_id
    /// 过滤会互相"抢答"）。
    async fn submit_and_wait_for_fill(
        &self,
        venue: Venue,
        symbol: Symbol,
        side: OrderSide,
        amount: OrderAmount,
        client_order_id: String,
        timeout: Duration,
        provider: &dyn OrderProvider,
    ) -> anyhow::Result<(OrderId, Decimal, Decimal)> {
        let mut event_stream = self.bus.subscribe::<OrderEvent>(Topic::order_event(self.name()));
        self.submit_order(venue, symbol, side, amount, Some(client_order_id.clone()), None, None);

        let order_id = self.resolve_order_id(&client_order_id).await?;
        self.wait_for_fill(&mut event_stream, timeout, &order_id, &client_order_id, provider).await
    }

    async fn resolve_order_id(&self, client_order_id: &str) -> anyhow::Result<OrderId> {
        tokio::time::timeout(RESOLVE_ORDER_ID_TIMEOUT, async {
            loop {
                if let Some(order) = self.order_manager.find_by_client_order_id(client_order_id) {
                    return order.order_id;
                }
                tokio::time::sleep(RESOLVE_ORDER_ID_POLL_INTERVAL).await;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "client_order_id={client_order_id} was not submitted to the order manager within {RESOLVE_ORDER_ID_TIMEOUT:?}"
            )
        })
    }

    /// 超时后不直接报错，而是先走一次 REST 兜底核对(`provider.query_order`)：
    /// 私有 WS 断连/丢消息是真实会发生的失败模式，此时订单可能早已在交易所侧
    /// 成交，直接报错会让调用方误以为下单失败。
    async fn wait_for_fill(
        &self,
        event_stream: &mut BoxTopicStream<OrderEvent>,
        timeout: Duration,
        order_id: &OrderId,
        client_order_id: &str,
        provider: &dyn OrderProvider,
    ) -> anyhow::Result<(OrderId, Decimal, Decimal)> {
        match tokio::time::timeout(timeout, wait_for_order_event(event_stream, order_id)).await {
            Ok(result) => return result,
            Err(_) => {
                log::warn!(
                    "wait_for_fill: order_id={order_id} client_order_id={client_order_id} timed out after {timeout:?} waiting on exchange private WS, \
                     falling back to a REST query_order reconciliation check"
                );
            }
        }

        self.reconcile_via_rest_query(event_stream, order_id, provider)
            .await
            .with_context(|| {
                format!(
                    "timed out after {timeout:?} waiting for order (order_id={order_id}, client_order_id={client_order_id}) to fill via exchange private WS, \
                     and REST 核对也未确认成交，需要人工检查 (check that the exchange order-update WS stream is connected)"
                )
            })
    }

    async fn reconcile_via_rest_query(
        &self,
        event_stream: &mut BoxTopicStream<OrderEvent>,
        order_id: &OrderId,
        provider: &dyn OrderProvider,
    ) -> anyhow::Result<(OrderId, Decimal, Decimal)> {
        let order = self
            .order_manager
            .get_order(order_id)
            .with_context(|| format!("order {order_id} not found in order manager, cannot reconcile via REST"))?;
        let exchange_order_id = order
            .exchange_order_id
            .clone()
            .with_context(|| format!("order {order_id} has no exchange_order_id yet, cannot reconcile via REST"))?;

        let result = provider
            .query_order(&order.request.symbol, &exchange_order_id)
            .await
            .with_context(|| format!("REST query_order failed for order {order_id} (exchange_order_id={exchange_order_id})"))?;

        log::info!(
            "wait_for_fill: REST reconciliation for order {order_id} (exchange_order_id={exchange_order_id}) \
             returned status={:?} filled_qty={} avg_price={:?}",
            result.status,
            result.filled_qty,
            result.avg_price
        );

        self.order_manager
            .handle_exchange_update(ExchangeOrderUpdate {
                venue: order.request.venue.clone(),
                symbol: order.request.symbol.clone(),
                client_order_id: order.request.client_order_id.clone(),
                exchange_order_id: Some(exchange_order_id),
                status: result.status,
                filled_qty: result.filled_qty,
                avg_price: result.avg_price,
                fee: result.fee,
                fee_asset: result.fee_asset,
                ts_ms: now_ms(),
            })
            .await;

        match tokio::time::timeout(RECONCILE_REWAIT, wait_for_order_event(event_stream, order_id)).await {
            Ok(result) => result,
            Err(_) => anyhow::bail!(
                "REST 核对返回 status={:?} filled_qty={}，仍未确认成交",
                result.status,
                result.filled_qty
            ),
        }
    }

    /// 用成交结果 + `OrderManager` 里记录的交易所原始订单号拼一个
    /// `OrderResult`，`order_id` 字段优先用交易所原始订单号（比 `OrderManager`
    /// 内部的 `ORD-xxx` 对 CLI 使用者更有意义，方便去交易所后台核对）。
    fn build_order_result(&self, fill: (OrderId, Decimal, Decimal)) -> anyhow::Result<OrderResult> {
        let (order_id, filled_qty, avg_price) = fill;
        let order = self
            .order_manager
            .get_order(&order_id)
            .with_context(|| format!("order {order_id} disappeared from order manager after fill confirmation"))?;
        Ok(OrderResult {
            order_id: order.exchange_order_id.clone().unwrap_or_else(|| order_id.to_string()),
            status: order.status,
            filled_qty,
            avg_price: Some(avg_price),
            fee: None,
            fee_asset: None,
        })
    }
}

/// 循环消费事件流，只处理匹配 `target` order_id 的 `Filled`/`RejectedByRisk`/
/// `RejectedByExchange`，其余（包括别的 order_id 的事件）跳过继续等——这是
/// 允许多条腿并发共用同一个 `strategy_id` 下的事件流、又不会互相"抢答"的
/// 关键。
async fn wait_for_order_event(
    event_stream: &mut BoxTopicStream<OrderEvent>,
    target: &OrderId,
) -> anyhow::Result<(OrderId, Decimal, Decimal)> {
    loop {
        match event_stream.next().await {
            Some((_, OrderEvent::Filled { order_id, filled_qty, avg_price })) if &order_id == target => {
                return Ok((order_id, filled_qty, avg_price));
            }
            Some((_, OrderEvent::RejectedByRisk { order_id, reason })) if &order_id == target => {
                anyhow::bail!("order {order_id} rejected by risk: {reason}");
            }
            Some((_, OrderEvent::RejectedByExchange { order_id, reason })) if &order_id == target => {
                anyhow::bail!("order {order_id} rejected by exchange: {reason}");
            }
            Some(_) => continue,
            None => anyhow::bail!("order event stream closed while waiting for order {target} to fill"),
        }
    }
}

/// 单条平仓腿的 dry_run 路径：`qty=None` 直接跳过、不发任何请求；
/// `provider=None` 但 `qty=Some` 视为调用方配置错误，报错点名是哪条腿。
async fn close_leg_dry_run(
    provider: Option<&dyn OrderProvider>,
    symbol: &Symbol,
    qty: Option<Decimal>,
    side: OrderSide,
    client_order_id: Option<String>,
    leg_name: &str,
) -> anyhow::Result<Option<OrderResult>> {
    let Some(qty) = qty else {
        return Ok(None);
    };
    let Some(provider) = provider else {
        anyhow::bail!(
            "close_hedged_position: {leg_name} qty {qty} was provided but no provider is configured for this leg"
        );
    };
    let order = provider
        .place_market_order(MarketOrderRequest {
            symbol: symbol.clone(),
            side,
            amount: OrderAmount::Base(qty),
            client_order_id,
            dry_run: true,
        })
        .await?;
    Ok(Some(order))
}

/// 把一条腿的 `Result<Option<OrderResult>>` 描述成人类可读的一段话，用于拼
/// `close_hedged_position` 失败时的聚合错误信息。
fn describe_leg_result(label: &str, result: &anyhow::Result<Option<OrderResult>>) -> String {
    match result {
        Ok(Some(order)) => format!("{label} succeeded ({order:?})"),
        Ok(None) => format!("{label} skipped"),
        Err(e) => format!("{label} failed ({e})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use dashmap::DashMap;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use crate::exchange_info::types::{MarketPrecision, QtyPrecision};
    use crate::order::types::OrderStatus;
    use crate::order_manager::risk_service::RiskLimits;
    use crate::order_manager::store::InMemoryOrderStore;
    use crate::order_manager::types::Order;
    use crate::order_manager::{ExchangeAdapter, ExecutionService, InMemoryOrderIdAllocator, RiskService};
    use crate::portfolio::{InMemoryPnlStore, PortfolioManager};
    use crate::position::{InMemoryPositionStore, PositionManager};
    use crate::types::Venue;

    fn btc_usdt() -> Symbol {
        Symbol::new("BTC", "USDT")
    }

    fn futures_precision_cache(qty_step: &str, min_qty: &str) -> PrecisionCache {
        let precision = QtyPrecision {
            qty_step: qty_step.parse().unwrap(),
            min_qty: min_qty.parse().unwrap(),
        };
        PrecisionCache::from_precisions(vec![MarketPrecision {
            symbol: btc_usdt(),
            market: precision,
            limit: precision,
            price_tick: Decimal::ZERO,
        }])
    }

    fn open_params(quote_amount: Decimal, dry_run: bool) -> OpenPositionParams {
        OpenPositionParams {
            symbol: btc_usdt(),
            quote_amount,
            client_order_id_prefix: Some("test".to_string()),
            dry_run,
            fill_timeout: Duration::from_millis(200),
        }
    }

    fn rotate_params(dry_run: bool) -> RotateInventoryParams {
        RotateInventoryParams {
            symbol: btc_usdt(),
            qty: Decimal::new(1, 1),
            client_order_id_prefix: Some("test".to_string()),
            dry_run,
            fill_timeout: Duration::from_millis(200),
        }
    }

    fn close_params(
        binance_spot_qty: Option<Decimal>,
        kraken_spot_qty: Option<Decimal>,
        futures_qty: Option<Decimal>,
        dry_run: bool,
    ) -> ClosePositionParams {
        ClosePositionParams {
            symbol: btc_usdt(),
            binance_spot_qty,
            kraken_spot_qty,
            futures_qty,
            client_order_id_prefix: Some("test".to_string()),
            dry_run,
            fill_timeout: Duration::from_millis(200),
        }
    }

    /// 现货测试替身：`place_market_order_raw` 只接受 `OrderAmount::Quote`，固定
    /// 返回 `filled_qty`，并记录被真正调用（即走过 dry_run 之外分支）的次数。
    struct FakeSpotProvider {
        filled_qty: Decimal,
        fail: bool,
        quote_raw_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OrderProvider for FakeSpotProvider {
        fn venue(&self) -> Venue {
            Venue::new("fake-spot")
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            let OrderAmount::Quote(_) = req.amount else {
                unreachable!("spot leg only uses OrderAmount::Quote")
            };
            self.quote_raw_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                anyhow::bail!("simulated spot failure");
            }
            Ok(OrderResult {
                order_id: format!("spot-{}", req.symbol),
                status: OrderStatus::New,
                filled_qty: Decimal::ZERO,
                avg_price: None,
                fee: None,
                fee_asset: None,
            })
        }
    }

    /// 合约测试替身：记录每次真实下单的数量。
    struct FakeFuturesProvider {
        fail: bool,
        raw_calls: Arc<Mutex<Vec<Decimal>>>,
    }

    #[async_trait]
    impl OrderProvider for FakeFuturesProvider {
        fn venue(&self) -> Venue {
            Venue::new("fake-futures")
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            let OrderAmount::Base(quantity) = req.amount else {
                unreachable!("futures leg only uses OrderAmount::Base")
            };
            self.raw_calls.lock().unwrap().push(quantity);
            if self.fail {
                anyhow::bail!("simulated futures failure");
            }
            Ok(OrderResult {
                order_id: format!("futures-{}", req.symbol),
                status: OrderStatus::New,
                filled_qty: Decimal::ZERO,
                avg_price: None,
                fee: None,
                fee_asset: None,
            })
        }
    }

    /// `wait_for_fill` REST 兜底测试用的替身：REST 下单响应故意不带成交信息
    /// (`status=New`)，`query_order` 固定返回一笔成交，模拟"WS 没推送、REST
    /// 核对能查到已成交"的场景。
    struct QueryableProvider {
        venue: Venue,
        query_filled_qty: Decimal,
    }

    #[async_trait]
    impl OrderProvider for QueryableProvider {
        fn venue(&self) -> Venue {
            self.venue.clone()
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            Ok(OrderResult {
                order_id: format!("exchange-{}", req.symbol),
                status: OrderStatus::New,
                filled_qty: Decimal::ZERO,
                avg_price: None,
                fee: None,
                fee_asset: None,
            })
        }
        async fn query_order(&self, _symbol: &Symbol, exchange_order_id: &str) -> anyhow::Result<OrderResult> {
            Ok(OrderResult {
                order_id: exchange_order_id.to_string(),
                status: OrderStatus::Filled,
                filled_qty: self.query_filled_qty,
                avg_price: Some(Decimal::ONE),
                fee: None,
                fee_asset: None,
            })
        }
    }

    /// `rotate_inventory`/`close_hedged_position` live 路径测试替身：记录每次
    /// 真实下单（走过 dry_run 之外分支）的调用参数，`fail=true` 时对
    /// `place_market_order_raw` 报错。REST 响应本身不带成交状态(status=New)，
    /// 成交必须由测试通过 `drive_fill` 模拟私有 WS 推送来驱动，和真实交易所
    /// 的设计一致。
    struct FakeLegProvider {
        name: &'static str,
        fail: bool,
        raw_calls: Arc<Mutex<Vec<(OrderSide, Decimal)>>>,
    }

    #[async_trait]
    impl OrderProvider for FakeLegProvider {
        fn venue(&self) -> Venue {
            Venue::new(self.name)
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            let OrderAmount::Base(quantity) = req.amount else {
                unreachable!("rotate/close legs only use OrderAmount::Base")
            };
            self.raw_calls.lock().unwrap().push((req.side, quantity));
            if self.fail {
                anyhow::bail!("simulated {} failure", self.name);
            }
            Ok(OrderResult {
                order_id: format!("{}-{}", self.name, req.symbol),
                status: OrderStatus::New,
                filled_qty: Decimal::ZERO,
                avg_price: None,
                fee: None,
                fee_asset: None,
            })
        }
    }

    fn leg_provider(name: &'static str, fail: bool) -> (Arc<dyn OrderProvider>, Arc<Mutex<Vec<(OrderSide, Decimal)>>>) {
        let raw_calls = Arc::new(Mutex::new(Vec::new()));
        let provider: Arc<dyn OrderProvider> = Arc::new(FakeLegProvider {
            name,
            fail,
            raw_calls: raw_calls.clone(),
        });
        (provider, raw_calls)
    }

    /// `rotate_inventory`/`close_hedged_position` dry_run 路径测试替身：dry_run
    /// 分支不接触 bus/OrderManager，直接调用 `provider.place_market_order`，
    /// 所以可以直接同步返回 `Filled`。
    struct FakeDryRunProvider {
        name: &'static str,
        raw_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OrderProvider for FakeDryRunProvider {
        fn venue(&self) -> Venue {
            Venue::new(self.name)
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            self.raw_calls.fetch_add(1, Ordering::SeqCst);
            Ok(OrderResult {
                order_id: format!("{}-{}", self.name, req.symbol),
                status: OrderStatus::Filled,
                filled_qty: req.amount.value(),
                avg_price: Some(Decimal::ONE),
                fee: None,
                fee_asset: None,
            })
        }
    }

    /// 内存版全套依赖：`TopicBus` + `RiskService` + `ExecutionService` +
    /// `OrderManager`，供 live 路径测试使用。
    struct LiveEnv {
        bus: Arc<TopicBus>,
        order_manager: Arc<OrderManager>,
        position_manager: Arc<PositionManager>,
        _risk_handle: tokio::task::JoinHandle<()>,
        _execution_handle: tokio::task::JoinHandle<()>,
    }

    fn setup_live_env(providers: Vec<Arc<dyn OrderProvider>>) -> LiveEnv {
        let symbol = btc_usdt();
        let mut risk_limits = HashMap::new();
        let mut adapters = HashMap::new();
        for provider in &providers {
            let venue = provider.venue();
            risk_limits.insert(
                (venue.clone(), symbol.clone()),
                RiskLimits {
                    max_order_amount: Decimal::MAX,
                    max_position: Decimal::MAX,
                    max_orders_per_window: 10,
                },
            );
            adapters.insert(venue.clone(), Arc::new(ExchangeAdapter::new(venue, provider.clone())));
        }

        let bus = Arc::new(TopicBus::new());
        let position_manager = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let quote_cache = Arc::new(DashMap::new());
        let portfolio = Arc::new(PortfolioManager::new(
            position_manager.clone(),
            Arc::new(InMemoryPnlStore::new()),
            quote_cache,
            HashMap::new(),
        ));

        let order_store = Arc::new(InMemoryOrderStore::new());
        let order_id_allocator = Arc::new(InMemoryOrderIdAllocator::new());

        let risk_service = Arc::new(RiskService::new(
            bus.clone(),
            order_id_allocator,
            order_store.clone(),
            risk_limits,
            position_manager.clone(),
        ));

        let execution_service = Arc::new(ExecutionService::new(bus.clone(), adapters, order_store.clone()));

        let order_manager = Arc::new(OrderManager::new(
            bus.clone(),
            position_manager.clone(),
            portfolio,
            order_store,
            None,
        ));

        let risk_handle = risk_service.clone().start();
        let execution_handle = execution_service.clone().start();

        LiveEnv {
            bus,
            order_manager,
            position_manager,
            _risk_handle: risk_handle,
            _execution_handle: execution_handle,
        }
    }

    fn bare_manual_strategy() -> ManualStrategy {
        let env = setup_live_env(Vec::new());
        ManualStrategy::new(env.bus.clone(), env.order_manager.clone())
    }

    /// 轮询直到 `order_manager` 里出现指定 `client_order_id` 的订单——
    /// `submit_order` 对索引的写入发生在它自己的异步任务里，测试驱动 WS 推送
    /// 前需要等这个写入落地，否则 `handle_exchange_update` 会因为查不到订单
    /// 而静默丢弃推送。
    async fn poll_order_by_client_id(order_manager: &OrderManager, client_order_id: &str) -> Order {
        for _ in 0..500 {
            if let Some(order) = order_manager
                .all_orders()
                .into_iter()
                .find(|o| o.request.client_order_id.as_deref() == Some(client_order_id))
            {
                return order;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        panic!("order with client_order_id={client_order_id} was never submitted to the order manager");
    }

    /// 模拟交易所私有 WS 推送一次完全成交。
    async fn drive_fill(
        order_manager: &OrderManager,
        venue: &Venue,
        client_order_id: &str,
        filled_qty: Decimal,
        avg_price: Decimal,
    ) {
        let order = poll_order_by_client_id(order_manager, client_order_id).await;
        order_manager
            .handle_exchange_update(ExchangeOrderUpdate {
                venue: venue.clone(),
                symbol: order.request.symbol.clone(),
                client_order_id: Some(client_order_id.to_string()),
                exchange_order_id: order.exchange_order_id.clone(),
                status: OrderStatus::Filled,
                filled_qty,
                avg_price: Some(avg_price),
                fee: None,
                fee_asset: None,
                ts_ms: 1,
            })
            .await;
    }

    #[tokio::test]
    async fn dry_run_only_touches_spot_leg() {
        let spot_calls = Arc::new(AtomicUsize::new(0));
        let spot = FakeSpotProvider {
            filled_qty: Decimal::new(1, 1),
            fail: false,
            quote_raw_calls: spot_calls.clone(),
        };

        let report = open_hedged_position_dry_run(&spot, open_params(Decimal::new(100, 0), true))
            .await
            .unwrap();

        assert_eq!(report.spot_order.order_id, "dry-run");
        assert!(report.futures_order.is_none());
        assert!(report.spot_exchange_order_id.is_none());
        assert!(report.futures_exchange_order_id.is_none());
        assert!(report.note.unwrap().contains("dry_run=true"));
        assert_eq!(spot_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn zero_filled_qty_aborts_before_futures() {
        let spot_arc: Arc<dyn OrderProvider> = Arc::new(FakeSpotProvider {
            filled_qty: Decimal::ZERO,
            fail: false,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        });
        let futures_calls = Arc::new(Mutex::new(Vec::new()));
        let futures_arc: Arc<dyn OrderProvider> = Arc::new(FakeFuturesProvider {
            fail: false,
            raw_calls: futures_calls.clone(),
        });
        let futures_precision = futures_precision_cache("0.001", "0.001");

        let env = setup_live_env(vec![spot_arc.clone(), futures_arc.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let spot_venue = spot_arc.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &spot_venue, "test-spot", Decimal::ZERO, Decimal::ONE).await;
        });

        let err = strategy
            .open_hedged_position_live(spot_arc.as_ref(), futures_arc.as_ref(), &futures_precision, open_params(Decimal::new(100, 0), false))
            .await
            .unwrap_err();
        driver.await.unwrap();

        assert!(err.to_string().contains("filled_qty is zero"));
        assert!(futures_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn wait_for_fill_falls_back_to_rest_query_on_ws_timeout() {
        let venue = Venue::new("fake-queryable");
        let provider: Arc<dyn OrderProvider> = Arc::new(QueryableProvider {
            venue: venue.clone(),
            query_filled_qty: Decimal::new(5, 1), // 0.5
        });

        let env = setup_live_env(vec![provider.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let client_order_id = "test-query-fallback".to_string();
        let (order_id, filled_qty, avg_price) = strategy
            .submit_and_wait_for_fill(
                venue,
                btc_usdt(),
                OrderSide::Buy,
                OrderAmount::Base(Decimal::ONE),
                client_order_id,
                Duration::from_millis(50),
                provider.as_ref(),
            )
            .await
            .unwrap();

        assert_eq!(filled_qty, Decimal::new(5, 1));
        assert_eq!(avg_price, Decimal::ONE);

        let final_order = env.order_manager.get_order(&order_id).unwrap();
        assert_eq!(final_order.status, OrderStatus::Filled);
        assert_eq!(final_order.filled_qty, Decimal::new(5, 1));
    }

    #[tokio::test]
    async fn spot_failure_stops_before_futures() {
        let spot_arc: Arc<dyn OrderProvider> = Arc::new(FakeSpotProvider {
            filled_qty: Decimal::new(1, 1),
            fail: true,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        });
        let futures_calls = Arc::new(Mutex::new(Vec::new()));
        let futures_arc: Arc<dyn OrderProvider> = Arc::new(FakeFuturesProvider {
            fail: false,
            raw_calls: futures_calls.clone(),
        });
        let futures_precision = futures_precision_cache("0.001", "0.001");

        let env = setup_live_env(vec![spot_arc.clone(), futures_arc.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let err = strategy
            .open_hedged_position_live(spot_arc.as_ref(), futures_arc.as_ref(), &futures_precision, open_params(Decimal::new(100, 0), false))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("simulated spot failure"));
        assert!(futures_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn futures_failure_stops_after_spot_fill() {
        let spot_filled_qty = Decimal::new(1, 1);
        let spot_arc: Arc<dyn OrderProvider> = Arc::new(FakeSpotProvider {
            filled_qty: spot_filled_qty,
            fail: false,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        });
        let futures_arc: Arc<dyn OrderProvider> = Arc::new(FakeFuturesProvider {
            fail: true,
            raw_calls: Arc::new(Mutex::new(Vec::new())),
        });
        let futures_precision = futures_precision_cache("0.001", "0.001");

        let env = setup_live_env(vec![spot_arc.clone(), futures_arc.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let spot_venue = spot_arc.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &spot_venue, "test-spot", spot_filled_qty, Decimal::ONE).await;
        });

        let err = strategy
            .open_hedged_position_live(spot_arc.as_ref(), futures_arc.as_ref(), &futures_precision, open_params(Decimal::new(100, 0), false))
            .await
            .unwrap_err();
        driver.await.unwrap();

        assert!(err.to_string().contains("simulated futures failure"));
    }

    #[tokio::test]
    async fn futures_qty_below_min_aborts_before_order() {
        let spot_filled_qty = Decimal::new(5, 4); // 0.0005
        let spot_arc: Arc<dyn OrderProvider> = Arc::new(FakeSpotProvider {
            filled_qty: spot_filled_qty,
            fail: false,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        });
        let futures_calls = Arc::new(Mutex::new(Vec::new()));
        let futures_arc: Arc<dyn OrderProvider> = Arc::new(FakeFuturesProvider {
            fail: false,
            raw_calls: futures_calls.clone(),
        });
        let futures_precision = futures_precision_cache("0.001", "0.001");

        let env = setup_live_env(vec![spot_arc.clone(), futures_arc.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let spot_venue = spot_arc.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &spot_venue, "test-spot", spot_filled_qty, Decimal::ONE).await;
        });

        let err = strategy
            .open_hedged_position_live(spot_arc.as_ref(), futures_arc.as_ref(), &futures_precision, open_params(Decimal::new(100, 0), false))
            .await
            .unwrap_err();
        driver.await.unwrap();

        assert!(format!("{err:#}").contains("below min_qty"));
        assert!(futures_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn open_happy_path_hedges_both_legs_and_updates_positions() {
        let spot_filled_qty = Decimal::new(1234567, 6); // 1.234567
        let spot_arc: Arc<dyn OrderProvider> = Arc::new(FakeSpotProvider {
            filled_qty: spot_filled_qty,
            fail: false,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        });
        let futures_calls = Arc::new(Mutex::new(Vec::new()));
        let futures_arc: Arc<dyn OrderProvider> = Arc::new(FakeFuturesProvider {
            fail: false,
            raw_calls: futures_calls.clone(),
        });
        let futures_precision = futures_precision_cache("0.01", "0.01");

        let env = setup_live_env(vec![spot_arc.clone(), futures_arc.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let spot_venue = spot_arc.venue();
        let futures_venue = futures_arc.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &spot_venue, "test-spot", spot_filled_qty, Decimal::ONE).await;
            // 1.234567 向下取整到 0.01 的整数倍 -> 1.23
            drive_fill(&order_manager, &futures_venue, "test-futures", Decimal::new(123, 2), Decimal::ONE).await;
        });

        let report = strategy
            .open_hedged_position_live(spot_arc.as_ref(), futures_arc.as_ref(), &futures_precision, open_params(Decimal::new(100, 0), false))
            .await
            .unwrap();
        driver.await.unwrap();

        assert_eq!(futures_calls.lock().unwrap().len(), 1);
        assert_eq!(report.spot_exchange_order_id, Some("spot-BTC/USDT".to_string()));
        assert_eq!(report.futures_exchange_order_id, Some("futures-BTC/USDT".to_string()));
        assert!(report.note.is_none());

        // 这就是这次改造要保证的行为：成交后仓位真正落进 PositionManager。
        assert_eq!(env.position_manager.position(&spot_arc.venue(), &btc_usdt()), spot_filled_qty);
        assert_eq!(env.position_manager.position(&futures_arc.venue(), &btc_usdt()), -Decimal::new(123, 2));
    }

    #[tokio::test]
    async fn rotate_inventory_dry_run_skips_both_raw_calls() {
        let sell_calls = Arc::new(AtomicUsize::new(0));
        let buy_calls = Arc::new(AtomicUsize::new(0));
        let sell = FakeDryRunProvider { name: "sell-venue", raw_calls: sell_calls.clone() };
        let buy = FakeDryRunProvider { name: "buy-venue", raw_calls: buy_calls.clone() };

        let strategy = bare_manual_strategy();
        let report = strategy.rotate_inventory(&sell, &buy, rotate_params(true)).await.unwrap();

        assert_eq!(report.sell_order.order_id, "dry-run");
        assert_eq!(report.buy_order.order_id, "dry-run");
        assert_eq!(sell_calls.load(Ordering::SeqCst), 0);
        assert_eq!(buy_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rotate_inventory_happy_path_fills_both_legs_and_updates_positions() {
        let (sell, sell_calls) = leg_provider("sell-venue", false);
        let (buy, buy_calls) = leg_provider("buy-venue", false);

        let env = setup_live_env(vec![sell.clone(), buy.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let sell_venue = sell.venue();
        let buy_venue = buy.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &sell_venue, "test-sell", Decimal::new(1, 1), Decimal::ONE).await;
            drive_fill(&order_manager, &buy_venue, "test-buy", Decimal::new(1, 1), Decimal::ONE).await;
        });

        let report = strategy.rotate_inventory(sell.as_ref(), buy.as_ref(), rotate_params(false)).await.unwrap();
        driver.await.unwrap();

        assert_eq!(report.sell_order.order_id, "sell-venue-BTC/USDT");
        assert_eq!(report.buy_order.order_id, "buy-venue-BTC/USDT");
        assert_eq!(sell_calls.lock().unwrap().len(), 1);
        assert_eq!(buy_calls.lock().unwrap().len(), 1);

        // 修复前：rotate_inventory 完全绕过 OrderManager，仓位不会落账。
        assert_eq!(env.position_manager.position(&sell.venue(), &btc_usdt()), Decimal::new(-1, 1));
        assert_eq!(env.position_manager.position(&buy.venue(), &btc_usdt()), Decimal::new(1, 1));
    }

    #[tokio::test]
    async fn rotate_inventory_concurrent_legs_do_not_cross_match_out_of_order_fills() {
        let (sell, _sell_calls) = leg_provider("sell-venue", false);
        let (buy, _buy_calls) = leg_provider("buy-venue", false);

        let env = setup_live_env(vec![sell.clone(), buy.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let sell_venue = sell.venue();
        let buy_venue = buy.venue();
        let driver = tokio::spawn(async move {
            // 故意先驱动买腿成交、再驱动卖腿——验证两条腿各自的等待不会被
            // 对方的 Filled 事件"抢答"，各自拿到自己 order_id 对应的成交量/均价。
            drive_fill(&order_manager, &buy_venue, "test-buy", Decimal::new(12, 2), Decimal::TWO).await;
            drive_fill(&order_manager, &sell_venue, "test-sell", Decimal::new(11, 2), Decimal::ONE).await;
        });

        let report = strategy.rotate_inventory(sell.as_ref(), buy.as_ref(), rotate_params(false)).await.unwrap();
        driver.await.unwrap();

        assert_eq!(report.sell_order.filled_qty, Decimal::new(11, 2));
        assert_eq!(report.sell_order.avg_price, Some(Decimal::ONE));
        assert_eq!(report.buy_order.filled_qty, Decimal::new(12, 2));
        assert_eq!(report.buy_order.avg_price, Some(Decimal::TWO));
    }

    #[tokio::test]
    async fn rotate_inventory_sell_failure_still_places_buy_leg() {
        let (sell, sell_calls) = leg_provider("sell-venue", true);
        let (buy, buy_calls) = leg_provider("buy-venue", false);

        let env = setup_live_env(vec![sell.clone(), buy.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let buy_venue = buy.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &buy_venue, "test-buy", Decimal::new(1, 1), Decimal::ONE).await;
        });

        let err = strategy.rotate_inventory(sell.as_ref(), buy.as_ref(), rotate_params(false)).await.unwrap_err();
        driver.await.unwrap();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("buy-venue-BTC/USDT"));
        assert_eq!(sell_calls.lock().unwrap().len(), 1);
        assert_eq!(buy_calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn rotate_inventory_buy_failure_still_places_sell_leg() {
        let (sell, sell_calls) = leg_provider("sell-venue", false);
        let (buy, buy_calls) = leg_provider("buy-venue", true);

        let env = setup_live_env(vec![sell.clone(), buy.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let sell_venue = sell.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &sell_venue, "test-sell", Decimal::new(1, 1), Decimal::ONE).await;
        });

        let err = strategy.rotate_inventory(sell.as_ref(), buy.as_ref(), rotate_params(false)).await.unwrap_err();
        driver.await.unwrap();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("sell-venue-BTC/USDT"));
        assert_eq!(sell_calls.lock().unwrap().len(), 1);
        assert_eq!(buy_calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn rotate_inventory_both_legs_fail() {
        let (sell, _sell_calls) = leg_provider("sell-venue", true);
        let (buy, _buy_calls) = leg_provider("buy-venue", true);

        let env = setup_live_env(vec![sell.clone(), buy.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let err = strategy.rotate_inventory(sell.as_ref(), buy.as_ref(), rotate_params(false)).await.unwrap_err();

        assert!(err.to_string().contains("simulated sell-venue failure"));
        assert!(err.to_string().contains("simulated buy-venue failure"));
    }

    #[tokio::test]
    async fn close_all_three_legs_dry_run_skips_raw_calls() {
        let bs_calls = Arc::new(AtomicUsize::new(0));
        let ks_calls = Arc::new(AtomicUsize::new(0));
        let f_calls = Arc::new(AtomicUsize::new(0));
        let binance_spot = FakeDryRunProvider { name: "binance-spot", raw_calls: bs_calls.clone() };
        let kraken_spot = FakeDryRunProvider { name: "kraken-spot", raw_calls: ks_calls.clone() };
        let futures = FakeDryRunProvider { name: "futures", raw_calls: f_calls.clone() };

        let strategy = bare_manual_strategy();
        let report = strategy
            .close_hedged_position(
                Some(&binance_spot),
                Some(&kraken_spot),
                Some(&futures),
                close_params(Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), true),
            )
            .await
            .unwrap();

        assert_eq!(report.binance_spot_order.unwrap().order_id, "dry-run");
        assert_eq!(report.kraken_spot_order.unwrap().order_id, "dry-run");
        assert_eq!(report.futures_order.unwrap().order_id, "dry-run");
        assert_eq!(bs_calls.load(Ordering::SeqCst), 0);
        assert_eq!(ks_calls.load(Ordering::SeqCst), 0);
        assert_eq!(f_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn close_happy_path_fills_all_three_legs_and_updates_positions() {
        let (binance_spot, bs_calls) = leg_provider("binance-spot", false);
        let (kraken_spot, ks_calls) = leg_provider("kraken-spot", false);
        let (futures, f_calls) = leg_provider("futures", false);

        let env = setup_live_env(vec![binance_spot.clone(), kraken_spot.clone(), futures.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let bs_venue = binance_spot.venue();
        let ks_venue = kraken_spot.venue();
        let f_venue = futures.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &bs_venue, "test-close-binance-spot", Decimal::new(1, 1), Decimal::ONE).await;
            drive_fill(&order_manager, &ks_venue, "test-close-kraken-spot", Decimal::new(2, 1), Decimal::ONE).await;
            drive_fill(&order_manager, &f_venue, "test-close-futures", Decimal::new(3, 1), Decimal::ONE).await;
        });

        let report = strategy
            .close_hedged_position(
                Some(binance_spot.as_ref()),
                Some(kraken_spot.as_ref()),
                Some(futures.as_ref()),
                close_params(Some(Decimal::new(1, 1)), Some(Decimal::new(2, 1)), Some(Decimal::new(3, 1)), false),
            )
            .await
            .unwrap();
        driver.await.unwrap();

        assert_eq!(report.binance_spot_order.unwrap().order_id, "binance-spot-BTC/USDT");
        assert_eq!(report.kraken_spot_order.unwrap().order_id, "kraken-spot-BTC/USDT");
        assert_eq!(report.futures_order.unwrap().order_id, "futures-BTC/USDT");
        assert_eq!(bs_calls.lock().unwrap().as_slice(), &[(OrderSide::Sell, Decimal::new(1, 1))]);
        assert_eq!(ks_calls.lock().unwrap().as_slice(), &[(OrderSide::Sell, Decimal::new(2, 1))]);
        assert_eq!(f_calls.lock().unwrap().as_slice(), &[(OrderSide::Buy, Decimal::new(3, 1))]);

        assert_eq!(env.position_manager.position(&binance_spot.venue(), &btc_usdt()), Decimal::new(-1, 1));
        assert_eq!(env.position_manager.position(&kraken_spot.venue(), &btc_usdt()), Decimal::new(-2, 1));
        assert_eq!(env.position_manager.position(&futures.venue(), &btc_usdt()), Decimal::new(3, 1));
    }

    #[tokio::test]
    async fn close_skips_leg_with_qty_none() {
        let (binance_spot, bs_calls) = leg_provider("binance-spot", false);
        let (kraken_spot, ks_calls) = leg_provider("kraken-spot", false);
        let (futures, f_calls) = leg_provider("futures", false);

        let env = setup_live_env(vec![binance_spot.clone(), kraken_spot.clone(), futures.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let f_venue = futures.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &f_venue, "test-close-futures", Decimal::new(1, 1), Decimal::ONE).await;
        });

        let report = strategy
            .close_hedged_position(
                Some(binance_spot.as_ref()),
                Some(kraken_spot.as_ref()),
                Some(futures.as_ref()),
                close_params(None, None, Some(Decimal::new(1, 1)), false),
            )
            .await
            .unwrap();
        driver.await.unwrap();

        assert!(report.binance_spot_order.is_none());
        assert!(report.kraken_spot_order.is_none());
        assert!(report.futures_order.is_some());
        assert!(bs_calls.lock().unwrap().is_empty());
        assert!(ks_calls.lock().unwrap().is_empty());
        assert_eq!(f_calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn close_binance_spot_failure_still_places_other_two_legs() {
        let (binance_spot, _) = leg_provider("binance-spot", true);
        let (kraken_spot, ks_calls) = leg_provider("kraken-spot", false);
        let (futures, f_calls) = leg_provider("futures", false);

        let env = setup_live_env(vec![binance_spot.clone(), kraken_spot.clone(), futures.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let order_manager = env.order_manager.clone();
        let ks_venue = kraken_spot.venue();
        let f_venue = futures.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&order_manager, &ks_venue, "test-close-kraken-spot", Decimal::new(1, 1), Decimal::ONE).await;
            drive_fill(&order_manager, &f_venue, "test-close-futures", Decimal::new(1, 1), Decimal::ONE).await;
        });

        let err = strategy
            .close_hedged_position(
                Some(binance_spot.as_ref()),
                Some(kraken_spot.as_ref()),
                Some(futures.as_ref()),
                close_params(Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), false),
            )
            .await
            .unwrap_err();
        driver.await.unwrap();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("binance_spot failed"));
        assert_eq!(ks_calls.lock().unwrap().len(), 1);
        assert_eq!(f_calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn close_all_three_legs_fail_combined_error_message() {
        let (binance_spot, _) = leg_provider("binance-spot", true);
        let (kraken_spot, _) = leg_provider("kraken-spot", true);
        let (futures, _) = leg_provider("futures", true);

        let env = setup_live_env(vec![binance_spot.clone(), kraken_spot.clone(), futures.clone()]);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let strategy = ManualStrategy::new(env.bus.clone(), env.order_manager.clone());

        let err = strategy
            .close_hedged_position(
                Some(binance_spot.as_ref()),
                Some(kraken_spot.as_ref()),
                Some(futures.as_ref()),
                close_params(Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), false),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("simulated binance-spot failure"));
        assert!(err.to_string().contains("simulated kraken-spot failure"));
        assert!(err.to_string().contains("simulated futures failure"));
    }

    #[tokio::test]
    async fn close_validation_error_when_qty_is_non_positive() {
        let strategy = bare_manual_strategy();
        let (binance_spot, bs_calls) = leg_provider("binance-spot", false);

        let err = strategy
            .close_hedged_position(Some(binance_spot.as_ref()), None, None, close_params(Some(Decimal::ZERO), None, None, false))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("binance_spot qty must be positive"));
        assert!(bs_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn close_validation_error_when_no_leg_requested() {
        let strategy = bare_manual_strategy();
        let err = strategy
            .close_hedged_position(None, None, None, close_params(None, None, None, false))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("at least one of"));
    }

    #[tokio::test]
    async fn close_qty_provided_without_matching_provider_errors() {
        let strategy = bare_manual_strategy();
        let err = strategy
            .close_hedged_position(None, None, None, close_params(Some(Decimal::new(1, 1)), None, None, false))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("binance_spot"));
        assert!(err.to_string().contains("no provider is configured"));
    }
}
