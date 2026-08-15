use std::collections::HashSet;
use std::time::Duration;

use anyhow::Context;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::exchange_info::types::PrecisionKind;
use crate::exchange_info::PrecisionCache;
use crate::market_data::now_ms;
use crate::order::types::{MarketOrderRequest, OrderAmount, OrderResult, OrderSide};
use crate::order::OrderProvider;
use crate::order_manager::stream::ExchangeOrderUpdate;
use crate::order_manager::types::{OrderEvent, OrderId, OrderRequest};
use crate::order_manager::OrderManager;
use crate::types::Symbol;
use crate::wallet::types::{WithdrawRequest, WithdrawResult};
use crate::wallet::WalletProvider;

/// 开仓参数：现货按 USDT 金额买入，合约等量做空对冲，买到的一半可选划转到 Kraken。
#[derive(Debug, Clone)]
pub struct OpenPositionParams {
    pub symbol: Symbol,
    pub quote_amount: Decimal,
    /// 划转到 Kraken 的资产代码，通常等于 `symbol.base`。
    pub transfer_asset: String,
    pub client_order_id_prefix: Option<String>,
    pub dry_run: bool,
    /// true 时才在两条腿都对冲完成后把买入量的一半划转到 Kraken；默认应为
    /// false（跳过），只做建仓和记账。
    pub transfer_to_kraken: bool,
    /// 等待 [`OrderManager`] 通过交易所私有 WS 确认成交的超时时间；只在
    /// `open_hedged_position_live` 里使用。
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
    pub transfer_qty: Option<Decimal>,
    pub withdraw: Option<WithdrawResult>,
    pub note: Option<String>,
}

/// dry_run 路径：只调用现货 `place_market_order(dry_run=true)` 做参数校验和
/// 模拟，完全不接触 `OrderManager`/风控/Redis，和 `open_hedged_position_live`
/// 相互隔离——`ExchangeAdapter::submit` 本身就没有 dry-run 概念，硬塞进同一个
/// 函数只会让真实下单路径多一层不必要的分支判断。
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
        transfer_qty: None,
        withdraw: None,
        note: Some(
            "dry_run=true：仅校验并模拟了现货买入这一步；合约对冲和划转数量依赖真实成交量，dry-run 下不做模拟。"
                .to_string(),
        ),
    })
}

/// live 路径：现货买入、合约对冲两条腿都通过 `order_manager.submit_order`
/// 走完整的风控 -> 执行引擎 -> 交易所私有 WS 成交确认流水线（`wait_for_fill`），
/// 而不是直接用 REST 同步响应记账——这样 `OrderManager::handle_exchange_update`
/// 才会被触发，仓位/盈亏才会真正落进 `PositionManager`/`PortfolioManager`。
/// 只有 `params.transfer_to_kraken=true` 时才在最后划转到 Kraken；任何一步
/// 失败都直接 `?` 向上传播，不做自动回滚——半吊子仓位需要人工介入。
pub async fn open_hedged_position_live(
    spot: &dyn OrderProvider,
    futures: &dyn OrderProvider,
    binance_wallet: &dyn WalletProvider,
    kraken_wallet: &dyn WalletProvider,
    order_manager: &OrderManager,
    event_rx: &mut mpsc::Receiver<OrderEvent>,
    futures_precision: &PrecisionCache,
    params: OpenPositionParams,
) -> anyhow::Result<OpenPositionReport> {
    let spot_request = OrderRequest {
        strategy_name: "manual-open".to_string(),
        venue: spot.venue(),
        symbol: params.symbol.clone(),
        side: OrderSide::Buy,
        amount: OrderAmount::Quote(params.quote_amount),
        client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-spot")),
        group_id: None,
        metadata: None,
    };
    let spot_response = order_manager.submit_order(spot_request).await;
    let spot_order_id = spot_response.order_id.clone();
    spot_response
        .result_rx
        .await
        .context("order manager dropped result channel for spot order")?
        .map_err(|reason| anyhow::anyhow!("spot order rejected: {reason}"))?;

    let (filled_qty, spot_avg_price) =
        wait_for_fill(event_rx, &spot_order_id, params.fill_timeout, spot, order_manager).await?;
    if filled_qty <= Decimal::ZERO {
        anyhow::bail!("spot buy filled_qty is zero, aborting hedge/transfer");
    }
    log::info!("open_hedged_position_live: spot buy filled qty={filled_qty} avg_price={spot_avg_price}");
    let spot_order_after_fill = order_manager
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

    let futures_request = OrderRequest {
        strategy_name: "manual-open".to_string(),
        venue: futures.venue(),
        symbol: params.symbol.clone(),
        side: OrderSide::Sell,
        amount: OrderAmount::Base(futures_qty),
        client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-futures")),
        group_id: None,
        metadata: None,
    };
    let futures_response = order_manager.submit_order(futures_request).await;
    let futures_order_id = futures_response.order_id.clone();
    futures_response
        .result_rx
        .await
        .context("order manager dropped result channel for futures order")?
        .map_err(|reason| anyhow::anyhow!("futures order rejected: {reason}"))?;

    let (futures_filled_qty, futures_avg_price) =
        wait_for_fill(event_rx, &futures_order_id, params.fill_timeout, futures, order_manager).await?;
    log::info!("open_hedged_position_live: futures hedge filled qty={futures_filled_qty} avg_price={futures_avg_price}");
    let futures_order_after_fill = order_manager
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

    if !params.transfer_to_kraken {
        return Ok(OpenPositionReport {
            spot_order: spot_result,
            futures_order: Some(futures_result),
            spot_exchange_order_id,
            futures_exchange_order_id,
            transfer_qty: None,
            withdraw: None,
            note: Some("transfer_to_kraken=false（默认）：跳过划转到 Kraken 这一步。".to_string()),
        });
    }

    let (transfer_qty, withdraw) = transfer_half_to_kraken(
        binance_wallet,
        kraken_wallet,
        TransferHalfParams {
            filled_qty,
            transfer_asset: params.transfer_asset.clone(),
            dry_run: false,
        },
    )
    .await?;

    Ok(OpenPositionReport {
        spot_order: spot_result,
        futures_order: Some(futures_result),
        spot_exchange_order_id,
        futures_exchange_order_id,
        transfer_qty: Some(transfer_qty),
        withdraw: Some(withdraw),
        note: None,
    })
}

/// 循环读 `event_rx` 直到收到 `order_id` 对应的 `Filled` 事件(返回累计成交量
/// 和均价)，或者收到该订单被风控/交易所拒绝的事件(直接报错)。不匹配
/// `order_id` 的事件会被忽略——同一个 `event_rx` 在 `open_hedged_position_live`
/// 里被现货、合约两条腿先后共用。`timeout` 防止私有 WS 没连上时无限等待。
///
/// 超时后不直接报错，而是先走一次 REST 兜底核对(`provider.query_order`)：
/// 私有 WS 断连/丢消息是真实会发生的失败模式，此时订单可能早已在交易所侧
/// 成交，直接报错会让调用方误以为下单失败。查到的结果通过
/// `order_manager.handle_exchange_update` 写回(自动完成 Redis upsert + 风控/
/// 账本更新，和 WS 推送走完全相同的路径)，再对 `event_rx` 做一次短超时
/// 等待去捕获这次 reconcile 触发的事件。REST 核对也未能确认成交(查询失败、
/// 或订单在交易所侧确实还不是终态)才真正报错。
async fn wait_for_fill(
    event_rx: &mut mpsc::Receiver<OrderEvent>,
    order_id: &OrderId,
    timeout: Duration,
    provider: &dyn OrderProvider,
    order_manager: &OrderManager,
) -> anyhow::Result<(Decimal, Decimal)> {
    match tokio::time::timeout(timeout, wait_for_fill_events(event_rx, order_id)).await {
        Ok(result) => return result,
        Err(_) => {
            log::warn!(
                "wait_for_fill: order {order_id} timed out after {timeout:?} waiting on exchange private WS, \
                 falling back to a REST query_order reconciliation check"
            );
        }
    }

    reconcile_via_rest_query(event_rx, order_id, provider, order_manager)
        .await
        .with_context(|| {
            format!(
                "timed out after {timeout:?} waiting for order {order_id} to fill via exchange private WS, \
                 and REST 核对也未确认成交，需要人工检查 (check that the exchange order-update WS stream is connected)"
            )
        })
}

async fn wait_for_fill_events(
    event_rx: &mut mpsc::Receiver<OrderEvent>,
    order_id: &OrderId,
) -> anyhow::Result<(Decimal, Decimal)> {
    loop {
        match event_rx.recv().await {
            Some(OrderEvent::Filled { order_id: id, filled_qty, avg_price }) if &id == order_id => {
                return Ok((filled_qty, avg_price));
            }
            Some(OrderEvent::RejectedByRisk { order_id: id, reason }) if &id == order_id => {
                anyhow::bail!("order {order_id} rejected by risk: {reason}");
            }
            Some(OrderEvent::RejectedByExchange { order_id: id, reason }) if &id == order_id => {
                anyhow::bail!("order {order_id} rejected by exchange: {reason}");
            }
            Some(_) => continue,
            None => anyhow::bail!("order event channel closed while waiting for order {order_id} to fill"),
        }
    }
}

/// `wait_for_fill` 超时后的 REST 兜底：查订单当前存的 `exchange_order_id`，
/// 拿去问交易所，结果喂回 `handle_exchange_update`，再短暂等一次事件。
const RECONCILE_REWAIT: Duration = Duration::from_secs(5);

async fn reconcile_via_rest_query(
    event_rx: &mut mpsc::Receiver<OrderEvent>,
    order_id: &OrderId,
    provider: &dyn OrderProvider,
    order_manager: &OrderManager,
) -> anyhow::Result<(Decimal, Decimal)> {
    let order = order_manager
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

    order_manager
        .handle_exchange_update(ExchangeOrderUpdate {
            venue: order.request.venue.clone(),
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

    match tokio::time::timeout(RECONCILE_REWAIT, wait_for_fill_events(event_rx, order_id)).await {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "REST 核对返回 status={:?} filled_qty={}，仍未确认成交",
            result.status,
            result.filled_qty
        ),
    }
}

/// 划转参数：与 [`open_hedged_position_live`] 内部划转步骤使用完全相同的
/// `filled_qty / 2` 截断算法，用于"现货已买入、合约已对冲，只从划转步骤
/// 继续"的场景（见 `main.rs` 里 `open --from-transfer`）。
#[derive(Debug, Clone)]
pub struct TransferHalfParams {
    pub filled_qty: Decimal,
    /// 划转到 Kraken 的资产代码。
    pub transfer_asset: String,
    /// true 时只做链路/额度校验，不真正发起提币，见 `WalletProvider::withdraw`。
    pub dry_run: bool,
}

/// 库存轮转参数：在 `sell_provider` 卖出、`buy_provider` 买入同等数量的同一
/// 资产，两条腿真实市价单并发发起。用于在两个交易所之间调整现货库存，比链上
/// 划转更快。数量统一按基础币指定（而不是像 `open_hedged_position_live` 现货腿
/// 那样按计价币金额），因为 Kraken 市价单只支持按基础币数量下单，统一单位才能让
/// 两条腿共用同一套 `OrderProvider::place_market_order` 校验路径。
#[derive(Debug, Clone)]
pub struct RotateInventoryParams {
    pub symbol: Symbol,
    pub qty: Decimal,
    pub client_order_id_prefix: Option<String>,
    pub dry_run: bool,
}

/// 库存轮转结果：两条腿都成功才会返回。
#[derive(Debug, Clone)]
pub struct RotateInventoryReport {
    pub sell_order: OrderResult,
    pub buy_order: OrderResult,
}

/// 并发向 `sell_provider` 发一笔卖单、向 `buy_provider` 发一笔等量买单。两条腿
/// 互相独立，不做自动回滚：如果一条腿失败、另一条已经成交，会留下单边仓位，
/// 返回的错误里会带上已成交那一条腿的完整订单信息，需要人工介入对账。
pub async fn rotate_inventory(
    sell_provider: &dyn OrderProvider,
    buy_provider: &dyn OrderProvider,
    params: RotateInventoryParams,
) -> anyhow::Result<RotateInventoryReport> {
    let sell_req = MarketOrderRequest {
        symbol: params.symbol.clone(),
        side: OrderSide::Sell,
        amount: OrderAmount::Base(params.qty),
        client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-sell")),
        dry_run: params.dry_run,
    };
    let buy_req = MarketOrderRequest {
        symbol: params.symbol.clone(),
        side: OrderSide::Buy,
        amount: OrderAmount::Base(params.qty),
        client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-buy")),
        dry_run: params.dry_run,
    };

    let (sell_result, buy_result) = tokio::join!(
        sell_provider.place_market_order(sell_req),
        buy_provider.place_market_order(buy_req),
    );

    match (sell_result, buy_result) {
        (Ok(sell_order), Ok(buy_order)) => {
            log::info!("rotate_inventory: sell={:?} buy={:?}", sell_order, buy_order);
            Ok(RotateInventoryReport { sell_order, buy_order })
        }
        (Err(sell_err), Ok(buy_order)) => {
            log::error!(
                "rotate_inventory: sell leg failed ({sell_err}), buy leg already filled = {:?} -- manual reconciliation needed",
                buy_order
            );
            Err(sell_err.context(format!(
                "rotate_inventory: sell leg failed but buy leg already filled (buy_order={buy_order:?}), manual reconciliation needed"
            )))
        }
        (Ok(sell_order), Err(buy_err)) => {
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

/// 平仓参数：三条腿相互独立，每条腿的数量都可选——`None` 表示不平这条腿(比如
/// 某个币种从没转过 Kraken，就不用传 `kraken_spot_qty`)。数量统一按基础币指定，
/// 原因和 [`RotateInventoryParams`] 一样：Kraken 市价单只支持按基础币下单。
#[derive(Debug, Clone)]
pub struct ClosePositionParams {
    pub symbol: Symbol,
    pub binance_spot_qty: Option<Decimal>,
    pub kraken_spot_qty: Option<Decimal>,
    pub futures_qty: Option<Decimal>,
    pub client_order_id_prefix: Option<String>,
    pub dry_run: bool,
}

/// 平仓结果：只有三条 `Result` 都成功才会返回，未请求的腿对应字段为 `None`。
#[derive(Debug, Clone)]
pub struct ClosePositionReport {
    pub binance_spot_order: Option<OrderResult>,
    pub kraken_spot_order: Option<OrderResult>,
    pub futures_order: Option<OrderResult>,
}

/// 并发平掉币安现货、Kraken 现货、币安合约三条腿：现货两条腿是卖出，合约腿是
/// 买回(对应 [`open_hedged_position_live`] 里合约腿卖出开空，平仓自然是买回平空)。
/// 三条腿互相独立、不做自动回滚：任意一条腿失败，其它已经成交的腿不会被撤销，
/// 返回的错误里会带上三条腿各自的成交/跳过/失败情况，需要人工介入对账。
pub async fn close_hedged_position(
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
                anyhow::bail!("close_hedged_position: {leg} qty must be positive, got {q} (omit the flag to skip this leg)");
            }
        }
    }
    if params.binance_spot_qty.is_none() && params.kraken_spot_qty.is_none() && params.futures_qty.is_none() {
        anyhow::bail!(
            "close_hedged_position: at least one of binance_spot_qty/kraken_spot_qty/futures_qty must be provided"
        );
    }

    let (binance_spot_result, kraken_spot_result, futures_result) = tokio::join!(
        close_leg(
            binance_spot,
            &params.symbol,
            params.binance_spot_qty,
            OrderSide::Sell,
            params.client_order_id_prefix.as_ref().map(|p| format!("{p}-close-binance-spot")),
            params.dry_run,
            "binance_spot",
        ),
        close_leg(
            kraken_spot,
            &params.symbol,
            params.kraken_spot_qty,
            OrderSide::Sell,
            params.client_order_id_prefix.as_ref().map(|p| format!("{p}-close-kraken-spot")),
            params.dry_run,
            "kraken_spot",
        ),
        close_leg(
            binance_futures,
            &params.symbol,
            params.futures_qty,
            OrderSide::Buy,
            params.client_order_id_prefix.as_ref().map(|p| format!("{p}-close-futures")),
            params.dry_run,
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

/// 单条平仓腿：`qty=None` 直接跳过、不发任何请求；`provider=None` 但 `qty=Some`
/// 视为调用方配置错误，报错点名是哪条腿。
async fn close_leg(
    provider: Option<&dyn OrderProvider>,
    symbol: &Symbol,
    qty: Option<Decimal>,
    side: OrderSide,
    client_order_id: Option<String>,
    dry_run: bool,
    leg_name: &str,
) -> anyhow::Result<Option<OrderResult>> {
    let Some(qty) = qty else {
        return Ok(None);
    };
    let Some(provider) = provider else {
        anyhow::bail!("close_hedged_position: {leg_name} qty {qty} was provided but no provider is configured for this leg");
    };
    let order = provider
        .place_market_order(MarketOrderRequest {
            symbol: symbol.clone(),
            side,
            amount: OrderAmount::Base(qty),
            client_order_id,
            dry_run,
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

/// 把 `filled_qty` 的一半划转到 Kraken：先精确匹配双边共同链，再取 Kraken 收款
/// 地址，最后从币安提币。返回实际划转数量和提币结果。
pub async fn transfer_half_to_kraken(
    binance_wallet: &dyn WalletProvider,
    kraken_wallet: &dyn WalletProvider,
    params: TransferHalfParams,
) -> anyhow::Result<(Decimal, WithdrawResult)> {
    let transfer_qty = (params.filled_qty / Decimal::TWO).trunc_with_scale(8);
    if transfer_qty <= Decimal::ZERO {
        anyhow::bail!(
            "transfer_qty rounds down to zero, aborting transfer (filled_qty={})",
            params.filled_qty
        );
    }

    let network = resolve_transfer_network(binance_wallet, kraken_wallet, &params.transfer_asset).await?;
    log::info!(
        "transfer_half_to_kraken: resolved transfer network for {} -> {network}",
        params.transfer_asset
    );

    let deposit_address = kraken_wallet.deposit_address(&params.transfer_asset, &network).await?;
    let withdraw = binance_wallet
        .withdraw(WithdrawRequest {
            asset: params.transfer_asset.clone(),
            network,
            address: deposit_address.address,
            tag: deposit_address.tag,
            amount: transfer_qty,
            dry_run: params.dry_run,
        })
        .await?;
    log::info!("transfer_half_to_kraken: withdraw result = {:?}", withdraw);

    Ok((transfer_qty, withdraw))
}

/// 自动匹配币安可提币网络和 Kraken 可充值网络：两边的 `AssetInfo.networks`
/// 都已经是标准链名(以币安 `network` 代码为准，Kraken 一侧的原生名称翻译
/// 见 `wallet::kraken`)，这里只需要按 `network` 字段精确求交集。必须恰好
/// 命中一个共同网络才返回，零个或多个都直接报错，把两边全部可用网络列出来，
/// 不做无凭据的猜测。
async fn resolve_transfer_network(
    binance_wallet: &dyn WalletProvider,
    kraken_wallet: &dyn WalletProvider,
    asset: &str,
) -> anyhow::Result<String> {
    let binance_info = binance_wallet.asset_info(asset).await?;
    let kraken_info = kraken_wallet.asset_info(asset).await?;

    let kraken_networks: HashSet<&str> = kraken_info
        .networks
        .iter()
        .filter(|n| n.deposit_enabled)
        .map(|n| n.network.as_str())
        .collect();

    let candidates: Vec<&str> = binance_info
        .networks
        .iter()
        .filter(|n| n.withdraw_enabled && kraken_networks.contains(n.network.as_str()))
        .map(|n| n.network.as_str())
        .collect();

    match candidates.len() {
        1 => Ok(candidates[0].to_string()),
        0 => anyhow::bail!(
            "无法为 {asset} 自动确定共同的划转网络。币安可提币网络: {:?}；Kraken 可存款网络: {:?}",
            binance_info
                .networks
                .iter()
                .filter(|n| n.withdraw_enabled)
                .map(|n| &n.network)
                .collect::<Vec<_>>(),
            kraken_info.networks.iter().map(|n| &n.network).collect::<Vec<_>>(),
        ),
        _ => anyhow::bail!("为 {asset} 匹配到多个候选划转网络，存在歧义，需要人工确认: {candidates:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use dashmap::DashMap;

    use crate::exchange_info::types::{MarketPrecision, QtyPrecision};
    use crate::order::types::OrderStatus;
    use crate::order_manager::store::InMemoryOrderStore;
    use crate::order_manager::types::Order;
    use crate::order_manager::{
        ExchangeAdapter, ExchangeOrderUpdate, ExecutionEngine, InMemoryOrderIdAllocator, RiskEngine, RiskLimits,
    };
    use crate::portfolio::{InMemoryPnlStore, PortfolioManager};
    use crate::position::{InMemoryPositionStore, PositionManager};
    use crate::wallet::types::{AssetInfo, ChainInfo, DepositAddress};
    use crate::types::Venue;

    /// 现货测试替身：`place_market_order_raw` 只接受 `OrderAmount::Quote`，固定
    /// 返回 `filled_qty`，并记录被真正调用（即走过 dry_run 之外分支）的次数。
    /// 收到 `OrderAmount::Base` 即视为测试写错了流程。
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
                status: OrderStatus::Filled,
                filled_qty: self.filled_qty,
                avg_price: Some(Decimal::ONE),
                fee: None,
                fee_asset: None,
            })
        }
    }

    /// 合约测试替身：记录每次真实下单的数量，用于验证 `PrecisionCache::round_qty`
    /// 取整结果和"未被调用"两类断言。
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
                status: OrderStatus::Filled,
                filled_qty: quantity,
                avg_price: Some(Decimal::ONE),
                fee: None,
                fee_asset: None,
            })
        }
    }

    /// `wait_for_fill` REST 兜底测试用的替身：REST 下单响应故意不带成交信息
    /// (`status=New`，和真实交易所的设计一致——成交状态不由 REST 驱动)，
    /// `query_order` 固定返回一笔成交，模拟"WS 没推送、REST 核对能查到已成交"
    /// 的场景。
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

    /// 钱包测试替身：固定返回一份网络列表，记录 `withdraw_raw` 调用参数。
    struct FakeWalletProvider {
        name: &'static str,
        networks: Vec<ChainInfo>,
        deposit_addr: Option<DepositAddress>,
        withdraw_calls: Arc<Mutex<Vec<WithdrawRequest>>>,
    }

    #[async_trait]
    impl WalletProvider for FakeWalletProvider {
        fn venue(&self) -> Venue {
            Venue::new(self.name)
        }
        async fn asset_info(&self, asset: &str) -> anyhow::Result<AssetInfo> {
            Ok(AssetInfo {
                asset: asset.to_string(),
                networks: self.networks.clone(),
            })
        }
        async fn deposit_address(&self, asset: &str, network: &str) -> anyhow::Result<DepositAddress> {
            self.deposit_addr
                .clone()
                .map(|mut d| {
                    d.asset = asset.to_string();
                    d.network = network.to_string();
                    d
                })
                .ok_or_else(|| anyhow::anyhow!("no deposit address configured"))
        }
        async fn withdraw_raw(&self, req: &WithdrawRequest) -> anyhow::Result<WithdrawResult> {
            self.withdraw_calls.lock().unwrap().push(req.clone());
            Ok(WithdrawResult {
                id: format!("withdraw-{}", req.asset),
            })
        }
    }

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

    fn binance_btc_chain() -> ChainInfo {
        ChainInfo {
            network: "BTC".to_string(),
            name: "Bitcoin".to_string(),
            deposit_enabled: true,
            withdraw_enabled: true,
            withdraw_fee: Decimal::new(1, 4),
            withdraw_min: Decimal::new(1, 5),
            min_confirm: 2,
            contract_address: None,
        }
    }

    fn kraken_btc_chain() -> ChainInfo {
        ChainInfo {
            // WalletProvider 返回给调用方的数据已经是翻译后的标准链名(见
            // wallet::kraken 里的映射表)，测试替身直接模拟这个已翻译状态。
            network: "BTC".to_string(),
            name: "Bitcoin".to_string(),
            deposit_enabled: true,
            withdraw_enabled: true,
            withdraw_fee: Decimal::ZERO,
            withdraw_min: Decimal::ZERO,
            min_confirm: 3,
            contract_address: None,
        }
    }

    fn no_op_wallet(name: &'static str) -> FakeWalletProvider {
        FakeWalletProvider {
            name,
            networks: vec![],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn params(quote_amount: Decimal, dry_run: bool) -> OpenPositionParams {
        OpenPositionParams {
            symbol: btc_usdt(),
            quote_amount,
            transfer_asset: "BTC".to_string(),
            client_order_id_prefix: Some("test".to_string()),
            dry_run,
            transfer_to_kraken: false,
            fill_timeout: Duration::from_secs(2),
        }
    }

    /// `open_hedged_position_live` 测试用的一整套内存版依赖：风控/执行引擎/
    /// 订单历史都是纯内存实现，`spot`/`futures` 复用调用方传入的 provider（同一个
    /// `Arc` 既用来构造 `ExchangeAdapter`，也直接传给被测函数，这样 raw 调用计数
    /// 才能反映 `ExecutionEngine` 真正发起的下单）。
    struct LiveEnv {
        order_manager: Arc<OrderManager>,
        event_rx: mpsc::Receiver<OrderEvent>,
    }

    fn setup_live_env(spot: Arc<dyn OrderProvider>, futures: Arc<dyn OrderProvider>) -> LiveEnv {
        let symbol = btc_usdt();
        let spot_venue = spot.venue();
        let futures_venue = futures.venue();

        let mut risk_limits = HashMap::new();
        risk_limits.insert(
            (spot_venue.clone(), symbol.clone()),
            RiskLimits {
                max_order_amount: Decimal::MAX,
                max_position: Decimal::MAX,
                max_orders_per_window: 10,
            },
        );
        risk_limits.insert(
            (futures_venue.clone(), symbol.clone()),
            RiskLimits {
                max_order_amount: Decimal::MAX,
                max_position: Decimal::MAX,
                max_orders_per_window: 10,
            },
        );

        let position_manager = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let risk_engine = Arc::new(RiskEngine::new(risk_limits, position_manager.clone()));
        let portfolio = Arc::new(PortfolioManager::new(
            position_manager,
            Arc::new(InMemoryPnlStore::new()),
            Arc::new(DashMap::new()),
            HashMap::new(),
        ));

        let (event_tx, event_rx) = mpsc::channel(100);

        let mut adapters = HashMap::new();
        adapters.insert(spot_venue.clone(), Arc::new(ExchangeAdapter::new(spot_venue, spot)));
        adapters.insert(futures_venue.clone(), Arc::new(ExchangeAdapter::new(futures_venue, futures)));

        let execution_engine = Arc::new(ExecutionEngine::new(adapters, event_tx.clone()));
        let order_store = Arc::new(InMemoryOrderStore::new());
        let order_id_allocator = Arc::new(InMemoryOrderIdAllocator::new());
        let order_manager = Arc::new(OrderManager::new(
            risk_engine,
            execution_engine,
            portfolio,
            event_tx,
            order_store,
            order_id_allocator,
        ));

        LiveEnv { order_manager, event_rx }
    }

    /// 轮询直到 `order_manager` 里出现指定 `client_order_id` 的订单——
    /// `submit_order` 对 `orders`/`client_order_index` 的写入发生在它自己的
    /// 异步任务里，测试驱动 WS 推送前需要等这个写入落地，否则
    /// `handle_exchange_update` 会因为查不到订单而静默丢弃推送。
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

    /// 模拟交易所私有 WS 推送一次完全成交，驱动 `wait_for_fill` 返回。
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

        let report = open_hedged_position_dry_run(&spot, params(Decimal::new(100, 0), true))
            .await
            .unwrap();

        assert_eq!(report.spot_order.order_id, "dry-run");
        assert!(report.futures_order.is_none());
        assert!(report.spot_exchange_order_id.is_none());
        assert!(report.futures_exchange_order_id.is_none());
        assert!(report.transfer_qty.is_none());
        assert!(report.withdraw.is_none());
        assert!(report.note.unwrap().contains("dry_run=true"));
        // dry_run 由 place_market_order 的 trait 默认方法拦截，raw 从未被调用。
        assert_eq!(spot_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn zero_filled_qty_aborts_before_futures_and_wallet() {
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
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let LiveEnv { order_manager, mut event_rx } = setup_live_env(spot_arc.clone(), futures_arc.clone());
        let driver_order_manager = order_manager.clone();
        let spot_venue = spot_arc.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&driver_order_manager, &spot_venue, "test-spot", Decimal::ZERO, Decimal::ONE).await;
        });

        let err = open_hedged_position_live(
            spot_arc.as_ref(),
            futures_arc.as_ref(),
            &binance_wallet,
            &kraken_wallet,
            order_manager.as_ref(),
            &mut event_rx,
            &futures_precision,
            params(Decimal::new(100, 0), false),
        )
        .await
        .unwrap_err();
        driver.await.unwrap();

        assert!(err.to_string().contains("filled_qty is zero"));
        assert!(futures_calls.lock().unwrap().is_empty());
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn wait_for_fill_falls_back_to_rest_query_on_ws_timeout() {
        let venue = Venue::new("fake-queryable");
        let provider: Arc<dyn OrderProvider> = Arc::new(QueryableProvider {
            venue: venue.clone(),
            query_filled_qty: Decimal::new(5, 1), // 0.5
        });

        let LiveEnv { order_manager, mut event_rx } = setup_live_env(provider.clone(), provider.clone());

        let request = OrderRequest {
            strategy_name: "test".to_string(),
            venue: venue.clone(),
            symbol: btc_usdt(),
            side: OrderSide::Buy,
            amount: OrderAmount::Base(Decimal::ONE),
            client_order_id: Some("test-query-fallback".to_string()),
            group_id: None,
            metadata: None,
        };
        let response = order_manager.submit_order(request).await;
        let order_id = response.order_id.clone();
        // 等 process_order 跑完：exchange_order_id 已经写入，但 REST 响应本身
        // 不带成交状态(status=New)，也没有任何 WS 推送——这是要测的超时场景。
        response.result_rx.await.unwrap().unwrap();

        let (filled_qty, avg_price) = wait_for_fill(
            &mut event_rx,
            &order_id,
            Duration::from_millis(50),
            provider.as_ref(),
            order_manager.as_ref(),
        )
        .await
        .unwrap();

        assert_eq!(filled_qty, Decimal::new(5, 1));
        assert_eq!(avg_price, Decimal::ONE);

        let final_order = order_manager.get_order(&order_id).unwrap();
        assert_eq!(final_order.status, OrderStatus::Filled);
        assert_eq!(final_order.filled_qty, Decimal::new(5, 1));
    }

    #[tokio::test]
    async fn spot_failure_stops_before_futures_and_wallet() {
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
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        // 现货 REST 调用同步失败，`ExecutionEngine` 会直接把订单标记为 Rejected
        // 并通过 `result_rx` 报错，不需要驱动任何 WS 推送。
        let LiveEnv { order_manager, mut event_rx } = setup_live_env(spot_arc.clone(), futures_arc.clone());

        let err = open_hedged_position_live(
            spot_arc.as_ref(),
            futures_arc.as_ref(),
            &binance_wallet,
            &kraken_wallet,
            order_manager.as_ref(),
            &mut event_rx,
            &futures_precision,
            params(Decimal::new(100, 0), false),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("simulated spot failure"));
        assert!(futures_calls.lock().unwrap().is_empty());
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn futures_failure_stops_before_wallet() {
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
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let LiveEnv { order_manager, mut event_rx } = setup_live_env(spot_arc.clone(), futures_arc.clone());
        let driver_order_manager = order_manager.clone();
        let spot_venue = spot_arc.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&driver_order_manager, &spot_venue, "test-spot", spot_filled_qty, Decimal::ONE).await;
        });

        let err = open_hedged_position_live(
            spot_arc.as_ref(),
            futures_arc.as_ref(),
            &binance_wallet,
            &kraken_wallet,
            order_manager.as_ref(),
            &mut event_rx,
            &futures_precision,
            params(Decimal::new(100, 0), false),
        )
        .await
        .unwrap_err();
        driver.await.unwrap();

        assert!(err.to_string().contains("simulated futures failure"));
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
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
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let LiveEnv { order_manager, mut event_rx } = setup_live_env(spot_arc.clone(), futures_arc.clone());
        let driver_order_manager = order_manager.clone();
        let spot_venue = spot_arc.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&driver_order_manager, &spot_venue, "test-spot", spot_filled_qty, Decimal::ONE).await;
        });

        let err = open_hedged_position_live(
            spot_arc.as_ref(),
            futures_arc.as_ref(),
            &binance_wallet,
            &kraken_wallet,
            order_manager.as_ref(),
            &mut event_rx,
            &futures_precision,
            params(Decimal::new(100, 0), false),
        )
        .await
        .unwrap_err();
        driver.await.unwrap();

        assert!(format!("{err:#}").contains("below min_qty"));
        assert!(futures_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn happy_path_hedges_and_transfers_half() {
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
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![kraken_btc_chain()],
            deposit_addr: Some(DepositAddress {
                asset: "BTC".to_string(),
                network: "BTC".to_string(),
                address: "kraken-addr".to_string(),
                tag: None,
            }),
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        let LiveEnv { order_manager, mut event_rx } = setup_live_env(spot_arc.clone(), futures_arc.clone());
        let driver_order_manager = order_manager.clone();
        let spot_venue = spot_arc.venue();
        let futures_venue = futures_arc.venue();
        let driver = tokio::spawn(async move {
            drive_fill(&driver_order_manager, &spot_venue, "test-spot", spot_filled_qty, Decimal::ONE).await;
            // 1.234567 向下取整到 0.01 的整数倍 -> 1.23
            drive_fill(&driver_order_manager, &futures_venue, "test-futures", Decimal::new(123, 2), Decimal::ONE).await;
        });

        let mut p = params(Decimal::new(100, 0), false);
        p.transfer_to_kraken = true;

        let report = open_hedged_position_live(
            spot_arc.as_ref(),
            futures_arc.as_ref(),
            &binance_wallet,
            &kraken_wallet,
            order_manager.as_ref(),
            &mut event_rx,
            &futures_precision,
            p,
        )
        .await
        .unwrap();
        driver.await.unwrap();

        assert_eq!(futures_calls.lock().unwrap().as_slice(), &[Decimal::new(123, 2)]);
        // live 路径下 `spot_order`/`futures_order` 的 order_id 是 OrderManager 内部
        // 生成的 ORD-xxx，交易所原始订单号改由 `*_exchange_order_id` 承载。
        assert_eq!(report.spot_exchange_order_id, Some("spot-BTC/USDT".to_string()));
        assert_eq!(report.futures_exchange_order_id, Some("futures-BTC/USDT".to_string()));
        // 1.234567 / 2 = 0.6172835,已经<=8位小数,截断后不变
        assert_eq!(report.transfer_qty, Some(Decimal::new(6172835, 7)));

        let withdraws = binance_wallet.withdraw_calls.lock().unwrap();
        assert_eq!(withdraws.len(), 1);
        assert_eq!(withdraws[0].network, "BTC");
        assert_eq!(withdraws[0].address, "kraken-addr");
        assert_eq!(withdraws[0].amount, Decimal::new(6172835, 7));
        assert_eq!(report.withdraw.unwrap().id, "withdraw-BTC");
        assert!(report.note.is_none());
    }

    #[tokio::test]
    async fn transfer_half_to_kraken_zero_qty_errors() {
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let err = transfer_half_to_kraken(
            &binance_wallet,
            &kraken_wallet,
            TransferHalfParams {
                filled_qty: Decimal::new(1, 9), // 0.000000001，减半截断到 8 位小数后为 0
                transfer_asset: "BTC".to_string(),
                dry_run: false,
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("transfer_qty rounds down to zero"));
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn transfer_half_to_kraken_dry_run_skips_withdraw_raw() {
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![kraken_btc_chain()],
            deposit_addr: Some(DepositAddress {
                asset: "BTC".to_string(),
                network: "BTC".to_string(),
                address: "kraken-addr".to_string(),
                tag: None,
            }),
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        let (transfer_qty, withdraw) = transfer_half_to_kraken(
            &binance_wallet,
            &kraken_wallet,
            TransferHalfParams {
                filled_qty: Decimal::new(2, 0),
                transfer_asset: "BTC".to_string(),
                dry_run: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(transfer_qty, Decimal::ONE);
        assert_eq!(withdraw.id, "dry-run");
        // dry_run 由 WalletProvider::withdraw 的默认实现拦截，withdraw_raw 从未被调用，
        // 但 asset_info/deposit_address 仍是真实调用，走完了完整的链路校验。
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn transfer_half_to_kraken_happy_path() {
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![kraken_btc_chain()],
            deposit_addr: Some(DepositAddress {
                asset: "BTC".to_string(),
                network: "BTC".to_string(),
                address: "kraken-addr".to_string(),
                tag: None,
            }),
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        let (transfer_qty, withdraw) = transfer_half_to_kraken(
            &binance_wallet,
            &kraken_wallet,
            TransferHalfParams {
                filled_qty: Decimal::new(1234567, 6), // 1.234567
                transfer_asset: "BTC".to_string(),
                dry_run: false,
            },
        )
        .await
        .unwrap();

        // 1.234567 / 2 = 0.6172835,已经<=8位小数,截断后不变
        assert_eq!(transfer_qty, Decimal::new(6172835, 7));
        assert_eq!(withdraw.id, "withdraw-BTC");

        let withdraws = binance_wallet.withdraw_calls.lock().unwrap();
        assert_eq!(withdraws.len(), 1);
        assert_eq!(withdraws[0].network, "BTC");
        assert_eq!(withdraws[0].address, "kraken-addr");
        assert_eq!(withdraws[0].amount, Decimal::new(6172835, 7));
    }

    #[tokio::test]
    async fn resolve_transfer_network_unique_match() {
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![kraken_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        let network = resolve_transfer_network(&binance_wallet, &kraken_wallet, "BTC")
            .await
            .unwrap();
        assert_eq!(network, "BTC");
    }

    #[tokio::test]
    async fn resolve_transfer_network_no_match_errors() {
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        // Kraken 一侧没有任何网络，和币安的 "BTC" 求交集为空。
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        let err = resolve_transfer_network(&binance_wallet, &kraken_wallet, "BTC")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("无法为 BTC 自动确定共同的划转网络"));
    }

    #[tokio::test]
    async fn resolve_transfer_network_multiple_candidates_errors() {
        let mut eth_chain = binance_btc_chain();
        eth_chain.network = "ETH".to_string();
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain(), eth_chain],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let mut kraken_eth_chain = kraken_btc_chain();
        kraken_eth_chain.network = "ETH".to_string();
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![kraken_btc_chain(), kraken_eth_chain],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        // asset 参数在这个测试替身实现里不影响返回的 networks,两个币安网络
        // (BTC/ETH)在 Kraken 一侧都有对应的标准链名,导致两个候选、产生歧义。
        let err = resolve_transfer_network(&binance_wallet, &kraken_wallet, "BTC")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("匹配到多个候选划转网络"));
    }

    /// `rotate_inventory` 测试替身：`fail=true` 时对 `place_market_order_raw`
    /// 报错，并记录每次真实下单（走过 dry_run 之外分支）的调用次数，用于验证
    /// 并发下单时"一边失败不会让另一边被跳过"。
    struct FakeRotateProvider {
        name: &'static str,
        fail: bool,
        raw_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OrderProvider for FakeRotateProvider {
        fn venue(&self) -> Venue {
            Venue::new(self.name)
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            let OrderAmount::Base(quantity) = req.amount else {
                unreachable!("rotate_inventory only uses OrderAmount::Base")
            };
            self.raw_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                anyhow::bail!("simulated {} failure", self.name);
            }
            Ok(OrderResult {
                order_id: format!("{}-{}", self.name, req.symbol),
                status: OrderStatus::Filled,
                filled_qty: quantity,
                avg_price: Some(Decimal::ONE),
                fee: None,
                fee_asset: None,
            })
        }
    }

    fn rotate_provider(name: &'static str, fail: bool) -> (FakeRotateProvider, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = FakeRotateProvider {
            name,
            fail,
            raw_calls: calls.clone(),
        };
        (provider, calls)
    }

    fn rotate_params(dry_run: bool) -> RotateInventoryParams {
        RotateInventoryParams {
            symbol: btc_usdt(),
            qty: Decimal::new(1, 1),
            client_order_id_prefix: Some("test".to_string()),
            dry_run,
        }
    }

    #[tokio::test]
    async fn rotate_inventory_dry_run_skips_both_raw_calls() {
        let (sell, sell_calls) = rotate_provider("sell-venue", false);
        let (buy, buy_calls) = rotate_provider("buy-venue", false);

        let report = rotate_inventory(&sell, &buy, rotate_params(true)).await.unwrap();

        assert_eq!(report.sell_order.order_id, "dry-run");
        assert_eq!(report.buy_order.order_id, "dry-run");
        assert_eq!(sell_calls.load(Ordering::SeqCst), 0);
        assert_eq!(buy_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rotate_inventory_happy_path_fills_both_legs() {
        let (sell, sell_calls) = rotate_provider("sell-venue", false);
        let (buy, buy_calls) = rotate_provider("buy-venue", false);

        let report = rotate_inventory(&sell, &buy, rotate_params(false)).await.unwrap();

        assert_eq!(report.sell_order.order_id, "sell-venue-BTC/USDT");
        assert_eq!(report.buy_order.order_id, "buy-venue-BTC/USDT");
        assert_eq!(sell_calls.load(Ordering::SeqCst), 1);
        assert_eq!(buy_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rotate_inventory_sell_failure_still_places_buy_leg() {
        let (sell, _sell_calls) = rotate_provider("sell-venue", true);
        let (buy, buy_calls) = rotate_provider("buy-venue", false);

        let err = rotate_inventory(&sell, &buy, rotate_params(false)).await.unwrap_err();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("buy-venue-BTC/USDT"));
        // 并发下单：卖出腿失败不应该让买入腿被跳过。
        assert_eq!(buy_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rotate_inventory_buy_failure_still_places_sell_leg() {
        let (sell, sell_calls) = rotate_provider("sell-venue", false);
        let (buy, _buy_calls) = rotate_provider("buy-venue", true);

        let err = rotate_inventory(&sell, &buy, rotate_params(false)).await.unwrap_err();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("sell-venue-BTC/USDT"));
        assert_eq!(sell_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rotate_inventory_both_legs_fail() {
        let (sell, _sell_calls) = rotate_provider("sell-venue", true);
        let (buy, _buy_calls) = rotate_provider("buy-venue", true);

        let err = rotate_inventory(&sell, &buy, rotate_params(false)).await.unwrap_err();

        assert!(err.to_string().contains("simulated sell-venue failure"));
        assert!(err.to_string().contains("simulated buy-venue failure"));
    }

    /// `close_hedged_position` 测试替身：记录每条腿真实下单（走过 dry_run 之外
    /// 分支）的调用参数，用于区分"完全跳过的腿"和"真正下了单的腿"。
    struct FakeCloseProvider {
        name: &'static str,
        fail: bool,
        raw_calls: Arc<Mutex<Vec<(OrderSide, Decimal)>>>,
    }

    #[async_trait]
    impl OrderProvider for FakeCloseProvider {
        fn venue(&self) -> Venue {
            Venue::new(self.name)
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            let OrderAmount::Base(quantity) = req.amount else {
                unreachable!("close_hedged_position only uses OrderAmount::Base")
            };
            self.raw_calls.lock().unwrap().push((req.side, quantity));
            if self.fail {
                anyhow::bail!("simulated {} failure", self.name);
            }
            Ok(OrderResult {
                order_id: format!("{}-{}", self.name, req.symbol),
                status: OrderStatus::Filled,
                filled_qty: quantity,
                avg_price: Some(Decimal::ONE),
                fee: None,
                fee_asset: None,
            })
        }
    }

    fn close_provider(name: &'static str, fail: bool) -> (FakeCloseProvider, Arc<Mutex<Vec<(OrderSide, Decimal)>>>) {
        let raw_calls = Arc::new(Mutex::new(Vec::new()));
        let provider = FakeCloseProvider {
            name,
            fail,
            raw_calls: raw_calls.clone(),
        };
        (provider, raw_calls)
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
        }
    }

    #[tokio::test]
    async fn close_all_three_legs_dry_run_skips_raw_calls() {
        let (binance_spot, bs_raw) = close_provider("binance-spot", false);
        let (kraken_spot, ks_raw) = close_provider("kraken-spot", false);
        let (futures, f_raw) = close_provider("futures", false);

        let report = close_hedged_position(
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
        assert!(bs_raw.lock().unwrap().is_empty());
        assert!(ks_raw.lock().unwrap().is_empty());
        assert!(f_raw.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn close_happy_path_fills_all_three_legs() {
        let (binance_spot, bs_raw) = close_provider("binance-spot", false);
        let (kraken_spot, ks_raw) = close_provider("kraken-spot", false);
        let (futures, f_raw) = close_provider("futures", false);

        let report = close_hedged_position(
            Some(&binance_spot),
            Some(&kraken_spot),
            Some(&futures),
            close_params(Some(Decimal::new(1, 1)), Some(Decimal::new(2, 1)), Some(Decimal::new(3, 1)), false),
        )
        .await
        .unwrap();

        assert_eq!(report.binance_spot_order.unwrap().order_id, "binance-spot-BTC/USDT");
        assert_eq!(report.kraken_spot_order.unwrap().order_id, "kraken-spot-BTC/USDT");
        assert_eq!(report.futures_order.unwrap().order_id, "futures-BTC/USDT");
        assert_eq!(bs_raw.lock().unwrap().as_slice(), &[(OrderSide::Sell, Decimal::new(1, 1))]);
        assert_eq!(ks_raw.lock().unwrap().as_slice(), &[(OrderSide::Sell, Decimal::new(2, 1))]);
        assert_eq!(f_raw.lock().unwrap().as_slice(), &[(OrderSide::Buy, Decimal::new(3, 1))]);
    }

    #[tokio::test]
    async fn close_skips_leg_with_qty_none() {
        let (binance_spot, bs_raw) = close_provider("binance-spot", false);
        let (kraken_spot, ks_raw) = close_provider("kraken-spot", false);
        let (futures, f_raw) = close_provider("futures", false);

        let report = close_hedged_position(
            Some(&binance_spot),
            Some(&kraken_spot),
            Some(&futures),
            close_params(None, None, Some(Decimal::new(1, 1)), false),
        )
        .await
        .unwrap();

        assert!(report.binance_spot_order.is_none());
        assert!(report.kraken_spot_order.is_none());
        assert!(report.futures_order.is_some());
        assert!(bs_raw.lock().unwrap().is_empty());
        assert!(ks_raw.lock().unwrap().is_empty());
        assert_eq!(f_raw.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn close_binance_spot_failure_still_places_other_two_legs() {
        let (binance_spot, _) = close_provider("binance-spot", true);
        let (kraken_spot, ks_raw) = close_provider("kraken-spot", false);
        let (futures, f_raw) = close_provider("futures", false);

        let err = close_hedged_position(
            Some(&binance_spot),
            Some(&kraken_spot),
            Some(&futures),
            close_params(Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), false),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("binance_spot failed"));
        assert_eq!(ks_raw.lock().unwrap().len(), 1);
        assert_eq!(f_raw.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn close_kraken_spot_failure_still_places_other_two_legs() {
        let (binance_spot, bs_raw) = close_provider("binance-spot", false);
        let (kraken_spot, _) = close_provider("kraken-spot", true);
        let (futures, f_raw) = close_provider("futures", false);

        let err = close_hedged_position(
            Some(&binance_spot),
            Some(&kraken_spot),
            Some(&futures),
            close_params(Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), false),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("kraken_spot failed"));
        assert_eq!(bs_raw.lock().unwrap().len(), 1);
        assert_eq!(f_raw.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn close_futures_failure_still_places_other_two_legs() {
        let (binance_spot, bs_raw) = close_provider("binance-spot", false);
        let (kraken_spot, ks_raw) = close_provider("kraken-spot", false);
        let (futures, _) = close_provider("futures", true);

        let err = close_hedged_position(
            Some(&binance_spot),
            Some(&kraken_spot),
            Some(&futures),
            close_params(Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), Some(Decimal::new(1, 1)), false),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("futures failed"));
        assert_eq!(bs_raw.lock().unwrap().len(), 1);
        assert_eq!(ks_raw.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn close_all_three_legs_fail_combined_error_message() {
        let (binance_spot, _) = close_provider("binance-spot", true);
        let (kraken_spot, _) = close_provider("kraken-spot", true);
        let (futures, _) = close_provider("futures", true);

        let err = close_hedged_position(
            Some(&binance_spot),
            Some(&kraken_spot),
            Some(&futures),
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
        let (binance_spot, bs_raw) = close_provider("binance-spot", false);

        let err = close_hedged_position(
            Some(&binance_spot),
            None,
            None,
            close_params(Some(Decimal::ZERO), None, None, false),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("binance_spot qty must be positive"));
        assert!(bs_raw.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn close_validation_error_when_no_leg_requested() {
        let err = close_hedged_position(None, None, None, close_params(None, None, None, false))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("at least one of"));
    }

    #[tokio::test]
    async fn close_qty_provided_without_matching_provider_errors() {
        let err = close_hedged_position(
            None,
            None,
            None,
            close_params(Some(Decimal::new(1, 1)), None, None, false),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("binance_spot"));
        assert!(err.to_string().contains("no provider is configured"));
    }
}
