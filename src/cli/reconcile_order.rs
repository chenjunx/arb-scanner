//! `reconcile-order` 子命令：一次性核对/修正卡在非终态(通常是 `New`)的历史
//! 订单，用于修复 `process_order` 并发覆盖写这个历史 bug 遗留下来的脏数据
//! (根因见 manager.rs 里 `process_order` 的注释)。只处理 `binance_spot`/
//! `binance_futures` 两个场所(Kraken 的 `query_order` 没有实现)。
//!
//! 默认只读：从 Redis 读订单 -> 按 exchange_order_id 查交易所 REST -> 打印
//! 结果，不落库。确认输出和交易所后台一致后，加 `--confirm` 重新执行一次
//! 才会真正调用 `handle_exchange_update` 写回 Redis——这是修改生产数据的一步，
//! 刻意要求分两次执行、不给默认写权限。

use std::sync::Arc;

use anyhow::Context;
use clap::Args;
use dashmap::DashMap;
use log::info;

use crate::net;
use crate::order::OrderProvider;
use crate::order::binance::BinanceOrderProvider;
use crate::order::binance_futures::BinanceFuturesOrderProvider;
use crate::order_manager::types::OrderId;
use crate::order_manager::{ExchangeOrderUpdate, OrderManager, OrderStore, RedisOrderStore};
use crate::topic::TopicBus;
use crate::types::Venue;

use super::args::now_ms;
use super::wiring;

#[derive(Args, Debug)]
pub struct ReconcileOrderArgs {
    /// 要核对的订单号，如 ORD-...
    #[arg(long)]
    pub order_id: String,

    /// 使用币安测试网
    #[arg(long)]
    pub testnet: bool,

    /// 把 REST 查到的结果真正写回 Redis(默认只读打印)
    #[arg(long)]
    pub confirm: bool,
}

pub async fn run(args: ReconcileOrderArgs) -> anyhow::Result<()> {
    let ReconcileOrderArgs { order_id, testnet, confirm } = args;
    let order_id = OrderId::new(order_id);

    let redis_url = wiring::redis_url();
    info!("reconcile-order: connecting to redis at {redis_url}");
    let order_store = Arc::new(RedisOrderStore::new(&redis_url).context("failed to connect RedisOrderStore to redis")?);
    let bus = Arc::new(TopicBus::new());
    let (position_manager, _portfolio_manager) = wiring::build_portfolio_stack(&redis_url, Arc::new(DashMap::new()))?;

    let order = order_store
        .get(&order_id)
        .with_context(|| format!("order {order_id} not found in redis"))?;
    let trade = order
        .request
        .as_trade()
        .with_context(|| format!("order {order_id} 是划转单而非交易单，reconcile-order 不适用"))?;

    info!(
        "reconcile-order: 从 Redis 读到订单 venue={} symbol={} status={:?} filled_qty={} avg_price={:?} exchange_order_id={:?}",
        trade.venue, trade.symbol, order.status, order.filled_qty, order.avg_price, order.exchange_order_id
    );

    let exchange_order_id = order
        .exchange_order_id
        .clone()
        .with_context(|| format!("order {order_id} 没有 exchange_order_id，无法通过 REST 核对"))?;

    let proxy = net::proxy_from_env();
    let spot_venue = Venue::new("binance_spot");
    let futures_venue = Venue::new("binance_futures");
    let provider: Arc<dyn OrderProvider> = if trade.venue == spot_venue {
        Arc::new(BinanceOrderProvider::from_env(spot_venue.clone(), testnet, proxy.as_deref())?)
    } else if trade.venue == futures_venue {
        Arc::new(BinanceFuturesOrderProvider::from_env(futures_venue.clone(), testnet, proxy.as_deref())?)
    } else {
        anyhow::bail!(
            "reconcile-order: venue {} 不支持 REST 核对(目前只实现了 binance_spot/binance_futures)",
            trade.venue
        );
    };

    let result = provider
        .query_order(&trade.symbol, &exchange_order_id)
        .await
        .with_context(|| format!("REST query_order 失败 (exchange_order_id={exchange_order_id})"))?;

    info!(
        "reconcile-order: REST 查询结果 status={:?} filled_qty={} avg_price={:?} fee={:?} fee_asset={:?}",
        result.status, result.filled_qty, result.avg_price, result.fee, result.fee_asset
    );
    println!("REST query_order result: {result:#?}");

    if !confirm {
        println!(
            "只读模式(默认)：以上是 REST 查到的结果，尚未写入 Redis。确认和交易所后台一致后，加 --confirm 重新执行以落库。"
        );
        return Ok(());
    }

    info!("reconcile-order: --confirm 已指定，写入 handle_exchange_update 落库");

    let order_manager = Arc::new(OrderManager::new(bus.clone(), position_manager, order_store, None));

    order_manager.seed_order(order.clone());

    order_manager
        .handle_exchange_update(ExchangeOrderUpdate {
            venue: trade.venue.clone(),
            symbol: Some(trade.symbol.clone()),
            client_order_id: trade.client_order_id.clone(),
            exchange_order_id: Some(exchange_order_id),
            status: result.status,
            filled_qty: result.filled_qty,
            avg_price: result.avg_price,
            fee: result.fee,
            fee_asset: result.fee_asset,
            ts_ms: now_ms(),
        })
        .await;

    let final_order = order_manager.get_order(&order_id);
    println!("落库后订单状态: {final_order:#?}");

    Ok(())
}
