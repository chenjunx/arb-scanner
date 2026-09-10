//! `transfer` 子命令：手动发起一次跨交易所划转，走既有但此前从未被任何调用方
//! 触达的划转订单管道 `RiskService -> ExecutionService::handle_transfer(真正
//! 提币 + 乐观记账) -> TransferMonitor/OrderManager::confirm_transfer(到账确认
//! + 手续费修正)`(见 037ce84/28e1b12)。
//!
//! `--dry-run`(默认)在 CLI 层直接短路，只调用 `wallet::transfer::transfer_asset`
//! 做链路校验(查 asset_info/deposit_address，真实网络请求但不产生资金变动)，
//! 完全不碰 `TransferRequest`/`OrderManager`。**不能**简单把
//! `TransferRequest.dry_run` 设为 true 后仍搭建完整流水线——`WalletProvider::
//! withdraw` 的默认实现拦截 dry_run 时仍返回"成功"，会导致乐观记账真的执行、
//! `mark_transfer_pending` 真的打上 pending，但因为没有真实提币，永远不会有
//! `BalanceUpdate` 触发确认，留下一笔洗不掉的 `pending_qty`。所以 `--live`
//! 路径下 `TransferRequest.dry_run` 恒为 `false`。
//!
//! `--live` 默认额外阻塞等到账确认(`--no-wait` 可跳过)：目前代码库里没有任何
//! 常驻服务在跑 `TransferMonitor`，提币受理后如果没人主动等，仓位会永远停在
//! 乐观记账的 pending_qty，没有其它机制兜底。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use futures_util::StreamExt;
use log::info;
use rust_decimal::Decimal;

use crate::accounting::balance_stream::BalanceStreamSource;
use crate::accounting::binance::BinanceBalanceStream;
use crate::accounting::kraken::KrakenBalanceStream;
use crate::net;
use crate::order_manager::types::OrderEvent;
use crate::strategy::Strategy;
use crate::strategy::manual::ManualStrategy;
use crate::topic::{Topic, TopicBus};
use crate::types::{Symbol, Venue};
use crate::wallet::WalletProvider;
use crate::wallet::transfer::{TransferParams, transfer_asset};
use crate::wallet::transfer_monitor::TransferMonitor;

use super::args::{DryRunArgs, now_ms, parse_symbol};
use super::wiring;

/// 等余额 WS 建连+鉴权完成的超时。
const STREAM_READY_TIMEOUT: Duration = Duration::from_secs(20);
/// 等 `submit_transfer` 被 RiskService/ExecutionService 受理的超时。
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Args, Debug)]
pub struct TransferArgs {
    /// 交易对，如 BTC/USDT——只有 base 参与划转，quote 仅用于标注仓位
    #[arg(long, value_parser = parse_symbol)]
    pub symbol: Symbol,

    /// 转出交易所：binance / kraken
    #[arg(long)]
    pub from: String,

    /// 转入交易所：binance / kraken
    #[arg(long)]
    pub to: String,

    /// 划转数量
    #[arg(long)]
    pub amount: Decimal,

    /// 指定提币链名，不传则由 wallet provider 自行选择两边共有的链
    #[arg(long)]
    pub network: Option<String>,

    /// 提币受理后不等到账确认就返回(仓位会保持 pending，需要之后人工核对)
    #[arg(long)]
    pub no_wait: bool,

    /// 等待到账确认的超时(秒)
    #[arg(long, default_value_t = 1800)]
    pub confirm_timeout_secs: u64,

    #[command(flatten)]
    pub dry: DryRunArgs,
}

pub async fn run(args: TransferArgs) -> anyhow::Result<()> {
    let dry_run = args.dry.is_dry_run();
    let testnet = args.dry.testnet;
    let symbol = args.symbol.clone();
    let from_venue_name = args.from.clone();
    let to_venue_name = args.to.clone();
    let amount = args.amount;
    let network = args.network.clone();

    if from_venue_name == to_venue_name {
        anyhow::bail!("--from and --to must be different venues, got '{from_venue_name}' for both");
    }
    let from_venue = wiring::transfer_venue(&from_venue_name)?;
    let to_venue = wiring::transfer_venue(&to_venue_name)?;
    let transfer_asset_name = symbol.base.to_string();

    let proxy = net::proxy_from_env();
    info!(
        "transfer: symbol={symbol} asset={transfer_asset_name} from={from_venue} to={to_venue} amount={amount} testnet={testnet} dry_run={dry_run}"
    );

    if dry_run {
        info!("transfer: dry_run=true (default), pass --live to actually withdraw");
        let from_wallet = wiring::build_transfer_wallet_provider(&from_venue_name, testnet, proxy.as_deref())?;
        let to_wallet = wiring::build_transfer_wallet_provider(&to_venue_name, testnet, proxy.as_deref())?;
        let (transfer_qty, withdraw) = transfer_asset(
            from_wallet.as_ref(),
            to_wallet.as_ref(),
            TransferParams {
                asset: transfer_asset_name,
                amount,
                network,
                dry_run: true,
            },
        )
        .await?;
        println!("transfer_qty={transfer_qty:?}");
        println!("withdraw={withdraw:?}");
        return Ok(());
    }

    // --live：走 RiskService -> ExecutionService -> OrderManager 全链路，真正
    // 提币并对仓位做乐观记账，和 `open --live`/`rotate --live` 一样要求 Redis。
    let redis_url = wiring::redis_url();
    info!("transfer --live: connecting to redis at {redis_url}");
    let bus = Arc::new(TopicBus::new());

    let mut wallet_providers: HashMap<Venue, Arc<dyn WalletProvider>> = HashMap::new();
    wallet_providers.insert(
        from_venue.clone(),
        wiring::build_transfer_wallet_provider(&from_venue_name, testnet, proxy.as_deref())?,
    );
    wallet_providers.insert(
        to_venue.clone(),
        wiring::build_transfer_wallet_provider(&to_venue_name, testnet, proxy.as_deref())?,
    );

    let pipeline = wiring::build_transfer_pipeline(&redis_url, bus.clone(), wallet_providers).await?;

    let strategy = ManualStrategy::new(bus.clone(), pipeline.order_manager.clone());
    let client_order_id = format!(
        "{}transfer-{}",
        args.dry.client_order_id_prefix.as_ref().map(|p| format!("{p}-")).unwrap_or_default(),
        now_ms()
    );
    let accepted = strategy
        .submit_transfer_and_wait(
            from_venue,
            to_venue.clone(),
            symbol,
            amount,
            network,
            client_order_id,
            SUBMIT_TIMEOUT,
        )
        .await?;
    println!("提币已受理: {accepted:#?}");

    if args.no_wait {
        println!(
            "--no-wait 已指定：未等待到账确认，订单 {} 的仓位将保持 pending，直到有人手动/后续对账工具核对。",
            accepted.order_id
        );
        return Ok(());
    }

    let confirm_timeout_secs = args.confirm_timeout_secs;
    info!("transfer: 等待到账确认 to_venue={to_venue} confirm_timeout_secs={confirm_timeout_secs}");

    // 提前订阅，避免和后面启动的余额流/TransferMonitor 之间出现"事件已发出但
    // 还没人订阅"的竞态。
    let mut confirm_stream = bus.subscribe::<OrderEvent>(Topic::order_event(strategy.name()));

    let balance_stream: Box<dyn BalanceStreamSource> = if to_venue == Venue::new("binance_spot") {
        Box::new(BinanceBalanceStream::from_env(to_venue.clone(), testnet, proxy.as_deref())?)
    } else {
        Box::new(KrakenBalanceStream::from_env(to_venue.clone(), proxy.as_deref())?)
    };

    let balance_handle = balance_stream.spawn(bus.clone());
    tokio::time::timeout(STREAM_READY_TIMEOUT, balance_handle.ready)
        .await
        .with_context(|| format!("等待 {to_venue} 余额 WS 就绪超时"))?
        .with_context(|| format!("{to_venue} 余额 WS 未能就绪就退出了(检查 API Key/网络)"))?;

    let monitor = Arc::new(TransferMonitor::new(
        bus.clone(),
        pipeline.order_store.clone(),
        pipeline.order_manager.clone(),
        vec![to_venue.clone()],
    ));
    let _monitor_handles = monitor.start();

    let order_id = accepted.order_id.clone();
    let confirm_result = tokio::time::timeout(Duration::from_secs(confirm_timeout_secs), async {
        loop {
            match confirm_stream.next().await {
                Some((
                    _,
                    OrderEvent::TransferConfirmed { order_id: oid, to_venue, asset, actual_delta, .. },
                )) if oid == order_id => {
                    return Some((to_venue, asset, actual_delta));
                }
                Some(_) => continue,
                None => return None,
            }
        }
    })
    .await;

    balance_handle.join.abort();

    match confirm_result {
        Ok(Some((confirmed_venue, asset, actual_delta))) => {
            println!("到账确认: to_venue={confirmed_venue} asset={asset} actual_delta={actual_delta}");
            let final_order = pipeline.order_manager.get_order(&order_id);
            println!("最终订单状态: {final_order:#?}");
            Ok(())
        }
        Ok(None) => {
            anyhow::bail!("order event stream closed while waiting for transfer order {order_id} deposit confirmation");
        }
        Err(_) => {
            println!(
                "提币已受理但到账未在 {confirm_timeout_secs}s 超时内确认，订单 {order_id} 仍处于 Transferred 状态，请之后人工用 TransferMonitor/后续对账工具核对。"
            );
            anyhow::bail!("timed out waiting for transfer order {order_id} deposit confirmation");
        }
    }
}
