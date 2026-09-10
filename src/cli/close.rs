//! `close` 子命令：平掉币安现货、Kraken 现货、币安合约三条腿，互相独立、可以
//! 只传其中一部分。每条腿的数量都要在命令行里显式指定——这个代码库里没有余额
//! /持仓查询接口，没法自动算出"全部"是多少，需要调用方自己核对仓位后传入。
//! 同样不接入 engine 主循环，也不读取 `config.toml`。
//!
//! 只有对应 `--xxx-qty` 被传入时才会构造那个交易所的 provider，所以只平币安
//! 一侧时不需要配置 Kraken 的 API key。

use std::collections::HashMap;
use std::sync::Arc;

use clap::Args;
use log::info;
use rust_decimal::Decimal;

use crate::net;
use crate::order::OrderProvider;
use crate::order::binance::BinanceOrderProvider;
use crate::order::binance_futures::BinanceFuturesOrderProvider;
use crate::order::kraken::{KrakenOrderProvider, KrakenPrivateOrderStream};
use crate::strategy::manual::ClosePositionParams;
use crate::types::{Symbol, Venue};

use super::args::{DryRunArgs, FillTimeoutArgs, parse_symbol};
use super::wiring::{self, ManualLeg};

#[derive(Args, Debug)]
pub struct CloseArgs {
    /// 交易对，如 BTC/USDT
    #[arg(long, value_parser = parse_symbol)]
    pub symbol: Symbol,

    /// 要平掉的币安现货数量
    #[arg(long)]
    pub binance_spot_qty: Option<Decimal>,

    /// 要平掉的 Kraken 现货数量
    #[arg(long)]
    pub kraken_spot_qty: Option<Decimal>,

    /// 要平掉的币安 U 本位合约数量
    #[arg(long)]
    pub futures_qty: Option<Decimal>,

    #[command(flatten)]
    pub dry: DryRunArgs,

    #[command(flatten)]
    pub fill: FillTimeoutArgs,
}

pub async fn run(args: CloseArgs) -> anyhow::Result<()> {
    let dry_run = args.dry.is_dry_run();
    let testnet = args.dry.testnet;
    let symbol = args.symbol.clone();
    let (binance_spot_qty, kraken_spot_qty, futures_qty) =
        (args.binance_spot_qty, args.kraken_spot_qty, args.futures_qty);

    if binance_spot_qty.is_none() && kraken_spot_qty.is_none() && futures_qty.is_none() {
        anyhow::bail!("at least one of --binance-spot-qty / --kraken-spot-qty / --futures-qty is required");
    }

    let proxy = net::proxy_from_env();
    let binance_spot_venue = Venue::new("binance_spot");
    let kraken_spot_venue = Venue::new("kraken_spot");
    let futures_venue = Venue::new("binance_futures");

    let binance_spot: Option<Arc<dyn OrderProvider>> = binance_spot_qty
        .is_some()
        .then(|| BinanceOrderProvider::from_env(binance_spot_venue.clone(), testnet, proxy.as_deref()))
        .transpose()?
        .map(|p| Arc::new(p) as Arc<dyn OrderProvider>);
    // Kraken 这条腿要留一份具体类型的 Arc：私有订单流是从 provider 自己持有的
    // 共享 WS 连接上分出来的(`shared_ws()`)，`dyn OrderProvider` 上没有这个方法。
    let kraken_spot_arc: Option<Arc<KrakenOrderProvider>> = kraken_spot_qty
        .is_some()
        .then(|| KrakenOrderProvider::from_env(kraken_spot_venue.clone(), proxy.as_deref()).map(Arc::new))
        .transpose()?;
    let kraken_spot: Option<Arc<dyn OrderProvider>> = kraken_spot_arc
        .as_ref()
        .map(|p| Arc::clone(p) as Arc<dyn OrderProvider>);
    let binance_futures: Option<Arc<dyn OrderProvider>> = futures_qty
        .is_some()
        .then(|| BinanceFuturesOrderProvider::from_env(futures_venue.clone(), testnet, proxy.as_deref()))
        .transpose()?
        .map(|p| Arc::new(p) as Arc<dyn OrderProvider>);

    info!(
        "close: symbol={symbol} binance_spot_qty={binance_spot_qty:?} kraken_spot_qty={kraken_spot_qty:?} futures_qty={futures_qty:?} testnet={testnet} dry_run={dry_run}"
    );

    let params = ClosePositionParams {
        symbol: symbol.clone(),
        binance_spot_qty,
        kraken_spot_qty,
        futures_qty,
        client_order_id_prefix: args.dry.client_order_id_prefix.clone(),
        dry_run,
        fill_timeout: args.fill.fill_timeout(),
    };

    if dry_run {
        info!("close: dry_run=true (default), pass --live to actually place orders");
        let strategy = wiring::bare_manual_strategy();
        let report = strategy
            .close_hedged_position(
                binance_spot.as_deref(),
                kraken_spot.as_deref(),
                binance_futures.as_deref(),
                params,
            )
            .await?;
        println!("{report:#?}");
        return Ok(());
    }

    // --live：只给传了 qty 的腿建完整的 OrderManager 流水线，成交结果落进
    // PositionManager/PortfolioManager，和 `open --live`/`rotate --live` 一致。
    let mut legs: Vec<ManualLeg> = Vec::new();
    if let Some(provider) = &binance_spot {
        legs.push(ManualLeg {
            venue: provider.venue(),
            provider: provider.clone(),
            stream: wiring::binance_spot_stream(binance_spot_venue, testnet, proxy.as_deref(), &symbol)?,
            limits: wiring::manual_risk_limits(),
        });
    }
    if let Some(provider) = &kraken_spot_arc {
        legs.push(ManualLeg {
            venue: provider.venue(),
            provider: Arc::clone(kraken_spot.as_ref().expect("kraken_spot mirrors kraken_spot_arc")),
            stream: Box::new(KrakenPrivateOrderStream::from_shared_ws(provider.shared_ws())),
            limits: wiring::manual_risk_limits(),
        });
    }
    if let Some(provider) = &binance_futures {
        legs.push(ManualLeg {
            venue: provider.venue(),
            provider: provider.clone(),
            stream: wiring::binance_futures_stream(futures_venue, testnet, proxy.as_deref(), &symbol)?,
            limits: wiring::manual_risk_limits(),
        });
    }

    let report = wiring::run_manual_live("close", &symbol, legs, HashMap::new(), |strategy| async move {
        strategy
            .close_hedged_position(
                binance_spot.as_deref(),
                kraken_spot.as_deref(),
                binance_futures.as_deref(),
                params,
            )
            .await
    })
    .await?;

    println!("{report:#?}");
    Ok(())
}
