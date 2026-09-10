//! `rotate` 子命令：独立于 `open` 的另一种手动操作——库存轮转。在一个交易所
//! 卖出、另一个交易所买入等量同一资产，两条腿真实市价单并发发起，不涉及链上
//! 划转。同样不接入 engine 主循环，也不读取 `config.toml`，参数全部来自命令行。
//!
//! `--testnet` 只影响 binance 一侧：这个代码库里 Kraken 的下单客户端不支持
//! testnet。

use std::collections::HashMap;

use anyhow::Context;
use clap::Args;
use log::info;
use rust_decimal::Decimal;

use crate::net;
use crate::strategy::manual::RotateInventoryParams;
use crate::types::Symbol;

use super::args::{DryRunArgs, FillTimeoutArgs, parse_symbol};
use super::wiring::{self, ManualLeg};

#[derive(Args, Debug)]
pub struct RotateArgs {
    /// 交易对，如 BTC/USDT
    #[arg(long, value_parser = parse_symbol)]
    pub symbol: Symbol,

    /// 轮转数量(基础币)
    #[arg(long)]
    pub qty: Decimal,

    /// 卖出腿所在交易所：binance / kraken / gate
    #[arg(long)]
    pub sell: String,

    /// 买入腿所在交易所：binance / kraken / gate
    #[arg(long)]
    pub buy: String,

    #[command(flatten)]
    pub dry: DryRunArgs,

    #[command(flatten)]
    pub fill: FillTimeoutArgs,
}

pub async fn run(args: RotateArgs) -> anyhow::Result<()> {
    let dry_run = args.dry.is_dry_run();
    let testnet = args.dry.testnet;
    let RotateArgs { symbol, qty, sell: sell_venue_name, buy: buy_venue_name, .. } = &args;

    if sell_venue_name == buy_venue_name {
        anyhow::bail!("--sell and --buy must be different venues, got '{sell_venue_name}' for both");
    }

    let proxy = net::proxy_from_env();
    let sell_provider = wiring::build_order_provider(sell_venue_name, testnet, proxy.as_deref())?;
    let buy_provider = wiring::build_order_provider(buy_venue_name, testnet, proxy.as_deref())?;

    info!(
        "rotate: symbol={symbol} qty={qty} sell={sell_venue_name} buy={buy_venue_name} testnet={testnet} dry_run={dry_run}"
    );

    let params = RotateInventoryParams {
        symbol: symbol.clone(),
        qty: *qty,
        client_order_id_prefix: args.dry.client_order_id_prefix.clone(),
        dry_run,
        fill_timeout: args.fill.fill_timeout(),
    };

    if dry_run {
        info!("rotate: dry_run=true (default), pass --live to actually place orders");
        let strategy = wiring::bare_manual_strategy();
        let report = strategy.rotate_inventory(sell_provider.as_ref(), buy_provider.as_ref(), params).await?;
        println!("{report:#?}");
        return Ok(());
    }

    // --live：两条腿都要走完整的 OrderManager 流水线，成交结果才会真正落进
    // PositionManager/PortfolioManager，和 `open --live` 一致。
    let legs = vec![
        ManualLeg {
            venue: sell_provider.venue(),
            provider: sell_provider.clone(),
            stream: wiring::build_order_stream_source(sell_venue_name, testnet, proxy.as_deref(), symbol)
                .with_context(|| format!("failed to start {sell_venue_name} private order stream"))?,
            limits: wiring::manual_risk_limits(),
        },
        ManualLeg {
            venue: buy_provider.venue(),
            provider: buy_provider.clone(),
            stream: wiring::build_order_stream_source(buy_venue_name, testnet, proxy.as_deref(), symbol)
                .with_context(|| format!("failed to start {buy_venue_name} private order stream"))?,
            limits: wiring::manual_risk_limits(),
        },
    ];

    let sell_for_action = sell_provider.clone();
    let buy_for_action = buy_provider.clone();
    let report = wiring::run_manual_live("rotate", symbol, legs, HashMap::new(), |strategy| async move {
        strategy
            .rotate_inventory(sell_for_action.as_ref(), buy_for_action.as_ref(), params)
            .await
    })
    .await?;

    println!("{report:#?}");
    Ok(())
}
