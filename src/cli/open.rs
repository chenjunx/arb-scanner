//! `open` 子命令：手动触发一次"币安现货按 USDT 金额买入 -> 币安 U 本位合约等量
//! 做空对冲"流程。不接入 engine 主循环，也不读取 `config.toml`，参数全部来自
//! 命令行。
//!
//! 加 `--from-transfer` 时跳过开仓，只做"划转一半到 Kraken"这一步——用于现货
//! 买入和合约对冲已经手动/之前跑过完成，只是划转步骤需要重跑的场景，此时用
//! `--filled-qty` 传入原始现货成交量。**行为变化**：以前 `--live
//! --transfer-to-kraken` 能一步做完开仓+划转，现在划转永远是单独一步——手动
//! 策略(`ManualStrategy`)只管下单，钱包划转挪到了 `wallet::transfer`。

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use clap::Args;
use log::info;
use rust_decimal::Decimal;

use crate::exchange_info::PrecisionCache;
use crate::exchange_info::binance::BinanceExchangeInfoProvider;
use crate::net;
use crate::order::OrderProvider;
use crate::order::binance::BinanceOrderProvider;
use crate::order::binance_futures::BinanceFuturesOrderProvider;
use crate::order_manager::risk_service::RiskLimits;
use crate::strategy::manual::{OpenPositionParams, open_hedged_position_dry_run};
use crate::types::{Symbol, Venue};
use crate::wallet::binance::BinanceWalletProvider;
use crate::wallet::kraken::KrakenWalletProvider;
use crate::wallet::transfer::{TransferHalfParams, transfer_half_to_kraken};

use super::args::{DryRunArgs, FillTimeoutArgs, parse_symbol};
use super::wiring::{self, ManualLeg};

#[derive(Args, Debug)]
pub struct OpenArgs {
    /// 交易对，如 BTC/USDT。`--from-transfer` 模式下可以改用 `--asset` 指定
    #[arg(long, value_parser = parse_symbol)]
    pub symbol: Option<Symbol>,

    /// 现货买入的计价币金额(USDT)
    #[arg(long)]
    pub amount: Option<Decimal>,

    /// `--from-transfer` 模式下要划转的资产名，不传则取 `--symbol` 的 base
    #[arg(long)]
    pub asset: Option<String>,

    /// 跳过开仓，只重跑"划转一半到 Kraken"这一步
    #[arg(long)]
    pub from_transfer: bool,

    /// `--from-transfer` 模式下原始现货买入的成交量
    #[arg(long)]
    pub filled_qty: Option<Decimal>,

    #[command(flatten)]
    pub dry: DryRunArgs,

    #[command(flatten)]
    pub fill: FillTimeoutArgs,
}

pub async fn run(args: OpenArgs) -> anyhow::Result<()> {
    let dry_run = args.dry.is_dry_run();
    let testnet = args.dry.testnet;

    if args.from_transfer {
        return run_from_transfer(&args, testnet, dry_run).await;
    }

    let symbol = args.symbol.context("--symbol is required, e.g. --symbol BTC/USDT")?;
    let quote_amount = args.amount.context("--amount is required, e.g. --amount 1000")?;
    let spot_venue = Venue::new("binance_spot");
    let futures_venue = Venue::new("binance_futures");

    let proxy = net::proxy_from_env();
    let spot: Arc<dyn OrderProvider> =
        Arc::new(BinanceOrderProvider::from_env(spot_venue.clone(), testnet, proxy.as_deref())?);
    let futures: Arc<dyn OrderProvider> =
        Arc::new(BinanceFuturesOrderProvider::from_env(futures_venue.clone(), testnet, proxy.as_deref())?);

    // 启动时一次性加载合约下单精度缓存，凭证/网络问题在下单前就暴露（fail-fast），
    // 而不是现货腿已经成交了才发现；即使 dry_run 分支用不到它也没关系——一次性
    // 启动成本，不是每次下单都要付的代价。
    let exchange_info = BinanceExchangeInfoProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let futures_precision = PrecisionCache::load_perpetual(&exchange_info)
        .await
        .context("failed to load futures market precision cache")?;

    info!("open: symbol={symbol} amount={quote_amount} testnet={testnet} dry_run={dry_run}");

    let params = OpenPositionParams {
        symbol: symbol.clone(),
        quote_amount,
        client_order_id_prefix: args.dry.client_order_id_prefix.clone(),
        dry_run,
        fill_timeout: args.fill.fill_timeout(),
    };

    if dry_run {
        info!("open: dry_run=true (default), pass --live to actually place orders");
        let report = open_hedged_position_dry_run(spot.as_ref(), params).await?;
        println!("{report:#?}");
        return Ok(());
    }

    // --live：两条腿都要走完整的 OrderManager 流水线(风控 -> 执行引擎 -> 交易所
    // 私有 WS 成交确认)，成交结果才会真正落进 PositionManager/PortfolioManager。
    // Redis 连不上直接快速失败，不能等下单后才发现存不进去。
    let legs = vec![
        ManualLeg {
            venue: spot_venue.clone(),
            provider: spot.clone(),
            stream: wiring::binance_spot_stream(spot_venue, testnet, proxy.as_deref(), &symbol)?,
            // 现货腿是唯一按计价币金额下单的一条，把 `--amount` 直接当风控上限，
            // 多花一分钱都会被 RiskService 拦下。
            limits: RiskLimits {
                max_order_amount: quote_amount,
                ..wiring::manual_risk_limits()
            },
        },
        ManualLeg {
            venue: futures_venue.clone(),
            provider: futures.clone(),
            stream: wiring::binance_futures_stream(futures_venue, testnet, proxy.as_deref(), &symbol)?,
            limits: wiring::manual_risk_limits(),
        },
    ];

    let report = wiring::run_manual_live("open", &symbol, legs, HashMap::new(), |strategy| async move {
        strategy
            .open_hedged_position_live(spot.as_ref(), futures.as_ref(), &futures_precision, params)
            .await
    })
    .await?;

    println!("{report:#?}");
    Ok(())
}

async fn run_from_transfer(args: &OpenArgs, testnet: bool, dry_run: bool) -> anyhow::Result<()> {
    let filled_qty = args.filled_qty.context(
        "--filled-qty is required when --from-transfer is set (this is the original spot buy's filled quantity)",
    )?;
    let transfer_asset = args
        .asset
        .clone()
        .or_else(|| args.symbol.as_ref().map(|s| s.base.to_string()))
        .context("--asset (or --symbol) is required to determine the transfer asset")?;

    let proxy = net::proxy_from_env();
    let binance_wallet = BinanceWalletProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let kraken_wallet = KrakenWalletProvider::from_env(Venue::new("kraken"), proxy.as_deref())?;

    info!("open --from-transfer: asset={transfer_asset} filled_qty={filled_qty} testnet={testnet} dry_run={dry_run}");
    if dry_run {
        info!("open --from-transfer: dry_run=true (default), pass --live to actually withdraw");
    }

    let (transfer_qty, withdraw) = transfer_half_to_kraken(
        &binance_wallet,
        &kraken_wallet,
        TransferHalfParams {
            filled_qty,
            transfer_asset,
            dry_run,
        },
    )
    .await?;

    println!("transfer_qty={transfer_qty:?}");
    println!("withdraw={withdraw:?}");
    Ok(())
}
