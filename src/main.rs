use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use futures_util::StreamExt;
use futures_util::stream;
use log::info;
use rust_decimal::Decimal;

use arb_scanner::config::{AppConfig, ScanConfig, VenueConfig};
use arb_scanner::engine::ArbitrageEngine;
use arb_scanner::exchange_info::ExchangeInfoProvider;
use arb_scanner::exchange_info::binance::BinanceExchangeInfoProvider;
use arb_scanner::exchange_info::kraken::KrakenExchangeInfoProvider;
use arb_scanner::exchange_info::types::TradingFee;
use arb_scanner::execution;
use arb_scanner::logging;
use arb_scanner::market_data::MarketDataSource;
use arb_scanner::market_data::binance::BinanceSpotSource;
use arb_scanner::market_data::kraken::KrakenSpotSource;
use arb_scanner::market_data::mock::{MockSource, MockSymbolConfig};
use arb_scanner::net;
use arb_scanner::order::OrderProvider;
use arb_scanner::order::binance::BinanceOrderProvider;
use arb_scanner::order::binance_futures::BinanceFuturesOrderProvider;
use arb_scanner::order::kraken::KrakenOrderProvider;
use arb_scanner::scan;
use arb_scanner::sink::OpportunitySink;
use arb_scanner::sink::log_sink::LogSink;
use arb_scanner::strategy::cross_exchange::{CrossExchangeStrategy, compute_profit_bps};
use arb_scanner::strategy::triangular::{LegSide, TriangularLeg, TriangularPath, TriangularStrategy};
use arb_scanner::strategy::{FeeSchedule, Strategy};
use arb_scanner::types::{Symbol, Venue};
use arb_scanner::wallet::binance::BinanceWalletProvider;
use arb_scanner::wallet::kraken::KrakenWalletProvider;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init_logging();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("open") {
        return run_open_command(&args[2..]).await;
    }
    if args.get(1).map(String::as_str) == Some("rotate") {
        return run_rotate_command(&args[2..]).await;
    }
    if args.get(1).map(String::as_str) == Some("close") {
        return run_close_command(&args[2..]).await;
    }
    if args.get(1).map(String::as_str) == Some("scan") {
        return run_scan_command(&args[2..]).await;
    }
    if args.get(1).map(String::as_str) == Some("monitor") {
        return run_monitor_command(&args[2..]).await;
    }

    let config_path = args.get(1).cloned().unwrap_or_else(|| "config.toml".to_string());
    let config = AppConfig::load(&config_path)
        .with_context(|| format!("failed to load config from {config_path}"))?;

    let fees: HashMap<Venue, FeeSchedule> = config
        .venues
        .iter()
        .map(|v| (Venue::new(v.name.clone()), FeeSchedule::new(v.taker_fee_bps)))
        .collect();
    let symbols: Vec<Symbol> = config
        .symbols
        .iter()
        .map(|s| Symbol::new(s.base.clone(), s.quote.clone()))
        .collect();

    let proxy = net::proxy_from_env();
    match &proxy {
        Some(addr) => info!("ARB_SCANNER_PROXY set, outbound exchange connections will use proxy {addr}"),
        None => info!("ARB_SCANNER_PROXY not set, connecting to exchanges directly"),
    }

    let (tx, rx) = tokio::sync::mpsc::channel(1024);

    let mock_symbol_configs: Vec<MockSymbolConfig> = config
        .symbols
        .iter()
        .map(|s| MockSymbolConfig {
            symbol: Symbol::new(s.base.clone(), s.quote.clone()),
            initial_mid: s.initial_mid,
            volatility: s.volatility,
            spread: s.spread,
        })
        .collect();

    let mut source_handles = Vec::new();
    for venue_config in &config.venues {
        let venue = Venue::new(venue_config.name.clone());
        let source: Box<dyn MarketDataSource> = match venue_config.source.as_str() {
            "binance_spot" => {
                info!("starting binance spot market data source for venue={venue}");
                Box::new(BinanceSpotSource::new(
                    venue.clone(),
                    symbols.clone(),
                    venue_config.testnet,
                    proxy.clone(),
                ))
            }
            "kraken_spot" => {
                info!("starting kraken spot market data source for venue={venue}");
                Box::new(KrakenSpotSource::new(venue.clone(), symbols.clone(), proxy.clone()))
            }
            _ => {
                info!("starting mock market data source for venue={venue}");
                Box::new(MockSource::new(
                    venue.clone(),
                    mock_symbol_configs.clone(),
                    Duration::from_millis(config.tick_interval_ms),
                ))
            }
        };
        source_handles.push(source.spawn(tx.clone()));
    }
    drop(tx);

    let triangular_paths: Vec<TriangularPath> = config
        .triangular_paths
        .iter()
        .map(|p| {
            let legs: Vec<TriangularLeg> = p
                .legs
                .iter()
                .map(|leg| TriangularLeg {
                    symbol: Symbol::new(leg.base.clone(), leg.quote.clone()),
                    side: match leg.side.as_str() {
                        "buy" => LegSide::Buy,
                        "sell" => LegSide::Sell,
                        other => panic!("invalid triangular leg side '{other}', expected buy/sell"),
                    },
                })
                .collect();
            TriangularPath {
                venue: Venue::new(p.venue.clone()),
                legs: legs.try_into().expect("triangular path must have exactly 3 legs"),
            }
        })
        .collect();

    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(CrossExchangeStrategy::new(
            symbols,
            fees.clone(),
            config.min_profit_bps,
        )),
        Box::new(TriangularStrategy::new(
            triangular_paths,
            fees,
            config.min_profit_bps,
        )),
    ];
    let sinks: Vec<Box<dyn OpportunitySink>> = vec![Box::new(LogSink)];

    info!("arb-scanner engine starting");
    let engine = ArbitrageEngine::new(strategies, sinks);
    engine.run(rx).await;

    for handle in source_handles {
        let _ = handle.await;
    }

    Ok(())
}

/// `open` 子命令：手动触发一次"币安现货按 USDT 金额买入 -> 币安 U 本位合约等量
/// 做空对冲 -> 买入量的一半划转到 Kraken 现货"流程。不接入 engine 主循环，
/// 也不读取 `config.toml`，参数全部来自命令行。
///
/// 加 `--from-transfer` 时跳过前两步，只从"划转一半到 Kraken"这一步继续——
/// 用于现货买入和合约对冲已经手动/之前跑过完成，只是划转步骤需要重跑的场景，
/// 此时用 `--filled-qty` 传入原始现货成交量。
async fn run_open_command(args: &[String]) -> anyhow::Result<()> {
    let mut symbol: Option<Symbol> = None;
    let mut amount: Option<Decimal> = None;
    let mut asset: Option<String> = None;
    let mut testnet = false;
    let mut dry_run = true;
    let mut client_order_id_prefix: Option<String> = None;
    let mut from_transfer = false;
    let mut filled_qty: Option<Decimal> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--symbol" => {
                let v = args.get(i + 1).context("--symbol requires a value")?;
                let (base, quote) = v
                    .split_once('/')
                    .context("--symbol must be in Base/Quote format, e.g. BTC/USDT")?;
                symbol = Some(Symbol::new(base, quote));
                i += 2;
            }
            "--amount" => {
                let v = args.get(i + 1).context("--amount requires a value")?;
                amount = Some(v.parse().context("--amount must be a valid decimal number")?);
                i += 2;
            }
            "--asset" => {
                asset = Some(args.get(i + 1).context("--asset requires a value")?.clone());
                i += 2;
            }
            "--testnet" => {
                testnet = true;
                i += 1;
            }
            "--dry-run" => {
                dry_run = true;
                i += 1;
            }
            "--live" => {
                dry_run = false;
                i += 1;
            }
            "--client-order-id-prefix" => {
                client_order_id_prefix = Some(
                    args.get(i + 1)
                        .context("--client-order-id-prefix requires a value")?
                        .clone(),
                );
                i += 2;
            }
            "--from-transfer" => {
                from_transfer = true;
                i += 1;
            }
            "--filled-qty" => {
                let v = args.get(i + 1).context("--filled-qty requires a value")?;
                filled_qty = Some(v.parse().context("--filled-qty must be a valid decimal number")?);
                i += 2;
            }
            other => anyhow::bail!("unknown argument '{other}' for 'open' subcommand"),
        }
    }

    if from_transfer {
        let filled_qty = filled_qty.context(
            "--filled-qty is required when --from-transfer is set (this is the original spot buy's filled quantity)",
        )?;
        let transfer_asset = asset
            .or_else(|| symbol.as_ref().map(|s| s.base.to_string()))
            .context("--asset (or --symbol) is required to determine the transfer asset")?;

        let proxy = net::proxy_from_env();
        let binance_wallet = BinanceWalletProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
        let kraken_wallet = KrakenWalletProvider::from_env(Venue::new("kraken"), proxy.as_deref())?;

        info!(
            "open --from-transfer: asset={transfer_asset} filled_qty={filled_qty} testnet={testnet} dry_run={dry_run}"
        );
        if dry_run {
            info!("open --from-transfer: dry_run=true (default), pass --live to actually withdraw");
        }

        let (transfer_qty, withdraw) = execution::transfer_half_to_kraken(
            &binance_wallet,
            &kraken_wallet,
            execution::TransferHalfParams {
                filled_qty,
                transfer_asset,
                dry_run,
            },
        )
        .await?;

        println!("transfer_qty={transfer_qty:?}");
        println!("withdraw={withdraw:?}");
        return Ok(());
    }

    let symbol = symbol.context("--symbol is required, e.g. --symbol BTC/USDT")?;
    let quote_amount = amount.context("--amount is required, e.g. --amount 1000")?;
    let transfer_asset = asset.unwrap_or_else(|| symbol.base.to_string());

    let proxy = net::proxy_from_env();
    let spot = BinanceOrderProvider::from_env(Venue::new("binance_spot"), testnet, proxy.as_deref())?;
    let futures = BinanceFuturesOrderProvider::from_env(Venue::new("binance_futures"), testnet, proxy.as_deref())?;
    let binance_wallet = BinanceWalletProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let kraken_wallet = KrakenWalletProvider::from_env(Venue::new("kraken"), proxy.as_deref())?;

    info!(
        "open: symbol={symbol} amount={quote_amount} transfer_asset={transfer_asset} testnet={testnet} dry_run={dry_run}"
    );
    if dry_run {
        info!("open: dry_run=true (default), pass --live to actually place orders/withdraw");
    }

    let report = execution::open_hedged_position(
        &spot,
        &futures,
        &binance_wallet,
        &kraken_wallet,
        execution::OpenPositionParams {
            symbol,
            quote_amount,
            transfer_asset,
            client_order_id_prefix,
            dry_run,
        },
    )
    .await?;

    println!("{report:#?}");
    Ok(())
}

/// `rotate` 子命令：独立于 `open` 的另一种手动操作——库存轮转。在一个交易所
/// 卖出、另一个交易所买入等量同一资产，两条腿真实市价单并发发起，不涉及链上
/// 划转。同样不接入 engine 主循环，也不读取 `config.toml`，参数全部来自命令行。
///
/// `--testnet` 只影响 binance 一侧：这个代码库里 Kraken 的下单客户端不支持
/// testnet。
async fn run_rotate_command(args: &[String]) -> anyhow::Result<()> {
    let mut symbol: Option<Symbol> = None;
    let mut qty: Option<Decimal> = None;
    let mut sell_venue: Option<String> = None;
    let mut buy_venue: Option<String> = None;
    let mut testnet = false;
    let mut dry_run = true;
    let mut client_order_id_prefix: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--symbol" => {
                let v = args.get(i + 1).context("--symbol requires a value")?;
                let (base, quote) = v
                    .split_once('/')
                    .context("--symbol must be in Base/Quote format, e.g. BTC/USDT")?;
                symbol = Some(Symbol::new(base, quote));
                i += 2;
            }
            "--qty" => {
                let v = args.get(i + 1).context("--qty requires a value")?;
                qty = Some(v.parse().context("--qty must be a valid decimal number")?);
                i += 2;
            }
            "--sell" => {
                sell_venue = Some(args.get(i + 1).context("--sell requires a value")?.clone());
                i += 2;
            }
            "--buy" => {
                buy_venue = Some(args.get(i + 1).context("--buy requires a value")?.clone());
                i += 2;
            }
            "--testnet" => {
                testnet = true;
                i += 1;
            }
            "--dry-run" => {
                dry_run = true;
                i += 1;
            }
            "--live" => {
                dry_run = false;
                i += 1;
            }
            "--client-order-id-prefix" => {
                client_order_id_prefix = Some(
                    args.get(i + 1)
                        .context("--client-order-id-prefix requires a value")?
                        .clone(),
                );
                i += 2;
            }
            other => anyhow::bail!("unknown argument '{other}' for 'rotate' subcommand"),
        }
    }

    let symbol = symbol.context("--symbol is required, e.g. --symbol BTC/USDT")?;
    let qty = qty.context("--qty is required, e.g. --qty 0.5")?;
    let sell_venue = sell_venue.context("--sell is required, e.g. --sell binance")?;
    let buy_venue = buy_venue.context("--buy is required, e.g. --buy kraken")?;
    if sell_venue == buy_venue {
        anyhow::bail!("--sell and --buy must be different venues, got '{sell_venue}' for both");
    }

    let proxy = net::proxy_from_env();
    let sell_provider = build_order_provider(&sell_venue, testnet, proxy.as_deref())?;
    let buy_provider = build_order_provider(&buy_venue, testnet, proxy.as_deref())?;

    info!(
        "rotate: symbol={symbol} qty={qty} sell={sell_venue} buy={buy_venue} testnet={testnet} dry_run={dry_run}"
    );
    if dry_run {
        info!("rotate: dry_run=true (default), pass --live to actually place orders");
    }

    let report = execution::rotate_inventory(
        sell_provider.as_ref(),
        buy_provider.as_ref(),
        execution::RotateInventoryParams {
            symbol,
            qty,
            client_order_id_prefix,
            dry_run,
        },
    )
    .await?;

    println!("{report:#?}");
    Ok(())
}

/// `close` 子命令：平掉币安现货、Kraken 现货、币安合约三条腿，互相独立、可以
/// 只传其中一部分。每条腿的数量都要在命令行里显式指定——这个代码库里没有余额
/// /持仓查询接口，没法自动算出"全部"是多少，需要调用方自己核对仓位后传入。
/// 同样不接入 engine 主循环，也不读取 `config.toml`。
///
/// 只有对应 `--xxx-qty` 被传入时才会构造那个交易所的 provider，所以只平币安
/// 一侧时不需要配置 Kraken 的 API key。
async fn run_close_command(args: &[String]) -> anyhow::Result<()> {
    let mut symbol: Option<Symbol> = None;
    let mut binance_spot_qty: Option<Decimal> = None;
    let mut kraken_spot_qty: Option<Decimal> = None;
    let mut futures_qty: Option<Decimal> = None;
    let mut testnet = false;
    let mut dry_run = true;
    let mut client_order_id_prefix: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--symbol" => {
                let v = args.get(i + 1).context("--symbol requires a value")?;
                let (base, quote) = v
                    .split_once('/')
                    .context("--symbol must be in Base/Quote format, e.g. BTC/USDT")?;
                symbol = Some(Symbol::new(base, quote));
                i += 2;
            }
            "--binance-spot-qty" => {
                let v = args.get(i + 1).context("--binance-spot-qty requires a value")?;
                binance_spot_qty = Some(v.parse().context("--binance-spot-qty must be a valid decimal number")?);
                i += 2;
            }
            "--kraken-spot-qty" => {
                let v = args.get(i + 1).context("--kraken-spot-qty requires a value")?;
                kraken_spot_qty = Some(v.parse().context("--kraken-spot-qty must be a valid decimal number")?);
                i += 2;
            }
            "--futures-qty" => {
                let v = args.get(i + 1).context("--futures-qty requires a value")?;
                futures_qty = Some(v.parse().context("--futures-qty must be a valid decimal number")?);
                i += 2;
            }
            "--testnet" => {
                testnet = true;
                i += 1;
            }
            "--dry-run" => {
                dry_run = true;
                i += 1;
            }
            "--live" => {
                dry_run = false;
                i += 1;
            }
            "--client-order-id-prefix" => {
                client_order_id_prefix = Some(
                    args.get(i + 1)
                        .context("--client-order-id-prefix requires a value")?
                        .clone(),
                );
                i += 2;
            }
            other => anyhow::bail!("unknown argument '{other}' for 'close' subcommand"),
        }
    }

    let symbol = symbol.context("--symbol is required, e.g. --symbol BTC/USDT")?;
    if binance_spot_qty.is_none() && kraken_spot_qty.is_none() && futures_qty.is_none() {
        anyhow::bail!(
            "at least one of --binance-spot-qty / --kraken-spot-qty / --futures-qty is required"
        );
    }

    let proxy = net::proxy_from_env();
    let binance_spot = binance_spot_qty
        .is_some()
        .then(|| BinanceOrderProvider::from_env(Venue::new("binance_spot"), testnet, proxy.as_deref()))
        .transpose()?;
    let kraken_spot = kraken_spot_qty
        .is_some()
        .then(|| KrakenOrderProvider::from_env(Venue::new("kraken_spot"), proxy.as_deref()))
        .transpose()?;
    let binance_futures = futures_qty
        .is_some()
        .then(|| BinanceFuturesOrderProvider::from_env(Venue::new("binance_futures"), testnet, proxy.as_deref()))
        .transpose()?;

    info!(
        "close: symbol={symbol} binance_spot_qty={binance_spot_qty:?} kraken_spot_qty={kraken_spot_qty:?} futures_qty={futures_qty:?} testnet={testnet} dry_run={dry_run}"
    );
    if dry_run {
        info!("close: dry_run=true (default), pass --live to actually place orders");
    }

    let report = execution::close_hedged_position(
        binance_spot.as_ref().map(|p| p as &dyn OrderProvider),
        kraken_spot.as_ref().map(|p| p as &dyn OrderProvider),
        binance_futures.as_ref().map(|p| p as &dyn OrderProvider),
        execution::ClosePositionParams {
            symbol,
            binance_spot_qty,
            kraken_spot_qty,
            futures_qty,
            client_order_id_prefix,
            dry_run,
        },
    )
    .await?;

    println!("{report:#?}");
    Ok(())
}

/// `scan` 子命令：只读地找出币安和 Kraken"有交集"的币种——币安有 USDT 本位永续
/// 合约、Kraken 有 USDT 现货、且两边钱包信息里至少共享一条可转账的标准链，打印
/// 每个币种的基本信息，作为后续 `open`/`rotate` 操作前的选币依据。不接入 engine
/// 主循环，也不读取 `config.toml`。
async fn run_scan_command(args: &[String]) -> anyhow::Result<()> {
    let mut testnet = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--testnet" => {
                testnet = true;
                i += 1;
            }
            other => anyhow::bail!("unknown argument '{other}' for 'scan' subcommand"),
        }
    }

    let proxy = net::proxy_from_env();
    let binance_info = BinanceExchangeInfoProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let kraken_info = KrakenExchangeInfoProvider::from_env(Venue::new("kraken"), proxy.as_deref())?;
    let binance_wallet = BinanceWalletProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let kraken_wallet = KrakenWalletProvider::from_env(Venue::new("kraken"), proxy.as_deref())?;

    let blacklist = ScanConfig::load_blacklist("config.toml");
    info!(
        "scan: looking for symbols overlapping between binance (usdt perpetual) and kraken (usdt spot) testnet={testnet} blacklist={blacklist:?}"
    );

    let result = scan::find_overlap(&binance_info, &kraken_info, &binance_wallet, &kraken_wallet, &blacklist).await?;
    info!(
        "scan: binance_spot_symbols={} kraken_spot_symbols={} overlapping_symbols={} skipped={} blacklisted={}",
        result.binance_spot_symbols.len(),
        result.kraken_spot_symbols.len(),
        result.overlaps.len(),
        result.skipped.len(),
        result.blacklisted.len()
    );

    println!("== Binance USDT Spot Symbols With Perp Hedge ({}) ==", result.binance_spot_symbols.len());
    println!("{}", scan::format_symbol_list(&result.binance_spot_symbols));
    println!();
    println!("== Kraken USDT Spot Symbols ({}) ==", result.kraken_spot_symbols.len());
    println!("{}", scan::format_symbol_list(&result.kraken_spot_symbols));
    println!();
    println!("== Overlapping Symbols ({}) ==", result.overlaps.len());
    println!("{}", scan::format_overlap_table(&result.overlaps));
    println!();
    println!("== Skipped Candidates ({}) ==", result.skipped.len());
    println!("{}", scan::format_skipped_list(&result.skipped));
    println!();
    println!("== Blacklisted Coins (excluded, not queried) ({}) ==", result.blacklisted.len());
    println!("{}", scan::format_blacklisted_list(&result.blacklisted));
    Ok(())
}

/// `spot_trading_fee` 查询并发上限，避免对候选币逐个查询手续费时触发限流，
/// 和 `scan/mod.rs` 里 `KRAKEN_WALLET_CONCURRENCY` 同样的考虑。
const FEE_QUERY_CONCURRENCY: usize = 4;

/// `monitor` 命令的两种运行模式，通过 `--mode` 互斥选择。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MonitorMode {
    /// 事件驱动，只在扣费后价差 >= `--min-profit-bps` 时打印(默认，historic 行为)。
    PositiveOnly,
    /// 定时器驱动，每隔 `--periodic-interval-secs` 打印一次所有监控币种的双向价差，
    /// 不管正负，只额外标注是否达到 `--min-profit-bps` 阈值。
    Periodic,
}

/// `monitor` 子命令：复用 `scan::find_overlap` 筛出的币安/Kraken 交集币种，持续监控
/// 两边现货价差。每个币的手续费用 [`ExchangeInfoProvider::spot_trading_fee`] 查询两边
/// 真实账户 taker 费率(而不是固定值)，币安这边再乘上 `config.toml` `[[venues]]` 里
/// 币安条目的 `fee_discount` 折扣(如 BNB 抵扣手续费，默认 1 不打折，见
/// [`VenueConfig::load_fee_discount`])，Kraken 不打折。
///
/// `--mode`(默认 `positive-only`)控制打印方式:
/// - `positive-only`:接入现成的 `CrossExchangeStrategy` + `LogSink` 管线，只在扣费后
///   价差 >= `--min-profit-bps`(默认 0，即扣费后为正)时打印。
/// - `periodic`:不注册任何 `Strategy`/`Sink`，改为在独立的定时任务里，每隔
///   `--periodic-interval-secs`(默认 5)读一次引擎的行情缓存快照，为每个监控币打印
///   双向价差(不管正负)，并标注 `profitable` 是否达到 `--min-profit-bps` 阈值。
///
/// `positive-only` 模式下 `CrossExchangeStrategy` 的手续费 map 不区分 symbol，因此给
/// 每个币单独构造一个只监控该币、只装这个币真实手续费的 `CrossExchangeStrategy` 实例，
/// 而不是像默认主流程那样所有 symbol 共享一份手续费配置。不接入 `config.toml` 驱动的
/// 默认主循环。
async fn run_monitor_command(args: &[String]) -> anyhow::Result<()> {
    let mut testnet = false;
    let mut min_profit_bps = Decimal::ZERO;
    let mut mode = MonitorMode::PositiveOnly;
    let mut periodic_interval_secs: u64 = 5;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--testnet" => {
                testnet = true;
                i += 1;
            }
            "--min-profit-bps" => {
                let v = args.get(i + 1).context("--min-profit-bps requires a value")?;
                min_profit_bps = v.parse().context("--min-profit-bps must be a valid decimal number")?;
                i += 2;
            }
            "--mode" => {
                let v = args.get(i + 1).context("--mode requires a value")?;
                mode = match v.as_str() {
                    "positive-only" => MonitorMode::PositiveOnly,
                    "periodic" => MonitorMode::Periodic,
                    other => anyhow::bail!(
                        "invalid value '{other}' for --mode, expected 'positive-only' or 'periodic'"
                    ),
                };
                i += 2;
            }
            "--periodic-interval-secs" => {
                let v = args.get(i + 1).context("--periodic-interval-secs requires a value")?;
                periodic_interval_secs =
                    v.parse().context("--periodic-interval-secs must be a valid non-negative integer")?;
                i += 2;
            }
            other => anyhow::bail!("unknown argument '{other}' for 'monitor' subcommand"),
        }
    }

    let proxy = net::proxy_from_env();
    let binance_info = BinanceExchangeInfoProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let kraken_info = KrakenExchangeInfoProvider::from_env(Venue::new("kraken"), proxy.as_deref())?;
    let binance_wallet = BinanceWalletProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let kraken_wallet = KrakenWalletProvider::from_env(Venue::new("kraken"), proxy.as_deref())?;

    let blacklist = ScanConfig::load_blacklist("config.toml");
    let binance_fee_discount = VenueConfig::load_fee_discount("config.toml", "binance");
    info!(
        "monitor: looking for symbols overlapping between binance (usdt perpetual) and kraken (usdt spot) testnet={testnet} blacklist={blacklist:?} binance_fee_discount={binance_fee_discount}"
    );
    let scan_result =
        scan::find_overlap(&binance_info, &kraken_info, &binance_wallet, &kraken_wallet, &blacklist).await?;
    info!(
        "monitor: blacklisted={} ({})",
        scan_result.blacklisted.len(),
        scan::format_blacklisted_list(&scan_result.blacklisted)
    );
    if scan_result.overlaps.is_empty() {
        println!("no overlapping symbols found, nothing to monitor");
        return Ok(());
    }

    info!("monitor: querying real taker fees for {} candidate symbols", scan_result.overlaps.len());
    let binance_info_ref = &binance_info;
    let kraken_info_ref = &kraken_info;
    let fee_results: Vec<(String, Symbol, anyhow::Result<(TradingFee, TradingFee)>)> =
        stream::iter(scan_result.overlaps)
            .map(|overlap| async move {
                let binance_symbol = Symbol::new(overlap.coin.clone(), "USDT");
                let result = tokio::try_join!(
                    binance_info_ref.spot_trading_fee(&binance_symbol),
                    kraken_info_ref.spot_trading_fee(&overlap.kraken_spot_symbol)
                );
                (overlap.coin, binance_symbol, result)
            })
            .buffer_unordered(FEE_QUERY_CONCURRENCY)
            .collect()
            .await;

    let mut symbols = Vec::new();
    let mut coin_fees: Vec<(String, Symbol, HashMap<Venue, FeeSchedule>)> = Vec::new();
    let mut monitored_summary = Vec::new();
    let mut skipped = Vec::new();
    for (coin, symbol, result) in fee_results {
        match result {
            Ok((binance_fee, kraken_fee)) => {
                let binance_effective_bps = binance_fee.taker_bps * binance_fee_discount;
                let fees: HashMap<Venue, FeeSchedule> = HashMap::from([
                    (Venue::new("binance"), FeeSchedule::new(binance_effective_bps)),
                    (Venue::new("kraken"), FeeSchedule::new(kraken_fee.taker_bps)),
                ]);
                monitored_summary.push(format!(
                    "{coin:<10}  binance_taker_bps={} x{binance_fee_discount}={binance_effective_bps}  kraken_taker_bps={}",
                    binance_fee.taker_bps, kraken_fee.taker_bps
                ));
                symbols.push(symbol.clone());
                coin_fees.push((coin, symbol, fees));
            }
            Err(err) => {
                let reason = format!("failed to fetch trading fee: {err:#}");
                log::warn!("monitor: {coin} {reason}, skipping");
                skipped.push(format!("{coin:<10}  {reason}"));
            }
        }
    }

    if symbols.is_empty() {
        println!("failed to fetch trading fees for every candidate symbol, nothing to monitor");
        return Ok(());
    }

    monitored_summary.sort();
    println!("== Monitoring {} Symbols (mode={mode:?} min_profit_bps={min_profit_bps}) ==", symbols.len());
    println!("{}", monitored_summary.join("\n"));
    if !skipped.is_empty() {
        skipped.sort();
        println!();
        println!("== Skipped ({}) ==", skipped.len());
        println!("{}", skipped.join("\n"));
    }

    let (tx, rx) = tokio::sync::mpsc::channel(1024);
    let mut source_handles = Vec::new();
    let binance_source: Box<dyn MarketDataSource> = Box::new(BinanceSpotSource::new(
        Venue::new("binance"),
        symbols.clone(),
        testnet,
        proxy.clone(),
    ));
    source_handles.push(binance_source.spawn(tx.clone()));
    let kraken_source: Box<dyn MarketDataSource> =
        Box::new(KrakenSpotSource::new(Venue::new("kraken"), symbols.clone(), proxy.clone()));
    source_handles.push(kraken_source.spawn(tx.clone()));
    drop(tx);

    let (strategies, sinks): (Vec<Box<dyn Strategy>>, Vec<Box<dyn OpportunitySink>>) = match mode {
        MonitorMode::PositiveOnly => {
            let strategies = coin_fees
                .iter()
                .map(|(_, symbol, fees)| {
                    Box::new(CrossExchangeStrategy::new(vec![symbol.clone()], fees.clone(), min_profit_bps))
                        as Box<dyn Strategy>
                })
                .collect();
            (strategies, vec![Box::new(LogSink) as Box<dyn OpportunitySink>])
        }
        MonitorMode::Periodic => (Vec::new(), Vec::new()),
    };

    let engine = ArbitrageEngine::new(strategies, sinks);
    if mode == MonitorMode::Periodic {
        let shared_cache = engine.shared_cache();
        let periodic_coins = coin_fees;
        let binance_venue = Venue::new("binance");
        let kraken_venue = Venue::new("kraken");
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(periodic_interval_secs));
            loop {
                ticker.tick().await;
                for (coin, symbol, fees) in &periodic_coins {
                    let Some(binance_quote) =
                        shared_cache.get(&(binance_venue.clone(), symbol.clone())).map(|e| *e.value())
                    else {
                        continue;
                    };
                    let Some(kraken_quote) =
                        shared_cache.get(&(kraken_venue.clone(), symbol.clone())).map(|e| *e.value())
                    else {
                        continue;
                    };
                    let binance_fee = fees.get(&binance_venue).copied().unwrap_or(FeeSchedule::new(0));
                    let kraken_fee = fees.get(&kraken_venue).copied().unwrap_or(FeeSchedule::new(0));

                    for (buy_label, buy_quote, buy_fee, sell_label, sell_quote, sell_fee) in [
                        ("binance", binance_quote, binance_fee, "kraken", kraken_quote, kraken_fee),
                        ("kraken", kraken_quote, kraken_fee, "binance", binance_quote, binance_fee),
                    ] {
                        let Some(profit_bps) = compute_profit_bps(buy_quote.ask, buy_fee, sell_quote.bid, sell_fee)
                        else {
                            continue;
                        };
                        let profitable = profit_bps >= min_profit_bps;
                        log::info!(
                            "[periodic] {coin:<10} buy={buy_label:<7} sell={sell_label:<7} profit_bps={profit_bps:>10} profitable={profitable}"
                        );
                    }
                }
            }
        });
    }

    info!("monitor: engine starting");
    engine.run(rx).await;

    for handle in source_handles {
        let _ = handle.await;
    }

    Ok(())
}

/// 把 `"binance"` / `"kraken"` 映射到对应的现货 `OrderProvider`，供 `rotate`
/// 子命令按名字选择交易所。
fn build_order_provider(name: &str, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Box<dyn OrderProvider>> {
    match name {
        "binance" => Ok(Box::new(BinanceOrderProvider::from_env(
            Venue::new("binance_spot"),
            testnet,
            proxy,
        )?)),
        "kraken" => Ok(Box::new(KrakenOrderProvider::from_env(Venue::new("kraken_spot"), proxy)?)),
        other => anyhow::bail!("unknown venue '{other}' for 'rotate' subcommand, expected 'binance' or 'kraken'"),
    }
}
