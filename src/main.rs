use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use log::info;
use rust_decimal::Decimal;

use arb_scanner::config::AppConfig;
use arb_scanner::engine::ArbitrageEngine;
use arb_scanner::execution;
use arb_scanner::logging;
use arb_scanner::market_data::MarketDataSource;
use arb_scanner::market_data::binance::BinanceSpotSource;
use arb_scanner::market_data::kraken::KrakenSpotSource;
use arb_scanner::market_data::mock::{MockSource, MockSymbolConfig};
use arb_scanner::net;
use arb_scanner::order::binance::BinanceOrderProvider;
use arb_scanner::order::binance_futures::BinanceFuturesOrderProvider;
use arb_scanner::sink::OpportunitySink;
use arb_scanner::sink::log_sink::LogSink;
use arb_scanner::strategy::cross_exchange::CrossExchangeStrategy;
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
