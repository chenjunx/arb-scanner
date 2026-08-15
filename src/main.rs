use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use dashmap::DashMap;
use futures_util::StreamExt;
use futures_util::stream;
use log::info;
use rust_decimal::Decimal;

use arb_scanner::accounting::{FundingFeeProvider, FundingFeeTracker, RedisFundingCursorStore};
use arb_scanner::config::{AppConfig, ScanConfig, VenueConfig};
use arb_scanner::engine::ArbitrageEngine;
use arb_scanner::exchange_info::ExchangeInfoProvider;
use arb_scanner::exchange_info::PrecisionCache;
use arb_scanner::exchange_info::binance::BinanceExchangeInfoProvider;
use arb_scanner::exchange_info::kraken::KrakenExchangeInfoProvider;
use arb_scanner::exchange_info::types::TradingFee;
use arb_scanner::execution;
use arb_scanner::logging;
use arb_scanner::market_data::MarketDataSource;
use arb_scanner::market_data::binance::BinanceSpotSource;
use arb_scanner::market_data::binance_futures::BinanceFuturesSource;
use arb_scanner::market_data::kraken::KrakenSpotSource;
use arb_scanner::market_data::mock::{MockSource, MockSymbolConfig};
use arb_scanner::net;
use arb_scanner::order::OrderProvider;
use arb_scanner::order::binance::{BinanceOrderProvider, BinanceUserDataStream};
use arb_scanner::order::binance_futures::{BinanceFuturesOrderProvider, BinanceFuturesUserDataStream};
use arb_scanner::order::kraken::KrakenOrderProvider;
use arb_scanner::order_manager::{
    ExchangeAdapter, ExecutionEngine, OrderManager, OrderStreamSource, RedisOrderIdAllocator, RedisOrderStore,
    RiskEngine, RiskLimits,
};
use arb_scanner::portfolio::{PortfolioManager, RedisPnlStore};
use arb_scanner::position::{PositionManager, RedisPositionStore};
use arb_scanner::report::channels::LogChannel;
use arb_scanner::report::{OrderSection, PortfolioSection, PositionSection, ReportChannel, ReportTracker};
use arb_scanner::scan;
use arb_scanner::sink::OpportunitySink;
use arb_scanner::sink::log_sink::LogSink;
use arb_scanner::strategy::cross_exchange::CrossExchangeStrategy;
use arb_scanner::strategy::triangular::{LegSide, TriangularLeg, TriangularPath, TriangularStrategy};
use arb_scanner::strategy::{FeeSchedule, Strategy};
use arb_scanner::types::{Quote, Symbol, Venue};
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
    if args.get(1).map(String::as_str) == Some("accounting") {
        return run_accounting_command(&args[2..]).await;
    }
    if args.get(1).map(String::as_str) == Some("report") {
        return run_report_command(&args[2..]).await;
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

/// 连 Redis 建出 `PositionManager`/`PortfolioManager` 这套仓位/组合盈亏技术栈，
/// 供 `open --live`/`monitor`/`accounting`/`report` 共用，避免各自重复一遍
/// "连 Redis -> RedisPositionStore/RedisPnlStore -> PositionManager/PortfolioManager"
/// 的引导代码。`quote_cache` 由调用方决定：不需要浮动盈亏(纯记账场景)传空
/// `Arc::new(DashMap::new())`，需要 mark-to-market 就传接了实时行情的
/// `ArbitrageEngine::shared_cache()`。手续费配置固定用空 `HashMap`(按
/// `FeeConfig::default()` 估算)，目前没有命令需要自定义。
fn build_portfolio_stack(
    redis_url: &str,
    quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
) -> anyhow::Result<(Arc<PositionManager>, Arc<PortfolioManager>)> {
    let position_store =
        RedisPositionStore::new(redis_url).context("failed to connect RedisPositionStore to redis")?;
    let pnl_store = RedisPnlStore::new(redis_url).context("failed to connect RedisPnlStore to redis")?;

    let position_manager = Arc::new(PositionManager::new(Arc::new(position_store)));
    let portfolio_manager = Arc::new(PortfolioManager::new(
        position_manager.clone(),
        Arc::new(pnl_store),
        quote_cache,
        HashMap::new(),
    ));
    Ok((position_manager, portfolio_manager))
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
    let mut transfer_to_kraken = false;
    let mut fill_timeout_secs: u64 = 60;

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
            "--transfer-to-kraken" => {
                transfer_to_kraken = true;
                i += 1;
            }
            "--fill-timeout-secs" => {
                let v = args.get(i + 1).context("--fill-timeout-secs requires a value")?;
                fill_timeout_secs = v.parse().context("--fill-timeout-secs must be a valid non-negative integer")?;
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
    let spot_venue = Venue::new("binance_spot");
    let futures_venue = Venue::new("binance_futures");

    let proxy = net::proxy_from_env();
    let spot: Arc<dyn OrderProvider> =
        Arc::new(BinanceOrderProvider::from_env(spot_venue.clone(), testnet, proxy.as_deref())?);
    let futures: Arc<dyn OrderProvider> =
        Arc::new(BinanceFuturesOrderProvider::from_env(futures_venue.clone(), testnet, proxy.as_deref())?);
    let binance_wallet = BinanceWalletProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let kraken_wallet = KrakenWalletProvider::from_env(Venue::new("kraken"), proxy.as_deref())?;

    // 启动时一次性加载合约下单精度缓存，凭证/网络问题在下单前就暴露（fail-fast），
    // 而不是现货腿已经成交了才发现；即使 dry_run 分支用不到它也没关系——一次性
    // 启动成本，不是每次下单都要付的代价。
    let exchange_info = BinanceExchangeInfoProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let futures_precision = PrecisionCache::load_perpetual(&exchange_info)
        .await
        .context("failed to load futures market precision cache")?;

    info!(
        "open: symbol={symbol} amount={quote_amount} transfer_asset={transfer_asset} testnet={testnet} dry_run={dry_run} transfer_to_kraken={transfer_to_kraken}"
    );

    if dry_run {
        info!("open: dry_run=true (default), pass --live to actually place orders/withdraw");
        let report = execution::open_hedged_position_dry_run(
            spot.as_ref(),
            execution::OpenPositionParams {
                symbol,
                quote_amount,
                transfer_asset,
                client_order_id_prefix,
                dry_run,
                transfer_to_kraken,
                fill_timeout: Duration::from_secs(fill_timeout_secs),
            },
        )
        .await?;
        println!("{report:#?}");
        return Ok(());
    }

    // --live：两条腿都要走完整的 OrderManager 流水线(风控 -> 执行引擎 -> 交易所
    // 私有 WS 成交确认)，成交结果才会真正落进 PositionManager/PortfolioManager。
    // Redis 连不上直接快速失败，不能等下单后才发现存不进去。
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    info!("open --live: connecting to redis at {redis_url}");
    let order_store = RedisOrderStore::new(&redis_url).context("failed to connect RedisOrderStore to redis")?;
    let order_id_allocator =
        RedisOrderIdAllocator::new(&redis_url).context("failed to connect RedisOrderIdAllocator to redis")?;
    let (position_manager, portfolio_manager) = build_portfolio_stack(&redis_url, Arc::new(DashMap::new()))?;

    let mut risk_limits = HashMap::new();
    risk_limits.insert(
        (spot_venue.clone(), symbol.clone()),
        RiskLimits {
            max_order_amount: quote_amount,
            max_position: Decimal::MAX,
            max_orders_per_window: 3,
        },
    );
    risk_limits.insert(
        (futures_venue.clone(), symbol.clone()),
        RiskLimits {
            max_order_amount: Decimal::MAX,
            max_position: Decimal::MAX,
            max_orders_per_window: 3,
        },
    );
    let risk_engine = Arc::new(RiskEngine::new(risk_limits, position_manager));

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(100);
    let mut adapters = HashMap::new();
    adapters.insert(spot_venue.clone(), Arc::new(ExchangeAdapter::new(spot_venue.clone(), spot.clone())));
    adapters.insert(futures_venue.clone(), Arc::new(ExchangeAdapter::new(futures_venue.clone(), futures.clone())));
    let execution_engine = Arc::new(ExecutionEngine::new(adapters, event_tx.clone()));
    let order_manager = Arc::new(OrderManager::new(
        risk_engine,
        execution_engine,
        portfolio_manager,
        event_tx,
        Arc::new(order_store),
        Arc::new(order_id_allocator),
    ));

    let (update_tx, mut update_rx) = tokio::sync::mpsc::channel(100);
    let spot_stream = BinanceUserDataStream::from_env(spot_venue.clone(), testnet, proxy.as_deref())
        .context("failed to start binance spot user data stream")?;
    let futures_stream = BinanceFuturesUserDataStream::from_env(futures_venue.clone(), testnet, proxy.as_deref())
        .context("failed to start binance futures user data stream")?;
    let spot_handle = Box::new(spot_stream).spawn(update_tx.clone());
    let futures_handle = Box::new(futures_stream).spawn(update_tx.clone());
    drop(update_tx);

    // 等两条私有流真正建连+鉴权/订阅完成，再开始下单——否则市价单可能在
    // WS 就绪前就已成交，导致 executionReport 被永久错过（WS API 不重放）。
    const STREAM_READY_TIMEOUT: Duration = Duration::from_secs(20);
    tokio::time::timeout(STREAM_READY_TIMEOUT, spot_handle.ready)
        .await
        .context("等待现货私有 WS 就绪超时")?
        .context("现货私有 WS 未能就绪就退出了(检查 API Key/网络)")?;
    tokio::time::timeout(STREAM_READY_TIMEOUT, futures_handle.ready)
        .await
        .context("等待合约私有 WS 就绪超时")?
        .context("合约私有 WS 未能就绪就退出了(检查 API Key/网络)")?;

    let forward_handle = {
        let order_manager = order_manager.clone();
        tokio::spawn(async move {
            while let Some(update) = update_rx.recv().await {
                order_manager.handle_exchange_update(update).await;
            }
        })
    };

    let live_result = execution::open_hedged_position_live(
        spot.as_ref(),
        futures.as_ref(),
        &binance_wallet,
        &kraken_wallet,
        order_manager.as_ref(),
        &mut event_rx,
        &futures_precision,
        execution::OpenPositionParams {
            symbol,
            quote_amount,
            transfer_asset,
            client_order_id_prefix,
            dry_run,
            transfer_to_kraken,
            fill_timeout: Duration::from_secs(fill_timeout_secs),
        },
    )
    .await;

    spot_handle.join.abort();
    futures_handle.join.abort();
    forward_handle.abort();

    let report = live_result?;
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

/// `monitor` 子命令：复用 `scan::find_overlap` 筛出的币安/Kraken 交集币种，接入现成的
/// 行情源 + `CrossExchangeStrategy` + `LogSink` 管线，持续监控两边现货价差。每个币的
/// 手续费用 [`ExchangeInfoProvider::spot_trading_fee`] 查询两边真实账户 taker 费率(而
/// 不是固定值)，币安这边再乘上 `config.toml` `[[venues]]` 里币安条目的 `fee_discount`
/// 折扣(如 BNB 抵扣手续费，默认 1 不打折，见 [`VenueConfig::load_fee_discount`])，
/// Kraken 不打折。扣费后价差只要 >= `--min-profit-bps`(默认 0，即扣费后为正)就打印。
///
/// `CrossExchangeStrategy` 的手续费 map 不区分 symbol，因此给每个币单独构造一个只监控
/// 该币、只装这个币真实手续费的 `CrossExchangeStrategy` 实例，而不是像默认主流程那样
/// 所有 symbol 共享一份手续费配置。不接入 `config.toml` 驱动的默认主循环。
///
/// 除非传了 `--no-portfolio`，否则默认把仓位/组合盈亏/资金费/定期报告这几个"基础服务"
/// 一起跑起来：额外起一个 `BinanceFuturesSource` 把期货行情喂进
/// `ArbitrageEngine::shared_cache()`，供 `PortfolioManager` 做 mark-to-market；连接 Redis
/// 读取 `open`/`close` 写入的仓位并持续追踪；起 `FundingFeeTracker`/`ReportTracker` 定期
/// 结算资金费、打印报告。Redis 连不上时直接报错退出(和 `accounting`/`report` 现有行为
/// 一致)，不想连 Redis 就加 `--no-portfolio` 退回纯价差扫描。
async fn run_monitor_command(args: &[String]) -> anyhow::Result<()> {
    let mut testnet = false;
    let mut min_profit_bps = Decimal::ZERO;
    let mut no_portfolio = false;
    let mut funding_interval_secs: u64 = 1800;
    let mut funding_initial_lookback_hours: u64 = 168;
    let mut report_interval_secs: u64 = 300;

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
            "--no-portfolio" => {
                no_portfolio = true;
                i += 1;
            }
            "--funding-interval-secs" => {
                let v = args.get(i + 1).context("--funding-interval-secs requires a value")?;
                funding_interval_secs =
                    v.parse().context("--funding-interval-secs must be a valid non-negative integer")?;
                i += 2;
            }
            "--funding-initial-lookback-hours" => {
                let v = args.get(i + 1).context("--funding-initial-lookback-hours requires a value")?;
                funding_initial_lookback_hours = v
                    .parse()
                    .context("--funding-initial-lookback-hours must be a valid non-negative integer")?;
                i += 2;
            }
            "--report-interval-secs" => {
                let v = args.get(i + 1).context("--report-interval-secs requires a value")?;
                report_interval_secs =
                    v.parse().context("--report-interval-secs must be a valid non-negative integer")?;
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
    println!("== Monitoring {} Symbols (min_profit_bps={min_profit_bps}) ==", symbols.len());
    println!("{}", monitored_summary.join("\n"));
    if !skipped.is_empty() {
        skipped.sort();
        println!();
        println!("== Skipped ({}) ==", skipped.len());
        println!("{}", skipped.join("\n"));
    }

    let strategies: Vec<Box<dyn Strategy>> = coin_fees
        .iter()
        .map(|(_, symbol, fees)| {
            Box::new(CrossExchangeStrategy::new(vec![symbol.clone()], fees.clone(), min_profit_bps))
                as Box<dyn Strategy>
        })
        .collect();
    let sinks: Vec<Box<dyn OpportunitySink>> = vec![Box::new(LogSink) as Box<dyn OpportunitySink>];
    let engine = ArbitrageEngine::new(strategies, sinks);

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

    if !no_portfolio {
        let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
        info!("monitor: connecting to redis at {redis_url}");
        let (position_manager, portfolio_manager) = build_portfolio_stack(&redis_url, engine.shared_cache())?;

        let futures_venue = Venue::new("binance_futures");
        let futures_source: Box<dyn MarketDataSource> =
            Box::new(BinanceFuturesSource::new(futures_venue.clone(), symbols.clone(), testnet, proxy.clone()));
        source_handles.push(futures_source.spawn(tx.clone()));

        let futures_provider: Arc<dyn FundingFeeProvider> =
            Arc::new(BinanceFuturesOrderProvider::from_env(futures_venue.clone(), testnet, proxy.as_deref())?);
        let providers: HashMap<Venue, Arc<dyn FundingFeeProvider>> =
            HashMap::from([(futures_venue, futures_provider)]);
        let cursor_store =
            RedisFundingCursorStore::new(&redis_url).context("failed to connect RedisFundingCursorStore to redis")?;
        let funding_tracker = Arc::new(FundingFeeTracker::new(
            providers,
            position_manager.clone(),
            portfolio_manager.clone(),
            Arc::new(cursor_store),
            Duration::from_secs(funding_interval_secs),
            Duration::from_secs(funding_initial_lookback_hours * 3600),
        ));
        funding_tracker.spawn();

        let order_store = RedisOrderStore::new(&redis_url).context("failed to connect RedisOrderStore to redis")?;
        let order_store: Arc<dyn arb_scanner::order_manager::OrderStore> = Arc::new(order_store);
        let sections: Vec<Arc<dyn arb_scanner::report::ReportSection>> = vec![
            Arc::new(PortfolioSection::new(position_manager.clone(), portfolio_manager)),
            Arc::new(PositionSection::new(position_manager)),
            Arc::new(OrderSection::new(order_store)),
        ];
        let channels: Vec<Arc<dyn ReportChannel>> = vec![Arc::new(LogChannel)];
        let report_tracker =
            Arc::new(ReportTracker::new(sections, channels, Duration::from_secs(report_interval_secs)));
        report_tracker.spawn();

        info!(
            "monitor: portfolio tracking enabled funding_interval_secs={funding_interval_secs} funding_initial_lookback_hours={funding_initial_lookback_hours} report_interval_secs={report_interval_secs}"
        );
    }

    drop(tx);

    info!("monitor: engine starting");
    engine.run(rx).await;

    for handle in source_handles {
        let _ = handle.await;
    }

    Ok(())
}

/// 独立的常驻进程：定期轮询交易所资金费流水，累加进 `PortfolioManager` 的
/// `PnlStore`(`funding_pnl` 字段)。跟踪对象是 `PositionManager`(Redis 支撑)里
/// 每次轮询时读到的当前非零仓位，而不是启动时固定的一份列表，所以
/// `open`/`close` 开平的期货仓位不需要重启这个进程就能被自动跟踪/停止跟踪。
/// 如果 `monitor` 已经在跑且没加 `--no-portfolio`，通常不需要单独起本命令；只需要
/// 资金费追踪、不想启动价差扫描和行情连接时单独使用。
async fn run_accounting_command(args: &[String]) -> anyhow::Result<()> {
    let mut testnet = false;
    let mut interval_secs: u64 = 1800;
    let mut initial_lookback_hours: u64 = 168;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--testnet" => {
                testnet = true;
                i += 1;
            }
            "--interval-secs" => {
                let v = args.get(i + 1).context("--interval-secs requires a value")?;
                interval_secs = v.parse().context("--interval-secs must be a valid non-negative integer")?;
                i += 2;
            }
            "--initial-lookback-hours" => {
                let v = args.get(i + 1).context("--initial-lookback-hours requires a value")?;
                initial_lookback_hours =
                    v.parse().context("--initial-lookback-hours must be a valid non-negative integer")?;
                i += 2;
            }
            other => anyhow::bail!("unknown argument '{other}' for 'accounting' subcommand"),
        }
    }

    let proxy = net::proxy_from_env();
    let futures_venue = Venue::new("binance_futures");
    let futures_provider: Arc<dyn FundingFeeProvider> =
        Arc::new(BinanceFuturesOrderProvider::from_env(futures_venue.clone(), testnet, proxy.as_deref())?);
    let providers: HashMap<Venue, Arc<dyn FundingFeeProvider>> = HashMap::from([(futures_venue, futures_provider)]);

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    info!("accounting: connecting to redis at {redis_url}");
    let (position_manager, portfolio_manager) = build_portfolio_stack(&redis_url, Arc::new(DashMap::new()))?;
    let cursor_store =
        RedisFundingCursorStore::new(&redis_url).context("failed to connect RedisFundingCursorStore to redis")?;

    let tracker = Arc::new(FundingFeeTracker::new(
        providers,
        position_manager,
        portfolio_manager,
        Arc::new(cursor_store),
        Duration::from_secs(interval_secs),
        Duration::from_secs(initial_lookback_hours * 3600),
    ));
    tracker.spawn();

    info!(
        "accounting: tracking funding fees testnet={testnet} interval_secs={interval_secs} initial_lookback_hours={initial_lookback_hours}, press ctrl-c to stop"
    );
    tokio::signal::ctrl_c().await.context("failed to listen for ctrl-c")?;
    info!("accounting: received ctrl-c, shutting down");
    Ok(())
}

/// 独立的常驻进程：定期把投资组合盈亏/仓位明细/订单概览汇总成一份报告并
/// 分发给各个已注册的 `ReportChannel`(目前只有 `LogChannel`)。只连接
/// Redis 读取数据，不接入实时行情，所以报告里的 `market_value`/
/// `unrealized_pnl` 会显示为 "N/A"(和 `accounting` 命令同样的既有限制，见
/// `arb_scanner::report::sections::PortfolioSection` 的说明；如果通过 `monitor`
/// (未加 `--no-portfolio`)驱动，接了实时行情，会有真实数字)。如果 `monitor` 已经
/// 在跑且没加 `--no-portfolio`，通常不需要单独起本命令；只需要报告、不想启动价差
/// 扫描和行情连接时单独使用。
async fn run_report_command(args: &[String]) -> anyhow::Result<()> {
    let mut interval_secs: u64 = 300;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--interval-secs" => {
                let v = args.get(i + 1).context("--interval-secs requires a value")?;
                interval_secs = v.parse().context("--interval-secs must be a valid non-negative integer")?;
                i += 2;
            }
            other => anyhow::bail!("unknown argument '{other}' for 'report' subcommand"),
        }
    }

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    info!("report: connecting to redis at {redis_url}");
    let (position_manager, portfolio_manager) = build_portfolio_stack(&redis_url, Arc::new(DashMap::new()))?;
    let order_store = RedisOrderStore::new(&redis_url).context("failed to connect RedisOrderStore to redis")?;
    let order_store: Arc<dyn arb_scanner::order_manager::OrderStore> = Arc::new(order_store);

    let sections: Vec<Arc<dyn arb_scanner::report::ReportSection>> = vec![
        Arc::new(PortfolioSection::new(position_manager.clone(), portfolio_manager.clone())),
        Arc::new(PositionSection::new(position_manager)),
        Arc::new(OrderSection::new(order_store)),
    ];
    let channels: Vec<Arc<dyn ReportChannel>> = vec![Arc::new(LogChannel)];

    let tracker = Arc::new(ReportTracker::new(sections, channels, Duration::from_secs(interval_secs)));
    tracker.spawn();

    info!("report: reporting every interval_secs={interval_secs}, press ctrl-c to stop");
    tokio::signal::ctrl_c().await.context("failed to listen for ctrl-c")?;
    info!("report: received ctrl-c, shutting down");
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
