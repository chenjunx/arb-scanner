//! `monitor` 子命令：复用 `scan::find_overlap` 筛出的币安/副交易所交集币种，接入现成的
//! 行情源 + `CrossExchangeStrategy` 管线，持续监控两边现货价差。每个币的
//! 手续费用 [`crate::exchange_info::ExchangeInfoProvider::spot_trading_fee`] 查询两边真实
//! 账户 taker 费率(而不是固定值)，币安这边再乘上 `config.toml` `[[venues]]` 里币安条目的
//! `fee_discount` 折扣(如 BNB 抵扣手续费，默认 1 不打折，见 [`VenueConfig::load_fee_discount`])，
//! 副交易所不打折。扣费后价差只要 >= `--min-profit-bps`(默认 0，即扣费后为正)就打印。
//! 每条链路都订阅一个心跳探针交易对，只要 `--link-health-window-ms`(默认 5000ms)内没再
//! 收到它的推送就判定该链路不健康、跳过比较——防止某一侧 WS 断线/卡住后，一直拿旧报价
//! 和另一侧的新报价比出虚假价差。见 `LinkHealthMonitor`。
//!
//! `CrossExchangeStrategy` 的手续费 map 不区分 symbol，因此给每个币单独构造一个只监控
//! 该币、只装这个币真实手续费的 `CrossExchangeStrategy` 实例，而不是像默认主流程那样
//! 所有 symbol 共享一份手续费配置。不接入 `config.toml` 驱动的默认主循环。
//!
//! 除非传了 `--no-portfolio`，否则默认把仓位/组合盈亏/资金费/定期报告这几个"基础服务"
//! 一起跑起来：额外起一个 `BinanceFuturesSource` 把期货行情喂进共享的 `TopicBus`，
//! 供 `PortfolioManager` 做 mark-to-market(`CrossExchangeStrategy` 不会订阅它，因为它
//! 没被配进任何一个币的手续费表)；连接 Redis
//! 读取 `open`/`close` 写入的仓位并持续追踪；起 `FundingFeeTracker`/`ReportTracker` 定期
//! 结算资金费、打印报告。Redis 连不上时直接报错退出(和 `accounting`/`report` 现有行为
//! 一致)，不想连 Redis 就加 `--no-portfolio` 退回纯价差扫描。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use futures_util::StreamExt;
use futures_util::stream;
use log::info;
use rust_decimal::Decimal;

use crate::accounting::{FundingFeeTracker, RedisFundingCursorStore};
use crate::config::{ScanConfig, VenueConfig};
use crate::engine::ArbitrageEngine;
use crate::exchange_info::ExchangeInfoProvider;
use crate::exchange_info::binance::BinanceExchangeInfoProvider;
use crate::exchange_info::types::TradingFee;
use crate::market_data::MarketDataSource;
use crate::market_data::binance::BinanceSpotSource;
use crate::market_data::binance_futures::BinanceFuturesSource;
use crate::market_data::cache::MarketDataCache;
use crate::market_data::link_health::LinkHealthMonitor;
use crate::net;
use crate::strategy::cross_exchange::CrossExchangeStrategy;
use crate::strategy::{FeeSchedule, Strategy};
use crate::topic::{Topic, TopicBus};
use crate::types::{Symbol, Venue};
use crate::wallet::binance::BinanceWalletProvider;

use super::wiring;

/// `spot_trading_fee` 查询并发上限，避免对候选币逐个查询手续费时触发限流，
/// 和 `scan/mod.rs` 里 `SECONDARY_WALLET_CONCURRENCY` 同样的考虑。
const FEE_QUERY_CONCURRENCY: usize = 4;

#[derive(Args, Debug)]
pub struct MonitorArgs {
    /// 使用币安测试网
    #[arg(long)]
    pub testnet: bool,

    /// 副交易所：kraken / coinex / nonkyc(行情源目前只有 kraken 实现)
    #[arg(long, default_value = "kraken")]
    pub secondary: String,

    /// 扣费后价差达到多少 bps 才打印机会
    #[arg(long, default_value_t = Decimal::ZERO)]
    pub min_profit_bps: Decimal,

    /// 心跳探针多久没收到推送就判定该行情链路不健康(毫秒)
    #[arg(long, default_value_t = 5000)]
    pub link_health_window_ms: u64,

    /// 只跑价差扫描，不连 Redis、不起仓位/资金费/报告这几个基础服务
    #[arg(long)]
    pub no_portfolio: bool,

    /// 轮询资金费流水的间隔(秒)
    #[arg(long, default_value_t = 1800)]
    pub funding_interval_secs: u64,

    /// 首次轮询资金费时向前回溯多久(小时)
    #[arg(long, default_value_t = 168)]
    pub funding_initial_lookback_hours: u64,

    /// 出报告的间隔(秒)
    #[arg(long, default_value_t = 300)]
    pub report_interval_secs: u64,
}

pub async fn run(args: MonitorArgs) -> anyhow::Result<()> {
    let MonitorArgs {
        testnet,
        secondary,
        min_profit_bps,
        link_health_window_ms,
        no_portfolio,
        funding_interval_secs,
        funding_initial_lookback_hours,
        report_interval_secs,
    } = args;

    let proxy = net::proxy_from_env();
    let binance_info = BinanceExchangeInfoProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let secondary_info = wiring::build_secondary_exchange_info(&secondary, proxy.as_deref())?;
    let binance_wallet = BinanceWalletProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let secondary_wallet = wiring::build_secondary_wallet_provider(&secondary, proxy.as_deref())?;

    let blacklist = ScanConfig::load_blacklist("config.toml");
    let binance_fee_discount = VenueConfig::load_fee_discount("config.toml", "binance");
    info!(
        "monitor: looking for symbols overlapping between binance (usdt perpetual) and {secondary} (usdt spot) testnet={testnet} blacklist={blacklist:?} binance_fee_discount={binance_fee_discount}"
    );
    let scan_result = crate::scan::find_overlap(
        &binance_info,
        secondary_info.as_ref(),
        &binance_wallet,
        secondary_wallet.as_ref(),
        &blacklist,
    )
    .await?;
    info!(
        "monitor: blacklisted={} ({})",
        scan_result.blacklisted.len(),
        crate::scan::format_blacklisted_list(&scan_result.blacklisted)
    );
    if scan_result.overlaps.is_empty() {
        println!("no overlapping symbols found, nothing to monitor");
        return Ok(());
    }

    info!("monitor: querying real taker fees for {} candidate symbols", scan_result.overlaps.len());
    let binance_info_ref = &binance_info;
    let secondary_info_ref = secondary_info.as_ref();
    let fee_results: Vec<(String, Symbol, anyhow::Result<(TradingFee, TradingFee)>)> =
        stream::iter(scan_result.overlaps)
            .map(|overlap| async move {
                let binance_symbol = Symbol::new(overlap.coin.clone(), "USDT");
                let result = tokio::try_join!(
                    binance_info_ref.spot_trading_fee(&binance_symbol),
                    secondary_info_ref.spot_trading_fee(&overlap.secondary_spot_symbol)
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
            Ok((binance_fee, secondary_fee)) => {
                let binance_effective_bps = binance_fee.taker_bps * binance_fee_discount;
                let fees: HashMap<Venue, FeeSchedule> = HashMap::from([
                    (Venue::new("binance_spot"), FeeSchedule::new(binance_effective_bps)),
                    (Venue::new(secondary.as_str()), FeeSchedule::new(secondary_fee.taker_bps)),
                ]);
                monitored_summary.push(format!(
                    "{coin:<10}  binance_taker_bps={} x{binance_fee_discount}={binance_effective_bps}  {secondary}_taker_bps={}",
                    binance_fee.taker_bps, secondary_fee.taker_bps
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
    println!(
        "== Monitoring {} Symbols (min_profit_bps={min_profit_bps}, link_health_window_ms={link_health_window_ms}) ==",
        symbols.len()
    );
    println!("{}", monitored_summary.join("\n"));
    if !skipped.is_empty() {
        skipped.sort();
        println!();
        println!("== Skipped ({}) ==", skipped.len());
        println!("{}", skipped.join("\n"));
    }

    let bus = Arc::new(TopicBus::new());

    // 每条参与价差计算的 venue 链路额外订阅一个心跳探针交易对：只要在
    // link_health_window_ms 内持续收到它的报价推送，就认为该链路健康。见
    // `LinkHealthMonitor`。探针币种按 venue 分别指定——副交易所目前只有
    // Kraken，用它的原生报价币种 BTC/USD，binance 沿用 BTC/USDT。
    let binance_venue = Venue::new("binance_spot");
    let secondary_venue = Venue::new(secondary.as_str());
    let binance_probe = Symbol::new("BTC", "USDT");
    let secondary_probe = wiring::secondary_probe_symbol(&secondary);
    let probe_symbols = HashMap::from([
        (binance_venue.clone(), binance_probe.clone()),
        (secondary_venue.clone(), secondary_probe.clone()),
    ]);
    let link_health = Arc::new(LinkHealthMonitor::new(probe_symbols, link_health_window_ms));
    let mut source_handles = Vec::new();
    source_handles.push(link_health.clone().spawn(bus.clone()));

    let strategies: Vec<Box<dyn Strategy>> = coin_fees
        .iter()
        .map(|(_, symbol, fees)| {
            Box::new(CrossExchangeStrategy::new(
                vec![symbol.clone()],
                fees.clone(),
                min_profit_bps,
                link_health.clone(),
                bus.clone(),
            )) as Box<dyn Strategy>
        })
        .collect();
    let engine = ArbitrageEngine::new(strategies);

    // WS 实际订阅的 symbol 列表，在套利币种之外补上各自的心跳探针（若不在
    // 其中），确保 link_health 真的能收到探针币种的行情推送。
    let with_probe = |probe: &Symbol| -> Vec<Symbol> {
        let mut s = symbols.clone();
        if !s.contains(probe) {
            s.push(probe.clone());
        }
        s
    };

    // 用 "binance_spot" 而不是策略层惯用的 "binance"，是为了和
    // `PositionManager`/`PortfolioManager` 里现货仓位统一用的 venue 命名对齐——
    // 否则 `PortfolioManager::valuation_for` 按仓位的 venue 去 TopicBus 查
    // mark price 时，现货这条腿永远查不到 (之前拿 "binance" 存的行情)，导致
    // 现货 market_value/unrealized_pnl 恒为 None。
    let binance_source: Box<dyn MarketDataSource> = Box::new(BinanceSpotSource::new(
        binance_venue,
        with_probe(&binance_probe),
        testnet,
        proxy.clone(),
    ));
    source_handles.push(binance_source.spawn(bus.clone()));
    let secondary_source = wiring::build_secondary_market_data_source(
        &secondary,
        secondary_venue,
        with_probe(&secondary_probe),
        proxy.clone(),
    )?;
    source_handles.push(secondary_source.spawn(bus.clone()));

    if !no_portfolio {
        let redis_url = wiring::redis_url();
        info!("monitor: connecting to redis at {redis_url}");

        let futures_venue = Venue::new("binance_futures");

        // 独立的行情缓存：订阅三条腿(现货/期货/副交易所)的最新价格，喂给
        // `PortfolioManager::quote_cache` 做 mark-to-market 估值，
        // 参见 `MarketDataCache` 文档。
        let quote_topics: Vec<Topic> =
            [Venue::new("binance_spot"), Venue::new(secondary.as_str()), futures_venue.clone()]
                .into_iter()
                .flat_map(|venue| symbols.iter().map(move |symbol| Topic::quote(venue.clone(), symbol.clone())))
                .collect();
        let market_data_cache = Arc::new(MarketDataCache::new());
        source_handles.push(market_data_cache.clone().spawn(bus.clone(), quote_topics));

        let (position_manager, portfolio_manager) =
            wiring::build_portfolio_stack(&redis_url, market_data_cache.snapshot())?;

        let futures_source: Box<dyn MarketDataSource> =
            Box::new(BinanceFuturesSource::new(futures_venue.clone(), symbols.clone(), testnet, proxy.clone()));
        source_handles.push(futures_source.spawn(bus.clone()));

        let providers =
            wiring::binance_futures_funding_providers(futures_venue, testnet, proxy.as_deref())?;
        let cursor_store =
            RedisFundingCursorStore::new(&redis_url).context("failed to connect RedisFundingCursorStore to redis")?;
        let funding_tracker = Arc::new(FundingFeeTracker::new(
            providers,
            position_manager.clone(),
            Arc::new(cursor_store),
            Duration::from_secs(funding_interval_secs),
            Duration::from_secs(funding_initial_lookback_hours * 3600),
        ));
        funding_tracker.spawn();

        wiring::spawn_report_tracker(&redis_url, portfolio_manager, Duration::from_secs(report_interval_secs))?;

        info!(
            "monitor: portfolio tracking enabled funding_interval_secs={funding_interval_secs} funding_initial_lookback_hours={funding_initial_lookback_hours} report_interval_secs={report_interval_secs}"
        );
    }

    info!("monitor: engine starting");
    engine.run(bus).await;

    for handle in source_handles {
        let _ = handle.await;
    }

    Ok(())
}
