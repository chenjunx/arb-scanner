//! 不带子命令时的默认行为：读 `config.toml`，按里面的 `[[venues]]`/`[[symbols]]`
//! 起行情源，装上 `CrossExchangeStrategy` + `TriangularStrategy` 跑常驻套利引擎。
//!
//! `build_cross_execution_config` 也留在这里而不是 `wiring`：它是唯一由
//! `config.toml` 的 `[cross_exchange_execution]` 驱动的装配路径，只有本模块会调。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use log::info;

use crate::config::{AppConfig, CrossExchangeExecutionConfig};
use crate::engine::ArbitrageEngine;
use crate::exchange_info::PrecisionCache;
use crate::exchange_info::binance::BinanceExchangeInfoProvider;
use crate::exchange_info::kraken::KrakenExchangeInfoProvider;
use crate::exchange_info::types::PrecisionKind;
use crate::market_data::MarketDataSource;
use crate::market_data::binance::BinanceSpotSource;
use crate::market_data::coinex::CoinexSpotSource;
use crate::market_data::gate::GateSpotSource;
use crate::market_data::kraken::KrakenSpotSource;
use crate::market_data::link_health::LinkHealthMonitor;
use crate::market_data::mock::{MockSource, MockSymbolConfig};
use crate::net;
use crate::order::OrderProvider;
use crate::order::binance::{BinanceOrderProvider, BinanceUserDataStream};
use crate::order::kraken::{KrakenOrderProvider, KrakenPrivateOrderStream};
use crate::order_manager::OrderStreamSource;
use crate::order_manager::risk_service::{AssetImbalanceLimit, RiskLimits};
use crate::strategy::cross_exchange::{CrossExchangeStrategy, CrossExecutionConfig};
use crate::strategy::triangular::{LegSide, TriangularLeg, TriangularPath, TriangularStrategy};
use crate::strategy::{FeeSchedule, Strategy};
use crate::topic::TopicBus;
use crate::types::{Symbol, Venue};

use super::wiring::{self, ManualLeg};

pub async fn run(config_path: &str) -> anyhow::Result<()> {
    let config =
        AppConfig::load(config_path).with_context(|| format!("failed to load config from {config_path}"))?;

    let fees: HashMap<Venue, FeeSchedule> = config
        .venues
        .iter()
        .map(|v| {
            let schedule = FeeSchedule::new(v.taker_fee_bps).with_maker_bps(v.maker_fee_bps.unwrap_or(v.taker_fee_bps));
            (Venue::new(v.name.clone()), schedule)
        })
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

    let bus = Arc::new(TopicBus::new());

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
            "coinex_spot" => {
                info!("starting coinex spot market data source for venue={venue}");
                Box::new(CoinexSpotSource::new(venue.clone(), symbols.clone(), proxy.clone()))
            }
            "gate_spot" => {
                info!("starting gate spot market data source for venue={venue}");
                Box::new(GateSpotSource::new(venue.clone(), symbols.clone(), proxy.clone()))
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
        source_handles.push(source.spawn(bus.clone()));
    }

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

    // cross_exchange_execution 只有 enabled+live 都为真才会真正接 Redis/私有 WS
    // 下单;enabled=true 但 live=false 时保持现状的"只记录机会、不下单"行为——
    // OrderManager 流水线里成交只能靠交易所私有 WS 推送确认(见
    // `ExecutionService::handle_trade`,同步 REST 结果不会直接产生
    // `OrderEvent::Filled`),没有真实下单就没有真实 WS 推送,没法在这条流水线里
    // 伪造出一条"完整链路成交"的 dry run,所以 live=false 时干脆不接
    // `with_execution`,而不是接上一个永远等不到终态的假流水线。
    let cross_execution: Option<Arc<CrossExecutionConfig>> = match &config.cross_exchange_execution {
        Some(cfg) if cfg.enabled && cfg.live => {
            let testnet = config
                .venues
                .iter()
                .find(|v| v.name == cfg.binance_venue)
                .map(|v| v.testnet)
                .unwrap_or(false);
            info!(
                "cross_exchange_execution: live=true, wiring kraken_venue={} binance_venue={}",
                cfg.kraken_venue, cfg.binance_venue
            );
            Some(Arc::new(
                build_cross_execution_config(cfg, &symbols, testnet, proxy.clone(), bus.clone()).await?,
            ))
        }
        Some(cfg) if cfg.enabled => {
            info!(
                "cross_exchange_execution: enabled but live=false, staying in observe-only mode (opportunities are logged, no orders placed); set live=true to trade"
            );
            None
        }
        _ => None,
    };

    let mut cross_exchange_strategy = CrossExchangeStrategy::new(
        symbols,
        fees.clone(),
        config.min_profit_bps,
        Arc::new(LinkHealthMonitor::always_healthy()),
        bus.clone(),
    );
    if let Some(execution) = cross_execution {
        cross_exchange_strategy = cross_exchange_strategy.with_execution(execution);
    }

    let strategies: Vec<Box<dyn Strategy>> = vec![
        Box::new(cross_exchange_strategy),
        Box::new(TriangularStrategy::new(
            triangular_paths,
            fees,
            config.min_profit_bps,
            bus.clone(),
        )),
    ];

    info!("arb-scanner engine starting");
    let engine = ArbitrageEngine::new(strategies);
    engine.run(bus).await;

    for handle in source_handles {
        let _ = handle.await;
    }

    Ok(())
}

/// 为 `CrossExchangeStrategy` 的自动下单搭建执行依赖：加载两边现货精度缓存、
/// 预算每个 symbol 的下单量(两边 min_qty 中较大者，再各自 floor 到合法步进)，
/// 并通过 [`wiring::build_manual_pipeline`] 建好 RiskService(带资产失衡闸)/
/// ExecutionService/OrderManager + 两条私有 WS 流。只在 `cross_exchange_execution.live
/// = true` 时被调用——`live=false` 由调用方直接跳过、不建这整套流水线。
async fn build_cross_execution_config(
    cfg: &CrossExchangeExecutionConfig,
    symbols: &[Symbol],
    testnet: bool,
    proxy: Option<String>,
    bus: Arc<TopicBus>,
) -> anyhow::Result<CrossExecutionConfig> {
    let kraken_venue = Venue::new(cfg.kraken_venue.clone());
    let binance_venue = Venue::new(cfg.binance_venue.clone());

    let kraken_provider_concrete = KrakenOrderProvider::from_env(kraken_venue.clone(), proxy.as_deref())?;
    let kraken_shared_ws = kraken_provider_concrete.shared_ws();
    let kraken_provider: Arc<dyn OrderProvider> = Arc::new(kraken_provider_concrete);
    let binance_provider: Arc<dyn OrderProvider> =
        Arc::new(BinanceOrderProvider::from_env(binance_venue.clone(), testnet, proxy.as_deref())?);

    let kraken_info = KrakenExchangeInfoProvider::from_env(Venue::new("kraken"), proxy.as_deref())?;
    let binance_info = BinanceExchangeInfoProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let kraken_precision = Arc::new(
        PrecisionCache::load_spot(&kraken_info)
            .await
            .context("failed to load kraken spot market precision cache")?,
    );
    let binance_precision = Arc::new(
        PrecisionCache::load_spot(&binance_info)
            .await
            .context("failed to load binance spot market precision cache")?,
    );

    let mut order_qty_by_symbol = HashMap::new();
    for symbol in symbols {
        let kraken_min = match kraken_precision.min_qty(symbol, PrecisionKind::Limit) {
            Ok(q) => q,
            Err(err) => {
                info!("cross_exchange_execution: symbol={symbol} 在 kraken 没有精度信息，跳过该 symbol 的自动下单: {err:#}");
                continue;
            }
        };
        let binance_min = match binance_precision.min_qty(symbol, PrecisionKind::Market) {
            Ok(q) => q,
            Err(err) => {
                info!("cross_exchange_execution: symbol={symbol} 在 binance 没有精度信息，跳过该 symbol 的自动下单: {err:#}");
                continue;
            }
        };
        let raw_qty = kraken_min.max(binance_min);
        let kraken_qty = kraken_precision.round_qty(symbol, PrecisionKind::Limit, raw_qty)?;
        let binance_qty = binance_precision.round_qty(symbol, PrecisionKind::Market, raw_qty)?;
        let qty = kraken_qty.min(binance_qty);
        info!(
            "cross_exchange_execution: symbol={symbol} 预加载下单量={qty} (kraken_min={kraken_min} binance_min={binance_min})"
        );
        order_qty_by_symbol.insert(symbol.clone(), qty);
    }

    let redis_url = wiring::redis_url();
    info!("cross_exchange_execution: connecting to redis at {redis_url}");

    let kraken_stream =
        Box::new(KrakenPrivateOrderStream::from_shared_ws(kraken_shared_ws)) as Box<dyn OrderStreamSource>;
    let binance_stream = Box::new(BinanceUserDataStream::from_env(
        binance_venue.clone(),
        testnet,
        proxy.as_deref(),
        symbols.to_vec(),
    )?) as Box<dyn OrderStreamSource>;

    let mut asset_imbalance_limits: HashMap<String, AssetImbalanceLimit> = cfg
        .asset_imbalance_limits
        .iter()
        .map(|(asset, max_diff_ratio)| {
            (
                asset.clone(),
                AssetImbalanceLimit {
                    venue_a: kraken_venue.clone(),
                    venue_b: binance_venue.clone(),
                    max_diff_ratio: *max_diff_ratio,
                },
            )
        })
        .collect();
    // 没在 asset_imbalance_limits 里单独配置的资产，退回用全局默认比例（如果配了的话）。
    if let Some(default_ratio) = cfg.default_asset_imbalance_ratio {
        for symbol in symbols {
            asset_imbalance_limits.entry(symbol.base.to_string()).or_insert_with(|| AssetImbalanceLimit {
                venue_a: kraken_venue.clone(),
                venue_b: binance_venue.clone(),
                max_diff_ratio: default_ratio,
            });
        }
    }

    let pipeline = wiring::build_manual_pipeline(
        &redis_url,
        bus,
        symbols,
        vec![
            ManualLeg {
                venue: kraken_venue.clone(),
                provider: kraken_provider,
                stream: kraken_stream,
                limits: RiskLimits::default(),
            },
            ManualLeg {
                venue: binance_venue.clone(),
                provider: binance_provider,
                stream: binance_stream,
                limits: RiskLimits::default(),
            },
        ],
        asset_imbalance_limits,
    )
    .await
    .context("failed to build cross_exchange_execution OrderManager pipeline")?;

    Ok(CrossExecutionConfig {
        secondary_venue: kraken_venue.clone(),
        secondary_trade_venue: kraken_venue,
        binance_venue: binance_venue.clone(),
        binance_trade_venue: binance_venue,
        secondary_precision: kraken_precision,
        binance_precision,
        order_manager: pipeline.order_manager,
        order_qty_by_symbol,
        ioc_wait_timeout: Duration::from_millis(cfg.ioc_wait_timeout_ms),
        hedge_wait_timeout: Duration::from_millis(cfg.ioc_wait_timeout_ms),
    })
}
