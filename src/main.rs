use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use log::info;

use arb_scanner::config::AppConfig;
use arb_scanner::engine::ArbitrageEngine;
use arb_scanner::logging;
use arb_scanner::market_data::MarketDataSource;
use arb_scanner::market_data::binance::BinanceSpotSource;
use arb_scanner::market_data::kraken::KrakenSpotSource;
use arb_scanner::market_data::mock::{MockSource, MockSymbolConfig};
use arb_scanner::net;
use arb_scanner::sink::OpportunitySink;
use arb_scanner::sink::log_sink::LogSink;
use arb_scanner::strategy::cross_exchange::CrossExchangeStrategy;
use arb_scanner::strategy::triangular::{LegSide, TriangularLeg, TriangularPath, TriangularStrategy};
use arb_scanner::strategy::{FeeSchedule, Strategy};
use arb_scanner::types::{Symbol, Venue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init_logging();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let config_path = std::env::args().nth(1).unwrap_or_else(|| "config.toml".to_string());
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
