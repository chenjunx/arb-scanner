use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rust_decimal::Decimal;

use arb_scanner::engine::ArbitrageEngine;
use arb_scanner::sink::OpportunitySink;
use arb_scanner::strategy::cross_exchange::CrossExchangeStrategy;
use arb_scanner::strategy::{Opportunity, Strategy};
use arb_scanner::types::{MarketEvent, Quote, Symbol, Venue};

struct CollectingSink {
    opportunities: Arc<Mutex<Vec<Opportunity>>>,
}

impl OpportunitySink for CollectingSink {
    fn handle(&self, opportunity: &Opportunity) {
        self.opportunities
            .lock()
            .unwrap()
            .push(opportunity.clone());
    }
}

fn quote(bid: &str, ask: &str, ts_ms: u64) -> Quote {
    Quote {
        bid: bid.parse().unwrap(),
        bid_size: Decimal::ONE,
        ask: ask.parse().unwrap(),
        ask_size: Decimal::ONE,
        ts_ms,
    }
}

async fn run_engine_with(events: Vec<MarketEvent>, min_profit_bps: Decimal) -> Vec<Opportunity> {
    let symbol = Symbol::new("BTC", "USDT");
    let strategies: Vec<Box<dyn Strategy>> = vec![Box::new(CrossExchangeStrategy::new(
        vec![symbol],
        HashMap::new(),
        min_profit_bps,
    ))];
    let opportunities = Arc::new(Mutex::new(Vec::new()));
    let sinks: Vec<Box<dyn OpportunitySink>> = vec![Box::new(CollectingSink {
        opportunities: opportunities.clone(),
    })];

    let engine = ArbitrageEngine::new(strategies, sinks);
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    for event in events {
        tx.send(event).await.unwrap();
    }
    drop(tx);

    engine.run(rx).await;

    let found = opportunities.lock().unwrap().clone();
    found
}

#[tokio::test]
async fn engine_reports_cross_exchange_opportunity_end_to_end() {
    let symbol = Symbol::new("BTC", "USDT");
    let venue_a = Venue::new("a");
    let venue_b = Venue::new("b");

    let events = vec![
        MarketEvent {
            venue: venue_a.clone(),
            symbol: symbol.clone(),
            quote: quote("100.0", "100.5", 1),
        },
        MarketEvent {
            venue: venue_b.clone(),
            symbol: symbol.clone(),
            quote: quote("105.0", "105.5", 2),
        },
    ];

    let found = run_engine_with(events, Decimal::from(10)).await;

    assert!(!found.is_empty());
    assert!(found.iter().all(|o| o.expected_profit_bps > Decimal::ZERO));
}

#[tokio::test]
async fn engine_reports_nothing_when_no_cross_venue_spread_exists() {
    let symbol = Symbol::new("BTC", "USDT");
    let venue_a = Venue::new("a");

    let events = vec![MarketEvent {
        venue: venue_a,
        symbol,
        quote: quote("100.0", "100.5", 1),
    }];

    let found = run_engine_with(events, Decimal::from(10)).await;

    assert!(found.is_empty());
}
