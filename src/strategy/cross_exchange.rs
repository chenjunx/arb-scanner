use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use log::{debug, info};
use rust_decimal::Decimal;

use crate::market_data::link_health::LinkHealthMonitor;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{FeeSchedule, Opportunity, OpportunityKind, Strategy};

/// 跨交易所同交易对价差套利：在多个 venue 上监控同一批 symbol，
/// 若某 venue 的卖一价（扣费后）低于另一 venue 的买一价（扣费后），则存在套利空间。
pub struct CrossExchangeStrategy {
    symbols: Vec<Symbol>,
    fees: HashMap<Venue, FeeSchedule>,
    min_profit_bps: Decimal,
    health: Arc<LinkHealthMonitor>,
    latest: Mutex<HashMap<Symbol, HashMap<Venue, Quote>>>,
    bus: Arc<TopicBus>,
}

impl CrossExchangeStrategy {
    pub fn new(
        symbols: Vec<Symbol>,
        fees: HashMap<Venue, FeeSchedule>,
        min_profit_bps: Decimal,
        health: Arc<LinkHealthMonitor>,
        bus: Arc<TopicBus>,
    ) -> Self {
        Self {
            symbols,
            fees,
            min_profit_bps,
            health,
            latest: Mutex::new(HashMap::new()),
            bus,
        }
    }

    /// `subscriptions()` 就是从 `fees` 派生出来的，所以这里查不到只可能是调用方
    /// 传入的行情不是自己订阅范围内的——修 bug 而不是兜底掩盖它。
    fn fee_for(&self, venue: &Venue) -> FeeSchedule {
        self.fees
            .get(venue)
            .copied()
            .expect("CrossExchangeStrategy received a quote for a venue outside its subscriptions")
    }
}

/// 给定买/卖两侧的价格和各自手续费，返回扣费后的价差(基点)。买价 <= 0 时返回
/// `None`(报价还没来)。供 `on_quote` 使用。
pub fn compute_profit_bps(
    buy_ask: Decimal,
    buy_fee: FeeSchedule,
    sell_bid: Decimal,
    sell_fee: FeeSchedule,
) -> Option<Decimal> {
    if buy_ask <= Decimal::ZERO {
        return None;
    }
    let buy_cost = buy_ask * buy_fee.buy_multiplier();
    let sell_proceeds = sell_bid * sell_fee.sell_multiplier();
    Some((sell_proceeds - buy_cost) / buy_cost * Decimal::from(10_000))
}

fn log_opportunity(opportunity: &Opportunity) {
    let OpportunityKind::CrossExchange {
        symbol,
        buy_venue,
        sell_venue,
    } = &opportunity.kind
    else {
        return;
    };
    info!(
        "[{}] {} buy={} sell={} profit_bps={} detail={}",
        opportunity.strategy, symbol, buy_venue, sell_venue, opportunity.expected_profit_bps, opportunity.detail
    );
}

impl Strategy for CrossExchangeStrategy {
    fn name(&self) -> &str {
        "cross_exchange"
    }

    fn subscriptions(&self) -> Vec<Topic> {
        self.fees
            .keys()
            .flat_map(|venue| {
                self.symbols
                    .iter()
                    .map(move |symbol| Topic::quote(venue.clone(), symbol.clone()))
            })
            .collect()
    }

    fn bus(&self) -> &Arc<TopicBus> {
        &self.bus
    }

    fn on_quote(&self, topic: &Topic, quote: &Quote) {
        let (venue, symbol) = match topic {
            Topic::Quote { venue, symbol } => (venue, symbol),
            _ => return,
        };
        let mut latest = self.latest.lock().unwrap();
        let symbol_quotes = latest.entry(symbol.clone()).or_default();
        symbol_quotes.insert(venue.clone(), *quote);

        let mut found = Vec::new();
        for (buy_venue, buy_quote) in symbol_quotes.iter() {
            if buy_quote.ask <= Decimal::ZERO {
                continue;
            }
            let buy_fee = self.fee_for(buy_venue);

            for (sell_venue, sell_quote) in symbol_quotes.iter() {
                if buy_venue == sell_venue {
                    continue;
                }
                let sell_fee = self.fee_for(sell_venue);

                if !self.health.is_healthy(buy_venue) || !self.health.is_healthy(sell_venue) {
                    continue;
                }

                let Some(profit_bps) = compute_profit_bps(buy_quote.ask, buy_fee, sell_quote.bid, sell_fee) else {
                    continue;
                };

                debug!(
                    "{sym} spread: buy={bv}@{ba} sell={sv}@{sb} profit_bps={p}",
                    sym = symbol,
                    bv = buy_venue,
                    ba = buy_quote.ask,
                    sv = sell_venue,
                    sb = sell_quote.bid,
                    p = profit_bps
                );

                if profit_bps < self.min_profit_bps {
                    continue;
                }

                let buy_cost = buy_quote.ask * buy_fee.buy_multiplier();
                let sell_proceeds = sell_quote.bid * sell_fee.sell_multiplier();
                found.push(Opportunity {
                    strategy: "cross_exchange",
                    kind: OpportunityKind::CrossExchange {
                        symbol: symbol.clone(),
                        buy_venue: buy_venue.clone(),
                        sell_venue: sell_venue.clone(),
                    },
                    expected_profit_bps: profit_bps,
                    detail: format!(
                        "buy {symbol} on {buy_venue} @ {buy_ask} (cost {buy_cost}), sell on {sell_venue} @ {sell_bid} (proceeds {sell_proceeds})",
                        buy_ask = buy_quote.ask,
                        sell_bid = sell_quote.bid,
                    ),
                    ts_ms: quote.ts_ms,
                });
            }
        }
        drop(latest);

        for opportunity in &found {
            log_opportunity(opportunity);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote {
            bid: bid.parse().unwrap(),
            bid_size: Decimal::ONE,
            ask: ask.parse().unwrap(),
            ask_size: Decimal::ONE,
            ts_ms: 1,
        }
    }

    fn fees_for(venues: &[&Venue]) -> HashMap<Venue, FeeSchedule> {
        venues.iter().map(|v| ((*v).clone(), FeeSchedule::new(0))).collect()
    }

    #[test]
    fn subscriptions_only_cover_configured_venues_and_symbols() {
        let watched = Symbol::new("BTC", "USDT");
        let venue_a = Venue::new("a");
        let strategy = CrossExchangeStrategy::new(
            vec![watched.clone()],
            fees_for(&[&venue_a]),
            Decimal::ZERO,
            Arc::new(LinkHealthMonitor::always_healthy()),
            Arc::new(TopicBus::new()),
        );

        let subs = strategy.subscriptions();
        assert_eq!(subs, vec![Topic::quote(venue_a, watched)]);
    }

    #[test]
    fn detects_cross_exchange_opportunity_above_threshold() {
        let symbol = Symbol::new("BTC", "USDT");
        let venue_a = Venue::new("a");
        let venue_b = Venue::new("b");
        let strategy = CrossExchangeStrategy::new(
            vec![symbol.clone()],
            fees_for(&[&venue_a, &venue_b]),
            Decimal::from(1),
            Arc::new(LinkHealthMonitor::always_healthy()),
            Arc::new(TopicBus::new()),
        );

        strategy.on_quote(&Topic::quote(venue_a.clone(), symbol.clone()), &quote("100.0", "100.5"));
        strategy.on_quote(&Topic::quote(venue_b.clone(), symbol.clone()), &quote("102.0", "102.5"));

        let latest = strategy.latest.lock().unwrap();
        let symbol_quotes = &latest[&symbol];
        let profit_bps = compute_profit_bps(
            symbol_quotes[&venue_a].ask,
            strategy.fee_for(&venue_a),
            symbol_quotes[&venue_b].bid,
            strategy.fee_for(&venue_b),
        )
        .unwrap();
        assert!(profit_bps > Decimal::from(1));
    }

    #[test]
    fn no_opportunity_when_spread_below_threshold() {
        let symbol = Symbol::new("BTC", "USDT");
        let venue_a = Venue::new("a");
        let venue_b = Venue::new("b");
        let strategy = CrossExchangeStrategy::new(
            vec![symbol.clone()],
            fees_for(&[&venue_a, &venue_b]),
            Decimal::from(50),
            Arc::new(LinkHealthMonitor::always_healthy()),
            Arc::new(TopicBus::new()),
        );

        strategy.on_quote(&Topic::quote(venue_a.clone(), symbol.clone()), &quote("100.0", "100.1"));
        strategy.on_quote(&Topic::quote(venue_b.clone(), symbol.clone()), &quote("100.05", "100.15"));

        let latest = strategy.latest.lock().unwrap();
        let symbol_quotes = &latest[&symbol];
        let profit_bps = compute_profit_bps(
            symbol_quotes[&venue_a].ask,
            strategy.fee_for(&venue_a),
            symbol_quotes[&venue_b].bid,
            strategy.fee_for(&venue_b),
        )
        .unwrap();
        assert!(profit_bps < Decimal::from(50));
    }
}
