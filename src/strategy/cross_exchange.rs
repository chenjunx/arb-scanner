use std::collections::HashMap;

use log::debug;
use rust_decimal::Decimal;

use crate::engine::MarketView;
use crate::types::{MarketEvent, Symbol, Venue};

use super::{FeeSchedule, Opportunity, OpportunityKind, Strategy};

/// 跨交易所同交易对价差套利：在多个 venue 上监控同一批 symbol，
/// 若某 venue 的卖一价（扣费后）低于另一 venue 的买一价（扣费后），则存在套利空间。
pub struct CrossExchangeStrategy {
    symbols: Vec<Symbol>,
    fees: HashMap<Venue, FeeSchedule>,
    min_profit_bps: Decimal,
}

impl CrossExchangeStrategy {
    pub fn new(
        symbols: Vec<Symbol>,
        fees: HashMap<Venue, FeeSchedule>,
        min_profit_bps: Decimal,
    ) -> Self {
        Self {
            symbols,
            fees,
            min_profit_bps,
        }
    }

    fn fee_for(&self, venue: &Venue) -> FeeSchedule {
        self.fees
            .get(venue)
            .copied()
            .unwrap_or(FeeSchedule::new(0))
    }
}

impl Strategy for CrossExchangeStrategy {
    fn name(&self) -> &str {
        "cross_exchange"
    }

    fn on_update(&self, view: &MarketView, changed: &MarketEvent) -> Vec<Opportunity> {
        if !self.symbols.contains(&changed.symbol) {
            return Vec::new();
        }

        let quotes = view.quotes_for_symbol(&changed.symbol);
        let mut opportunities = Vec::new();

        for (buy_venue, buy_quote) in &quotes {
            if buy_quote.ask <= Decimal::ZERO {
                continue;
            }
            let buy_cost = buy_quote.ask * self.fee_for(buy_venue).buy_multiplier();

            for (sell_venue, sell_quote) in &quotes {
                if buy_venue == sell_venue {
                    continue;
                }
                let sell_proceeds = sell_quote.bid * self.fee_for(sell_venue).sell_multiplier();

                let profit_bps = (sell_proceeds - buy_cost) / buy_cost * Decimal::from(10_000);

                debug!(
                    "{sym} spread: buy={bv}@{ba} sell={sv}@{sb} profit_bps={p}",
                    sym = changed.symbol,
                    bv = buy_venue,
                    ba = buy_quote.ask,
                    sv = sell_venue,
                    sb = sell_quote.bid,
                    p = profit_bps
                );

                if profit_bps < self.min_profit_bps {
                    continue;
                }

                opportunities.push(Opportunity {
                    strategy: "cross_exchange",
                    kind: OpportunityKind::CrossExchange {
                        symbol: changed.symbol.clone(),
                        buy_venue: buy_venue.clone(),
                        sell_venue: sell_venue.clone(),
                    },
                    expected_profit_bps: profit_bps,
                    detail: format!(
                        "buy {symbol} on {buy_venue} @ {buy_ask} (cost {buy_cost}), sell on {sell_venue} @ {sell_bid} (proceeds {sell_proceeds})",
                        symbol = changed.symbol,
                        buy_ask = buy_quote.ask,
                        sell_bid = sell_quote.bid,
                    ),
                    ts_ms: changed.quote.ts_ms,
                });
            }
        }

        opportunities
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Quote;
    use dashmap::DashMap;

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote {
            bid: bid.parse().unwrap(),
            bid_size: Decimal::ONE,
            ask: ask.parse().unwrap(),
            ask_size: Decimal::ONE,
            ts_ms: 1,
        }
    }

    fn view_with(cache: &DashMap<(Venue, Symbol), Quote>) -> MarketView<'_> {
        MarketView::new(cache)
    }

    #[test]
    fn detects_cross_exchange_opportunity_above_threshold() {
        let symbol = Symbol::new("BTC", "USDT");
        let venue_a = Venue::new("a");
        let venue_b = Venue::new("b");
        let cache: DashMap<(Venue, Symbol), Quote> = DashMap::new();
        cache.insert((venue_a.clone(), symbol.clone()), quote("100.0", "100.5"));
        cache.insert((venue_b.clone(), symbol.clone()), quote("102.0", "102.5"));

        let strategy = CrossExchangeStrategy::new(vec![symbol.clone()], HashMap::new(), Decimal::from(1));
        let changed = MarketEvent {
            venue: venue_b.clone(),
            symbol: symbol.clone(),
            quote: quote("102.0", "102.5"),
        };

        let view = view_with(&cache);
        let opportunities = strategy.on_update(&view, &changed);

        assert!(
            opportunities
                .iter()
                .any(|o| matches!(&o.kind, OpportunityKind::CrossExchange { buy_venue, sell_venue, .. }
                    if buy_venue == &venue_a && sell_venue == &venue_b))
        );
    }

    #[test]
    fn ignores_unwatched_symbol() {
        let watched = Symbol::new("BTC", "USDT");
        let other = Symbol::new("ETH", "USDT");
        let cache: DashMap<(Venue, Symbol), Quote> = DashMap::new();
        let strategy = CrossExchangeStrategy::new(vec![watched], HashMap::new(), Decimal::ZERO);
        let changed = MarketEvent {
            venue: Venue::new("a"),
            symbol: other,
            quote: quote("1", "1"),
        };

        let view = view_with(&cache);
        assert!(strategy.on_update(&view, &changed).is_empty());
    }

    #[test]
    fn no_opportunity_when_spread_below_threshold() {
        let symbol = Symbol::new("BTC", "USDT");
        let venue_a = Venue::new("a");
        let venue_b = Venue::new("b");
        let cache: DashMap<(Venue, Symbol), Quote> = DashMap::new();
        cache.insert((venue_a.clone(), symbol.clone()), quote("100.0", "100.1"));
        cache.insert((venue_b.clone(), symbol.clone()), quote("100.05", "100.15"));

        let strategy =
            CrossExchangeStrategy::new(vec![symbol.clone()], HashMap::new(), Decimal::from(50));
        let changed = MarketEvent {
            venue: venue_b,
            symbol,
            quote: quote("100.05", "100.15"),
        };

        let view = view_with(&cache);
        assert!(strategy.on_update(&view, &changed).is_empty());
    }
}
