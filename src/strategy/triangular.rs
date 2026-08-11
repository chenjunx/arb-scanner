use rust_decimal::Decimal;

use crate::engine::MarketView;
use crate::types::{MarketEvent, Symbol, Venue};

use super::{FeeSchedule, Opportunity, OpportunityKind, Strategy};

/// 三角套利路径中的一腿：Buy 表示用 quote 买入 base（按 ask 成交），
/// Sell 表示用 base 换回 quote（按 bid 成交）。三条腿依次执行需首尾相接，
/// 形成一个从某种货币出发、最终回到同种货币的闭环。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct TriangularLeg {
    pub symbol: Symbol,
    pub side: LegSide,
}

/// 单交易所内的三角套利路径，例如 USDT -> BTC -> ETH -> USDT：
/// legs = [(BTC/USDT, Buy), (ETH/BTC, Buy), (ETH/USDT, Sell)]
#[derive(Debug, Clone)]
pub struct TriangularPath {
    pub venue: Venue,
    pub legs: [TriangularLeg; 3],
}

pub struct TriangularStrategy {
    paths: Vec<TriangularPath>,
    fees: std::collections::HashMap<Venue, FeeSchedule>,
    min_profit_bps: Decimal,
}

impl TriangularStrategy {
    pub fn new(
        paths: Vec<TriangularPath>,
        fees: std::collections::HashMap<Venue, FeeSchedule>,
        min_profit_bps: Decimal,
    ) -> Self {
        Self {
            paths,
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

    /// 从 1 单位起始货币出发，依次执行三腿，返回最终换回的数量（同一货币）。
    /// 任意一腿缺少行情时返回 None。
    fn simulate(&self, view: &MarketView, path: &TriangularPath) -> Option<Decimal> {
        let fee = self.fee_for(&path.venue);
        let mut amount = Decimal::ONE;
        for leg in &path.legs {
            let quote = view.get(&path.venue, &leg.symbol)?;
            amount = match leg.side {
                LegSide::Buy => {
                    if quote.ask <= Decimal::ZERO {
                        return None;
                    }
                    amount / quote.ask * fee.sell_multiplier()
                }
                LegSide::Sell => amount * quote.bid * fee.sell_multiplier(),
            };
        }
        Some(amount)
    }
}

impl Strategy for TriangularStrategy {
    fn name(&self) -> &str {
        "triangular"
    }

    fn on_update(&self, view: &MarketView, changed: &MarketEvent) -> Vec<Opportunity> {
        let mut opportunities = Vec::new();

        for path in &self.paths {
            if path.venue != changed.venue {
                continue;
            }
            if !path.legs.iter().any(|leg| leg.symbol == changed.symbol) {
                continue;
            }

            let Some(final_amount) = self.simulate(view, path) else {
                continue;
            };
            let profit_bps = (final_amount - Decimal::ONE) * Decimal::from(10_000);
            if profit_bps < self.min_profit_bps {
                continue;
            }

            let legs = [
                path.legs[0].symbol.clone(),
                path.legs[1].symbol.clone(),
                path.legs[2].symbol.clone(),
            ];
            opportunities.push(Opportunity {
                strategy: "triangular",
                kind: OpportunityKind::Triangular {
                    venue: path.venue.clone(),
                    legs,
                },
                expected_profit_bps: profit_bps,
                detail: format!(
                    "triangular loop on {} starting from 1 unit ends with {}",
                    path.venue, final_amount
                ),
                ts_ms: changed.quote.ts_ms,
            });
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

    #[test]
    fn detects_profitable_triangular_loop() {
        let venue = Venue::new("a");
        let btc_usdt = Symbol::new("BTC", "USDT");
        let eth_btc = Symbol::new("ETH", "BTC");
        let eth_usdt = Symbol::new("ETH", "USDT");

        let cache: DashMap<(Venue, Symbol), Quote> = DashMap::new();
        // 1 USDT -> 0.01 BTC (ask=100) -> 0.5 ETH (ask 0.02 BTC/ETH) -> sell ETH at 210 USDT/ETH
        cache.insert((venue.clone(), btc_usdt.clone()), quote("99.0", "100.0"));
        cache.insert((venue.clone(), eth_btc.clone()), quote("0.0195", "0.02"));
        cache.insert((venue.clone(), eth_usdt.clone()), quote("210.0", "211.0"));

        let path = TriangularPath {
            venue: venue.clone(),
            legs: [
                TriangularLeg {
                    symbol: btc_usdt.clone(),
                    side: LegSide::Buy,
                },
                TriangularLeg {
                    symbol: eth_btc.clone(),
                    side: LegSide::Buy,
                },
                TriangularLeg {
                    symbol: eth_usdt.clone(),
                    side: LegSide::Sell,
                },
            ],
        };

        let strategy = TriangularStrategy::new(
            vec![path],
            std::collections::HashMap::new(),
            Decimal::from(1),
        );
        let changed = MarketEvent {
            venue,
            symbol: eth_usdt,
            quote: quote("210.0", "211.0"),
        };

        let view = MarketView::new(&cache);
        let opportunities = strategy.on_update(&view, &changed);

        assert_eq!(opportunities.len(), 1);
        assert!(opportunities[0].expected_profit_bps > Decimal::ZERO);
    }

    #[test]
    fn no_opportunity_when_leg_quote_missing() {
        let venue = Venue::new("a");
        let btc_usdt = Symbol::new("BTC", "USDT");
        let eth_btc = Symbol::new("ETH", "BTC");
        let eth_usdt = Symbol::new("ETH", "USDT");

        let cache: DashMap<(Venue, Symbol), Quote> = DashMap::new();
        cache.insert((venue.clone(), btc_usdt.clone()), quote("99.0", "100.0"));
        // eth_btc quote missing on purpose

        let path = TriangularPath {
            venue: venue.clone(),
            legs: [
                TriangularLeg {
                    symbol: btc_usdt.clone(),
                    side: LegSide::Buy,
                },
                TriangularLeg {
                    symbol: eth_btc,
                    side: LegSide::Buy,
                },
                TriangularLeg {
                    symbol: eth_usdt.clone(),
                    side: LegSide::Sell,
                },
            ],
        };

        let strategy = TriangularStrategy::new(
            vec![path],
            std::collections::HashMap::new(),
            Decimal::ZERO,
        );
        let changed = MarketEvent {
            venue,
            symbol: btc_usdt,
            quote: quote("99.0", "100.0"),
        };

        let view = MarketView::new(&cache);
        assert!(strategy.on_update(&view, &changed).is_empty());
    }
}
