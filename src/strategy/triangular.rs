use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use log::info;
use rust_decimal::Decimal;

use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

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
    fees: HashMap<Venue, FeeSchedule>,
    min_profit_bps: Decimal,
    latest: Mutex<HashMap<Venue, HashMap<Symbol, Quote>>>,
    bus: Arc<TopicBus>,
}

impl TriangularStrategy {
    pub fn new(
        paths: Vec<TriangularPath>,
        fees: HashMap<Venue, FeeSchedule>,
        min_profit_bps: Decimal,
        bus: Arc<TopicBus>,
    ) -> Self {
        Self {
            paths,
            fees,
            min_profit_bps,
            latest: Mutex::new(HashMap::new()),
            bus,
        }
    }

    /// `subscriptions()` 只会为 `paths` 里出现过的 venue 订阅行情，所以这里
    /// 查不到只可能是调用方传入的行情不在订阅范围内——修 bug 而不是兜底掩盖它。
    fn fee_for(&self, venue: &Venue) -> FeeSchedule {
        self.fees
            .get(venue)
            .copied()
            .expect("TriangularStrategy received a quote for a venue outside its subscriptions")
    }

    /// 从 1 单位起始货币出发，依次执行三腿，返回最终换回的数量（同一货币）。
    /// 任意一腿缺少行情时返回 None。
    fn simulate(&self, quotes: &HashMap<Symbol, Quote>, path: &TriangularPath) -> Option<Decimal> {
        let fee = self.fee_for(&path.venue);
        let mut amount = Decimal::ONE;
        for leg in &path.legs {
            let quote = quotes.get(&leg.symbol)?;
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

fn log_opportunity(opportunity: &Opportunity) {
    let OpportunityKind::Triangular { venue, legs } = &opportunity.kind else {
        return;
    };
    info!(
        "[{}] venue={} legs={}/{}/{} profit_bps={} detail={}",
        opportunity.strategy,
        venue,
        legs[0],
        legs[1],
        legs[2],
        opportunity.expected_profit_bps,
        opportunity.detail
    );
}

impl Strategy for TriangularStrategy {
    fn name(&self) -> &str {
        "triangular"
    }

    fn subscriptions(&self) -> Vec<Topic> {
        self.paths
            .iter()
            .flat_map(|path| {
                path.legs
                    .iter()
                    .map(move |leg| Topic::quote(path.venue.clone(), leg.symbol.clone()))
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
        latest
            .entry(venue.clone())
            .or_default()
            .insert(symbol.clone(), *quote);

        let venue_quotes = &latest[venue];

        let mut found = Vec::new();
        for path in self.paths.iter().filter(|path| &path.venue == venue) {
            let Some(final_amount) = self.simulate(venue_quotes, path) else {
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
            found.push(Opportunity {
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
                ts_ms: quote.ts_ms,
            });
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
    fn detects_profitable_triangular_loop() {
        let venue = Venue::new("a");
        let btc_usdt = Symbol::new("BTC", "USDT");
        let eth_btc = Symbol::new("ETH", "BTC");
        let eth_usdt = Symbol::new("ETH", "USDT");

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

        let strategy = TriangularStrategy::new(vec![path], fees_for(&[&venue]), Decimal::from(1), Arc::new(TopicBus::new()));

        // 1 USDT -> 0.01 BTC (ask=100) -> 0.5 ETH (ask 0.02 BTC/ETH) -> sell ETH at 210 USDT/ETH
        strategy.on_quote(&Topic::quote(venue.clone(), btc_usdt.clone()), &quote("99.0", "100.0"));
        strategy.on_quote(&Topic::quote(venue.clone(), eth_btc.clone()), &quote("0.0195", "0.02"));
        strategy.on_quote(&Topic::quote(venue.clone(), eth_usdt.clone()), &quote("210.0", "211.0"));

        let latest = strategy.latest.lock().unwrap();
        let venue_quotes = &latest[&venue];
        let final_amount = strategy.simulate(venue_quotes, &strategy.paths[0]).unwrap();
        let profit_bps = (final_amount - Decimal::ONE) * Decimal::from(10_000);
        assert!(profit_bps > Decimal::ZERO);
    }

    #[test]
    fn no_opportunity_when_leg_quote_missing() {
        let venue = Venue::new("a");
        let btc_usdt = Symbol::new("BTC", "USDT");
        let eth_btc = Symbol::new("ETH", "BTC");
        let eth_usdt = Symbol::new("ETH", "USDT");

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

        let strategy = TriangularStrategy::new(vec![path], fees_for(&[&venue]), Decimal::ZERO, Arc::new(TopicBus::new()));

        // eth_btc quote 故意不发布
        strategy.on_quote(&Topic::quote(venue.clone(), btc_usdt.clone()), &quote("99.0", "100.0"));

        let latest = strategy.latest.lock().unwrap();
        let venue_quotes = &latest[&venue];
        let result = strategy.simulate(venue_quotes, &strategy.paths[0]);
        assert!(result.is_none());
    }
}
