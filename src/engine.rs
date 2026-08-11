use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::sink::OpportunitySink;
use crate::strategy::Strategy;
use crate::types::{MarketEvent, Quote, Symbol, Venue};

/// (venue, symbol) -> 最新 Quote 缓存的只读视图，供策略在 on_update 中查询
/// 当前已知的全部行情快照（例如跨交易所比价时需要看到其它 venue 的最新价格）。
pub struct MarketView<'a> {
    cache: &'a DashMap<(Venue, Symbol), Quote>,
}

impl<'a> MarketView<'a> {
    pub(crate) fn new(cache: &'a DashMap<(Venue, Symbol), Quote>) -> Self {
        Self { cache }
    }

    pub fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<Quote> {
        self.cache
            .get(&(venue.clone(), symbol.clone()))
            .map(|entry| *entry.value())
    }

    /// 返回某个 symbol 在所有已知 venue 上的最新报价。
    pub fn quotes_for_symbol(&self, symbol: &Symbol) -> Vec<(Venue, Quote)> {
        self.cache
            .iter()
            .filter(|entry| &entry.key().1 == symbol)
            .map(|entry| (entry.key().0.clone(), *entry.value()))
            .collect()
    }
}

/// 套利引擎：消费行情事件，维护最新快照缓存，驱动所有注册的策略，
/// 并将产出的套利机会分发给所有注册的 sink。
pub struct ArbitrageEngine {
    cache: DashMap<(Venue, Symbol), Quote>,
    strategies: Vec<Box<dyn Strategy>>,
    sinks: Vec<Box<dyn OpportunitySink>>,
}

impl ArbitrageEngine {
    pub fn new(strategies: Vec<Box<dyn Strategy>>, sinks: Vec<Box<dyn OpportunitySink>>) -> Self {
        Self {
            cache: DashMap::new(),
            strategies,
            sinks,
        }
    }

    pub async fn run(self, mut rx: mpsc::Receiver<MarketEvent>) {
        while let Some(event) = rx.recv().await {
            self.cache
                .insert((event.venue.clone(), event.symbol.clone()), event.quote);

            let view = MarketView::new(&self.cache);
            for strategy in &self.strategies {
                for opportunity in strategy.on_update(&view, &event) {
                    for sink in &self.sinks {
                        sink.handle(&opportunity);
                    }
                }
            }
        }
    }
}
