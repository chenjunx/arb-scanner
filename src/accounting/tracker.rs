use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::warn;
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::market_data::now_ms;
use crate::portfolio::PortfolioManager;
use crate::position::PositionManager;
use crate::types::Venue;

use super::cursor_store::{FundingCursor, FundingCursorStore};
use super::provider::FundingFeeProvider;

/// 定期轮询各交易所的资金费流水，去重后累加进 `PortfolioManager` 的
/// `PnlStore`。每次 tick 都从 `PositionManager` 重新读取当前非零仓位，而不是
/// 启动时固定一份 symbol 列表，这样新开/平仓的期货仓位不需要重启这个任务
/// 就能被自动跟踪/停止跟踪。
pub struct FundingFeeTracker {
    providers: HashMap<Venue, Arc<dyn FundingFeeProvider>>,
    position_manager: Arc<PositionManager>,
    portfolio: Arc<PortfolioManager>,
    cursor_store: Arc<dyn FundingCursorStore>,
    poll_interval: Duration,
    /// 某个 (venue, symbol) 第一次被轮询、还没有游标时，往回补多久的历史。
    initial_lookback: Duration,
}

impl FundingFeeTracker {
    pub fn new(
        providers: HashMap<Venue, Arc<dyn FundingFeeProvider>>,
        position_manager: Arc<PositionManager>,
        portfolio: Arc<PortfolioManager>,
        cursor_store: Arc<dyn FundingCursorStore>,
        poll_interval: Duration,
        initial_lookback: Duration,
    ) -> Self {
        Self {
            providers,
            position_manager,
            portfolio,
            cursor_store,
            poll_interval,
            initial_lookback,
        }
    }

    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.poll_interval);
            loop {
                ticker.tick().await;
                self.poll_once().await;
            }
        })
    }

    async fn poll_once(&self) {
        for pos in self.position_manager.all_positions() {
            if pos.net_qty == Decimal::ZERO {
                continue;
            }
            let Some(provider) = self.providers.get(&pos.venue) else {
                continue;
            };

            let cursor = self.cursor_store.get(&pos.venue, &pos.symbol);
            let start_time_ms = cursor
                .map(|c| c.last_time_ms)
                .unwrap_or_else(|| now_ms().saturating_sub(self.initial_lookback.as_millis() as u64));

            let mut records = match provider.funding_income(&pos.symbol, Some(start_time_ms)).await {
                Ok(records) => records,
                Err(err) => {
                    warn!(
                        "funding tracker: failed to fetch funding income for venue={} symbol={}: {err:#}",
                        pos.venue, pos.symbol
                    );
                    continue;
                }
            };
            records.sort_by_key(|r| r.tran_id);

            let last_seen_tran_id = cursor.map(|c| c.last_tran_id).unwrap_or(-1);
            let mut newest_cursor = None;
            for record in records.into_iter().filter(|r| r.tran_id > last_seen_tran_id) {
                self.portfolio.record_funding_fee(&pos.venue, &pos.symbol, record.income, record.time_ms);
                newest_cursor = Some(FundingCursor { last_time_ms: record.time_ms, last_tran_id: record.tran_id });
            }
            if let Some(cursor) = newest_cursor {
                self.cursor_store.set(&pos.venue, &pos.symbol, cursor);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounting::cursor_store::InMemoryFundingCursorStore;
    use crate::accounting::provider::FundingIncomeRecord;
    use crate::order::types::OrderSide;
    use crate::portfolio::{FeeConfig, InMemoryPnlStore};
    use crate::position::InMemoryPositionStore;
    use crate::types::Symbol;
    use async_trait::async_trait;
    use dashmap::DashMap;
    use std::sync::Mutex;

    struct MockProvider {
        venue: Venue,
        records: Mutex<Vec<FundingIncomeRecord>>,
    }

    #[async_trait]
    impl FundingFeeProvider for MockProvider {
        fn venue(&self) -> Venue {
            self.venue.clone()
        }

        async fn funding_income(&self, _symbol: &Symbol, _start_time_ms: Option<u64>) -> anyhow::Result<Vec<FundingIncomeRecord>> {
            Ok(self.records.lock().unwrap().clone())
        }
    }

    fn venue() -> Venue {
        Venue::new("binance_futures")
    }
    fn symbol() -> Symbol {
        Symbol::new("BTC", "USDT")
    }

    fn setup(records: Vec<FundingIncomeRecord>) -> (Arc<FundingFeeTracker>, Arc<PortfolioManager>) {
        let position_manager = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        position_manager.on_filled(&venue(), &symbol(), OrderSide::Sell, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);

        let portfolio = Arc::new(PortfolioManager::new(
            position_manager.clone(),
            Arc::new(InMemoryPnlStore::new()),
            Arc::new(DashMap::new()),
            HashMap::<Venue, FeeConfig>::new(),
        ));

        let mut providers: HashMap<Venue, Arc<dyn FundingFeeProvider>> = HashMap::new();
        providers.insert(venue(), Arc::new(MockProvider { venue: venue(), records: Mutex::new(records) }));

        let tracker = Arc::new(FundingFeeTracker::new(
            providers,
            position_manager,
            portfolio.clone(),
            Arc::new(InMemoryFundingCursorStore::new()),
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));
        (tracker, portfolio)
    }

    #[tokio::test]
    async fn accumulates_funding_income_for_open_positions() {
        let (tracker, portfolio) = setup(vec![
            FundingIncomeRecord { symbol: symbol(), income: Decimal::new(-10, 0), time_ms: 1000, tran_id: 1 },
            FundingIncomeRecord { symbol: symbol(), income: Decimal::new(5, 0), time_ms: 2000, tran_id: 2 },
        ]);

        tracker.poll_once().await;

        let pnl = portfolio.venue_pnl(&venue(), &symbol()).unwrap();
        assert_eq!(pnl.funding_pnl, Decimal::new(-5, 0));
    }

    #[tokio::test]
    async fn repeated_polls_do_not_double_count() {
        let (tracker, portfolio) = setup(vec![FundingIncomeRecord {
            symbol: symbol(),
            income: Decimal::new(-10, 0),
            time_ms: 1000,
            tran_id: 1,
        }]);

        tracker.poll_once().await;
        tracker.poll_once().await;
        tracker.poll_once().await;

        let pnl = portfolio.venue_pnl(&venue(), &symbol()).unwrap();
        assert_eq!(pnl.funding_pnl, Decimal::new(-10, 0));
    }

    #[tokio::test]
    async fn ignores_venues_without_a_registered_provider() {
        let position_manager = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let other_venue = Venue::new("kraken_futures");
        position_manager.on_filled(&other_venue, &symbol(), OrderSide::Sell, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);
        let portfolio = Arc::new(PortfolioManager::new(
            position_manager.clone(),
            Arc::new(InMemoryPnlStore::new()),
            Arc::new(DashMap::new()),
            HashMap::<Venue, FeeConfig>::new(),
        ));
        let tracker = Arc::new(FundingFeeTracker::new(
            HashMap::new(),
            position_manager,
            portfolio.clone(),
            Arc::new(InMemoryFundingCursorStore::new()),
            Duration::from_secs(1),
            Duration::from_secs(3600),
        ));

        tracker.poll_once().await;

        assert!(portfolio.venue_pnl(&other_venue, &symbol()).is_none());
    }
}
