use std::collections::HashMap;
use std::sync::Mutex;

use crate::types::{Symbol, Venue};

use super::types::VenuePnl;

/// 盈亏/手续费持久化接口，和 `position::PositionStore` 同一套设计语言：只提供
/// `InMemoryPnlStore`，未来可无痛切换 Redis/sql 而不用改动 `PortfolioManager`
/// 的调用方式。`update()` 是原子读改写，理由和 `PositionStore::update` 一致——
/// 避免并发成交推送互相覆盖同一个 (venue, symbol) 的累计值。
pub trait PnlStore: Send + Sync {
    fn all(&self) -> Vec<VenuePnl>;
    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePnl>;

    fn update(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        f: Box<dyn FnOnce(Option<VenuePnl>) -> VenuePnl + Send>,
    );
}

/// 纯内存实现，重启即丢。
#[derive(Default)]
pub struct InMemoryPnlStore {
    entries: Mutex<HashMap<(Venue, Symbol), VenuePnl>>,
}

impl InMemoryPnlStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PnlStore for InMemoryPnlStore {
    fn all(&self) -> Vec<VenuePnl> {
        self.entries.lock().unwrap().values().cloned().collect()
    }

    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePnl> {
        self.entries.lock().unwrap().get(&(venue.clone(), symbol.clone())).cloned()
    }

    fn update(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        f: Box<dyn FnOnce(Option<VenuePnl>) -> VenuePnl + Send>,
    ) {
        let key = (venue.clone(), symbol.clone());
        let mut entries = self.entries.lock().unwrap();
        let current = entries.get(&key).cloned();
        entries.insert(key, f(current));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn update_creates_entry_when_missing() {
        let store = InMemoryPnlStore::new();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        store.update(
            &venue,
            &symbol,
            Box::new(|current| {
                assert!(current.is_none());
                VenuePnl {
                    venue: Venue::new("binance_spot"),
                    symbol: Symbol::new("BTC", "USDT"),
                    realized_pnl: Decimal::ONE,
                    trade_count: 1,
                    updated_at_ms: 1,
                }
            }),
        );

        let stored = store.get(&venue, &symbol).unwrap();
        assert_eq!(stored.realized_pnl, Decimal::ONE);
        assert_eq!(stored.trade_count, 1);
    }
}
