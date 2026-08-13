use std::collections::HashMap;
use std::sync::Mutex;

use crate::types::{Symbol, Venue};

use super::types::VenuePosition;

/// 仓位持久化接口。`PositionManager` 只依赖这个 trait，不关心具体存储后端，
/// 本次只提供 `InMemoryPositionStore`；未来可以加 Redis/sled 等实现而不用
/// 改动 `PositionManager` 的调用方式。
///
/// `update()` 设计成原子读改写而不是分离的 get/set，是为了避免并发成交推送
/// (例如同一 venue+symbol 上两个策略的订单同时成交) 互相覆盖；未来的 Redis
/// 实现可以用 `WATCH`/`MULTI` 或 Lua 脚本来实现同样的原子语义。
pub trait PositionStore: Send + Sync {
    fn all(&self) -> Vec<VenuePosition>;
    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePosition>;

    /// 原子地对单个 (venue, symbol) 做读-改-写。`f` 接收当前仓位(不存在则
    /// None)，返回更新后的仓位。
    fn update(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        f: Box<dyn FnOnce(Option<VenuePosition>) -> VenuePosition + Send>,
    );
}

/// 纯内存实现，重启即丢。
#[derive(Default)]
pub struct InMemoryPositionStore {
    positions: Mutex<HashMap<(Venue, Symbol), VenuePosition>>,
}

impl InMemoryPositionStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PositionStore for InMemoryPositionStore {
    fn all(&self) -> Vec<VenuePosition> {
        self.positions.lock().unwrap().values().cloned().collect()
    }

    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePosition> {
        self.positions.lock().unwrap().get(&(venue.clone(), symbol.clone())).cloned()
    }

    fn update(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        f: Box<dyn FnOnce(Option<VenuePosition>) -> VenuePosition + Send>,
    ) {
        let key = (venue.clone(), symbol.clone());
        let mut positions = self.positions.lock().unwrap();
        let current = positions.get(&key).cloned();
        positions.insert(key, f(current));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn update_creates_entry_when_missing() {
        let store = InMemoryPositionStore::new();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        store.update(
            &venue,
            &symbol,
            Box::new(|current| {
                assert!(current.is_none());
                VenuePosition {
                    venue: Venue::new("binance_spot"),
                    symbol: Symbol::new("BTC", "USDT"),
                    net_qty: Decimal::ONE,
                    avg_price: Some(Decimal::new(50000, 0)),
                    updated_at_ms: 1,
                }
            }),
        );

        let stored = store.get(&venue, &symbol).unwrap();
        assert_eq!(stored.net_qty, Decimal::ONE);
    }
}
