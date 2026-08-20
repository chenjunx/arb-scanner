use std::sync::Mutex;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::types::{Symbol, Venue};

use super::types::AdjustmentReason;

/// `PositionManager::apply_adjustment` 每次调用留下的一条审计记录，独立于
/// `VenuePosition` 持久化，避免热路径的仓位记录变成一个永远增长的数组。
#[derive(Debug, Clone, Serialize)]
pub struct AdjustmentRecord {
    pub venue: Venue,
    pub symbol: Symbol,
    pub amount: Decimal,
    pub reason: AdjustmentReason,
    pub realized_pnl_before: Decimal,
    pub realized_pnl_after: Decimal,
    pub ts_ms: u64,
}

/// 调整审计记录的持久化接口，风格照抄 `PositionStore`：只提供
/// `InMemoryAdjustmentLog`，未来要接 Redis/其它后端不用改
/// `PositionManager` 的调用方式。
pub trait AdjustmentLog: Send + Sync {
    fn record(&self, record: AdjustmentRecord);
}

/// 纯内存实现，重启即丢；也是 `PositionManager::new()` 的默认值。
#[derive(Default)]
pub struct InMemoryAdjustmentLog {
    records: Mutex<Vec<AdjustmentRecord>>,
}

impl InMemoryAdjustmentLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn all(&self) -> Vec<AdjustmentRecord> {
        self.records.lock().unwrap().clone()
    }
}

impl AdjustmentLog for InMemoryAdjustmentLog {
    fn record(&self, record: AdjustmentRecord) {
        self.records.lock().unwrap().push(record);
    }
}
