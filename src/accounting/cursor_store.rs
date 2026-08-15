use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::types::{Symbol, Venue};

/// 某个 (venue, symbol) 资金费流水的去重游标。Binance 的 income 接口只能按时间
/// 范围查询、没有"从某个 tranId 起"的增量参数，所以要同时存时间和 tranId：
/// 下次轮询用 `last_time_ms` 当 `startTime`(可能重复返回同一条)，再用
/// `last_tran_id` 过滤掉已经入账过的记录。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FundingCursor {
    pub last_time_ms: u64,
    pub last_tran_id: i64,
}

pub trait FundingCursorStore: Send + Sync {
    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<FundingCursor>;
    fn set(&self, venue: &Venue, symbol: &Symbol, cursor: FundingCursor);
}

/// 纯内存实现，重启即丢，供测试使用。
#[derive(Default)]
pub struct InMemoryFundingCursorStore {
    cursors: Mutex<HashMap<(Venue, Symbol), FundingCursor>>,
}

impl InMemoryFundingCursorStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl FundingCursorStore for InMemoryFundingCursorStore {
    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<FundingCursor> {
        self.cursors.lock().unwrap().get(&(venue.clone(), symbol.clone())).copied()
    }

    fn set(&self, venue: &Venue, symbol: &Symbol, cursor: FundingCursor) {
        self.cursors.lock().unwrap().insert((venue.clone(), symbol.clone()), cursor);
    }
}
