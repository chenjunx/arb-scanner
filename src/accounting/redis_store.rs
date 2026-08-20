use std::sync::Mutex;

use anyhow::Context;
use log::warn;
use redis::Commands;

use crate::types::{Symbol, Venue};

use super::cursor_store::{FundingCursor, FundingCursorStore};

const FUNDING_CURSOR_KEY: &str = "arb_scanner:funding_cursor";

/// `FundingCursorStore` 的 Redis 实现：单个 Hash(`arb_scanner:funding_cursor`)，
/// field=`"{venue}|{symbol}"`，value=JSON。
pub struct RedisFundingCursorStore {
    conn: Mutex<redis::Connection>,
}

impl RedisFundingCursorStore {
    /// 立即建连校验可用性，连不上直接返回错误。
    pub fn new(redis_url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url).with_context(|| format!("invalid redis url: {redis_url}"))?;
        let conn = client.get_connection().with_context(|| format!("failed to connect to redis at {redis_url}"))?;
        Ok(Self { conn: Mutex::new(conn) })
    }
}

fn field_key(venue: &Venue, symbol: &Symbol) -> String {
    format!("{venue}|{symbol}")
}

impl FundingCursorStore for RedisFundingCursorStore {
    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<FundingCursor> {
        let mut conn = self.conn.lock().unwrap();
        let field = field_key(venue, symbol);
        let value: Option<String> = match conn.hget(FUNDING_CURSOR_KEY, &field) {
            Ok(value) => value,
            Err(err) => {
                warn!("RedisFundingCursorStore: failed to read field={field}: {err}");
                return None;
            }
        };
        value
            .and_then(|v| serde_json::from_str(&v).map_err(|err| warn!("RedisFundingCursorStore: failed to deserialize field={field}: {err}")).ok())
    }

    fn set(&self, venue: &Venue, symbol: &Symbol, cursor: FundingCursor) {
        let field = field_key(venue, symbol);
        let mut conn = self.conn.lock().unwrap();
        match serde_json::to_string(&cursor) {
            Ok(json) => {
                let result: redis::RedisResult<()> = conn.hset(FUNDING_CURSOR_KEY, &field, json);
                if let Err(err) = result {
                    warn!("RedisFundingCursorStore: failed to write field={field}: {err}");
                }
            }
            Err(err) => warn!("RedisFundingCursorStore: failed to serialize field={field}: {err}"),
        }
    }
}
