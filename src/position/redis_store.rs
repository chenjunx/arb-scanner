use std::sync::Mutex;

use anyhow::Context;
use log::warn;
use redis::Commands;

use crate::types::{Symbol, Venue};

use super::store::PositionStore;
use super::types::VenuePosition;

const POSITIONS_KEY: &str = "arb_scanner:positions";

/// `PositionStore` 的 Redis 实现：单个 Hash(`arb_scanner:positions`)，
/// field=`"{venue}|{symbol}"`，value=JSON。`update()` 用 `Mutex<Connection>`
/// 整个锁住做"读-反序列化-调用 f-序列化-写"，保证进程内原子性，和
/// `InMemoryPositionStore` 的保证级别一致(多进程并发不保证，trait 文档里
/// 本来就预留了这个限制)。
pub struct RedisPositionStore {
    conn: Mutex<redis::Connection>,
}

impl RedisPositionStore {
    /// 立即建连校验可用性，连不上直接返回错误——不能等到下单后才发现存不进去。
    pub fn new(redis_url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url).with_context(|| format!("invalid redis url: {redis_url}"))?;
        let conn = client.get_connection().with_context(|| format!("failed to connect to redis at {redis_url}"))?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn from_env() -> anyhow::Result<Self> {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
        Self::new(&url)
    }
}

fn field_key(venue: &Venue, symbol: &Symbol) -> String {
    format!("{venue}|{symbol}")
}

impl PositionStore for RedisPositionStore {
    fn all(&self) -> Vec<VenuePosition> {
        let mut conn = self.conn.lock().unwrap();
        let entries: std::collections::HashMap<String, String> = match conn.hgetall(POSITIONS_KEY) {
            Ok(entries) => entries,
            Err(err) => {
                warn!("RedisPositionStore: failed to read {POSITIONS_KEY}: {err}");
                return Vec::new();
            }
        };
        entries
            .values()
            .filter_map(|v| serde_json::from_str(v).map_err(|err| warn!("RedisPositionStore: failed to deserialize position: {err}")).ok())
            .collect()
    }

    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePosition> {
        let mut conn = self.conn.lock().unwrap();
        let field = field_key(venue, symbol);
        let value: Option<String> = match conn.hget(POSITIONS_KEY, &field) {
            Ok(value) => value,
            Err(err) => {
                warn!("RedisPositionStore: failed to read field={field}: {err}");
                return None;
            }
        };
        value.and_then(|v| serde_json::from_str(&v).map_err(|err| warn!("RedisPositionStore: failed to deserialize field={field}: {err}")).ok())
    }

    fn update(&self, venue: &Venue, symbol: &Symbol, f: Box<dyn FnOnce(Option<VenuePosition>) -> VenuePosition + Send>) {
        let field = field_key(venue, symbol);
        let mut conn = self.conn.lock().unwrap();
        let current: Option<VenuePosition> = match conn.hget::<_, _, Option<String>>(POSITIONS_KEY, &field) {
            Ok(value) => value.and_then(|v| serde_json::from_str(&v).ok()),
            Err(err) => {
                warn!("RedisPositionStore: failed to read field={field} before update: {err}");
                None
            }
        };
        let updated = f(current);
        match serde_json::to_string(&updated) {
            Ok(json) => {
                let result: redis::RedisResult<()> = conn.hset(POSITIONS_KEY, &field, json);
                if let Err(err) = result {
                    warn!("RedisPositionStore: failed to write field={field}: {err}");
                }
            }
            Err(err) => warn!("RedisPositionStore: failed to serialize field={field}: {err}"),
        }
    }
}
