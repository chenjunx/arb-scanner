use std::sync::Mutex;

use anyhow::Context;
use log::warn;
use redis::Commands;

use crate::types::{Symbol, Venue};

use super::store::PnlStore;
use super::types::VenuePnl;

const PNL_KEY: &str = "arb_scanner:pnl";

/// `PnlStore` 的 Redis 实现，和 `position::redis_store::RedisPositionStore`
/// 结构对称：单个 Hash(`arb_scanner:pnl`)，field=`"{venue}|{symbol}"`，
/// value=JSON，`update()` 用 `Mutex<Connection>` 整个锁住做原子读改写。
pub struct RedisPnlStore {
    conn: Mutex<redis::Connection>,
}

impl RedisPnlStore {
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

impl PnlStore for RedisPnlStore {
    fn all(&self) -> Vec<VenuePnl> {
        let mut conn = self.conn.lock().unwrap();
        let entries: std::collections::HashMap<String, String> = match conn.hgetall(PNL_KEY) {
            Ok(entries) => entries,
            Err(err) => {
                warn!("RedisPnlStore: failed to read {PNL_KEY}: {err}");
                return Vec::new();
            }
        };
        entries
            .values()
            .filter_map(|v| serde_json::from_str(v).map_err(|err| warn!("RedisPnlStore: failed to deserialize pnl entry: {err}")).ok())
            .collect()
    }

    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePnl> {
        let mut conn = self.conn.lock().unwrap();
        let field = field_key(venue, symbol);
        let value: Option<String> = match conn.hget(PNL_KEY, &field) {
            Ok(value) => value,
            Err(err) => {
                warn!("RedisPnlStore: failed to read field={field}: {err}");
                return None;
            }
        };
        value.and_then(|v| serde_json::from_str(&v).map_err(|err| warn!("RedisPnlStore: failed to deserialize field={field}: {err}")).ok())
    }

    fn update(&self, venue: &Venue, symbol: &Symbol, f: Box<dyn FnOnce(Option<VenuePnl>) -> VenuePnl + Send>) {
        let field = field_key(venue, symbol);
        let mut conn = self.conn.lock().unwrap();
        let current: Option<VenuePnl> = match conn.hget::<_, _, Option<String>>(PNL_KEY, &field) {
            Ok(value) => value.and_then(|v| serde_json::from_str(&v).ok()),
            Err(err) => {
                warn!("RedisPnlStore: failed to read field={field} before update: {err}");
                None
            }
        };
        let updated = f(current);
        match serde_json::to_string(&updated) {
            Ok(json) => {
                let result: redis::RedisResult<()> = conn.hset(PNL_KEY, &field, json);
                if let Err(err) = result {
                    warn!("RedisPnlStore: failed to write field={field}: {err}");
                }
            }
            Err(err) => warn!("RedisPnlStore: failed to serialize field={field}: {err}"),
        }
    }
}
