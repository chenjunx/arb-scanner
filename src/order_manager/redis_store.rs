use std::sync::Mutex;

use anyhow::Context;
use log::warn;
use redis::Commands;

use super::store::OrderStore;
use super::types::{Order, OrderId};

const ORDERS_KEY: &str = "arb_scanner:orders";

/// `OrderStore` 的 Redis 实现：单个 Hash(`arb_scanner:orders`)，field=OrderId，
/// value=JSON。用阻塞式 `redis::Connection` + `Mutex` 而不是异步客户端，因为
/// `OrderStore` trait 本身就是同步的(`open` 是一次性 CLI 命令，不是常驻服务，
/// 不值得为此引入异步客户端和整条调用链的异步改造)。
pub struct RedisOrderStore {
    conn: Mutex<redis::Connection>,
}

impl RedisOrderStore {
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

impl OrderStore for RedisOrderStore {
    fn all(&self) -> Vec<Order> {
        let mut conn = self.conn.lock().unwrap();
        let entries: std::collections::HashMap<String, String> = match conn.hgetall(ORDERS_KEY) {
            Ok(entries) => entries,
            Err(err) => {
                warn!("RedisOrderStore: failed to read {ORDERS_KEY}: {err}");
                return Vec::new();
            }
        };
        entries
            .values()
            .filter_map(|v| serde_json::from_str(v).map_err(|err| warn!("RedisOrderStore: failed to deserialize order: {err}")).ok())
            .collect()
    }

    fn get(&self, order_id: &OrderId) -> Option<Order> {
        let mut conn = self.conn.lock().unwrap();
        let value: Option<String> = match conn.hget(ORDERS_KEY, order_id.to_string()) {
            Ok(value) => value,
            Err(err) => {
                warn!("RedisOrderStore: failed to read order_id={order_id}: {err}");
                return None;
            }
        };
        value.and_then(|v| serde_json::from_str(&v).map_err(|err| warn!("RedisOrderStore: failed to deserialize order_id={order_id}: {err}")).ok())
    }

    fn upsert(&self, order: Order) {
        let json = match serde_json::to_string(&order) {
            Ok(json) => json,
            Err(err) => {
                warn!("RedisOrderStore: failed to serialize order_id={}: {err}", order.order_id);
                return;
            }
        };
        let mut conn = self.conn.lock().unwrap();
        let result: redis::RedisResult<()> = conn.hset(ORDERS_KEY, order.order_id.to_string(), json);
        if let Err(err) = result {
            warn!("RedisOrderStore: failed to write order_id={}: {err}", order.order_id);
        }
    }
}
