//! k* 表([`KStarTable`])的 Redis 存储。每对腿(`PairRow::key`)：
//!
//! - `arb_scanner:kstar:<pair>:v:<version>`：一个版本的完整表(JSON)，版本号 = 生成时间(ms)；
//! - `arb_scanner:kstar:<pair>:versions`：ZSet，member/score 都是版本号；
//! - `arb_scanner:kstar:<pair>:active`：人工确认过、策略实际使用的版本号；
//! - `arb_scanner:kstar:pairs`：Set，有表的全部配对。
//!
//! 校准只写新版本，不改 `active`；只有 [`KStarStore::approve`] 会切换生效版本。
//! 策略启动时只读 `active`，没有就跳过这一对腿。

use std::sync::Mutex;

use anyhow::Context;
use redis::Commands;

use super::spread_wait::KStarTable;

const KEY_PREFIX: &str = "arb_scanner:kstar";

fn version_key(pair: &str, version: u64) -> String {
    format!("{KEY_PREFIX}:{pair}:v:{version}")
}

fn versions_key(pair: &str) -> String {
    format!("{KEY_PREFIX}:{pair}:versions")
}

fn pairs_key() -> String {
    format!("{KEY_PREFIX}:pairs")
}

fn active_key(pair: &str) -> String {
    format!("{KEY_PREFIX}:{pair}:active")
}

pub struct KStarStore {
    conn: Mutex<redis::Connection>,
}

impl KStarStore {
    /// 立即建连校验可用性，连不上直接返回错误。
    pub fn new(redis_url: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url).with_context(|| format!("invalid redis url: {redis_url}"))?;
        let conn = client.get_connection().with_context(|| format!("failed to connect to redis at {redis_url}"))?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// 写入一个新版本(不改生效版本)。同一版本号已存在时报错，不覆盖。
    pub fn save(&self, table: &KStarTable) -> anyhow::Result<()> {
        let json = serde_json::to_string(table).context("failed to serialize k* table")?;
        let key = version_key(&table.pair, table.version);
        let mut conn = self.conn.lock().unwrap();
        let created: bool = conn.set_nx(&key, json).with_context(|| format!("failed to write {key}"))?;
        if !created {
            anyhow::bail!("{key} already exists");
        }
        let _: () = conn
            .zadd(versions_key(&table.pair), table.version, table.version)
            .with_context(|| format!("failed to index {key}"))?;
        let _: () = conn.sadd(pairs_key(), &table.pair).with_context(|| format!("failed to index {key}"))?;
        Ok(())
    }

    pub fn get(&self, pair: &str, version: u64) -> anyhow::Result<Option<KStarTable>> {
        let key = version_key(pair, version);
        let json: Option<String> =
            self.conn.lock().unwrap().get(&key).with_context(|| format!("failed to read {key}"))?;
        json.map(|json| serde_json::from_str(&json).with_context(|| format!("failed to parse {key}")))
            .transpose()
    }

    /// 全部版本号，升序。
    pub fn versions(&self, pair: &str) -> anyhow::Result<Vec<u64>> {
        let key = versions_key(pair);
        self.conn.lock().unwrap().zrange(&key, 0, -1).with_context(|| format!("failed to read {key}"))
    }

    /// 有 k* 表的全部配对。
    pub fn pairs(&self) -> anyhow::Result<Vec<String>> {
        let mut pairs: Vec<String> =
            self.conn.lock().unwrap().smembers(pairs_key()).context("failed to list k* pairs")?;
        pairs.sort();
        Ok(pairs)
    }

    pub fn active_version(&self, pair: &str) -> anyhow::Result<Option<u64>> {
        let key = active_key(pair);
        self.conn.lock().unwrap().get(&key).with_context(|| format!("failed to read {key}"))
    }

    /// 生效中的表。`active` 指向的版本不存在视为数据损坏，报错而不是返回 None。
    pub fn active(&self, pair: &str) -> anyhow::Result<Option<KStarTable>> {
        let Some(version) = self.active_version(pair)? else { return Ok(None) };
        self.get(pair, version)?
            .map(Some)
            .ok_or_else(|| anyhow::anyhow!("{} points to missing version {version}", active_key(pair)))
    }

    /// 把某个已存在的版本设为生效版本。
    pub fn approve(&self, pair: &str, version: u64) -> anyhow::Result<()> {
        if self.get(pair, version)?.is_none() {
            anyhow::bail!("{pair} 没有版本 {version}");
        }
        let key = active_key(pair);
        let _: () = self.conn.lock().unwrap().set(&key, version).with_context(|| format!("failed to write {key}"))?;
        Ok(())
    }
}
