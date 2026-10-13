use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use log::{info, warn};

use crate::types::Venue;

const FUNDING_LEASE_KEY_PREFIX: &str = "arb_scanner:funding_lease:";

/// 资金费记账的按 venue 租约：同一个 venue 任何时刻只允许一个进程的
/// `FundingFeeTracker` 去拉流水、写 `realized_pnl`。
///
/// 游标"读 → 入账 → 写回"不是原子的，两个进程同时轮询同一 (venue, symbol)
/// 会把同一笔资金费各记一次。每个常驻进程都可能挂一个 tracker(`accounting`/
/// `monitor`/策略命令 live)，所以要靠租约互斥。按 venue 而不是全局一把锁，
/// 是因为不同进程注册的 provider 不一样(例如 `accounting` 只有币安、
/// `cross-funding` 有 Gate/OKX)，全局锁会让抢到锁的进程漏掉它没有 provider 的 venue。
pub trait FundingLease: Send + Sync {
    /// 抢占或续期 `venue` 的租约，返回本进程当前是否持有。已持有时续期，
    /// 被别的进程持有时返回 false；Redis 出错时也返回 false(宁可少记一轮也不重复记)。
    fn try_hold(&self, venue: &Venue) -> bool;
}

/// 永远持有，供单进程测试使用。
pub struct AlwaysHeldFundingLease;

impl FundingLease for AlwaysHeldFundingLease {
    fn try_hold(&self, _venue: &Venue) -> bool {
        true
    }
}

/// 不存在就占上；已经是自己的就续期；是别人的就返回 0。
const ACQUIRE_OR_RENEW: &str = r"
local current = redis.call('GET', KEYS[1])
if current == false then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1
end
if current == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
end
return 0
";

/// `FundingLease` 的 Redis 实现：每个 venue 一个 key
/// (`arb_scanner:funding_lease:{venue}`)，value 是进程级随机 token，带 TTL。
/// 不做显式释放——持有者退出后等 TTL 过期，其它进程下一轮轮询时接手；漏掉的
/// 流水由游标在接手后补回，不会丢。
pub struct RedisFundingLease {
    conn: Mutex<redis::Connection>,
    token: String,
    ttl: Duration,
    script: redis::Script,
    /// 上一轮各 venue 的持有状态，只用于在状态变化时打日志。
    held: Mutex<HashMap<Venue, bool>>,
}

impl RedisFundingLease {
    /// `ttl` 必须大于 tracker 的轮询间隔，否则持有者两轮之间租约就会过期、被别人
    /// 抢走。立即建连校验可用性，连不上直接返回错误。
    pub fn new(redis_url: &str, ttl: Duration) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url).with_context(|| format!("invalid redis url: {redis_url}"))?;
        let conn = client.get_connection().with_context(|| format!("failed to connect to redis at {redis_url}"))?;
        Ok(Self {
            conn: Mutex::new(conn),
            token: format!("{}-{:010}", std::process::id(), rand::random::<u32>()),
            ttl,
            script: redis::Script::new(ACQUIRE_OR_RENEW),
            held: Mutex::new(HashMap::new()),
        })
    }
}

impl FundingLease for RedisFundingLease {
    fn try_hold(&self, venue: &Venue) -> bool {
        let key = format!("{FUNDING_LEASE_KEY_PREFIX}{venue}");
        let result: redis::RedisResult<i64> = {
            let mut conn = self.conn.lock().unwrap();
            self.script.key(&key).arg(&self.token).arg(self.ttl.as_millis() as u64).invoke(&mut *conn)
        };
        let now_held = match result {
            Ok(v) => v == 1,
            Err(err) => {
                warn!("funding lease: failed to acquire/renew key={key}: {err}");
                false
            }
        };
        let previously = self.held.lock().unwrap().insert(venue.clone(), now_held);
        if previously != Some(now_held) {
            if now_held {
                info!("funding lease: holding venue={venue} (token={}), this process does funding accounting for it", self.token);
            } else {
                info!("funding lease: venue={venue} is held by another process, skipping its funding accounting");
            }
        }
        now_held
    }
}
