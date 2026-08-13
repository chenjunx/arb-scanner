use std::sync::{Arc, OnceLock};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// 各交易所适配器发 REST 请求前统一调用这个函数即可，不用关心限流算法或
/// 具体阈值——那些都在 [`bucket_config`] 里按 `host` 集中配置。同一个 `host`
/// 全局共享一个令牌桶，所以同一交易所下 exchange_info/order/wallet 等多个
/// provider 实例会正确地共用同一份真实限流预算。
pub async fn throttle(host: &str) {
    bucket_for(host).acquire(1.0).await;
}

/// 令牌桶：`capacity` 是突发上限，`refill_per_sec` 是稳态恢复速度。
/// `acquire` 不够 token 时睡到大概率够为止，再重新核对(避免并发下单次
/// sleep 计算的精度误差导致提前放行)。
struct TokenBucket {
    capacity: f64,
    refill_per_sec: f64,
    state: Mutex<BucketState>,
}

struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self {
            capacity,
            refill_per_sec,
            state: Mutex::new(BucketState {
                tokens: capacity,
                last_refill: Instant::now(),
            }),
        }
    }

    async fn acquire(&self, cost: f64) {
        loop {
            let wait = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(state.last_refill).as_secs_f64();
                state.tokens = (state.tokens + elapsed * self.refill_per_sec).min(self.capacity);
                state.last_refill = now;

                if state.tokens >= cost {
                    state.tokens -= cost;
                    None
                } else {
                    let deficit = cost - state.tokens;
                    Some(Duration::from_secs_f64(deficit / self.refill_per_sec))
                }
            };
            match wait {
                None => return,
                Some(d) => tokio::time::sleep(d).await,
            }
        }
    }
}

/// 每个交易所在这里配一行：`(突发上限 capacity, 每秒恢复 refill_per_sec)`。
/// 这是全仓库唯一需要感知"某个交易所限流到底多严格"的地方，其余代码只管
/// 调用 [`throttle`]。
///
/// 现在所有请求统一按 `cost = 1` 记账，不区分接口的真实权重，所以币安两档
/// 的数值特意定得比官方权重预算(现货 6000/分钟、合约 2400/分钟)更保守，
/// 留出余量。Kraken 现货私有接口的官方限制本身就是"计数器 +1、按秒衰减"，
/// 和这里的模型基本一致(未覆盖 ledger/trade-history 这类 +4 的重量级调用，
/// 本仓库目前也没有调用它们)。
fn bucket_config(host: &str) -> (f64, f64) {
    match host {
        "https://api.binance.com" | "https://testnet.binance.vision" => (60.0, 10.0),
        "https://fapi.binance.com" | "https://testnet.binancefuture.com" => (20.0, 3.0),
        "https://api.kraken.com" => (15.0, 0.33),
        "https://futures.kraken.com" => (10.0, 1.0),
        other => {
            log::warn!("ratelimit: no explicit config for host {other}, using conservative default (5 burst, 1/s)");
            (5.0, 1.0)
        }
    }
}

static BUCKETS: OnceLock<DashMap<String, Arc<TokenBucket>>> = OnceLock::new();

fn bucket_for(host: &str) -> Arc<TokenBucket> {
    let buckets = BUCKETS.get_or_init(DashMap::new);
    if let Some(existing) = buckets.get(host) {
        return existing.clone();
    }
    let (capacity, refill_per_sec) = bucket_config(host);
    let bucket = Arc::new(TokenBucket::new(capacity, refill_per_sec));
    buckets.entry(host.to_string()).or_insert(bucket).clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn acquire_within_capacity_does_not_wait() {
        let bucket = TokenBucket::new(3.0, 1.0);
        let start = Instant::now();
        bucket.acquire(1.0).await;
        bucket.acquire(1.0).await;
        bucket.acquire(1.0).await;
        assert_eq!(Instant::now(), start);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_beyond_capacity_waits_for_refill() {
        let bucket = TokenBucket::new(1.0, 2.0);
        bucket.acquire(1.0).await;

        let start = Instant::now();
        bucket.acquire(1.0).await;
        let elapsed = Instant::now().saturating_duration_since(start);
        assert!(elapsed >= Duration::from_millis(490), "expected ~500ms wait, got {elapsed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn refill_is_capped_at_capacity() {
        let bucket = TokenBucket::new(2.0, 100.0);
        tokio::time::advance(Duration::from_secs(10)).await;
        let start = Instant::now();
        bucket.acquire(2.0).await;
        assert_eq!(Instant::now(), start, "should not need to wait, tokens capped at capacity");
    }

    #[test]
    fn unknown_host_falls_back_to_conservative_default() {
        assert_eq!(bucket_config("https://unknown.example.com"), (5.0, 1.0));
    }

    #[test]
    fn known_hosts_are_differentiated_per_venue() {
        let binance_spot = bucket_config("https://api.binance.com");
        let binance_futures = bucket_config("https://fapi.binance.com");
        let kraken_spot = bucket_config("https://api.kraken.com");
        assert_ne!(binance_spot, binance_futures);
        assert_ne!(binance_spot, kraken_spot);
    }

    #[tokio::test]
    async fn bucket_for_returns_same_instance_for_same_host() {
        let a = bucket_for("https://api.binance.com");
        let b = bucket_for("https://api.binance.com");
        assert!(Arc::ptr_eq(&a, &b));
    }
}
