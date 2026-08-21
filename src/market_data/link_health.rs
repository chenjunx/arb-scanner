use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use futures_util::StreamExt;
use log::{info, warn};
use tokio::task::JoinHandle;

use super::now_ms;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

/// 单条链路健康判据。以后要新增判断规则（比如订单簿深度异常、下单延迟过高）
/// 只需实现这个 trait 并注册进 `LinkHealthMonitor::checks`，不用改调用方。
pub trait HealthCheck: Send + Sync {
    fn is_healthy(&self, venue: &Venue) -> bool;
}

/// 心跳新鲜度规则：每条 venue 链路额外订阅各自的一个"探针"交易对（如
/// binance 用 BTC/USDT、kraken 用 BTC/USD——不同交易所报价币种习惯不同，
/// 探针需要按 venue 配置），只要求在 `window_ms` 内收到过它的报价推送就算
/// 健康——不看价格是否变化，只看 WS 连接是否还在正常推送数据。用本地接收
/// 时间而不是交易所自带的时间戳，避免交易所时钟偏差影响判断。
struct HeartbeatFreshness {
    probe_symbols: HashMap<Venue, Symbol>,
    window_ms: u64,
    last_seen: DashMap<Venue, u64>,
}

impl HeartbeatFreshness {
    fn new(probe_symbols: HashMap<Venue, Symbol>, window_ms: u64) -> Self {
        Self {
            probe_symbols,
            window_ms,
            last_seen: DashMap::new(),
        }
    }
}

impl HealthCheck for HeartbeatFreshness {
    fn is_healthy(&self, venue: &Venue) -> bool {
        match self.last_seen.get(venue) {
            Some(entry) => now_ms().saturating_sub(*entry) <= self.window_ms,
            None => false,
        }
    }
}

/// 链路健康监控：一个 venue 要被判定为健康，必须通过所有已注册的规则。
/// `CrossExchangeStrategy` 用它替代原来"单条报价新鲜度"的过滤——只有
/// buy/sell 两侧链路都健康，算出的价差才计入 opportunity。
pub struct LinkHealthMonitor {
    heartbeat: Arc<HeartbeatFreshness>,
    checks: Vec<Arc<dyn HealthCheck>>,
    last_health: DashMap<Venue, bool>,
}

impl LinkHealthMonitor {
    /// `probe_symbols` 按 venue 各自指定探针交易对，key 即为参与心跳监控的
    /// venue 集合——`spawn` 直接从这里派生要订阅的 topic，不用再单独传一份
    /// venue 列表（避免两处列表脱节）。
    pub fn new(probe_symbols: HashMap<Venue, Symbol>, window_ms: u64) -> Self {
        let heartbeat = Arc::new(HeartbeatFreshness::new(probe_symbols, window_ms));
        Self {
            heartbeat: heartbeat.clone(),
            checks: vec![heartbeat],
            last_health: DashMap::new(),
        }
    }

    /// 不接心跳探针、永远视为健康——供不想接入链路监控的调用方使用。
    pub fn always_healthy() -> Self {
        Self {
            heartbeat: Arc::new(HeartbeatFreshness::new(HashMap::new(), 0)),
            checks: Vec::new(),
            last_health: DashMap::new(),
        }
    }

    pub fn is_healthy(&self, venue: &Venue) -> bool {
        let healthy = self.checks.iter().all(|check| check.is_healthy(venue));
        let prev = self.last_health.get(venue).map(|v| *v);
        if prev != Some(healthy) {
            self.last_health.insert(venue.clone(), healthy);
            if healthy {
                info!("link recovered: venue={venue}");
            } else {
                warn!("link unhealthy: venue={venue}");
            }
        }
        healthy
    }

    /// 后台订阅每个 venue 上各自探针交易对的行情，每收到一次推送就把该 venue
    /// 的"最近心跳时间"刷新为本地当前时间。
    pub fn spawn(self: Arc<Self>, bus: Arc<TopicBus>) -> JoinHandle<()> {
        let topics: Vec<Topic> = self
            .heartbeat
            .probe_symbols
            .iter()
            .map(|(venue, symbol)| Topic::quote(venue.clone(), symbol.clone()))
            .collect();
        tokio::spawn(async move {
            let mut quotes = bus.subscribe_many::<Quote>(topics);
            while let Some((topic, _)) = quotes.next().await {
                if let Topic::Quote { venue, .. } = topic {
                    self.heartbeat.last_seen.insert(venue, now_ms());
                }
            }
        })
    }

    #[cfg(test)]
    pub fn mark_seen_for_test(&self, venue: &Venue) {
        self.heartbeat.last_seen.insert(venue.clone(), now_ms());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quote(bid: &str, ask: &str) -> Quote {
        Quote {
            bid: bid.parse().unwrap(),
            bid_size: rust_decimal::Decimal::ONE,
            ask: ask.parse().unwrap(),
            ask_size: rust_decimal::Decimal::ONE,
            ts_ms: 1,
        }
    }

    fn probes_for(venues: &[&Venue]) -> HashMap<Venue, Symbol> {
        venues.iter().map(|v| ((*v).clone(), Symbol::new("BTC", "USDT"))).collect()
    }

    #[test]
    fn unhealthy_when_never_seen() {
        let venue = Venue::new("binance_spot");
        let monitor = LinkHealthMonitor::new(probes_for(&[&venue]), 5000);
        assert!(!monitor.is_healthy(&venue));
    }

    #[test]
    fn healthy_within_window_after_seen() {
        let venue = Venue::new("binance_spot");
        let monitor = LinkHealthMonitor::new(probes_for(&[&venue]), 5000);
        monitor.mark_seen_for_test(&venue);
        assert!(monitor.is_healthy(&venue));
    }

    #[test]
    fn unhealthy_after_window_elapses() {
        let venue = Venue::new("binance_spot");
        let monitor = LinkHealthMonitor::new(probes_for(&[&venue]), 10);
        monitor.mark_seen_for_test(&venue);
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(!monitor.is_healthy(&venue));
    }

    #[test]
    fn always_healthy_reports_healthy_for_any_venue() {
        let monitor = LinkHealthMonitor::always_healthy();
        assert!(monitor.is_healthy(&Venue::new("binance_spot")));
        assert!(monitor.is_healthy(&Venue::new("kraken")));
    }

    #[tokio::test]
    async fn spawn_updates_health_from_bus_heartbeat() {
        let bus = Arc::new(TopicBus::new());
        let venue = Venue::new("binance_spot");
        let probe_symbol = Symbol::new("BTC", "USDT");
        let topic = Topic::quote(venue.clone(), probe_symbol.clone());

        let monitor = Arc::new(LinkHealthMonitor::new(HashMap::from([(venue.clone(), probe_symbol)]), 5000));
        let handle = monitor.clone().spawn(bus.clone());

        assert!(!monitor.is_healthy(&venue));

        loop {
            bus.publish(topic.clone(), quote("100", "101"));
            if monitor.is_healthy(&venue) {
                break;
            }
            tokio::task::yield_now().await;
        }

        handle.abort();
    }
}
