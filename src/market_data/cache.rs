use std::sync::Arc;

use dashmap::DashMap;
use futures_util::StreamExt;
use tokio::task::JoinHandle;

use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

/// 独立于 `TopicBus` 维护的行情缓存：订阅一批 `Topic::Quote`，把每次收到的
/// 最新 `Quote` 按 `(venue, symbol)` 存下来，供不想成为 `TopicBus` 订阅者、
/// 只想偶尔查一次当前价格的调用方使用（如 `PortfolioManager` mark-to-market）。
pub struct MarketDataCache {
    quotes: Arc<DashMap<(Venue, Symbol), Quote>>,
}

impl MarketDataCache {
    pub fn new() -> Self {
        Self { quotes: Arc::new(DashMap::new()) }
    }

    /// 暴露内部存储，供需要 `Arc<DashMap<(Venue, Symbol), Quote>>` 的调用方
    /// 直接持有（如 `PortfolioManager::quote_cache`），共享同一份数据。
    pub fn snapshot(&self) -> Arc<DashMap<(Venue, Symbol), Quote>> {
        self.quotes.clone()
    }

    pub fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<Quote> {
        self.quotes.get(&(venue.clone(), symbol.clone())).map(|entry| *entry.value())
    }

    /// 后台订阅 `topics`（应为 `Topic::Quote` 集合），持续把最新行情写入缓存，
    /// 直到 `bus` 上对应 channel 全部关闭。
    pub fn spawn(self: Arc<Self>, bus: Arc<TopicBus>, topics: Vec<Topic>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut quotes = bus.subscribe_many::<Quote>(topics);
            while let Some((topic, quote)) = quotes.next().await {
                if let Topic::Quote { venue, symbol } = topic {
                    self.quotes.insert((venue, symbol), quote);
                }
            }
        })
    }
}

impl Default for MarketDataCache {
    fn default() -> Self {
        Self::new()
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

    #[tokio::test]
    async fn caches_latest_quote_for_subscribed_topics() {
        let bus = Arc::new(TopicBus::new());
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");
        let topic = Topic::quote(venue.clone(), symbol.clone());

        let cache = Arc::new(MarketDataCache::new());
        let handle = cache.clone().spawn(bus.clone(), vec![topic.clone()]);

        assert!(cache.get(&venue, &symbol).is_none());

        let first = quote("100", "101");
        bus.publish(topic.clone(), first);
        let second = quote("102", "103");
        loop {
            bus.publish(topic.clone(), second);
            if cache.get(&venue, &symbol) == Some(second) {
                break;
            }
            tokio::task::yield_now().await;
        }

        handle.abort();
    }

    #[tokio::test]
    async fn ignores_quotes_for_topics_not_subscribed() {
        let bus = Arc::new(TopicBus::new());
        let watched_venue = Venue::new("binance_spot");
        let other_venue = Venue::new("kraken");
        let symbol = Symbol::new("BTC", "USDT");

        let cache = Arc::new(MarketDataCache::new());
        let handle =
            cache.clone().spawn(bus.clone(), vec![Topic::quote(watched_venue.clone(), symbol.clone())]);

        bus.publish(Topic::quote(other_venue.clone(), symbol.clone()), quote("1", "2"));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(cache.get(&other_venue, &symbol).is_none());
        handle.abort();
    }
}
