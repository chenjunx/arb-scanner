use std::sync::Arc;

use futures_util::StreamExt;

use crate::strategy::Strategy;
use crate::topic::TopicBus;

/// 套利引擎：编排器。为每个策略按其 `subscriptions()` 向 `TopicBus` 订阅，
/// 各自独立跑一个 tokio task，收到行情后调用策略的 `on_quote` 回调。
/// 策略自己维护内部状态并在发现机会时打日志，引擎本身不做业务逻辑。
pub struct ArbitrageEngine {
    strategies: Vec<Box<dyn Strategy>>,
}

impl ArbitrageEngine {
    pub fn new(strategies: Vec<Box<dyn Strategy>>) -> Self {
        Self { strategies }
    }

    pub async fn run(self, bus: Arc<TopicBus>) {
        let mut strategy_handles = Vec::new();
        for strategy in self.strategies {
            let mut quotes = bus.subscribe_many(strategy.subscriptions());
            strategy_handles.push(tokio::spawn(async move {
                while let Some((topic, quote)) = quotes.next().await {
                    strategy.on_quote(&topic, &quote);
                }
            }));
        }

        for handle in strategy_handles {
            let _ = handle.await;
        }
    }
}
