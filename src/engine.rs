use std::sync::Arc;

use futures_util::StreamExt;

use crate::order_manager::types::OrderEvent;
use crate::strategy::Strategy;
use crate::topic::{Topic, TopicBus};

/// 套利引擎：编排器。为每个策略按其 `subscriptions()` 向 `TopicBus` 订阅行情，
/// 并额外订阅该策略名下的 `Topic::order_event`，各自独立跑一个 tokio task，
/// 收到行情调用 `on_quote`、收到订单事件调用 `on_order_event`。
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
            let mut order_events = bus.subscribe::<OrderEvent>(Topic::order_event(strategy.name().to_string()));
            strategy_handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        quote = quotes.next() => match quote {
                            Some((topic, quote)) => strategy.on_quote(&topic, &quote),
                            None => break,
                        },
                        event = order_events.next() => match event {
                            Some((_, event)) => strategy.on_order_event(&event),
                            None => break,
                        },
                    }
                }
            }));
        }

        for handle in strategy_handles {
            let _ = handle.await;
        }
    }
}
