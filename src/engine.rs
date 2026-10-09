use std::sync::Arc;

use anyhow::Context;
use futures_util::StreamExt;
use log::info;

use crate::order_manager::types::OrderEvent;
use crate::strategy::{Strategy, StrategySubscriber};
use crate::topic::{Topic, TopicBus};

/// 套利引擎：编排器。为每个策略订阅其名下的 `Topic::order_event`，调用
/// [`Strategy::init`] 让策略自己订阅行情，然后各自独立跑一个 tokio task，
/// 收到行情调用 `on_quote`、收到订单事件调用 `on_order_event`。
/// 策略自己维护内部状态并在发现机会时打日志，引擎本身不做业务逻辑。
pub struct ArbitrageEngine {
    strategies: Vec<Box<dyn Strategy>>,
}

impl ArbitrageEngine {
    pub fn new(strategies: Vec<Box<dyn Strategy>>) -> Self {
        Self { strategies }
    }

    /// 依次为每个策略订阅订单事件并调用 [`Strategy::init`]（策略在里面订阅行情），
    /// 全部成功后才进入各自的事件循环；任一策略初始化失败则返回错误、不启动任何策略。
    pub async fn run(self, bus: Arc<TopicBus>) -> anyhow::Result<()> {
        // 订阅流不是 `Sync`，只按值持有、不跨 await 借用，否则 `run` 的 future 不是 `Send`。
        let mut prepared = Vec::with_capacity(self.strategies.len());
        for strategy in self.strategies {
            let order_events = bus.subscribe::<OrderEvent>(Topic::order_event(strategy.name().to_string()));
            let mut subscriber = StrategySubscriber::new(bus.clone());
            info!("strategy {}: init start", strategy.name());
            strategy
                .init(&mut subscriber)
                .await
                .with_context(|| format!("strategy {} init failed", strategy.name()))?;
            info!(
                "strategy {}: init done, subscribed {} quote topics",
                strategy.name(),
                subscriber.topics().len()
            );
            prepared.push((strategy, subscriber.into_quote_stream(), order_events));
        }

        let mut strategy_handles = Vec::new();
        for (strategy, mut quotes, mut order_events) in prepared {
            strategy_handles.push(tokio::spawn(async move {
                // 两条流各自结束后只停止轮询那一条，都结束才退出。`init` 里没订阅
                // 任何行情的策略，行情流是 `select_all([])`，一开始就
                // 结束，不能因此连订单事件也不收了。
                let mut quotes_done = false;
                let mut order_events_done = false;
                while !(quotes_done && order_events_done) {
                    tokio::select! {
                        quote = quotes.next(), if !quotes_done => match quote {
                            Some((topic, quote)) => strategy.on_quote(&topic, &quote),
                            None => quotes_done = true,
                        },
                        event = order_events.next(), if !order_events_done => match event {
                            Some((_, event)) => strategy.on_order_event(&event),
                            None => order_events_done = true,
                        },
                    }
                }
            }));
        }

        for handle in strategy_handles {
            let _ = handle.await;
        }
        Ok(())
    }
}
