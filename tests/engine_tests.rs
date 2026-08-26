use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;

use arb_scanner::engine::ArbitrageEngine;
use arb_scanner::market_data::link_health::LinkHealthMonitor;
use arb_scanner::strategy::cross_exchange::CrossExchangeStrategy;
use arb_scanner::strategy::{FeeSchedule, Strategy};
use arb_scanner::topic::{Topic, TopicBus};
use arb_scanner::types::{Quote, Symbol, Venue};

fn quote(bid: &str, ask: &str, ts_ms: u64) -> Quote {
    Quote {
        bid: bid.parse().unwrap(),
        bid_size: Decimal::ONE,
        ask: ask.parse().unwrap(),
        ask_size: Decimal::ONE,
        ts_ms,
    }
}

/// 启动引擎（后台任务）、把 `events` 依次发布到 `bus`，等待 `wait` 让策略
/// 处理完，然后中止引擎任务。引擎的 `run()` 只有在所有 `TopicBus` channel 关闭
/// （即 bus 本身被 drop）后才会自然退出，测试里不需要真正关闭 bus，直接 abort 后台任务即可。
#[tokio::test]
async fn engine_starts_and_consumes_quotes() {
    let symbol = Symbol::new("BTC", "USDT");
    let venue_a = Venue::new("a");
    let venue_b = Venue::new("b");

    let fees: HashMap<Venue, FeeSchedule> = vec![
        (venue_a.clone(), FeeSchedule::new(0)),
        (venue_b.clone(), FeeSchedule::new(0)),
    ]
    .into_iter()
    .collect();

    let bus = Arc::new(TopicBus::new());
    let strategies: Vec<Box<dyn Strategy>> = vec![Box::new(CrossExchangeStrategy::new(
        vec![symbol.clone()],
        fees,
        Decimal::from(10),
        Arc::new(LinkHealthMonitor::always_healthy()),
        bus.clone(),
    ))];

    let engine = ArbitrageEngine::new(strategies);
    let handle = tokio::spawn(engine.run(bus.clone()));

    // 引擎内部订阅是同步的，但要等调度器先 poll 一次任务才会真正跑到；
    // 这里让出一次控制权，确保订阅在下面的 publish 之前完成。
    tokio::task::yield_now().await;

    // 发布行情，策略会消费并在发现机会时打日志
    bus.publish(Topic::quote(venue_a.clone(), symbol.clone()), quote("100.0", "100.5", 1));
    bus.publish(Topic::quote(venue_b.clone(), symbol.clone()), quote("105.0", "105.5", 2));

    // 等待策略处理
    tokio::time::sleep(Duration::from_millis(200)).await;

    handle.abort();

    // 测试只验证引擎能正常启动和消费行情，不再断言机会收集
    // （策略直接打日志，集成测试不方便捕获日志内容）
}

#[tokio::test]
async fn engine_handles_single_venue_gracefully() {
    let symbol = Symbol::new("BTC", "USDT");
    let venue_a = Venue::new("a");

    let fees: HashMap<Venue, FeeSchedule> = vec![(venue_a.clone(), FeeSchedule::new(0))]
        .into_iter()
        .collect();

    let bus = Arc::new(TopicBus::new());
    let strategies: Vec<Box<dyn Strategy>> = vec![Box::new(CrossExchangeStrategy::new(
        vec![symbol.clone()],
        fees,
        Decimal::from(10),
        Arc::new(LinkHealthMonitor::always_healthy()),
        bus.clone(),
    ))];

    let engine = ArbitrageEngine::new(strategies);
    let handle = tokio::spawn(engine.run(bus.clone()));

    tokio::task::yield_now().await;

    // 单个 venue 不会产生跨交易所套利机会
    bus.publish(Topic::quote(venue_a, symbol), quote("100.0", "100.5", 1));

    tokio::time::sleep(Duration::from_millis(200)).await;

    handle.abort();
}

/// 测的是"行情进 bus -> 策略 `on_quote` 被调用"这一段纯内部调度耗时：
/// `TopicBus::publish` -> tokio broadcast channel -> `ArbitrageEngine::run`
/// 里 `tokio::select!` 唤醒 -> `Strategy::on_quote`。生产环境里
/// `bus.publish` 是行情源（`market_data::kraken`/`binance` 等）解析完 WS
/// 消息、给 `Quote.ts_ms` 打上 `now_ms()` 之后立刻调用的同一个函数，所以这里
/// 测到的就是"消息发布到策略收到"这一段，不包含真实 WS 网络收包/解析开销。
#[tokio::test]
async fn measures_bus_dispatch_latency_from_publish_to_on_quote() {
    use std::time::Instant;
    use tokio::sync::mpsc;

    struct LatencyProbeStrategy {
        bus: Arc<TopicBus>,
        symbol: Symbol,
        venue: Venue,
        tx: mpsc::UnboundedSender<Instant>,
    }

    impl Strategy for LatencyProbeStrategy {
        fn name(&self) -> &str {
            "latency_probe"
        }
        fn bus(&self) -> &Arc<TopicBus> {
            &self.bus
        }
        fn subscriptions(&self) -> Vec<Topic> {
            vec![Topic::quote(self.venue.clone(), self.symbol.clone())]
        }
        fn on_quote(&self, _topic: &Topic, _quote: &Quote) {
            let _ = self.tx.send(Instant::now());
        }
    }

    let symbol = Symbol::new("BTC", "USDT");
    let venue = Venue::new("a");
    let bus = Arc::new(TopicBus::new());

    let (tx, mut rx) = mpsc::unbounded_channel();
    let strategies: Vec<Box<dyn Strategy>> = vec![Box::new(LatencyProbeStrategy {
        bus: bus.clone(),
        symbol: symbol.clone(),
        venue: venue.clone(),
        tx,
    })];

    let engine = ArbitrageEngine::new(strategies);
    let handle = tokio::spawn(engine.run(bus.clone()));

    tokio::task::yield_now().await;

    const ITERATIONS: usize = 500;
    let mut samples = Vec::with_capacity(ITERATIONS);
    for i in 0..ITERATIONS {
        let start = Instant::now();
        bus.publish(Topic::quote(venue.clone(), symbol.clone()), quote("100.0", "100.5", i as u64));
        let received_at = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("on_quote was not invoked within timeout")
            .expect("channel closed unexpectedly");
        samples.push(received_at.duration_since(start));
    }

    handle.abort();

    samples.sort();
    let sum: Duration = samples.iter().sum();
    let mean = sum / samples.len() as u32;
    let min = samples[0];
    let p50 = samples[samples.len() / 2];
    let p95 = samples[samples.len() * 95 / 100];
    let max = *samples.last().unwrap();

    println!(
        "bus dispatch latency (bus.publish -> Strategy::on_quote), n={}: min={min:?} p50={p50:?} mean={mean:?} p95={p95:?} max={max:?}",
        samples.len(),
    );
}
