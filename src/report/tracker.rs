use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use log::warn;
use tokio::task::JoinHandle;

use crate::market_data::now_ms;
use crate::topic::{BoxTopicStream, Topic, TopicBus};

use super::channel::ReportChannel;
use super::event::ReportEvent;
use super::section::ReportSection;
use super::types::{Report, ReportSectionOutput};

/// 独立常驻进程：定期把所有注册的 `ReportSection` 汇总成一份 `Report`，依次
/// 分发给所有注册的 `ReportChannel`。结构上仿照 `accounting::FundingFeeTracker`
/// ——`tokio::time::interval` 循环 + `spawn()`。某个 channel 发送失败只记录
/// 警告并继续尝试其它 channel，不影响下一轮报告生成。
///
/// 调用 [`Self::with_bus`] 后还会订阅 `Topic::report()`：各策略发布的状态快照
/// 按 `strategy_id` 只保留最新一份，每次出报告时跟在各 section 后面输出。
pub struct ReportTracker {
    core: Core,
    events: Option<BoxTopicStream<ReportEvent>>,
}

/// 出报告需要的部分，单独拆出来是因为订阅流不是 `Sync`，而 `&self` 要跨 await 使用。
struct Core {
    sections: Vec<Arc<dyn ReportSection>>,
    channels: Vec<Arc<dyn ReportChannel>>,
    interval: Duration,
}

/// 某个策略最新一份快照：`(发布时间, 内容)`。
type Snapshots = BTreeMap<String, (u64, ReportSectionOutput)>;

impl ReportTracker {
    pub fn new(sections: Vec<Arc<dyn ReportSection>>, channels: Vec<Arc<dyn ReportChannel>>, interval: Duration) -> Self {
        Self { core: Core { sections, channels, interval }, events: None }
    }

    /// 订阅 `Topic::report()` 接收策略快照。订阅在这里同步完成：总线对没有订阅者
    /// 的消息直接丢弃，所以必须在策略开始发布之前调用。
    pub fn with_bus(mut self, bus: &TopicBus) -> Self {
        self.events = Some(bus.subscribe(Topic::report()));
        self
    }

    pub fn spawn(self) -> JoinHandle<()> {
        let ReportTracker { core, mut events } = self;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(core.interval);
            let mut snapshots = Snapshots::new();
            loop {
                tokio::select! {
                    biased;
                    event = next_event(&mut events) => match event {
                        Some(ReportEvent::Snapshot { strategy_id, at_ms, section }) => {
                            snapshots.insert(strategy_id, (at_ms, section));
                        }
                        None => events = None,
                    },
                    _ = ticker.tick() => core.generate_and_dispatch(&snapshots).await,
                }
            }
        })
    }

}

impl Core {
    async fn generate_and_dispatch(&self, snapshots: &Snapshots) {
        let report = self.generate(snapshots);
        for channel in &self.channels {
            if let Err(err) = channel.send(&report).await {
                warn!("report: channel failed to send report: {err:#}");
            }
        }
    }

    fn generate(&self, snapshots: &Snapshots) -> Report {
        let now = now_ms();
        let mut sections: Vec<ReportSectionOutput> = self
            .sections
            .iter()
            .map(|s| ReportSectionOutput { title: s.title().to_string(), body: s.render() })
            .collect();
        let stale_after_ms = self.interval.as_millis() as u64 * 2;
        for (at_ms, section) in snapshots.values() {
            let age_ms = now.saturating_sub(*at_ms);
            let stale = if age_ms > stale_after_ms { " [过期]" } else { "" };
            sections.push(ReportSectionOutput {
                title: format!("{} ({}s前{stale})", section.title, age_ms / 1000),
                body: section.body.clone(),
            });
        }
        Report { generated_at_ms: now, sections }
    }
}

/// 下一条策略事件；没订阅(`None` 流)时永远 pending，让 `select!` 只等定时器。
/// 订阅流结束(总线已 drop)返回 `None`。
async fn next_event(events: &mut Option<BoxTopicStream<ReportEvent>>) -> Option<ReportEvent> {
    match events {
        Some(stream) => stream.next().await.map(|(_, event)| event),
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;

    struct FixedSection;
    impl ReportSection for FixedSection {
        fn title(&self) -> &str {
            "fixed"
        }
        fn render(&self) -> String {
            "body".to_string()
        }
    }

    struct CountingChannel {
        calls: AtomicUsize,
        last_report_sections: Mutex<usize>,
    }

    #[async_trait]
    impl ReportChannel for CountingChannel {
        async fn send(&self, report: &Report) -> anyhow::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last_report_sections.lock().unwrap() = report.sections.len();
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn spawn_ticks_and_dispatches_to_channel() {
        let channel = Arc::new(CountingChannel { calls: AtomicUsize::new(0), last_report_sections: Mutex::new(0) });
        let tracker = ReportTracker::new(vec![Arc::new(FixedSection)], vec![channel.clone()], Duration::from_secs(10));
        let handle = tracker.spawn();

        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;

        assert_eq!(channel.calls.load(Ordering::SeqCst), 2);
        assert_eq!(*channel.last_report_sections.lock().unwrap(), 1);
        handle.abort();
    }

    struct FailingChannel;
    #[async_trait]
    impl ReportChannel for FailingChannel {
        async fn send(&self, _report: &Report) -> anyhow::Result<()> {
            anyhow::bail!("boom")
        }
    }

    #[tokio::test(start_paused = true)]
    async fn one_failing_channel_does_not_block_others() {
        let ok_channel = Arc::new(CountingChannel { calls: AtomicUsize::new(0), last_report_sections: Mutex::new(0) });
        let tracker = ReportTracker::new(
            vec![Arc::new(FixedSection)],
            vec![Arc::new(FailingChannel), ok_channel.clone()],
            Duration::from_secs(5),
        );
        let handle = tracker.spawn();

        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;

        assert_eq!(ok_channel.calls.load(Ordering::SeqCst), 1);
        handle.abort();
    }

    /// 记录收到的每份报告。
    struct RecordingChannel {
        reports: Mutex<Vec<Report>>,
    }

    #[async_trait]
    impl ReportChannel for RecordingChannel {
        async fn send(&self, report: &Report) -> anyhow::Result<()> {
            self.reports.lock().unwrap().push(Report {
                generated_at_ms: report.generated_at_ms,
                sections: report.sections.clone(),
            });
            Ok(())
        }
    }

    fn snapshot(id: &str, at_ms: u64, body: &str) -> ReportEvent {
        ReportEvent::Snapshot {
            strategy_id: id.to_string(),
            at_ms,
            section: ReportSectionOutput { title: id.to_string(), body: body.to_string() },
        }
    }

    async fn tick(secs: u64) {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(secs)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn snapshots_keep_only_latest_per_strategy() {
        let bus = TopicBus::new();
        let channel = Arc::new(RecordingChannel { reports: Mutex::new(Vec::new()) });
        let handle = ReportTracker::new(vec![Arc::new(FixedSection)], vec![channel.clone()], Duration::from_secs(10))
            .with_bus(&bus)
            .spawn();

        let now = now_ms();
        bus.publish(Topic::report(), snapshot("s1", now, "old"));
        bus.publish(Topic::report(), snapshot("s1", now, "new"));
        bus.publish(Topic::report(), snapshot("s2", now, "other"));
        tick(10).await;

        let reports = channel.reports.lock().unwrap();
        let sections = &reports.last().unwrap().sections;
        assert_eq!(sections.len(), 3, "fixed + s1 + s2");
        assert_eq!(sections[0].title, "fixed");
        assert!(sections[1].title.starts_with("s1 ("));
        assert_eq!(sections[1].body, "new");
        assert_eq!(sections[2].body, "other");
        assert!(!sections[1].title.contains("过期"));
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn stale_snapshot_is_marked() {
        let bus = TopicBus::new();
        let channel = Arc::new(RecordingChannel { reports: Mutex::new(Vec::new()) });
        let handle = ReportTracker::new(vec![], vec![channel.clone()], Duration::from_secs(10)).with_bus(&bus).spawn();

        bus.publish(Topic::report(), snapshot("s1", 0, "ancient"));
        tick(10).await;

        let reports = channel.reports.lock().unwrap();
        assert_eq!(reports.last().unwrap().sections.len(), 1);
        assert!(reports.last().unwrap().sections[0].title.contains("[过期]"));
        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn without_bus_no_strategy_sections() {
        let bus = TopicBus::new();
        let channel = Arc::new(RecordingChannel { reports: Mutex::new(Vec::new()) });
        let handle = ReportTracker::new(vec![Arc::new(FixedSection)], vec![channel.clone()], Duration::from_secs(10)).spawn();

        bus.publish(Topic::report(), snapshot("s1", now_ms(), "ignored"));
        tick(10).await;

        let reports = channel.reports.lock().unwrap();
        assert_eq!(reports.last().unwrap().sections.len(), 1);
        handle.abort();
    }
}
