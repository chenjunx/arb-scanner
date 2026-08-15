use std::sync::Arc;
use std::time::Duration;

use log::warn;
use tokio::task::JoinHandle;

use crate::market_data::now_ms;

use super::channel::ReportChannel;
use super::section::ReportSection;
use super::types::{Report, ReportSectionOutput};

/// 独立常驻进程：定期把所有注册的 `ReportSection` 汇总成一份 `Report`，依次
/// 分发给所有注册的 `ReportChannel`。结构上仿照 `accounting::FundingFeeTracker`
/// ——`tokio::time::interval` 循环 + `spawn()`。某个 channel 发送失败只记录
/// 警告并继续尝试其它 channel，不影响下一轮报告生成。
pub struct ReportTracker {
    sections: Vec<Arc<dyn ReportSection>>,
    channels: Vec<Arc<dyn ReportChannel>>,
    interval: Duration,
}

impl ReportTracker {
    pub fn new(sections: Vec<Arc<dyn ReportSection>>, channels: Vec<Arc<dyn ReportChannel>>, interval: Duration) -> Self {
        Self { sections, channels, interval }
    }

    pub fn spawn(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.interval);
            loop {
                ticker.tick().await;
                self.generate_and_dispatch().await;
            }
        })
    }

    async fn generate_and_dispatch(&self) {
        let report = self.generate();
        for channel in &self.channels {
            if let Err(err) = channel.send(&report).await {
                warn!("report: channel failed to send report: {err:#}");
            }
        }
    }

    fn generate(&self) -> Report {
        Report {
            generated_at_ms: now_ms(),
            sections: self
                .sections
                .iter()
                .map(|s| ReportSectionOutput { title: s.title().to_string(), body: s.render() })
                .collect(),
        }
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
        let tracker = Arc::new(ReportTracker::new(
            vec![Arc::new(FixedSection)],
            vec![channel.clone()],
            Duration::from_secs(10),
        ));
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
        let tracker = Arc::new(ReportTracker::new(
            vec![Arc::new(FixedSection)],
            vec![Arc::new(FailingChannel), ok_channel.clone()],
            Duration::from_secs(5),
        ));
        let handle = tracker.spawn();

        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;

        assert_eq!(ok_channel.calls.load(Ordering::SeqCst), 1);
        handle.abort();
    }
}
