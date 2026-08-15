use async_trait::async_trait;
use log::info;

use crate::report::channel::ReportChannel;
use crate::report::types::Report;

/// 最简单的 channel 实现：把生成的报告逐个 section 打到日志里，参考
/// `sink::log_sink::LogSink` 的写法。
pub struct LogChannel;

#[async_trait]
impl ReportChannel for LogChannel {
    async fn send(&self, report: &Report) -> anyhow::Result<()> {
        info!("report: generated_at_ms={}", report.generated_at_ms);
        for section in &report.sections {
            info!("report[{}]:\n{}", section.title, section.body);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::types::ReportSectionOutput;

    #[tokio::test]
    async fn send_succeeds() {
        let report = Report {
            generated_at_ms: 1,
            sections: vec![ReportSectionOutput { title: "t".to_string(), body: "b".to_string() }],
        };
        assert!(LogChannel.send(&report).await.is_ok());
    }
}
