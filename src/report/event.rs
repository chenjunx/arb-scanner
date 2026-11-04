use super::types::ReportSectionOutput;

/// 策略发布到 `Topic::report()` 的消息，由 `ReportTracker` 订阅。
#[derive(Debug, Clone)]
pub enum ReportEvent {
    /// 策略当前状态的快照：`ReportTracker` 按 `strategy_id` 只保留最新一份，
    /// 下一次出报告时作为一节输出。
    Snapshot { strategy_id: String, at_ms: u64, section: ReportSectionOutput },
}
