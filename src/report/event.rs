use super::types::ReportSectionOutput;

/// 发布到 `Topic::report()` 的消息，由 `ReportTracker` 订阅。任何持有 `TopicBus` 的模块
/// (策略、行情源、各类 tracker……)都可以发布。
#[derive(Debug, Clone)]
pub enum ReportEvent {
    /// 某个来源当前状态的快照：`ReportTracker` 按 `source_id` 只保留最新一份，
    /// 下一次出报告时作为一节输出。
    Snapshot { source_id: String, at_ms: u64, section: ReportSectionOutput },
}

impl ReportEvent {
    /// 以当前时间构造一份快照；`source_id` 同时作为去重键，`title` 是报告里该节的标题。
    pub fn snapshot(source_id: impl Into<String>, title: impl Into<String>, body: String) -> Self {
        Self::Snapshot {
            source_id: source_id.into(),
            at_ms: crate::market_data::now_ms(),
            section: ReportSectionOutput { title: title.into(), body },
        }
    }
}
