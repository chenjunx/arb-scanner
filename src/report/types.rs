/// 单个 `ReportSection` 渲染出的一节内容，`body` 是已经格式化好的多行文本。
pub struct ReportSectionOutput {
    pub title: String,
    pub body: String,
}

/// 一次完整的报告快照，由 `ReportTracker` 汇总各 `ReportSection` 的输出后
/// 分发给所有 `ReportChannel`。
pub struct Report {
    pub generated_at_ms: u64,
    pub sections: Vec<ReportSectionOutput>,
}

impl Report {
    /// 各渠道通用的纯文本兜底格式化，按 section 顺序拼接标题和正文。
    pub fn as_plain_text(&self) -> String {
        let mut out = format!("=== 报告 (generated_at_ms={}) ===\n", self.generated_at_ms);
        for section in &self.sections {
            out.push_str(&format!("-- {} --\n{}\n", section.title, section.body));
        }
        out
    }
}
