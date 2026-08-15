use async_trait::async_trait;

use super::types::Report;

/// 报告输出渠道的扩展点：日志、邮箱、钉钉等都可以实现该 trait 接入，
/// `ReportTracker` 每次生成报告后会依次调用所有注册的 channel。用
/// `async_trait` 是因为未来的邮箱(SMTP)/钉钉(HTTP webhook)渠道都需要异步
/// I/O，和 `order::OrderProvider`/`accounting::FundingFeeProvider` 同样的
/// 设计语言。
#[async_trait]
pub trait ReportChannel: Send + Sync {
    async fn send(&self, report: &Report) -> anyhow::Result<()>;
}
