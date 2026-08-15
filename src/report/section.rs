/// 报告内容的扩展点：新增一类信息（风险指标、资金费明细等）只需实现该
/// trait 并注册进 `ReportTracker`，不用改动 tracker/channel 任何代码。
/// 现有的数据源（`PositionManager`/`PortfolioManager`/`OrderStore`）读方法
/// 全部是同步、无阻塞 I/O 的查询，所以 `render()` 是同步方法。
pub trait ReportSection: Send + Sync {
    fn title(&self) -> &str;
    fn render(&self) -> String;
}
