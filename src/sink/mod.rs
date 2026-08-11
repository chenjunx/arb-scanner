pub mod log_sink;

use crate::strategy::Opportunity;

/// 套利机会的输出目的地扩展点：日志、告警、落库、下游执行系统等都可以
/// 实现该 trait 接入，引擎发现机会后会依次调用所有注册的 sink。
pub trait OpportunitySink: Send + Sync {
    fn handle(&self, opportunity: &Opportunity);
}
