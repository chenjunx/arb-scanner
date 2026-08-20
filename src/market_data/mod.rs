pub mod binance;
pub mod binance_futures;
pub mod cache;
pub mod kraken;
pub mod link_health;
pub mod mock;

use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::topic::TopicBus;
use crate::types::Venue;

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// 行情数据源扩展点：每个交易所/场所实现一个 `MarketDataSource`，
/// 将行情发布到 `TopicBus` 上按 (数据类型, venue, symbol) 区分的 topic。
///
/// 接入真实交易所时，只需新增一个实现该 trait 的类型（如 WS 客户端），
/// 在 main.rs 中注册即可，无需改动 engine/strategy 代码。
pub trait MarketDataSource: Send + 'static {
    fn venue(&self) -> Venue;

    /// 消费 self 并在后台任务中运行，持续把行情发布到 bus 上。
    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> JoinHandle<()>;
}
