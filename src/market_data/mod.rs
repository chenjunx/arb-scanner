pub mod binance;
pub mod binance_futures;
pub mod kraken;
pub mod mock;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::types::{MarketEvent, Venue};

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// 行情数据源扩展点：每个交易所/场所实现一个 `MarketDataSource`，
/// 将行情转换为统一的 `MarketEvent` 推送到 channel。
///
/// 接入真实交易所时，只需新增一个实现该 trait 的类型（如 WS 客户端），
/// 在 main.rs 中注册即可，无需改动 engine/strategy 代码。
pub trait MarketDataSource: Send + 'static {
    fn venue(&self) -> Venue;

    /// 消费 self 并在后台任务中运行，持续向 tx 推送行情事件。
    fn spawn(self: Box<Self>, tx: mpsc::Sender<MarketEvent>) -> JoinHandle<()>;
}
