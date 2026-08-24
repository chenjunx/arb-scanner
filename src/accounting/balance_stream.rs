use std::sync::Arc;

use rust_decimal::Decimal;

use crate::order_manager::stream::StreamHandle;
use crate::topic::TopicBus;
use crate::types::Venue;

/// 交易所账户余额变动事件（来自私有 WS 推送）。
/// `delta` 为正表示入账（收款），为负表示出账（提币/手续费）。
#[derive(Debug, Clone)]
pub struct BalanceUpdate {
    pub venue: Venue,
    /// 币种代码，如 "BTC"、"USDT"
    pub asset: String,
    /// 本次变动量（正=入账，负=出账）
    pub delta: Decimal,
    pub ts_ms: u64,
}

/// 余额流扩展点：每个交易所实现一个 `BalanceStreamSource`，从私有 WS 推流中
/// 解析余额变动事件，通过 `TopicBus::publish(Topic::BalanceUpdate, ...)` 发布。
///
/// 和 `OrderStreamSource` 同构：接入新交易所只需新增一个实现该 trait 的类型，
/// 并在组装时注入 `TransferMonitor`。
pub trait BalanceStreamSource: Send + 'static {
    fn venue(&self) -> Venue;

    /// 消费 self 并在后台任务中运行，把解析出的每条余额变动事件发布到 `bus`。
    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> StreamHandle;
}
