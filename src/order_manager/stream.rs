use std::sync::Arc;

use rust_decimal::Decimal;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::order::types::OrderStatus;
use crate::types::{Symbol, Venue};

use super::manager::OrderManager;

/// 交易所私有 WS 推送的一条订单状态更新。`filled_qty`/`avg_price` 均为该订单
/// 截至当前的累计值(不是本次推送的增量)，方便消费方直接覆盖存储的订单状态。
#[derive(Debug, Clone)]
pub struct ExchangeOrderUpdate {
    pub venue: Venue,
    pub symbol: Symbol,
    /// 下单时透传给交易所的客户端订单号，用于关联回内部 `OrderId`。
    pub client_order_id: Option<String>,
    /// 交易所自己的订单号，client_order_id 关联失败时的兜底关联键。
    pub exchange_order_id: Option<String>,
    pub status: OrderStatus,
    pub filled_qty: Decimal,
    pub avg_price: Option<Decimal>,
    /// 本次推送(增量)对应的手续费，语义上对齐 Binance `executionReport` 的
    /// `n`/`N`——是这一次 fill_delta 的手续费，不是订单累计值，和 filled_qty/
    /// avg_price(累计值)刻意不同，调用方(OrderManager::handle_exchange_update)
    /// 用它换算 USDT 等值去冲减 `PositionManager` 的已实现盈亏。
    pub fee: Option<Decimal>,
    pub fee_asset: Option<String>,
    pub ts_ms: u64,
}

/// 订单私有流扩展点：每个交易所实现一个 `OrderStreamSource`(通常是一个私有
/// WebSocket 客户端)，将订单成交/状态变化统一转换成 `ExchangeOrderUpdate`，
/// 在后台任务里直接调用 `OrderManager::handle_exchange_update` 消费——不经过
/// `TopicBus`，因为 `OrderManager` 才是成交状态的唯一权威来源，没有其他订阅方
/// 需要这条推送。
///
/// 和 `market_data::MarketDataSource` 同构：接入新交易所只需新增一个实现该
/// trait 的类型，在组装 `OrderManager` 的地方注册即可。
/// `spawn` 返回的句柄：`join` 用于 abort/等待任务结束，`ready` 在流首次
/// 连接+鉴权/订阅成功后 resolve 一次(断线重连不会重复发送)，供调用方在
/// 下单前先等私有流真正就绪——避免市价单成交速度快于 WS 建连速度，导致
/// 成交推送在订阅生效前就被交易所发出而永久错过。
pub struct StreamHandle {
    pub join: JoinHandle<()>,
    pub ready: oneshot::Receiver<()>,
}

pub trait OrderStreamSource: Send + 'static {
    fn venue(&self) -> Venue;

    /// 消费 self 并在后台任务中运行，把解析出的每条更新直接喂给
    /// `order_manager.handle_exchange_update`。
    fn spawn(self: Box<Self>, order_manager: Arc<OrderManager>) -> StreamHandle;
}
