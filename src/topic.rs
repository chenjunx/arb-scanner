use std::fmt;
use std::pin::Pin;

use dashmap::DashMap;
use futures_util::stream::{Stream, StreamExt, select_all};
use log::warn;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::order_manager::types::{OrderEvent, OrderRequest};
use crate::types::{Quote, Symbol, Venue};

const CHANNEL_CAPACITY: usize = 1024;

/// Topic：区分总线上流转的消息种类和路由维度。每个 variant 对应唯一一种
/// payload 类型(通过 `BusMessage` trait 关联)，`TopicBus::publish`/`subscribe`
/// 因此可以是统一的泛型方法，不需要为每种数据类型起不同的方法名。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Topic {
    /// 行情数据：venue + symbol 级别，payload=`Quote`
    Quote { venue: Venue, symbol: Symbol },
    /// 订单提交：策略 -> 风控层，全局单一 topic，payload=`OrderRequest`
    OrderSubmit,
    /// 订单执行：风控层 -> 执行层，全局单一 topic，payload=`OrderRequest`
    OrderExecute,
    /// 订单事件：执行层/OrderManager -> 策略，按 strategy_id 路由，payload=`OrderEvent`
    OrderEvent { strategy_id: String },
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Quote { venue, symbol } => write!(f, "quote.{}.{}", venue, symbol),
            Self::OrderSubmit => write!(f, "orders.submit"),
            Self::OrderExecute => write!(f, "orders.execute"),
            Self::OrderEvent { strategy_id } => write!(f, "events.order.{}", strategy_id),
        }
    }
}

impl Topic {
    pub fn quote(venue: Venue, symbol: Symbol) -> Self {
        Self::Quote { venue, symbol }
    }

    pub fn order_submit() -> Self {
        Self::OrderSubmit
    }

    pub fn order_execute() -> Self {
        Self::OrderExecute
    }

    pub fn order_event(strategy_id: impl Into<String>) -> Self {
        Self::OrderEvent {
            strategy_id: strategy_id.into(),
        }
    }
}

pub type BoxTopicStream<T> = Pin<Box<dyn Stream<Item = (Topic, T)> + Send>>;

/// 把一个 `broadcast::Receiver<T>` 包装成 `(Topic, T)` 流：`Lagged` 时记 warn
/// 日志并跳过，不中断整条流。
fn lagged_filter_map<T>(
    topic: Topic,
    receiver: broadcast::Receiver<T>,
) -> impl Stream<Item = (Topic, T)> + Send + use<T>
where
    T: Clone + Send + 'static,
{
    BroadcastStream::new(receiver).filter_map(move |result| {
        let topic = topic.clone();
        std::future::ready(match result {
            Ok(value) => Some((topic, value)),
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                warn!("topic bus subscriber lagged by {n} messages for topic={topic}");
                None
            }
        })
    })
}

/// 通用的 pub/sub 总线。对外只暴露统一的 `publish(topic, data)` /
/// `subscribe(topic)` / `subscribe_many(topics)`，具体路由到哪个内部存储、
/// 用哪种 payload 类型，完全由 `topic` 的 variant 决定——调用方不需要为不同
/// 数据类型记住不同的方法名。这靠 `BusMessage` trait 把"给定 payload 类型
/// 该怎么存/怎么发"的逻辑关联到类型本身，`TopicBus` 的公开方法只是转发。
pub struct TopicBus {
    quote_channels: DashMap<Topic, broadcast::Sender<Quote>>,
    quote_latest: DashMap<Topic, Quote>,
    order_submit_channel: broadcast::Sender<OrderRequest>,
    order_execute_channel: broadcast::Sender<OrderRequest>,
    order_event_channels: DashMap<String, broadcast::Sender<OrderEvent>>,
}

impl TopicBus {
    pub fn new() -> Self {
        Self {
            quote_channels: DashMap::new(),
            quote_latest: DashMap::new(),
            order_submit_channel: broadcast::channel(CHANNEL_CAPACITY).0,
            order_execute_channel: broadcast::channel(CHANNEL_CAPACITY).0,
            order_event_channels: DashMap::new(),
        }
    }

    /// 发布一条消息。payload 类型 `T` 必须实现 `BusMessage`，具体路由(存到
    /// 哪个 channel、要不要更新"最新值"缓存)由 `T::publish` 决定。
    pub fn publish<T: BusMessage>(&self, topic: Topic, data: T) {
        T::publish(self, topic, data);
    }

    /// 订阅单个 topic。
    pub fn subscribe<T: BusMessage>(&self, topic: Topic) -> BoxTopicStream<T> {
        T::subscribe(self, topic)
    }

    /// 订阅多个 topic，合并成一条流(主要用于按 venue/symbol 拆分的 `Quote`
    /// topic)。传入的 topic 先去重，避免同一 topic 出现两次导致重复消费。
    pub fn subscribe_many<T: BusMessage>(&self, topics: Vec<Topic>) -> BoxTopicStream<T> {
        let mut seen = std::collections::HashSet::new();
        let streams: Vec<_> = topics
            .into_iter()
            .filter(|topic| seen.insert(topic.clone()))
            .map(|topic| T::subscribe(self, topic))
            .collect();
        Box::pin(select_all(streams))
    }

    /// 获取行情的最新快照，供不想成为订阅者、只想偶尔读一次当前值的调用方
    /// 使用(如 `PortfolioManager` mark-to-market)。
    pub fn latest_quote(&self, topic: &Topic) -> Option<Quote> {
        self.quote_latest.get(topic).map(|entry| *entry.value())
    }

    fn quote_sender_for(&self, topic: &Topic) -> broadcast::Sender<Quote> {
        self.quote_channels
            .entry(topic.clone())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .clone()
    }

    fn order_event_sender_for(&self, strategy_id: &str) -> broadcast::Sender<OrderEvent> {
        self.order_event_channels
            .entry(strategy_id.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .clone()
    }
}

impl Default for TopicBus {
    fn default() -> Self {
        Self::new()
    }
}

/// 把"某种 payload 类型该怎么在 `TopicBus` 上发布/订阅"关联到类型本身，
/// 让 `TopicBus::publish`/`subscribe` 能保持统一签名。每个 variant 对应
/// 的 topic 只允许用它关联的 payload 类型调用，类型不匹配在编译期就会报错
/// (例如不可能对 `Topic::Quote` 调用 `publish::<OrderEvent>`，因为
/// `Quote`/`OrderEvent` 各自的 `BusMessage` 实现里对不匹配的 topic 只会
/// warn 并丢弃，不会 panic)。
pub trait BusMessage: Clone + Send + 'static {
    fn publish(bus: &TopicBus, topic: Topic, data: Self);
    fn subscribe(bus: &TopicBus, topic: Topic) -> BoxTopicStream<Self>;
}

impl BusMessage for Quote {
    fn publish(bus: &TopicBus, topic: Topic, data: Self) {
        if !matches!(topic, Topic::Quote { .. }) {
            warn!("TopicBus::publish: topic {topic} does not carry Quote data");
            return;
        }
        bus.quote_latest.insert(topic.clone(), data);
        let sender = bus.quote_sender_for(&topic);
        let _ = sender.send(data);
    }

    fn subscribe(bus: &TopicBus, topic: Topic) -> BoxTopicStream<Self> {
        if !matches!(topic, Topic::Quote { .. }) {
            warn!("TopicBus::subscribe: topic {topic} does not carry Quote data");
            return Box::pin(futures_util::stream::empty());
        }
        let receiver = bus.quote_sender_for(&topic).subscribe();
        Box::pin(lagged_filter_map(topic, receiver))
    }
}

impl BusMessage for OrderRequest {
    fn publish(bus: &TopicBus, topic: Topic, data: Self) {
        match topic {
            Topic::OrderSubmit => {
                let _ = bus.order_submit_channel.send(data);
            }
            Topic::OrderExecute => {
                let _ = bus.order_execute_channel.send(data);
            }
            _ => warn!("TopicBus::publish: topic {topic} does not carry OrderRequest data"),
        }
    }

    fn subscribe(bus: &TopicBus, topic: Topic) -> BoxTopicStream<Self> {
        match topic {
            Topic::OrderSubmit => {
                let receiver = bus.order_submit_channel.subscribe();
                Box::pin(lagged_filter_map(topic, receiver))
            }
            Topic::OrderExecute => {
                let receiver = bus.order_execute_channel.subscribe();
                Box::pin(lagged_filter_map(topic, receiver))
            }
            _ => {
                warn!("TopicBus::subscribe: topic {topic} does not carry OrderRequest data");
                Box::pin(futures_util::stream::empty())
            }
        }
    }
}

impl BusMessage for OrderEvent {
    fn publish(bus: &TopicBus, topic: Topic, data: Self) {
        match &topic {
            Topic::OrderEvent { strategy_id } => {
                let sender = bus.order_event_sender_for(strategy_id);
                let _ = sender.send(data);
            }
            _ => warn!("TopicBus::publish: topic {topic} does not carry OrderEvent data"),
        }
    }

    fn subscribe(bus: &TopicBus, topic: Topic) -> BoxTopicStream<Self> {
        match &topic {
            Topic::OrderEvent { strategy_id } => {
                let receiver = bus.order_event_sender_for(strategy_id).subscribe();
                Box::pin(lagged_filter_map(topic.clone(), receiver))
            }
            _ => {
                warn!("TopicBus::subscribe: topic {topic} does not carry OrderEvent data");
                Box::pin(futures_util::stream::empty())
            }
        }
    }
}
