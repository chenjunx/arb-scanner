use rust_decimal::Decimal;
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::order::types::{OrderAmount, OrderSide, OrderStatus};
use crate::types::{Symbol, Venue};

/// 订单唯一标识符，由 OrderManager 生成
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderId(pub Arc<str>);

impl OrderId {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for OrderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 订单类型：市价单，或限价 IOC (Immediate-or-Cancel) 单。
/// `#[serde(default)]` 用在 `OrderRequest::order_kind` 上，反序列化旧 Redis
/// 记录（没有这个字段）时自动当作 `Market` 处理，向后兼容。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderKind {
    Market,
    LimitIoc { price: Decimal },
}

impl Default for OrderKind {
    fn default() -> Self {
        OrderKind::Market
    }
}

/// 策略提交的交易订单请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    #[serde(default)]
    pub strategy_id: String,
    pub venue: Venue,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub amount: OrderAmount,
    #[serde(default)]
    pub order_kind: OrderKind,
    pub client_order_id: Option<String>,
    pub group_id: Option<String>,
    pub metadata: Option<String>,
    pub order_id: Option<OrderId>,
}

/// 策略提交的划转请求：从一个交易所钱包提币到另一个交易所。
/// `symbol` 用于仓位追踪（如 BTC/USDT），`amount` 是 base 币种的数量。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferRequest {
    #[serde(default)]
    pub strategy_id: String,
    /// 提币方交易所
    pub from_venue: Venue,
    /// 收款方交易所
    pub to_venue: Venue,
    /// 仓位追踪用的交易对（如 BTC/USDT）；base 即为划转币种
    pub symbol: Symbol,
    /// 划转数量（base 币种单位）
    pub amount: Decimal,
    /// 链网络，None 时在 `from_venue`/`to_venue` 之间自动匹配共同链
    #[serde(default)]
    pub network: Option<String>,
    /// true 时只做校验，不真正发起提币
    #[serde(default)]
    pub dry_run: bool,
    pub client_order_id: Option<String>,
    pub group_id: Option<String>,
    pub metadata: Option<String>,
    pub order_id: Option<OrderId>,
}

/// 统一的订单提交类型，走同一条总线管道。
/// 序列化时写入 `"kind"` 标签字段；反序列化时若没有该字段（兼容旧 Redis 记录），
/// 自动当作 `Trade` 处理。
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AnyOrderRequest {
    Trade(OrderRequest),
    Transfer(TransferRequest),
}

impl AnyOrderRequest {
    pub fn strategy_id(&self) -> &str {
        match self {
            Self::Trade(r) => &r.strategy_id,
            Self::Transfer(r) => &r.strategy_id,
        }
    }

    pub fn order_id(&self) -> Option<&OrderId> {
        match self {
            Self::Trade(r) => r.order_id.as_ref(),
            Self::Transfer(r) => r.order_id.as_ref(),
        }
    }

    pub fn set_order_id(&mut self, id: OrderId) {
        match self {
            Self::Trade(r) => r.order_id = Some(id),
            Self::Transfer(r) => r.order_id = Some(id),
        }
    }

    pub fn client_order_id(&self) -> Option<&str> {
        match self {
            Self::Trade(r) => r.client_order_id.as_deref(),
            Self::Transfer(r) => r.client_order_id.as_deref(),
        }
    }

    /// 主 venue（Trade 的执行交易所；Transfer 的 from_venue）
    pub fn from_venue(&self) -> &Venue {
        match self {
            Self::Trade(r) => &r.venue,
            Self::Transfer(r) => &r.from_venue,
        }
    }

    pub fn as_trade(&self) -> Option<&OrderRequest> {
        if let Self::Trade(r) = self { Some(r) } else { None }
    }

    pub fn as_transfer(&self) -> Option<&TransferRequest> {
        if let Self::Transfer(r) = self { Some(r) } else { None }
    }
}

/// 兼容旧 Redis 记录（无 `kind` 字段）：若有 `kind` 标签按正常路径反序列化，
/// 否则按 `OrderRequest` 反序列化并包装成 `Trade`。
impl<'de> Deserialize<'de> for AnyOrderRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = serde_json::Value::deserialize(deserializer)?;
        if raw.get("kind").is_some() {
            #[derive(Deserialize)]
            #[serde(tag = "kind", rename_all = "snake_case")]
            enum Tagged {
                Trade(OrderRequest),
                Transfer(TransferRequest),
            }
            serde_json::from_value::<Tagged>(raw)
                .map(|t| match t {
                    Tagged::Trade(r) => AnyOrderRequest::Trade(r),
                    Tagged::Transfer(r) => AnyOrderRequest::Transfer(r),
                })
                .map_err(serde::de::Error::custom)
        } else {
            serde_json::from_value::<OrderRequest>(raw)
                .map(AnyOrderRequest::Trade)
                .map_err(serde::de::Error::custom)
        }
    }
}

/// 经过 RiskService 增强后的内部订单
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: OrderId,
    pub request: AnyOrderRequest,
    pub status: OrderStatus,
    /// 已成交/已划转数量
    pub filled_qty: Decimal,
    /// 平均成交价（划转单始终为 None）
    pub avg_price: Option<Decimal>,
    /// 交易所订单 ID 或提币 ID
    pub exchange_order_id: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub reject_reason: Option<String>,
}

/// 订单事件，用于通知策略
#[derive(Debug, Clone)]
pub enum OrderEvent {
    Submitted {
        order_id: OrderId,
        client_order_id: Option<String>,
    },
    Accepted {
        order_id: OrderId,
        client_order_id: Option<String>,
    },
    RejectedByRisk {
        order_id: OrderId,
        client_order_id: Option<String>,
        reason: String,
    },
    RejectedByExchange {
        order_id: OrderId,
        client_order_id: Option<String>,
        reason: String,
    },
    PartiallyFilled {
        order_id: OrderId,
        client_order_id: Option<String>,
        filled_qty: Decimal,
        avg_price: Decimal,
    },
    Filled {
        order_id: OrderId,
        client_order_id: Option<String>,
        filled_qty: Decimal,
        avg_price: Decimal,
    },
    /// 划转单已成功提币（仓位已同步更新）
    Transferred {
        order_id: OrderId,
        client_order_id: Option<String>,
        from_venue: Venue,
        to_venue: Venue,
        qty: Decimal,
        withdraw_id: String,
    },
    /// 划转到账确认：余额变动事件与划转单匹配，到账量与请求量偏差在 10% 以内
    TransferConfirmed {
        order_id: OrderId,
        client_order_id: Option<String>,
        to_venue: Venue,
        asset: String,
        actual_delta: Decimal,
    },
}

impl OrderEvent {
    pub fn order_id(&self) -> &OrderId {
        match self {
            Self::Submitted { order_id, .. }
            | Self::Accepted { order_id, .. }
            | Self::RejectedByRisk { order_id, .. }
            | Self::RejectedByExchange { order_id, .. }
            | Self::PartiallyFilled { order_id, .. }
            | Self::Filled { order_id, .. }
            | Self::Transferred { order_id, .. }
            | Self::TransferConfirmed { order_id, .. } => order_id,
        }
    }

    pub fn client_order_id(&self) -> Option<&str> {
        match self {
            Self::Submitted { client_order_id, .. }
            | Self::Accepted { client_order_id, .. }
            | Self::RejectedByRisk { client_order_id, .. }
            | Self::RejectedByExchange { client_order_id, .. }
            | Self::PartiallyFilled { client_order_id, .. }
            | Self::Filled { client_order_id, .. }
            | Self::Transferred { client_order_id, .. }
            | Self::TransferConfirmed { client_order_id, .. } => client_order_id.as_deref(),
        }
    }
}

/// 风控检查结果
#[derive(Debug, Clone)]
pub enum RiskCheckResult {
    Approved,
    Rejected { reason: String },
}

/// 订单提交的响应，包含订单ID和结果通道
pub struct OrderResponse {
    pub order_id: OrderId,
    pub result_rx: oneshot::Receiver<Result<Order, String>>,
}
