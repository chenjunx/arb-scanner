use rust_decimal::Decimal;
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

/// 策略提交的订单请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    /// 策略名称，用于跟踪和风控
    pub strategy_name: String,
    /// 目标交易所
    pub venue: Venue,
    /// 交易对
    pub symbol: Symbol,
    /// 买卖方向
    pub side: OrderSide,
    /// 订单数量
    pub amount: OrderAmount,
    /// 可选的客户端订单ID
    pub client_order_id: Option<String>,
    /// 用于关联多条腿的组ID (如套利的买卖两条腿)
    pub group_id: Option<String>,
    /// 策略附加元数据
    pub metadata: Option<String>,
}

/// 经过 OrderManager 增强后的内部订单
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// 内部订单ID
    pub order_id: OrderId,
    /// 原始请求
    pub request: OrderRequest,
    /// 订单状态
    pub status: OrderStatus,
    /// 已成交数量
    pub filled_qty: Decimal,
    /// 平均成交价
    pub avg_price: Option<Decimal>,
    /// 交易所返回的订单ID
    pub exchange_order_id: Option<String>,
    /// 创建时间戳 (毫秒)
    pub created_at_ms: u64,
    /// 最后更新时间戳 (毫秒)
    pub updated_at_ms: u64,
    /// 拒绝原因 (如果被风控或交易所拒绝)
    pub reject_reason: Option<String>,
}

/// 订单事件，用于通知策略
#[derive(Debug, Clone)]
pub enum OrderEvent {
    /// 订单已提交到风控引擎
    Submitted {
        order_id: OrderId,
    },
    /// 订单通过风控，已发送到交易所
    Accepted {
        order_id: OrderId,
    },
    /// 订单被风控拒绝
    RejectedByRisk {
        order_id: OrderId,
        reason: String,
    },
    /// 订单被交易所拒绝
    RejectedByExchange {
        order_id: OrderId,
        reason: String,
    },
    /// 部分成交
    PartiallyFilled {
        order_id: OrderId,
        filled_qty: Decimal,
        avg_price: Decimal,
    },
    /// 完全成交
    Filled {
        order_id: OrderId,
        filled_qty: Decimal,
        avg_price: Decimal,
    },
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
    /// 用于接收最终成交结果的通道
    pub result_rx: oneshot::Receiver<Result<Order, String>>,
}
