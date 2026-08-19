pub mod cross_exchange;
pub mod manual;
pub mod triangular;

use std::sync::Arc;

use rust_decimal::Decimal;

use crate::market_data::now_ms;
use crate::order::types::{OrderAmount, OrderSide};
use crate::order_manager::types::OrderRequest;
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

/// 某个 venue 的手续费配置，用于在计算套利收益时扣除成本。
#[derive(Debug, Clone, Copy)]
pub struct FeeSchedule {
    pub taker_bps: Decimal,
}

impl FeeSchedule {
    pub fn new(taker_bps: impl Into<Decimal>) -> Self {
        Self {
            taker_bps: taker_bps.into(),
        }
    }

    /// 买入时实际付出的价格 = ask * buy_multiplier（手续费推高实际成本）。
    pub fn buy_multiplier(&self) -> Decimal {
        Decimal::ONE + self.taker_bps / Decimal::from(10_000)
    }

    /// 卖出时实际收到的价格 = bid * sell_multiplier（手续费压低实际收益）。
    pub fn sell_multiplier(&self) -> Decimal {
        Decimal::ONE - self.taker_bps / Decimal::from(10_000)
    }
}

#[derive(Debug, Clone)]
pub enum OpportunityKind {
    CrossExchange {
        symbol: Symbol,
        buy_venue: Venue,
        sell_venue: Venue,
    },
    Triangular {
        venue: Venue,
        legs: [Symbol; 3],
    },
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub strategy: &'static str,
    pub kind: OpportunityKind,
    pub expected_profit_bps: Decimal,
    pub detail: String,
    pub ts_ms: u64,
}

/// 套利策略扩展点：每个策略声明自己关心的 topic 集合（`subscriptions`），
/// `ArbitrageEngine` 为每个策略订阅对应的行情流，收到行情后调用 `on_quote` 回调。
/// 策略维护内部状态（用内部可变性如 `Mutex`/`DashMap`），发现机会时直接打日志。
pub trait Strategy: Send + Sync {
    fn name(&self) -> &str;

    /// 声明这个策略关心哪些 topic；`ArbitrageEngine` 用它向 `TopicBus` 订阅。
    fn subscriptions(&self) -> Vec<Topic>;

    /// 行情回调：收到订阅的 topic 行情时被调用。策略内部维护状态，发现机会时打日志。
    fn on_quote(&self, topic: &Topic, quote: &Quote);

    /// 策略构造时存下的 `TopicBus` 引用，供默认方法 `submit_order` 发布订单请求。
    fn bus(&self) -> &Arc<TopicBus>;

    /// 提交订单到风控层：拼好 `OrderRequest`（`strategy_id` 用 `self.name()`，
    /// `client_order_id` 未指定时自动生成，`order_id` 留空待 `RiskService` 分配），
    /// 发布到 `Topic::OrderSubmit`。调用方（各策略实现）只需要关心订单本身的信息，
    /// 不用管 bus/topic 细节。
    fn submit_order(
        &self,
        venue: Venue,
        symbol: Symbol,
        side: OrderSide,
        amount: OrderAmount,
        client_order_id: Option<String>,
        group_id: Option<String>,
        metadata: Option<String>,
    ) {
        let client_order_id = client_order_id.unwrap_or_else(|| self.generate_client_order_id());
        let request = OrderRequest {
            strategy_id: self.name().to_string(),
            venue,
            symbol,
            side,
            amount,
            client_order_id: Some(client_order_id),
            group_id,
            metadata,
            order_id: None,
        };
        self.bus().publish(Topic::order_submit(), request);
    }

    /// 生成 client_order_id，带随机后缀避免同一策略在同一毫秒内提交多笔订单时
    /// 撞车（`RiskService` 不再兜底生成，见 `risk_service.rs` 的改动）。
    fn generate_client_order_id(&self) -> String {
        format!("{}-{}-{:05}", self.name(), now_ms(), rand::random::<u32>() % 100000)
    }
}
