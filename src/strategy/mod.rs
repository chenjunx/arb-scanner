pub mod cross_exchange;
pub mod manual;
pub mod triangular;

use std::sync::Arc;

use rust_decimal::Decimal;

use crate::market_data::now_ms;
use crate::order::types::{OrderAmount, OrderSide};
use crate::order_manager::types::{
    AnyOrderRequest, CancelRequest, OrderEvent, OrderKind, OrderRequest, TransferRequest,
};
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

/// 某个 venue 的手续费配置，用于在计算套利收益时扣除成本。`maker_bps` 默认
/// 等于 `taker_bps`（未显式配置更优的挂单费率时，不擅自假设成本更低）。
#[derive(Debug, Clone, Copy)]
pub struct FeeSchedule {
    pub taker_bps: Decimal,
    pub maker_bps: Decimal,
}

impl FeeSchedule {
    pub fn new(taker_bps: impl Into<Decimal>) -> Self {
        let taker_bps = taker_bps.into();
        Self {
            taker_bps,
            maker_bps: taker_bps,
        }
    }

    /// 覆盖 maker 费率（挂单成交时实际适用的费率，通常低于 taker）。
    pub fn with_maker_bps(mut self, maker_bps: impl Into<Decimal>) -> Self {
        self.maker_bps = maker_bps.into();
        self
    }

    /// 买入时实际付出的价格 = ask * buy_multiplier（手续费推高实际成本）。
    pub fn buy_multiplier(&self) -> Decimal {
        Decimal::ONE + self.taker_bps / Decimal::from(10_000)
    }

    /// 卖出时实际收到的价格 = bid * sell_multiplier（手续费压低实际收益）。
    pub fn sell_multiplier(&self) -> Decimal {
        Decimal::ONE - self.taker_bps / Decimal::from(10_000)
    }

    /// 挂单(maker)买入时实际付出的价格 = ask * maker_buy_multiplier。
    pub fn maker_buy_multiplier(&self) -> Decimal {
        Decimal::ONE + self.maker_bps / Decimal::from(10_000)
    }

    /// 挂单(maker)卖出时实际收到的价格 = bid * maker_sell_multiplier。
    pub fn maker_sell_multiplier(&self) -> Decimal {
        Decimal::ONE - self.maker_bps / Decimal::from(10_000)
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

    /// 订单事件回调：引擎为策略订阅了 `Topic::order_event(self.name())`，
    /// 收到该策略名下任意订单的状态变化时被调用。默认空实现——只有需要被动
    /// 感知订单状态（更新内部状态/记日志/触发后续动作）的策略才需要覆盖它；
    /// 像 `cross_exchange.rs::try_execute` 那样"下单后原地等这一笔的终态"的
    /// 同步流程不受影响，那是另一条独立的临时订阅。
    fn on_order_event(&self, event: &OrderEvent) {
        let _ = event;
    }

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
            order_kind: OrderKind::Market,
            client_order_id: Some(client_order_id),
            group_id,
            metadata,
            order_id: None,
        };
        self.bus().publish(Topic::order_submit(), AnyOrderRequest::Trade(request));
    }

    /// 提交限价 IOC 单到风控层：与 `submit_order` 相同的字段/发布逻辑，
    /// 只是 `amount` 固定按基础币数量、`order_kind` 带上限价。
    fn submit_limit_ioc_order(
        &self,
        venue: Venue,
        symbol: Symbol,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
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
            amount: OrderAmount::Base(quantity),
            order_kind: OrderKind::LimitIoc { price },
            client_order_id: Some(client_order_id),
            group_id,
            metadata,
            order_id: None,
        };
        self.bus().publish(Topic::order_submit(), AnyOrderRequest::Trade(request));
    }

    /// 提交 GTC 限价单到风控层：与 `submit_limit_ioc_order` 相同的字段/发布
    /// 逻辑，只是 `order_kind` 换成 `Limit`（挂单直到主动撤单或完全成交）。
    fn submit_limit_order(
        &self,
        venue: Venue,
        symbol: Symbol,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
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
            amount: OrderAmount::Base(quantity),
            order_kind: OrderKind::Limit { price },
            client_order_id: Some(client_order_id),
            group_id,
            metadata,
            order_id: None,
        };
        self.bus().publish(Topic::order_submit(), AnyOrderRequest::Trade(request));
    }

    /// 撤销一笔已提交的订单（通常是 `submit_limit_order` 挂出的 GTC 单）。
    /// 发布 `CancelRequest` 到 `Topic::order_cancel()`，真正的撤单执行和终态
    /// 确认由 `ExecutionService`/`OrderManager` 完成，策略只管发起请求。
    fn cancel_order(
        &self,
        venue: Venue,
        client_order_id: String,
        group_id: Option<String>,
        metadata: Option<String>,
    ) {
        let request = CancelRequest {
            strategy_id: self.name().to_string(),
            venue,
            client_order_id,
            group_id,
            metadata,
        };
        self.bus().publish(Topic::order_cancel(), request);
    }

    /// 提交划转单到风控层：在 `from_venue` 提币 `amount` 数量的 `symbol.base`，
    /// 划转到 `to_venue`；成功后仓位自动更新（由 ExecutionService 在提币成功时调用
    /// PositionManager::on_transfer 完成）。
    fn submit_transfer(
        &self,
        from_venue: Venue,
        to_venue: Venue,
        symbol: Symbol,
        amount: Decimal,
        network: Option<String>,
        dry_run: bool,
        client_order_id: Option<String>,
        group_id: Option<String>,
        metadata: Option<String>,
    ) {
        let client_order_id = client_order_id.unwrap_or_else(|| self.generate_client_order_id());
        let request = TransferRequest {
            strategy_id: self.name().to_string(),
            from_venue,
            to_venue,
            symbol,
            amount,
            network,
            dry_run,
            client_order_id: Some(client_order_id),
            group_id,
            metadata,
            order_id: None,
        };
        self.bus().publish(Topic::order_submit(), AnyOrderRequest::Transfer(request));
    }

    /// 生成 client_order_id，带随机后缀避免同一策略在同一毫秒内提交多笔订单时
    /// 撞车（`RiskService` 不再兜底生成，见 `risk_service.rs` 的改动）。
    fn generate_client_order_id(&self) -> String {
        format!("{}-{}-{:05}", self.name(), now_ms(), rand::random::<u32>() % 100000)
    }
}
