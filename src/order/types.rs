use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::types::Symbol;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// 市价单的下单量：按基础币数量，或按计价币金额(如 Binance 的
/// `quoteOrderQty`)。后者下单前不知道精确的基础币数量，买/卖多少基础币由
/// 交易所按下单那一刻的价格决定。目前只有 `order::binance::BinanceOrderProvider`
/// (现货)支持 `Quote`，其它交易所收到 `Quote` 会报错，见 `OrderProvider` trait 说明。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum OrderAmount {
    Base(Decimal),
    Quote(Decimal),
}

impl OrderAmount {
    pub fn value(self) -> Decimal {
        match self {
            OrderAmount::Base(v) => v,
            OrderAmount::Quote(v) => v,
        }
    }
}

/// 市价单请求。
#[derive(Debug, Clone, PartialEq)]
pub struct MarketOrderRequest {
    pub symbol: Symbol,
    pub side: OrderSide,
    pub amount: OrderAmount,
    /// 幂等去重用的客户端订单号；不同交易所是否必填/长度限制不同，
    /// 具体由各交易所实现自行处理。
    pub client_order_id: Option<String>,
    /// true 时只做下单前校验(数量为正；数量精度/最小下单量由调用方通过
    /// `exchange_info::PrecisionCache` 提前转换好，这里不重复校验)，不真正
    /// 提交订单。
    pub dry_run: bool,
}

/// 限价 IOC (Immediate-or-Cancel) 单请求：以指定价格立即成交，未成交部分
/// 立刻撤销。三家交易所的限价单目前都只接受按基础币数量下单(没有市价单
/// `quoteOrderQty` 那种按计价币金额下单的等价参数)，因此 `quantity` 直接用
/// `Decimal`，不复用 `OrderAmount` 枚举。
#[derive(Debug, Clone, PartialEq)]
pub struct LimitIocOrderRequest {
    pub symbol: Symbol,
    pub side: OrderSide,
    /// 基础币数量；精度由调用方通过 `PrecisionCache::round_qty` 提前算好，
    /// 这一层只校验为正。
    pub quantity: Decimal,
    /// 限价；精度由调用方通过 `PrecisionCache::round_price` 提前对齐价格
    /// 步长，这一层只校验为正。
    pub price: Decimal,
    /// 语义同 `MarketOrderRequest::client_order_id`。
    pub client_order_id: Option<String>,
    /// 语义同 `MarketOrderRequest::dry_run`。
    pub dry_run: bool,
}

/// 普通限价单 (GTC, Good-Til-Cancelled) 请求：以指定价格挂单，一直有效直到
/// 成交或被撤销。字段和 `LimitIocOrderRequest` 完全一致，但故意保持独立类型
/// (不复用/改名 `LimitIocOrderRequest`)——沿用 `MarketOrderRequest`/
/// `LimitIocOrderRequest` 各自独立成型的既有惯例，避免为了共享字段牵动 IOC
/// 那边已有测试覆盖的类型和调用点。
#[derive(Debug, Clone, PartialEq)]
pub struct LimitOrderRequest {
    pub symbol: Symbol,
    pub side: OrderSide,
    /// 语义同 `LimitIocOrderRequest::quantity`。
    pub quantity: Decimal,
    /// 语义同 `LimitIocOrderRequest::price`。
    pub price: Decimal,
    /// 语义同 `MarketOrderRequest::client_order_id`。
    pub client_order_id: Option<String>,
    /// 语义同 `MarketOrderRequest::dry_run`。
    pub dry_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    /// 划转单：提币已提交、资金已从源头划出，但尚未通过余额变动确认到账
    Transferred,
    /// 划转单：到账事件已通过余额变动确认
    DepositConfirmed,
    Rejected,
    /// 未完全成交就终结、非用户撤单：目前只用于限价 IOC 单未成交部分被
    /// 交易所自动撤销。是否走这个分支还是 `Cancelled` 由
    /// `OrderManager::handle_exchange_update` 依据原始 `OrderKind` 判定，
    /// 见该函数注释。
    Expired,
    /// 用户主动撤单（或交易所因故强制撤销一笔非 IOC 单）后的终态，
    /// 与 `Expired`（IOC 自动失效）语义上刻意区分开。
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderResult {
    pub order_id: String,
    pub status: OrderStatus,
    pub filled_qty: Decimal,
    pub avg_price: Option<Decimal>,
    /// 交易所真实返还的手续费，拿不到时为 None (如 Kraken REST AddOrder 本身
    /// 不同步返回成交信息、Binance 合约缺私有流)，由 Portfolio 退化为估算。
    pub fee: Option<Decimal>,
    pub fee_asset: Option<String>,
}
