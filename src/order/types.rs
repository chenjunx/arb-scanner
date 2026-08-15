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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Rejected,
    Expired,
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
