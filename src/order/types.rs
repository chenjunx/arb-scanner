use rust_decimal::Decimal;

use crate::types::Symbol;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// 市价单的下单量：按基础币数量，或按计价币金额(如 Binance 的
/// `quoteOrderQty`)。后者下单前不知道精确的基础币数量，买/卖多少基础币由
/// 交易所按下单那一刻的价格决定。目前只有 `order::binance::BinanceOrderProvider`
/// (现货)支持 `Quote`，其它交易所收到 `Quote` 会报错，见 `OrderProvider` trait 说明。
#[derive(Debug, Clone, Copy, PartialEq)]
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
    /// true 时只做下单前校验(数量精度/最小下单量，`Quote` 只校验金额为正)，
    /// 不真正提交订单。
    pub dry_run: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

/// 某个交易对的下单精度/最小下单量限制，用于下单前校验。
#[derive(Debug, Clone, PartialEq)]
pub struct MarketInfo {
    pub symbol: Symbol,
    /// 数量步进，下单数量必须是它的整数倍(如 0.001)。
    pub qty_step: Decimal,
    pub min_qty: Decimal,
}
