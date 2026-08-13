use rust_decimal::Decimal;

use crate::types::Symbol;

/// 某个交易对/合约在当前账户下的实际 maker/taker 手续费率，单位 bps
/// (基点，1bps = 0.01%)。和 `strategy::FeeSchedule` 里手动配置的固定值不同，
/// 这里的数值来自交易所 API 实时查询，反映账户当前的手续费等级/折扣。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradingFee {
    pub maker_bps: Decimal,
    pub taker_bps: Decimal,
}

/// 同一交易所内可互相对冲的一对现货/USDT 本位永续合约。`contract_multiplier`
/// 是永续合约一张相当于多少现货数量(如币安 `1000PEPEUSDT` 一张合约=1000个
/// 现货 PEPE，multiplier=1000)，绝大多数币种没有这个换算，multiplier=1。
/// 对冲下单时现货数量要除以这个倍数才是应下的永续张数。
#[derive(Debug, Clone, PartialEq)]
pub struct SpotPerpPair {
    pub spot_symbol: Symbol,
    pub perp_symbol: Symbol,
    pub contract_multiplier: u64,
}
