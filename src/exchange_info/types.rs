use rust_decimal::Decimal;

/// 某个交易对/合约在当前账户下的实际 maker/taker 手续费率，单位 bps
/// (基点，1bps = 0.01%)。和 `strategy::FeeSchedule` 里手动配置的固定值不同，
/// 这里的数值来自交易所 API 实时查询，反映账户当前的手续费等级/折扣。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TradingFee {
    pub maker_bps: Decimal,
    pub taker_bps: Decimal,
}
