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

/// 某种下单方式(市价/限价)各自的数量精度限制。币安市价单校验用
/// MARKET_LOT_SIZE、限价单用 LOT_SIZE，两者的 stepSize/minQty 都可能不同；
/// Kraken 没有这个区分，market/limit 两份填相同值。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QtyPrecision {
    pub qty_step: Decimal,
    pub min_qty: Decimal,
}

/// 某个 symbol 完整的下单精度信息。`price_tick` 目前只有限价单会用到
/// (市价单不传价格)，先占位。
#[derive(Debug, Clone, PartialEq)]
pub struct MarketPrecision {
    pub symbol: Symbol,
    pub market: QtyPrecision,
    pub limit: QtyPrecision,
    pub price_tick: Decimal,
}

/// 下单方式，决定 [`MarketPrecision`] 里查哪一份 [`QtyPrecision`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrecisionKind {
    Market,
    Limit,
}

/// 一次已经结算的资金费率。`rate` 是小数比例(如 `0.0001` 表示 0.01%)，
/// 正数=多头付给空头。
#[derive(Debug, Clone, PartialEq)]
pub struct FundingRateRecord {
    pub symbol: Symbol,
    pub rate: Decimal,
    pub funding_time_ms: u64,
}

/// 下一次(即将到来的那一次)结算的预测资金费率。各交易所字段命名不统一
/// (币安 `lastFundingRate`、Gate `funding_rate`、CoinEx `latest_funding_rate`)，
/// 实现方负责统一成"将在 `next_funding_time_ms` 结算的费率"这一语义。
///
/// `funding_interval_hours` 是该合约的结算周期：同样的 `rate`，1h 结算和 8h
/// 结算的年化收益差 8 倍，跨交易所/跨币种比较前必须按周期换算。
#[derive(Debug, Clone, PartialEq)]
pub struct PredictedFundingRate {
    pub symbol: Symbol,
    pub rate: Decimal,
    pub next_funding_time_ms: u64,
    pub funding_interval_hours: u32,
}

/// USDT 本位永续合约的计价/下单单位，用来把各交易所的报价和下单量统一换算到
/// "每个币"的口径。两个系数分开是因为交易所之间的约定不同：
/// - 币安 `1000PEPEUSDT`：报价是 1000 个 PEPE 的价格，数量 1 也是 1000 个
///   PEPE，两个系数都是 1000。
/// - Gate `PEPE_USDT`：报价是 1 个 PEPE 的价格(`price_unit=1`)，数量单位是
///   张，一张 = `quanto_multiplier` 个 PEPE(`qty_unit=10000000`)。
///
/// 每币价格 = 报价 / `price_unit`；交易所原生下单量 = 币数 / `qty_unit`。
///
/// 系统内部(订单请求、订单记录、成交回报、仓位)的数量统一用**合约 base 单位**
/// ——也就是报价所对应的那个单位(Gate/OKX 是 1 个币，币安 `1000PEPEUSDT` 是
/// 1000 个 PEPE)，和交易所界面显示的持仓数量一致，乘报价直接得到 USDT。
/// 张数只存在于 Gate/OKX 的下单网关里，换算系数见 [`Self::contract_size`]。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PerpContractSpec {
    pub price_unit: Decimal,
    pub qty_unit: Decimal,
}

impl PerpContractSpec {
    /// 1 个交易所原生下单单位(张)折合多少合约 base 单位：Gate `FUTUON_USDT`
    /// 是 0.01，币安/Bitget 恒为 1。
    pub fn contract_size(&self) -> Decimal {
        self.qty_unit / self.price_unit
    }

    /// 真实币数 → 合约 base 单位(只有带 `1000` 之类前缀的合约才不是 1:1)。
    pub fn coins_to_base(&self, coins: Decimal) -> Decimal {
        coins / self.price_unit
    }

    /// 合约 base 单位 → 真实币数。
    pub fn base_to_coins(&self, qty: Decimal) -> Decimal {
        qty * self.price_unit
    }
}
