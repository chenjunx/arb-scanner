use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::types::Symbol;

use super::ExchangeInfoProvider;
use super::types::{MarketPrecision, PrecisionKind, QtyPrecision};

/// 启动时一次性加载进内存的下单精度缓存。下单路径(手动 `execution`、以后
/// 的自动策略)都查这个缓存做精度转换，`order::OrderProvider` 不再关心精度，
/// 只信任调用方传进来的数量已经合法。
pub struct PrecisionCache {
    by_symbol: HashMap<Symbol, MarketPrecision>,
}

impl PrecisionCache {
    pub async fn load_spot(provider: &dyn ExchangeInfoProvider) -> anyhow::Result<Self> {
        let precisions = provider.spot_market_precisions().await?;
        Ok(Self::from_precisions(precisions))
    }

    pub async fn load_perpetual(provider: &dyn ExchangeInfoProvider) -> anyhow::Result<Self> {
        let precisions = provider.perpetual_market_precisions().await?;
        Ok(Self::from_precisions(precisions))
    }

    pub fn from_precisions(precisions: Vec<MarketPrecision>) -> Self {
        let by_symbol = precisions.into_iter().map(|p| (p.symbol.clone(), p)).collect();
        Self { by_symbol }
    }

    fn qty_precision(&self, symbol: &Symbol, kind: PrecisionKind) -> anyhow::Result<QtyPrecision> {
        let info = self
            .by_symbol
            .get(symbol)
            .ok_or_else(|| anyhow::anyhow!("no market precision cached for {symbol}"))?;
        Ok(match kind {
            PrecisionKind::Market => info.market,
            PrecisionKind::Limit => info.limit,
        })
    }

    /// 把下单数量向下抹到该 symbol、该下单方式合法的步进，并校验不低于
    /// min_qty(否则报错，调用方不需要自己再判断一次)。
    pub fn round_qty(&self, symbol: &Symbol, kind: PrecisionKind, qty: Decimal) -> anyhow::Result<Decimal> {
        let precision = self.qty_precision(symbol, kind)?;
        let rounded = floor_to_step(qty, precision.qty_step);
        if rounded < precision.min_qty {
            anyhow::bail!(
                "quantity {rounded} below min_qty {} for {symbol} ({kind:?})",
                precision.min_qty
            );
        }
        Ok(rounded)
    }

    pub fn round_price(&self, symbol: &Symbol, price: Decimal) -> anyhow::Result<Decimal> {
        let info = self
            .by_symbol
            .get(symbol)
            .ok_or_else(|| anyhow::anyhow!("no market precision cached for {symbol}"))?;
        Ok(floor_to_step(price, info.price_tick))
    }
}

/// 把 `qty` 向下取整到 `step` 的整数倍；`step<=0` 时原样返回。
fn floor_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return qty;
    }
    (qty / step).trunc() * step
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_precision(symbol: Symbol) -> MarketPrecision {
        MarketPrecision {
            symbol,
            market: QtyPrecision {
                qty_step: Decimal::new(1, 2),
                min_qty: Decimal::new(1, 1),
            },
            limit: QtyPrecision {
                qty_step: Decimal::new(1, 3),
                min_qty: Decimal::new(1, 2),
            },
            price_tick: Decimal::new(1, 4),
        }
    }

    #[test]
    fn floor_to_step_rounds_down_to_multiple() {
        assert_eq!(floor_to_step(Decimal::new(12345, 4), Decimal::new(1, 2)), Decimal::new(123, 2));
    }

    #[test]
    fn floor_to_step_passes_through_when_step_non_positive() {
        let qty = Decimal::new(12345, 4);
        assert_eq!(floor_to_step(qty, Decimal::ZERO), qty);
    }

    #[test]
    fn round_qty_uses_market_precision_when_requested() {
        let symbol = Symbol::new("BTC", "USDT");
        let cache = PrecisionCache::from_precisions(vec![sample_precision(symbol.clone())]);
        let rounded = cache
            .round_qty(&symbol, PrecisionKind::Market, "1.239".parse().unwrap())
            .expect("should round");
        assert_eq!(rounded, "1.23".parse().unwrap());
    }

    #[test]
    fn round_qty_uses_limit_precision_when_requested() {
        let symbol = Symbol::new("BTC", "USDT");
        let cache = PrecisionCache::from_precisions(vec![sample_precision(symbol.clone())]);
        let rounded = cache
            .round_qty(&symbol, PrecisionKind::Limit, "1.2399".parse().unwrap())
            .expect("should round");
        assert_eq!(rounded, "1.239".parse().unwrap());
    }

    #[test]
    fn round_qty_errors_when_below_min_qty() {
        let symbol = Symbol::new("BTC", "USDT");
        let cache = PrecisionCache::from_precisions(vec![sample_precision(symbol.clone())]);
        let err = cache
            .round_qty(&symbol, PrecisionKind::Market, "0.05".parse().unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("below min_qty"));
    }

    #[test]
    fn round_qty_errors_for_unknown_symbol() {
        let cache = PrecisionCache::from_precisions(Vec::new());
        let err = cache
            .round_qty(&Symbol::new("BTC", "USDT"), PrecisionKind::Market, Decimal::ONE)
            .unwrap_err();
        assert!(err.to_string().contains("no market precision cached"));
    }

    #[test]
    fn round_price_floors_to_tick() {
        let symbol = Symbol::new("BTC", "USDT");
        let cache = PrecisionCache::from_precisions(vec![sample_precision(symbol.clone())]);
        let rounded = cache.round_price(&symbol, "100.12349".parse().unwrap()).expect("should round");
        assert_eq!(rounded, "100.1234".parse().unwrap());
    }

    #[test]
    fn round_price_errors_for_unknown_symbol() {
        let cache = PrecisionCache::from_precisions(Vec::new());
        let err = cache.round_price(&Symbol::new("BTC", "USDT"), Decimal::ONE).unwrap_err();
        assert!(err.to_string().contains("no market precision cached"));
    }
}
