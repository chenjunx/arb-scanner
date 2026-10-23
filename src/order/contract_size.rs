use std::sync::Arc;

use anyhow::Context;
use dashmap::DashMap;
use rust_decimal::Decimal;

use crate::exchange_info::ExchangeInfoProvider;
use crate::order::types::OrderResult;
use crate::order_manager::stream::ExchangeOrderUpdate;
use crate::types::Symbol;

/// 按张下单的交易所(Gate/OKX 永续)的面值缓存：1 张折合多少合约 base 单位
/// (见 [`PerpContractSpec::contract_size`](crate::exchange_info::types::PerpContractSpec::contract_size))。
///
/// 下单网关和私有订单流共用同一份(`Arc`)：网关下单时查过的合约一定已经在
/// 缓存里，订单流收到这笔订单的推送时不需要再查接口。没查过的合约(比如网页
/// 手动下的单)按需现查。
pub struct ContractSizeCache {
    exchange_info: Arc<dyn ExchangeInfoProvider>,
    sizes: DashMap<Symbol, Decimal>,
}

impl ContractSizeCache {
    pub fn new(exchange_info: Arc<dyn ExchangeInfoProvider>) -> Self {
        Self { exchange_info, sizes: DashMap::new() }
    }

    pub async fn get(&self, symbol: &Symbol) -> anyhow::Result<Decimal> {
        if let Some(size) = self.sizes.get(symbol) {
            return Ok(*size);
        }
        let spec = self
            .exchange_info
            .perpetual_contract_spec(symbol)
            .await
            .with_context(|| format!("failed to load contract size for {symbol}"))?;
        let size = spec.contract_size();
        if size <= Decimal::ZERO {
            anyhow::bail!("invalid contract size {size} for {symbol}");
        }
        self.sizes.insert(symbol.clone(), size);
        Ok(size)
    }

    /// 合约 base 单位 → 张。
    pub async fn to_contracts(&self, symbol: &Symbol, qty: Decimal) -> anyhow::Result<Decimal> {
        Ok((qty / self.get(symbol).await?).normalize())
    }

    /// 张 → 合约 base 单位。
    pub async fn from_contracts(&self, symbol: &Symbol, contracts: Decimal) -> anyhow::Result<Decimal> {
        Ok((contracts * self.get(symbol).await?).normalize())
    }

    /// 把下单/查单 REST 结果里按张计的成交量换成合约 base 单位。
    pub async fn convert_result(&self, symbol: &Symbol, mut result: OrderResult) -> anyhow::Result<OrderResult> {
        result.filled_qty = self.from_contracts(symbol, result.filled_qty).await?;
        Ok(result)
    }

    /// 把私有订单流里按张计的成交量换成合约 base 单位。推送里没有合约名或者
    /// 查不到面值时返回 Err——调用方不能把张数原样交给 `OrderManager`，否则
    /// 仓位会按张入账。
    pub async fn convert_update(&self, mut update: ExchangeOrderUpdate) -> anyhow::Result<ExchangeOrderUpdate> {
        let symbol = update.symbol.clone().context("order update has no contract symbol")?;
        update.filled_qty = self.from_contracts(&symbol, update.filled_qty).await?;
        Ok(update)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::exchange_info::types::{MarketPrecision, PerpContractSpec, TradingFee};
    use crate::order::types::OrderStatus;
    use crate::types::Venue;

    /// 只实现 `perpetual_contract_spec` 的假 provider，记录被调用次数。
    pub(crate) struct FakeSpecs {
        pub(crate) specs: HashMap<Symbol, PerpContractSpec>,
        pub(crate) calls: AtomicUsize,
    }

    impl FakeSpecs {
        pub(crate) fn new(specs: &[(Symbol, PerpContractSpec)]) -> Self {
            Self { specs: specs.iter().cloned().collect(), calls: AtomicUsize::new(0) }
        }
    }

    #[async_trait]
    impl ExchangeInfoProvider for FakeSpecs {
        fn venue(&self) -> Venue {
            Venue::new("fake")
        }
        async fn spot_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
            unimplemented!()
        }
        async fn perpetual_trading_fee(&self, _symbol: &Symbol) -> anyhow::Result<TradingFee> {
            unimplemented!()
        }
        async fn usdt_spot_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
            unimplemented!()
        }
        async fn usdt_perpetual_symbols(&self) -> anyhow::Result<Vec<Symbol>> {
            unimplemented!()
        }
        async fn spot_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
            unimplemented!()
        }
        async fn perpetual_market_precisions(&self) -> anyhow::Result<Vec<MarketPrecision>> {
            unimplemented!()
        }
        async fn perpetual_contract_spec(&self, symbol: &Symbol) -> anyhow::Result<PerpContractSpec> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.specs.get(symbol).copied().with_context(|| format!("no spec for {symbol}"))
        }
    }

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn futuon() -> Symbol {
        Symbol::new("FUTUON", "USDT")
    }

    fn cache() -> (ContractSizeCache, Arc<FakeSpecs>) {
        let specs = Arc::new(FakeSpecs::new(&[(
            futuon(),
            PerpContractSpec { price_unit: Decimal::ONE, qty_unit: d("0.01") },
        )]));
        (ContractSizeCache::new(specs.clone()), specs)
    }

    #[tokio::test]
    async fn converts_between_base_units_and_contracts_and_caches_the_spec() {
        let (cache, specs) = cache();
        assert_eq!(cache.to_contracts(&futuon(), d("0.49")).await.unwrap(), Decimal::from(49));
        assert_eq!(cache.from_contracts(&futuon(), Decimal::from(-49)).await.unwrap(), d("-0.49"));
        assert_eq!(specs.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn binance_style_prefix_contract_has_size_one() {
        let pepe = Symbol::new("1000PEPE", "USDT");
        let specs = Arc::new(FakeSpecs::new(&[(
            pepe.clone(),
            PerpContractSpec { price_unit: Decimal::from(1000), qty_unit: Decimal::from(1000) },
        )]));
        let cache = ContractSizeCache::new(specs);
        assert_eq!(cache.to_contracts(&pepe, Decimal::from(20000)).await.unwrap(), Decimal::from(20000));
    }

    #[tokio::test]
    async fn convert_update_scales_filled_qty_and_rejects_unknown_contract() {
        let (cache, _specs) = cache();
        let update = |symbol: Option<Symbol>| ExchangeOrderUpdate {
            venue: Venue::new("gate_futures"),
            symbol,
            client_order_id: Some("cf1".to_string()),
            exchange_order_id: Some("1".to_string()),
            status: OrderStatus::Filled,
            filled_qty: Decimal::from(49),
            avg_price: Some(d("102.23")),
            fee: None,
            fee_asset: None,
            ts_ms: 1,
        };
        let converted = cache.convert_update(update(Some(futuon()))).await.unwrap();
        assert_eq!(converted.filled_qty, d("0.49"));
        assert_eq!(converted.avg_price, Some(d("102.23")));

        assert!(cache.convert_update(update(None)).await.is_err());
        assert!(cache.convert_update(update(Some(Symbol::new("XYZ", "USDT")))).await.is_err());
    }
}
