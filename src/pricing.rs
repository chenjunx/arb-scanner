use std::collections::HashMap;
use std::sync::Arc;

use log::warn;
use rust_decimal::Decimal;

use crate::order::OrderProvider;
use crate::types::{Symbol, Venue};

/// 手续费统一换算为 USDT 计价的服务。拆成"同步可解"和"异步查价"两个方法：
/// 前者不发任何网络请求，只在成交回报处理路径上内联调用；后者需要向交易所
/// 发 REST 请求查现价，调用方必须用 `tokio::spawn` 包起来，不能挡在
/// `OrderManager` 发布成交事件之前(见 `order_manager::manager::handle_exchange_update`)。
pub struct FeeUsdtConverter {
    providers: HashMap<Venue, Arc<dyn OrderProvider>>,
}

impl FeeUsdtConverter {
    pub fn new(providers: HashMap<Venue, Arc<dyn OrderProvider>>) -> Self {
        Self { providers }
    }

    /// 不发任何网络请求：手续费币种本身是 USDT 等值稳定币时直接透传；是这笔
    /// 成交的 base 币种、且 quote 是 USDT 等值稳定币时，复用这笔成交自身的
    /// 均价换算。两条规则都没命中(如 BNB/KFEE 这类抵扣币种)返回 `None`，
    /// 交给 [`Self::query_async`] 走后台查价。
    pub fn try_resolve_sync(
        &self,
        symbol: &Symbol,
        fee_amount: Decimal,
        fee_asset: &str,
        fill_price: Option<Decimal>,
    ) -> Option<Decimal> {
        if is_usdt_equivalent(fee_asset) {
            return Some(fee_amount);
        }
        if fee_asset.eq_ignore_ascii_case(&symbol.base) && is_usdt_equivalent(&symbol.quote) {
            if let Some(price) = fill_price {
                return Some(fee_amount * price);
            }
        }
        None
    }

    /// 向手续费实际产生的那个交易所(`venue`)发起 REST 请求查 `fee_asset` 现价，
    /// 换算成 USDT 等值。找不到对应 provider、或查价请求本身失败，都记录
    /// warn 日志并返回 `None`——调用方据此把这笔手续费标记为"换算未完成"
    /// (`fees_usdt_incomplete`)，不当 0 处理。
    pub async fn query_async(&self, venue: &Venue, fee_asset: &str, fee_amount: Decimal) -> Option<Decimal> {
        let Some(provider) = self.providers.get(venue) else {
            warn!("pricing: no OrderProvider registered for venue={venue}, cannot convert fee_asset={fee_asset} to USDT");
            return None;
        };
        match provider.quote_usdt_price(fee_asset).await {
            Ok(price) => Some(fee_amount * price),
            Err(err) => {
                warn!("pricing: failed to quote USDT price for venue={venue} asset={fee_asset}: {err:#}");
                None
            }
        }
    }
}

/// 系统里视为与 USDT 1:1 等值的稳定币/计价币种，覆盖 Binance(USDT/USDC/BUSD/FDUSD)
/// 和 Kraken(USD)常见的计价币种。展示层也可复用这个判断。
pub fn is_usdt_equivalent(asset: &str) -> bool {
    matches!(asset.to_ascii_uppercase().as_str(), "USDT" | "USDC" | "USD" | "BUSD" | "FDUSD")
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    use crate::order::types::{MarketOrderRequest, OrderResult};

    struct FakeProvider {
        venue: Venue,
        price: anyhow::Result<Decimal>,
    }

    #[async_trait]
    impl OrderProvider for FakeProvider {
        fn venue(&self) -> Venue {
            self.venue.clone()
        }

        async fn place_market_order_raw(&self, _req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            anyhow::bail!("not used in this test")
        }

        async fn quote_usdt_price(&self, _asset: &str) -> anyhow::Result<Decimal> {
            match &self.price {
                Ok(price) => Ok(*price),
                Err(err) => Err(anyhow::anyhow!("{err}")),
            }
        }
    }

    fn converter_with(venue: Venue, price: anyhow::Result<Decimal>) -> FeeUsdtConverter {
        let mut providers: HashMap<Venue, Arc<dyn OrderProvider>> = HashMap::new();
        providers.insert(venue.clone(), Arc::new(FakeProvider { venue, price }));
        FeeUsdtConverter::new(providers)
    }

    #[test]
    fn is_usdt_equivalent_matches_known_stablecoins() {
        assert!(is_usdt_equivalent("USDT"));
        assert!(is_usdt_equivalent("usdc"));
        assert!(is_usdt_equivalent("USD"));
        assert!(!is_usdt_equivalent("BNB"));
        assert!(!is_usdt_equivalent("KFEE"));
    }

    #[test]
    fn try_resolve_sync_passes_through_stablecoin_fee() {
        let converter = FeeUsdtConverter::new(HashMap::new());
        let symbol = Symbol::new("BTC", "USDT");
        let result = converter.try_resolve_sync(&symbol, Decimal::new(5, 1), "USDT", None);
        assert_eq!(result, Some(Decimal::new(5, 1)));
    }

    #[test]
    fn try_resolve_sync_uses_fill_price_when_fee_asset_is_base() {
        let converter = FeeUsdtConverter::new(HashMap::new());
        let symbol = Symbol::new("BTC", "USDT");
        let fill_price = Some(Decimal::new(50000, 0));
        let result = converter.try_resolve_sync(&symbol, Decimal::new(1, 3), "BTC", fill_price);
        assert_eq!(result, Some(Decimal::new(50, 0)));
    }

    #[test]
    fn try_resolve_sync_returns_none_for_unrelated_asset() {
        let converter = FeeUsdtConverter::new(HashMap::new());
        let symbol = Symbol::new("BTC", "USDT");
        let result = converter.try_resolve_sync(&symbol, Decimal::new(1, 2), "BNB", Some(Decimal::new(50000, 0)));
        assert_eq!(result, None);
    }

    #[test]
    fn try_resolve_sync_returns_none_when_fee_asset_is_base_but_no_fill_price() {
        let converter = FeeUsdtConverter::new(HashMap::new());
        let symbol = Symbol::new("BTC", "USDT");
        let result = converter.try_resolve_sync(&symbol, Decimal::new(1, 3), "BTC", None);
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn query_async_converts_using_provider_price() {
        let venue = Venue::new("binance");
        let converter = converter_with(venue.clone(), Ok(Decimal::new(600, 0)));
        let result = converter.query_async(&venue, "BNB", Decimal::new(1, 1)).await;
        assert_eq!(result, Some(Decimal::new(60, 0)));
    }

    #[tokio::test]
    async fn query_async_returns_none_when_provider_missing() {
        let converter = FeeUsdtConverter::new(HashMap::new());
        let result = converter.query_async(&Venue::new("binance"), "BNB", Decimal::new(1, 1)).await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn query_async_returns_none_when_quote_fails() {
        let venue = Venue::new("kraken");
        let converter = converter_with(venue.clone(), Err(anyhow::anyhow!("no such pair")));
        let result = converter.query_async(&venue, "KFEE", Decimal::new(100, 0)).await;
        assert_eq!(result, None);
    }
}
