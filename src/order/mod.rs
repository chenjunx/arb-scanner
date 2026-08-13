pub mod binance;
pub mod binance_futures;
pub mod kraken;
pub mod types;

use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::types::{Symbol, Venue};
use types::{MarketInfo, MarketOrderRequest, OrderAmount, OrderResult, OrderStatus};

/// 下单(执行层)扩展点：每个交易所实现市价单提交逻辑。这是按需调用的请求/响应
/// 接口，和 `wallet::WalletProvider` 一样不接入 engine 主循环，供需要真实下单
/// 的场景按需调用。
#[async_trait]
pub trait OrderProvider: Send + Sync {
    fn venue(&self) -> Venue;

    /// 查询某个交易对的下单精度/最小下单量限制，下单前用它做校验。
    async fn market_info(&self, symbol: &Symbol) -> anyhow::Result<MarketInfo>;

    /// 交易所具体的市价单提交调用。只应由 `place_market_order` 的默认实现在
    /// 校验通过后调用，各交易所实现不需要重复做数量精度/最小量校验。
    /// `req.amount` 为 `OrderAmount::Quote` 时，不支持按计价币金额下单的交易所
    /// 应直接报错，见 `OrderAmount` 说明。
    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult>;

    /// 市价单提交统一入口：校验下单量为正；`OrderAmount::Base` 额外校验是否
    /// 满足最小下单量、是否是步进的整数倍——`OrderAmount::Quote` 下单前不知道
    /// 基础币数量，没法做这类校验，交易所自己会在撮合时换算。`dry_run=true`
    /// 时校验通过后直接返回、不发起真实下单请求。所有交易所共用这一套安全
    /// 校验，不能被各交易所的实现绕过。
    async fn place_market_order(&self, req: MarketOrderRequest) -> anyhow::Result<OrderResult> {
        if req.amount.value() <= Decimal::ZERO {
            anyhow::bail!("order amount must be positive, got {}", req.amount.value());
        }
        if let OrderAmount::Base(quantity) = req.amount {
            let info = self.market_info(&req.symbol).await?;
            if quantity < info.min_qty {
                anyhow::bail!("quantity {} below min_qty {} for {}", quantity, info.min_qty, req.symbol);
            }
            if info.qty_step > Decimal::ZERO && (quantity % info.qty_step) != Decimal::ZERO {
                anyhow::bail!(
                    "quantity {} is not a multiple of qty_step {} for {}",
                    quantity,
                    info.qty_step,
                    req.symbol
                );
            }
        }
        if req.dry_run {
            log::info!("order place dry_run passed venue={} req={:?}", self.venue(), req);
            return Ok(OrderResult {
                order_id: "dry-run".to_string(),
                status: OrderStatus::New,
                filled_qty: Decimal::ZERO,
                avg_price: None,
                fee: None,
                fee_asset: None,
            });
        }
        self.place_market_order_raw(&req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use types::OrderSide;

    /// 测试替身:固定返回一份 market_info,并记录 place_market_order_raw 被真正
    /// 调用(即走过 dry_run 之外分支)的次数,用来验证默认方法的护栏逻辑,不依赖
    /// 任何真实交易所实现。`supports_quote=false` 时对 `OrderAmount::Quote`
    /// 报错,用来验证"不支持按计价币下单"的交易所该怎么接入这套 trait。
    struct FakeProvider {
        info: MarketInfo,
        raw_calls: Arc<AtomicUsize>,
        supports_quote: bool,
    }

    #[async_trait]
    impl OrderProvider for FakeProvider {
        fn venue(&self) -> Venue {
            Venue::new("fake")
        }

        async fn market_info(&self, _symbol: &Symbol) -> anyhow::Result<MarketInfo> {
            Ok(self.info.clone())
        }

        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            match req.amount {
                OrderAmount::Base(quantity) => {
                    self.raw_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(OrderResult {
                        order_id: format!("real-{}", req.symbol),
                        status: OrderStatus::Filled,
                        filled_qty: quantity,
                        avg_price: Some(Decimal::ONE),
                        fee: None,
                        fee_asset: None,
                    })
                }
                OrderAmount::Quote(quote_amount) => {
                    if !self.supports_quote {
                        anyhow::bail!("{} does not support quote-amount market orders", self.venue());
                    }
                    self.raw_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(OrderResult {
                        order_id: format!("real-quote-{}", req.symbol),
                        status: OrderStatus::Filled,
                        filled_qty: quote_amount / Decimal::new(2, 0),
                        avg_price: Some(Decimal::new(2, 0)),
                        fee: None,
                        fee_asset: None,
                    })
                }
            }
        }
    }

    fn info(qty_step: &str, min_qty: &str) -> MarketInfo {
        MarketInfo {
            symbol: Symbol::new("BTC", "USDT"),
            qty_step: qty_step.parse().unwrap(),
            min_qty: min_qty.parse().unwrap(),
        }
    }

    fn request(amount: OrderAmount, dry_run: bool) -> MarketOrderRequest {
        MarketOrderRequest {
            symbol: Symbol::new("BTC", "USDT"),
            side: OrderSide::Buy,
            amount,
            client_order_id: None,
            dry_run,
        }
    }

    fn provider(info: MarketInfo, supports_quote: bool) -> (FakeProvider, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = FakeProvider {
            info,
            raw_calls: calls.clone(),
            supports_quote,
        };
        (provider, calls)
    }

    #[tokio::test]
    async fn rejects_non_positive_quantity() {
        let (provider, calls) = provider(info("0.001", "0.001"), false);
        let err = provider
            .place_market_order(request(OrderAmount::Base(Decimal::ZERO), false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must be positive"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rejects_quantity_below_min() {
        let (provider, calls) = provider(info("0.001", "0.01"), false);
        let err = provider
            .place_market_order(request(OrderAmount::Base(Decimal::new(5, 3)), false)) // 0.005
            .await
            .unwrap_err();
        assert!(err.to_string().contains("below min_qty"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rejects_quantity_not_multiple_of_step() {
        let (provider, calls) = provider(info("0.01", "0.01"), false);
        let err = provider
            .place_market_order(request(OrderAmount::Base(Decimal::new(15, 3)), false)) // 0.015
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not a multiple of qty_step"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dry_run_short_circuits_before_raw_call() {
        let (provider, calls) = provider(info("0.001", "0.001"), false);
        let result = provider
            .place_market_order(request(OrderAmount::Base(Decimal::new(1, 2)), true)) // 0.01
            .await
            .unwrap();
        assert_eq!(result.order_id, "dry-run");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn valid_request_calls_place_market_order_raw_once() {
        let (provider, calls) = provider(info("0.001", "0.001"), false);
        let result = provider
            .place_market_order(request(OrderAmount::Base(Decimal::new(1, 2)), false)) // 0.01
            .await
            .unwrap();
        assert_eq!(result.order_id, "real-BTC/USDT");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn quote_order_rejects_non_positive_amount() {
        let (provider, calls) = provider(info("0.001", "0.001"), true);
        let err = provider
            .place_market_order(request(OrderAmount::Quote(Decimal::ZERO), false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must be positive"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn quote_order_dry_run_short_circuits_before_raw_call() {
        let (provider, calls) = provider(info("0.001", "0.001"), true);
        let result = provider
            .place_market_order(request(OrderAmount::Quote(Decimal::new(100, 0)), true))
            .await
            .unwrap();
        assert_eq!(result.order_id, "dry-run");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn quote_order_unsupported_venue_errors() {
        let (provider, calls) = provider(info("0.001", "0.001"), false);
        let err = provider
            .place_market_order(request(OrderAmount::Quote(Decimal::new(100, 0)), false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not support quote-amount market orders"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn quote_order_valid_request_calls_raw_once() {
        let (provider, calls) = provider(info("0.001", "0.001"), true);
        let result = provider
            .place_market_order(request(OrderAmount::Quote(Decimal::new(100, 0)), false))
            .await
            .unwrap();
        assert_eq!(result.order_id, "real-quote-BTC/USDT");
        assert_eq!(result.filled_qty, Decimal::new(50, 0));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
