pub mod binance;
pub mod binance_futures;
pub mod kraken;
pub mod types;

use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::types::{Symbol, Venue};
use types::{MarketOrderRequest, OrderResult, OrderStatus};

/// 下单(执行层)扩展点：每个交易所实现市价单提交逻辑。这是按需调用的请求/响应
/// 接口，和 `wallet::WalletProvider` 一样不接入 engine 主循环，供需要真实下单
/// 的场景按需调用。
#[async_trait]
pub trait OrderProvider: Send + Sync {
    fn venue(&self) -> Venue;

    /// 交易所具体的市价单提交调用。只应由 `place_market_order` 的默认实现在
    /// 校验通过后调用，各交易所实现不需要重复做数量精度/最小量校验——精度
    /// 转换是调用方(如 `execution`、以后的自动策略)通过
    /// `exchange_info::PrecisionCache` 提前算好的，这一层只信任传进来的数量。
    /// `req.amount` 为 `OrderAmount::Quote` 时，不支持按计价币金额下单的交易所
    /// 应直接报错，见 `OrderAmount` 说明。
    async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult>;

    /// 市价单提交统一入口：只校验下单量为正，数量精度/最小下单量由调用方
    /// 负责(见 `exchange_info::PrecisionCache`)。`dry_run=true` 时校验通过后
    /// 直接返回、不发起真实下单请求。
    async fn place_market_order(&self, req: MarketOrderRequest) -> anyhow::Result<OrderResult> {
        if req.amount.value() <= Decimal::ZERO {
            anyhow::bail!("order amount must be positive, got {}", req.amount.value());
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

    /// 按交易所自己的订单号回查一笔订单当前的状态/成交量/均价，用于
    /// `wait_for_fill` 在私有 WS 迟迟没有推送确认时的 REST 兜底核对
    /// (见 `execution::wait_for_fill`)——只在这一条防御路径上使用，正常
    /// 成交确认仍然只信任 WS，不改变 `place_market_order_raw` 的既有约定。
    /// 默认实现是报错，交易所没有对应查询接口(如 Kraken `AddOrder` 之外
    /// 没有实现)时不需要改动。
    async fn query_order(&self, _symbol: &Symbol, _exchange_order_id: &str) -> anyhow::Result<OrderResult> {
        anyhow::bail!("query_order not supported for venue {}", self.venue())
    }

    /// 查询 `asset` 相对 USDT(或等值稳定币)的现价，用于把非 base/quote 币种
    /// (如 Binance 现货的 BNB、Kraken 的 KFEE)支付的手续费换算成 USDT 计价。
    /// 只在 `pricing::FeeUsdtConverter::query_async` 里按需调用，是公开行情
    /// 查询，不需要签名。默认实现是报错，交易所没有合适的报价对时不需要改动，
    /// 由调用方按失败处理。
    async fn quote_usdt_price(&self, _asset: &str) -> anyhow::Result<Decimal> {
        anyhow::bail!("quote_usdt_price not supported for venue {}", self.venue())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::types::Symbol;
    use types::{OrderAmount, OrderSide};

    /// 测试替身:记录 place_market_order_raw 被真正调用(即走过 dry_run 之外
    /// 分支)的次数,用来验证默认方法的护栏逻辑,不依赖任何真实交易所实现。
    /// `supports_quote=false` 时对 `OrderAmount::Quote` 报错,用来验证"不支持
    /// 按计价币下单"的交易所该怎么接入这套 trait。
    struct FakeProvider {
        raw_calls: Arc<AtomicUsize>,
        supports_quote: bool,
    }

    #[async_trait]
    impl OrderProvider for FakeProvider {
        fn venue(&self) -> Venue {
            Venue::new("fake")
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

    fn request(amount: OrderAmount, dry_run: bool) -> MarketOrderRequest {
        MarketOrderRequest {
            symbol: Symbol::new("BTC", "USDT"),
            side: OrderSide::Buy,
            amount,
            client_order_id: None,
            dry_run,
        }
    }

    fn provider(supports_quote: bool) -> (FakeProvider, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = FakeProvider {
            raw_calls: calls.clone(),
            supports_quote,
        };
        (provider, calls)
    }

    #[tokio::test]
    async fn rejects_non_positive_quantity() {
        let (provider, calls) = provider(false);
        let err = provider
            .place_market_order(request(OrderAmount::Base(Decimal::ZERO), false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must be positive"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dry_run_short_circuits_before_raw_call() {
        let (provider, calls) = provider(false);
        let result = provider
            .place_market_order(request(OrderAmount::Base(Decimal::new(1, 2)), true)) // 0.01
            .await
            .unwrap();
        assert_eq!(result.order_id, "dry-run");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn valid_request_calls_place_market_order_raw_once() {
        let (provider, calls) = provider(false);
        let result = provider
            .place_market_order(request(OrderAmount::Base(Decimal::new(1, 2)), false)) // 0.01
            .await
            .unwrap();
        assert_eq!(result.order_id, "real-BTC/USDT");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn quote_order_rejects_non_positive_amount() {
        let (provider, calls) = provider(true);
        let err = provider
            .place_market_order(request(OrderAmount::Quote(Decimal::ZERO), false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must be positive"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn quote_order_dry_run_short_circuits_before_raw_call() {
        let (provider, calls) = provider(true);
        let result = provider
            .place_market_order(request(OrderAmount::Quote(Decimal::new(100, 0)), true))
            .await
            .unwrap();
        assert_eq!(result.order_id, "dry-run");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn quote_order_unsupported_venue_errors() {
        let (provider, calls) = provider(false);
        let err = provider
            .place_market_order(request(OrderAmount::Quote(Decimal::new(100, 0)), false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not support quote-amount market orders"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn quote_order_valid_request_calls_raw_once() {
        let (provider, calls) = provider(true);
        let result = provider
            .place_market_order(request(OrderAmount::Quote(Decimal::new(100, 0)), false))
            .await
            .unwrap();
        assert_eq!(result.order_id, "real-quote-BTC/USDT");
        assert_eq!(result.filled_qty, Decimal::new(50, 0));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
