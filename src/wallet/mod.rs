pub mod binance;
pub mod kraken;
pub mod transfer;
pub mod types;

use async_trait::async_trait;

use crate::types::Venue;
use types::{AssetInfo, WithdrawRequest, WithdrawResult};

/// 钱包(转账层)扩展点：每个交易所实现一套收款地址查询/链信息查询/提币逻辑。
/// 这是按需调用的请求/响应接口,不是像 `market_data::MarketDataSource` 那样常驻
/// 推流的后台任务,不接入 engine 主循环,供需要转账的场景按需调用。
#[async_trait]
pub trait WalletProvider: Send + Sync {
    fn venue(&self) -> Venue;

    /// 查询某个币种支持的全部链/网络信息(提币开关、最小提币量、手续费等)。
    async fn asset_info(&self, asset: &str) -> anyhow::Result<AssetInfo>;

    /// 查询某个币种在某条链上的收款地址。
    async fn deposit_address(&self, asset: &str, network: &str) -> anyhow::Result<types::DepositAddress>;

    /// 交易所具体的提币 API 调用。只应由 `withdraw` 的默认实现在校验通过后调用,
    /// 各交易所实现不需要重复做链信息/额度校验。
    async fn withdraw_raw(&self, req: &WithdrawRequest) -> anyhow::Result<WithdrawResult>;

    /// 提币统一入口:先校验目标网络是否开放提币、金额是否达到最小提币量,
    /// `dry_run=true` 时校验通过后直接返回、不发起真实提币请求。所有交易所
    /// 共用这一套安全校验,不能被各交易所的实现绕过。
    async fn withdraw(&self, req: WithdrawRequest) -> anyhow::Result<WithdrawResult> {
        let info = self.asset_info(&req.asset).await?;
        let chain = info
            .networks
            .iter()
            .find(|n| n.network == req.network)
            .ok_or_else(|| anyhow::anyhow!("unsupported network {} for asset {}", req.network, req.asset))?;
        if !chain.withdraw_enabled {
            anyhow::bail!("withdraw disabled for {}/{}", req.asset, req.network);
        }
        if req.amount < chain.withdraw_min {
            anyhow::bail!(
                "amount {} below withdraw_min {} for {}/{}",
                req.amount,
                chain.withdraw_min,
                req.asset,
                req.network
            );
        }
        if req.dry_run {
            log::info!("wallet withdraw dry_run passed venue={} req={:?}", self.venue(), req);
            return Ok(WithdrawResult {
                id: "dry-run".to_string(),
            });
        }
        self.withdraw_raw(&req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use rust_decimal::Decimal;
    use types::ChainInfo;

    /// 测试替身:固定返回一份 asset_info,并记录 withdraw_raw 是否被调用,用来验证
    /// `withdraw()` 默认方法的护栏逻辑不依赖任何真实交易所实现。
    struct FakeProvider {
        networks: Vec<ChainInfo>,
        withdraw_raw_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl WalletProvider for FakeProvider {
        fn venue(&self) -> Venue {
            Venue::new("fake")
        }

        async fn asset_info(&self, asset: &str) -> anyhow::Result<AssetInfo> {
            Ok(AssetInfo {
                asset: asset.to_string(),
                networks: self.networks.clone(),
            })
        }

        async fn deposit_address(&self, _asset: &str, _network: &str) -> anyhow::Result<types::DepositAddress> {
            unimplemented!("not exercised by these tests")
        }

        async fn withdraw_raw(&self, req: &WithdrawRequest) -> anyhow::Result<WithdrawResult> {
            self.withdraw_raw_calls.fetch_add(1, Ordering::SeqCst);
            Ok(WithdrawResult {
                id: format!("real-{}", req.asset),
            })
        }
    }

    fn enabled_chain() -> ChainInfo {
        ChainInfo {
            network: "ETH".to_string(),
            name: "Ethereum".to_string(),
            deposit_enabled: true,
            withdraw_enabled: true,
            withdraw_fee: Decimal::new(1, 2),
            withdraw_min: Decimal::new(10, 2),
            min_confirm: 12,
            contract_address: None,
        }
    }

    fn request(network: &str, amount: Decimal, dry_run: bool) -> WithdrawRequest {
        WithdrawRequest {
            asset: "USDT".to_string(),
            network: network.to_string(),
            address: "0xabc".to_string(),
            tag: None,
            amount,
            dry_run,
        }
    }

    fn provider(networks: Vec<ChainInfo>) -> (FakeProvider, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = FakeProvider {
            networks,
            withdraw_raw_calls: calls.clone(),
        };
        (provider, calls)
    }

    #[tokio::test]
    async fn rejects_unsupported_network() {
        let (provider, calls) = provider(vec![enabled_chain()]);
        let err = provider
            .withdraw(request("BSC", Decimal::new(1, 0), false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unsupported network"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rejects_when_withdraw_disabled() {
        let mut chain = enabled_chain();
        chain.withdraw_enabled = false;
        let (provider, calls) = provider(vec![chain]);
        let err = provider
            .withdraw(request("ETH", Decimal::new(1, 0), false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("withdraw disabled"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rejects_amount_below_minimum() {
        let (provider, calls) = provider(vec![enabled_chain()]);
        let err = provider
            .withdraw(request("ETH", Decimal::new(1, 3), false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("below withdraw_min"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dry_run_short_circuits_before_withdraw_raw() {
        let (provider, calls) = provider(vec![enabled_chain()]);
        let result = provider
            .withdraw(request("ETH", Decimal::new(1, 0), true))
            .await
            .unwrap();
        assert_eq!(result.id, "dry-run");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn valid_request_calls_withdraw_raw_once() {
        let (provider, calls) = provider(vec![enabled_chain()]);
        let result = provider
            .withdraw(request("ETH", Decimal::new(1, 0), false))
            .await
            .unwrap();
        assert_eq!(result.id, "real-USDT");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
