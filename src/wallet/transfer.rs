use std::collections::HashSet;

use rust_decimal::Decimal;

use super::types::{WithdrawRequest, WithdrawResult};
use super::WalletProvider;

/// 划转参数：`filled_qty / 2` 截断到 8 位小数，用于"现货已买入、合约已对冲，
/// 只从划转步骤继续"的场景（见 `main.rs` 里 `open --from-transfer`）。
#[derive(Debug, Clone)]
pub struct TransferHalfParams {
    pub filled_qty: Decimal,
    /// 划转到 Kraken 的资产代码。
    pub transfer_asset: String,
    /// true 时只做链路/额度校验，不真正发起提币，见 `WalletProvider::withdraw`。
    pub dry_run: bool,
}

/// 把 `filled_qty` 的一半划转到 Kraken：先精确匹配双边共同链，再取 Kraken 收款
/// 地址，最后从币安提币。返回实际划转数量和提币结果。
pub async fn transfer_half_to_kraken(
    binance_wallet: &dyn WalletProvider,
    kraken_wallet: &dyn WalletProvider,
    params: TransferHalfParams,
) -> anyhow::Result<(Decimal, WithdrawResult)> {
    let transfer_qty = (params.filled_qty / Decimal::TWO).trunc_with_scale(8);
    if transfer_qty <= Decimal::ZERO {
        anyhow::bail!(
            "transfer_qty rounds down to zero, aborting transfer (filled_qty={})",
            params.filled_qty
        );
    }

    let network = resolve_transfer_network(binance_wallet, kraken_wallet, &params.transfer_asset).await?;
    log::info!(
        "transfer_half_to_kraken: resolved transfer network for {} -> {network}",
        params.transfer_asset
    );

    let deposit_address = kraken_wallet.deposit_address(&params.transfer_asset, &network).await?;
    let withdraw = binance_wallet
        .withdraw(WithdrawRequest {
            asset: params.transfer_asset.clone(),
            network,
            address: deposit_address.address,
            tag: deposit_address.tag,
            amount: transfer_qty,
            dry_run: params.dry_run,
        })
        .await?;
    log::info!("transfer_half_to_kraken: withdraw result = {:?}", withdraw);

    Ok((transfer_qty, withdraw))
}

/// 自动匹配币安可提币网络和 Kraken 可充值网络：两边的 `AssetInfo.networks`
/// 都已经是标准链名(以币安 `network` 代码为准，Kraken 一侧的原生名称翻译
/// 见 `wallet::kraken`)，这里只需要按 `network` 字段精确求交集。必须恰好
/// 命中一个共同网络才返回，零个或多个都直接报错，把两边全部可用网络列出来，
/// 不做无凭据的猜测。
async fn resolve_transfer_network(
    binance_wallet: &dyn WalletProvider,
    kraken_wallet: &dyn WalletProvider,
    asset: &str,
) -> anyhow::Result<String> {
    let binance_info = binance_wallet.asset_info(asset).await?;
    let kraken_info = kraken_wallet.asset_info(asset).await?;

    let kraken_networks: HashSet<&str> = kraken_info
        .networks
        .iter()
        .filter(|n| n.deposit_enabled)
        .map(|n| n.network.as_str())
        .collect();

    let candidates: Vec<&str> = binance_info
        .networks
        .iter()
        .filter(|n| n.withdraw_enabled && kraken_networks.contains(n.network.as_str()))
        .map(|n| n.network.as_str())
        .collect();

    match candidates.len() {
        1 => Ok(candidates[0].to_string()),
        0 => anyhow::bail!(
            "无法为 {asset} 自动确定共同的划转网络。币安可提币网络: {:?}；Kraken 可存款网络: {:?}",
            binance_info
                .networks
                .iter()
                .filter(|n| n.withdraw_enabled)
                .map(|n| &n.network)
                .collect::<Vec<_>>(),
            kraken_info.networks.iter().map(|n| &n.network).collect::<Vec<_>>(),
        ),
        _ => anyhow::bail!("为 {asset} 匹配到多个候选划转网络，存在歧义，需要人工确认: {candidates:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    use crate::types::Venue;
    use crate::wallet::types::{AssetInfo, ChainInfo, DepositAddress};

    /// 钱包测试替身：固定返回一份网络列表，记录 `withdraw_raw` 调用参数。
    struct FakeWalletProvider {
        name: &'static str,
        networks: Vec<ChainInfo>,
        deposit_addr: Option<DepositAddress>,
        withdraw_calls: Arc<Mutex<Vec<WithdrawRequest>>>,
    }

    #[async_trait]
    impl WalletProvider for FakeWalletProvider {
        fn venue(&self) -> Venue {
            Venue::new(self.name)
        }
        async fn asset_info(&self, asset: &str) -> anyhow::Result<AssetInfo> {
            Ok(AssetInfo {
                asset: asset.to_string(),
                networks: self.networks.clone(),
            })
        }
        async fn deposit_address(&self, asset: &str, network: &str) -> anyhow::Result<DepositAddress> {
            self.deposit_addr
                .clone()
                .map(|mut d| {
                    d.asset = asset.to_string();
                    d.network = network.to_string();
                    d
                })
                .ok_or_else(|| anyhow::anyhow!("no deposit address configured"))
        }
        async fn withdraw_raw(&self, req: &WithdrawRequest) -> anyhow::Result<WithdrawResult> {
            self.withdraw_calls.lock().unwrap().push(req.clone());
            Ok(WithdrawResult {
                id: format!("withdraw-{}", req.asset),
            })
        }
    }

    fn binance_btc_chain() -> ChainInfo {
        ChainInfo {
            network: "BTC".to_string(),
            name: "Bitcoin".to_string(),
            deposit_enabled: true,
            withdraw_enabled: true,
            withdraw_fee: Decimal::new(1, 4),
            withdraw_min: Decimal::new(1, 5),
            min_confirm: 2,
            contract_address: None,
        }
    }

    fn kraken_btc_chain() -> ChainInfo {
        ChainInfo {
            // WalletProvider 返回给调用方的数据已经是翻译后的标准链名(见
            // wallet::kraken 里的映射表)，测试替身直接模拟这个已翻译状态。
            network: "BTC".to_string(),
            name: "Bitcoin".to_string(),
            deposit_enabled: true,
            withdraw_enabled: true,
            withdraw_fee: Decimal::ZERO,
            withdraw_min: Decimal::ZERO,
            min_confirm: 3,
            contract_address: None,
        }
    }

    fn no_op_wallet(name: &'static str) -> FakeWalletProvider {
        FakeWalletProvider {
            name,
            networks: vec![],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[tokio::test]
    async fn transfer_half_to_kraken_zero_qty_errors() {
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let err = transfer_half_to_kraken(
            &binance_wallet,
            &kraken_wallet,
            TransferHalfParams {
                filled_qty: Decimal::new(1, 9), // 0.000000001，减半截断到 8 位小数后为 0
                transfer_asset: "BTC".to_string(),
                dry_run: false,
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("transfer_qty rounds down to zero"));
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn transfer_half_to_kraken_dry_run_skips_withdraw_raw() {
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![kraken_btc_chain()],
            deposit_addr: Some(DepositAddress {
                asset: "BTC".to_string(),
                network: "BTC".to_string(),
                address: "kraken-addr".to_string(),
                tag: None,
            }),
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        let (transfer_qty, withdraw) = transfer_half_to_kraken(
            &binance_wallet,
            &kraken_wallet,
            TransferHalfParams {
                filled_qty: Decimal::new(2, 0),
                transfer_asset: "BTC".to_string(),
                dry_run: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(transfer_qty, Decimal::ONE);
        assert_eq!(withdraw.id, "dry-run");
        // dry_run 由 WalletProvider::withdraw 的默认实现拦截，withdraw_raw 从未被调用，
        // 但 asset_info/deposit_address 仍是真实调用，走完了完整的链路校验。
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn transfer_half_to_kraken_happy_path() {
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![kraken_btc_chain()],
            deposit_addr: Some(DepositAddress {
                asset: "BTC".to_string(),
                network: "BTC".to_string(),
                address: "kraken-addr".to_string(),
                tag: None,
            }),
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        let (transfer_qty, withdraw) = transfer_half_to_kraken(
            &binance_wallet,
            &kraken_wallet,
            TransferHalfParams {
                filled_qty: Decimal::new(1234567, 6), // 1.234567
                transfer_asset: "BTC".to_string(),
                dry_run: false,
            },
        )
        .await
        .unwrap();

        // 1.234567 / 2 = 0.6172835,已经<=8位小数,截断后不变
        assert_eq!(transfer_qty, Decimal::new(6172835, 7));
        assert_eq!(withdraw.id, "withdraw-BTC");

        let withdraws = binance_wallet.withdraw_calls.lock().unwrap();
        assert_eq!(withdraws.len(), 1);
        assert_eq!(withdraws[0].network, "BTC");
        assert_eq!(withdraws[0].address, "kraken-addr");
        assert_eq!(withdraws[0].amount, Decimal::new(6172835, 7));
    }

    #[tokio::test]
    async fn resolve_transfer_network_unique_match() {
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![kraken_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        let network = resolve_transfer_network(&binance_wallet, &kraken_wallet, "BTC")
            .await
            .unwrap();
        assert_eq!(network, "BTC");
    }

    #[tokio::test]
    async fn resolve_transfer_network_no_match_errors() {
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain()],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        // Kraken 一侧没有任何网络，和币安的 "BTC" 求交集为空。
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        let err = resolve_transfer_network(&binance_wallet, &kraken_wallet, "BTC")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("无法为 BTC 自动确定共同的划转网络"));
    }

    #[tokio::test]
    async fn resolve_transfer_network_multiple_candidates_errors() {
        let mut eth_chain = binance_btc_chain();
        eth_chain.network = "ETH".to_string();
        let binance_wallet = FakeWalletProvider {
            name: "binance",
            networks: vec![binance_btc_chain(), eth_chain],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let mut kraken_eth_chain = kraken_btc_chain();
        kraken_eth_chain.network = "ETH".to_string();
        let kraken_wallet = FakeWalletProvider {
            name: "kraken",
            networks: vec![kraken_btc_chain(), kraken_eth_chain],
            deposit_addr: None,
            withdraw_calls: Arc::new(Mutex::new(Vec::new())),
        };

        // asset 参数在这个测试替身实现里不影响返回的 networks,两个币安网络
        // (BTC/ETH)在 Kraken 一侧都有对应的标准链名,导致两个候选、产生歧义。
        let err = resolve_transfer_network(&binance_wallet, &kraken_wallet, "BTC")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("匹配到多个候选划转网络"));
    }
}
