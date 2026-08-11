use std::collections::HashSet;

use rust_decimal::Decimal;

use crate::order::types::{MarketOrderRequest, OrderAmount, OrderResult, OrderSide};
use crate::order::OrderProvider;
use crate::types::Symbol;
use crate::wallet::types::{WithdrawRequest, WithdrawResult};
use crate::wallet::WalletProvider;

/// 开仓参数：现货按 USDT 金额买入，合约等量做空对冲，买到的一半划转到 Kraken。
#[derive(Debug, Clone)]
pub struct OpenPositionParams {
    pub symbol: Symbol,
    pub quote_amount: Decimal,
    /// 划转到 Kraken 的资产代码，通常等于 `symbol.base`。
    pub transfer_asset: String,
    pub client_order_id_prefix: Option<String>,
    pub dry_run: bool,
}

/// 开仓结果汇总。`dry_run=true` 时只有 `spot_order` 有值，后续步骤未执行。
#[derive(Debug, Clone)]
pub struct OpenPositionReport {
    pub spot_order: OrderResult,
    pub futures_order: Option<OrderResult>,
    pub transfer_qty: Option<Decimal>,
    pub withdraw: Option<WithdrawResult>,
    pub note: Option<String>,
}

/// 串联"现货按金额买入 -> 合约等量做空对冲 -> 买入量的一半划转到 Kraken 现货"
/// 这三步。任何一步失败都直接 `?` 向上传播，不做自动回滚/重试——半吊子仓位
/// 需要人工介入，之前几步已经落地的日志就是唯一的进度记录。
pub async fn open_hedged_position(
    spot: &dyn OrderProvider,
    futures: &dyn OrderProvider,
    binance_wallet: &dyn WalletProvider,
    kraken_wallet: &dyn WalletProvider,
    params: OpenPositionParams,
) -> anyhow::Result<OpenPositionReport> {
    let spot_order = spot
        .place_market_order(MarketOrderRequest {
            symbol: params.symbol.clone(),
            side: OrderSide::Buy,
            amount: OrderAmount::Quote(params.quote_amount),
            client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-spot")),
            dry_run: params.dry_run,
        })
        .await?;
    log::info!("open_hedged_position: spot buy result = {:?}", spot_order);

    if params.dry_run {
        return Ok(OpenPositionReport {
            spot_order,
            futures_order: None,
            transfer_qty: None,
            withdraw: None,
            note: Some(
                "dry_run=true：仅校验并模拟了现货买入这一步；合约对冲和划转数量依赖真实成交量，dry-run 下不做模拟。"
                    .to_string(),
            ),
        });
    }

    let filled_qty = spot_order.filled_qty;
    if filled_qty <= Decimal::ZERO {
        anyhow::bail!("spot buy filled_qty is zero, aborting hedge/transfer");
    }

    let futures_info = futures.market_info(&params.symbol).await?;
    let futures_qty = floor_to_step(filled_qty, futures_info.qty_step);
    if futures_qty < futures_info.min_qty {
        anyhow::bail!(
            "hedge quantity {futures_qty} below futures min_qty {} for {}",
            futures_info.min_qty,
            params.symbol
        );
    }
    if futures_qty != filled_qty {
        log::warn!(
            "open_hedged_position: futures qty_step rounding, spot filled {filled_qty}, hedging {futures_qty}, residual {}",
            filled_qty - futures_qty
        );
    }
    let futures_order = futures
        .place_market_order(MarketOrderRequest {
            symbol: params.symbol.clone(),
            side: OrderSide::Sell,
            amount: OrderAmount::Base(futures_qty),
            client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-futures")),
            dry_run: false,
        })
        .await?;
    log::info!("open_hedged_position: futures hedge result = {:?}", futures_order);

    let transfer_qty = (filled_qty / Decimal::TWO).trunc_with_scale(8);
    if transfer_qty <= Decimal::ZERO {
        anyhow::bail!("transfer_qty rounds down to zero, aborting transfer (filled_qty={filled_qty})");
    }

    let network = resolve_transfer_network(binance_wallet, kraken_wallet, &params.transfer_asset).await?;
    log::info!(
        "open_hedged_position: resolved transfer network for {} -> {network}",
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
            dry_run: false,
        })
        .await?;
    log::info!("open_hedged_position: withdraw result = {:?}", withdraw);

    Ok(OpenPositionReport {
        spot_order,
        futures_order: Some(futures_order),
        transfer_qty: Some(transfer_qty),
        withdraw: Some(withdraw),
        note: None,
    })
}

/// 把 `qty` 向下取整到 `step` 的整数倍；`step<=0` 时原样返回，和
/// `order::OrderProvider::place_market_order` 里 `qty_step>0` 才校验的边界
/// 处理保持一致。
fn floor_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return qty;
    }
    (qty / step).trunc() * step
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use crate::order::types::{MarketInfo, OrderStatus};
    use crate::wallet::types::{AssetInfo, ChainInfo, DepositAddress};
    use crate::types::Venue;

    /// 现货测试替身：`place_market_order_raw` 只接受 `OrderAmount::Quote`，固定
    /// 返回 `filled_qty`，并记录被真正调用（即走过 dry_run 之外分支）的次数。
    /// 收到 `OrderAmount::Base` 即视为测试写错了流程。
    struct FakeSpotProvider {
        filled_qty: Decimal,
        fail: bool,
        quote_raw_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OrderProvider for FakeSpotProvider {
        fn venue(&self) -> Venue {
            Venue::new("fake-spot")
        }
        async fn market_info(&self, _symbol: &Symbol) -> anyhow::Result<MarketInfo> {
            unreachable!("spot leg never queries market_info")
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            let OrderAmount::Quote(_) = req.amount else {
                unreachable!("spot leg only uses OrderAmount::Quote")
            };
            self.quote_raw_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                anyhow::bail!("simulated spot failure");
            }
            Ok(OrderResult {
                order_id: format!("spot-{}", req.symbol),
                status: OrderStatus::Filled,
                filled_qty: self.filled_qty,
                avg_price: Some(Decimal::ONE),
            })
        }
    }

    /// 合约测试替身：固定返回 `market_info`，并记录每次真实下单的数量，用于验证
    /// `floor_to_step` 取整结果和"未被调用"两类断言。
    struct FakeFuturesProvider {
        info: MarketInfo,
        fail: bool,
        raw_calls: Arc<Mutex<Vec<Decimal>>>,
    }

    #[async_trait]
    impl OrderProvider for FakeFuturesProvider {
        fn venue(&self) -> Venue {
            Venue::new("fake-futures")
        }
        async fn market_info(&self, _symbol: &Symbol) -> anyhow::Result<MarketInfo> {
            Ok(self.info.clone())
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            let OrderAmount::Base(quantity) = req.amount else {
                unreachable!("futures leg only uses OrderAmount::Base")
            };
            self.raw_calls.lock().unwrap().push(quantity);
            if self.fail {
                anyhow::bail!("simulated futures failure");
            }
            Ok(OrderResult {
                order_id: format!("futures-{}", req.symbol),
                status: OrderStatus::Filled,
                filled_qty: quantity,
                avg_price: Some(Decimal::ONE),
            })
        }
    }

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

    fn btc_usdt() -> Symbol {
        Symbol::new("BTC", "USDT")
    }

    fn futures_info(qty_step: &str, min_qty: &str) -> MarketInfo {
        MarketInfo {
            symbol: btc_usdt(),
            qty_step: qty_step.parse().unwrap(),
            min_qty: min_qty.parse().unwrap(),
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

    fn params(quote_amount: Decimal, dry_run: bool) -> OpenPositionParams {
        OpenPositionParams {
            symbol: btc_usdt(),
            quote_amount,
            transfer_asset: "BTC".to_string(),
            client_order_id_prefix: Some("test".to_string()),
            dry_run,
        }
    }

    #[tokio::test]
    async fn dry_run_only_touches_spot_leg() {
        let spot_calls = Arc::new(AtomicUsize::new(0));
        let spot = FakeSpotProvider {
            filled_qty: Decimal::new(1, 1),
            fail: false,
            quote_raw_calls: spot_calls.clone(),
        };
        let futures_calls = Arc::new(Mutex::new(Vec::new()));
        let futures = FakeFuturesProvider {
            info: futures_info("0.001", "0.001"),
            fail: false,
            raw_calls: futures_calls.clone(),
        };
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let report = open_hedged_position(&spot, &futures, &binance_wallet, &kraken_wallet, params(Decimal::new(100, 0), true))
            .await
            .unwrap();

        assert_eq!(report.spot_order.order_id, "dry-run");
        assert!(report.futures_order.is_none());
        assert!(report.transfer_qty.is_none());
        assert!(report.withdraw.is_none());
        assert!(report.note.unwrap().contains("dry_run=true"));
        // dry_run 由 place_market_order 的 trait 默认方法拦截，raw 从未被调用。
        assert_eq!(spot_calls.load(Ordering::SeqCst), 0);
        assert!(futures_calls.lock().unwrap().is_empty());
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn zero_filled_qty_aborts_before_futures_and_wallet() {
        let spot = FakeSpotProvider {
            filled_qty: Decimal::ZERO,
            fail: false,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        };
        let futures_calls = Arc::new(Mutex::new(Vec::new()));
        let futures = FakeFuturesProvider {
            info: futures_info("0.001", "0.001"),
            fail: false,
            raw_calls: futures_calls.clone(),
        };
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let err = open_hedged_position(&spot, &futures, &binance_wallet, &kraken_wallet, params(Decimal::new(100, 0), false))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("filled_qty is zero"));
        assert!(futures_calls.lock().unwrap().is_empty());
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn spot_failure_stops_before_futures_and_wallet() {
        let spot = FakeSpotProvider {
            filled_qty: Decimal::new(1, 1),
            fail: true,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        };
        let futures_calls = Arc::new(Mutex::new(Vec::new()));
        let futures = FakeFuturesProvider {
            info: futures_info("0.001", "0.001"),
            fail: false,
            raw_calls: futures_calls.clone(),
        };
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let err = open_hedged_position(&spot, &futures, &binance_wallet, &kraken_wallet, params(Decimal::new(100, 0), false))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("simulated spot failure"));
        assert!(futures_calls.lock().unwrap().is_empty());
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn futures_failure_stops_before_wallet() {
        let spot = FakeSpotProvider {
            filled_qty: Decimal::new(1, 1),
            fail: false,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        };
        let futures = FakeFuturesProvider {
            info: futures_info("0.001", "0.001"),
            fail: true,
            raw_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let err = open_hedged_position(&spot, &futures, &binance_wallet, &kraken_wallet, params(Decimal::new(100, 0), false))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("simulated futures failure"));
        assert!(binance_wallet.withdraw_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn futures_qty_below_min_aborts_before_order() {
        let spot = FakeSpotProvider {
            filled_qty: Decimal::new(5, 4), // 0.0005
            fail: false,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        };
        let futures_calls = Arc::new(Mutex::new(Vec::new()));
        let futures = FakeFuturesProvider {
            info: futures_info("0.001", "0.001"),
            fail: false,
            raw_calls: futures_calls.clone(),
        };
        let binance_wallet = no_op_wallet("binance");
        let kraken_wallet = no_op_wallet("kraken");

        let err = open_hedged_position(&spot, &futures, &binance_wallet, &kraken_wallet, params(Decimal::new(100, 0), false))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("below futures min_qty"));
        assert!(futures_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn happy_path_hedges_and_transfers_half() {
        let spot = FakeSpotProvider {
            filled_qty: Decimal::new(1234567, 6), // 1.234567
            fail: false,
            quote_raw_calls: Arc::new(AtomicUsize::new(0)),
        };
        let futures_calls = Arc::new(Mutex::new(Vec::new()));
        let futures = FakeFuturesProvider {
            info: futures_info("0.01", "0.01"),
            fail: false,
            raw_calls: futures_calls.clone(),
        };
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

        let report = open_hedged_position(&spot, &futures, &binance_wallet, &kraken_wallet, params(Decimal::new(100, 0), false))
            .await
            .unwrap();

        // 1.234567 向下取整到 0.01 的整数倍 -> 1.23
        assert_eq!(futures_calls.lock().unwrap().as_slice(), &[Decimal::new(123, 2)]);
        assert_eq!(report.futures_order.unwrap().order_id, "futures-BTC/USDT");
        // 1.234567 / 2 = 0.6172835,已经<=8位小数,截断后不变
        assert_eq!(report.transfer_qty, Some(Decimal::new(6172835, 7)));

        let withdraws = binance_wallet.withdraw_calls.lock().unwrap();
        assert_eq!(withdraws.len(), 1);
        assert_eq!(withdraws[0].network, "BTC");
        assert_eq!(withdraws[0].address, "kraken-addr");
        assert_eq!(withdraws[0].amount, Decimal::new(6172835, 7));
        assert_eq!(report.withdraw.unwrap().id, "withdraw-BTC");
        assert!(report.note.is_none());
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
