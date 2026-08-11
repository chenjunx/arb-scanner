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

    let (transfer_qty, withdraw) = transfer_half_to_kraken(
        binance_wallet,
        kraken_wallet,
        TransferHalfParams {
            filled_qty,
            transfer_asset: params.transfer_asset.clone(),
            dry_run: false,
        },
    )
    .await?;

    Ok(OpenPositionReport {
        spot_order,
        futures_order: Some(futures_order),
        transfer_qty: Some(transfer_qty),
        withdraw: Some(withdraw),
        note: None,
    })
}

/// 划转参数：与 [`open_hedged_position`] 内部划转步骤使用完全相同的
/// `filled_qty / 2` 截断算法，用于"现货已买入、合约已对冲，只从划转步骤
/// 继续"的场景（见 `main.rs` 里 `open --from-transfer`）。
#[derive(Debug, Clone)]
pub struct TransferHalfParams {
    pub filled_qty: Decimal,
    /// 划转到 Kraken 的资产代码。
    pub transfer_asset: String,
    /// true 时只做链路/额度校验，不真正发起提币，见 `WalletProvider::withdraw`。
    pub dry_run: bool,
}

/// 库存轮转参数：在 `sell_provider` 卖出、`buy_provider` 买入同等数量的同一
/// 资产，两条腿真实市价单并发发起。用于在两个交易所之间调整现货库存，比链上
/// 划转更快。数量统一按基础币指定（而不是像 `open_hedged_position` 现货腿那样
/// 按计价币金额），因为 Kraken 市价单只支持按基础币数量下单，统一单位才能让
/// 两条腿共用同一套 `OrderProvider::place_market_order` 校验路径。
#[derive(Debug, Clone)]
pub struct RotateInventoryParams {
    pub symbol: Symbol,
    pub qty: Decimal,
    pub client_order_id_prefix: Option<String>,
    pub dry_run: bool,
}

/// 库存轮转结果：两条腿都成功才会返回。
#[derive(Debug, Clone)]
pub struct RotateInventoryReport {
    pub sell_order: OrderResult,
    pub buy_order: OrderResult,
}

/// 并发向 `sell_provider` 发一笔卖单、向 `buy_provider` 发一笔等量买单。两条腿
/// 互相独立，不做自动回滚：如果一条腿失败、另一条已经成交，会留下单边仓位，
/// 返回的错误里会带上已成交那一条腿的完整订单信息，需要人工介入对账。
pub async fn rotate_inventory(
    sell_provider: &dyn OrderProvider,
    buy_provider: &dyn OrderProvider,
    params: RotateInventoryParams,
) -> anyhow::Result<RotateInventoryReport> {
    let sell_req = MarketOrderRequest {
        symbol: params.symbol.clone(),
        side: OrderSide::Sell,
        amount: OrderAmount::Base(params.qty),
        client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-sell")),
        dry_run: params.dry_run,
    };
    let buy_req = MarketOrderRequest {
        symbol: params.symbol.clone(),
        side: OrderSide::Buy,
        amount: OrderAmount::Base(params.qty),
        client_order_id: params.client_order_id_prefix.as_ref().map(|p| format!("{p}-buy")),
        dry_run: params.dry_run,
    };

    let (sell_result, buy_result) = tokio::join!(
        sell_provider.place_market_order(sell_req),
        buy_provider.place_market_order(buy_req),
    );

    match (sell_result, buy_result) {
        (Ok(sell_order), Ok(buy_order)) => {
            log::info!("rotate_inventory: sell={:?} buy={:?}", sell_order, buy_order);
            Ok(RotateInventoryReport { sell_order, buy_order })
        }
        (Err(sell_err), Ok(buy_order)) => {
            log::error!(
                "rotate_inventory: sell leg failed ({sell_err}), buy leg already filled = {:?} -- manual reconciliation needed",
                buy_order
            );
            Err(sell_err.context(format!(
                "rotate_inventory: sell leg failed but buy leg already filled (buy_order={buy_order:?}), manual reconciliation needed"
            )))
        }
        (Ok(sell_order), Err(buy_err)) => {
            log::error!(
                "rotate_inventory: buy leg failed ({buy_err}), sell leg already filled = {:?} -- manual reconciliation needed",
                sell_order
            );
            Err(buy_err.context(format!(
                "rotate_inventory: buy leg failed but sell leg already filled (sell_order={sell_order:?}), manual reconciliation needed"
            )))
        }
        (Err(sell_err), Err(buy_err)) => {
            log::error!("rotate_inventory: both legs failed, sell_err={sell_err}, buy_err={buy_err}");
            Err(anyhow::anyhow!(
                "rotate_inventory: both legs failed; sell leg error: {sell_err}; buy leg error: {buy_err}"
            ))
        }
    }
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

    /// `rotate_inventory` 测试替身：固定返回 `market_info`，`fail=true` 时对
    /// `place_market_order_raw` 报错，并记录每次真实下单（走过 dry_run 之外
    /// 分支）的调用次数，用于验证并发下单时"一边失败不会让另一边被跳过"。
    struct FakeRotateProvider {
        name: &'static str,
        info: MarketInfo,
        fail: bool,
        raw_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl OrderProvider for FakeRotateProvider {
        fn venue(&self) -> Venue {
            Venue::new(self.name)
        }
        async fn market_info(&self, _symbol: &Symbol) -> anyhow::Result<MarketInfo> {
            Ok(self.info.clone())
        }
        async fn place_market_order_raw(&self, req: &MarketOrderRequest) -> anyhow::Result<OrderResult> {
            let OrderAmount::Base(quantity) = req.amount else {
                unreachable!("rotate_inventory only uses OrderAmount::Base")
            };
            self.raw_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                anyhow::bail!("simulated {} failure", self.name);
            }
            Ok(OrderResult {
                order_id: format!("{}-{}", self.name, req.symbol),
                status: OrderStatus::Filled,
                filled_qty: quantity,
                avg_price: Some(Decimal::ONE),
            })
        }
    }

    fn rotate_info() -> MarketInfo {
        MarketInfo {
            symbol: btc_usdt(),
            qty_step: Decimal::new(1, 3),
            min_qty: Decimal::new(1, 3),
        }
    }

    fn rotate_provider(name: &'static str, fail: bool) -> (FakeRotateProvider, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = FakeRotateProvider {
            name,
            info: rotate_info(),
            fail,
            raw_calls: calls.clone(),
        };
        (provider, calls)
    }

    fn rotate_params(dry_run: bool) -> RotateInventoryParams {
        RotateInventoryParams {
            symbol: btc_usdt(),
            qty: Decimal::new(1, 1),
            client_order_id_prefix: Some("test".to_string()),
            dry_run,
        }
    }

    #[tokio::test]
    async fn rotate_inventory_dry_run_skips_both_raw_calls() {
        let (sell, sell_calls) = rotate_provider("sell-venue", false);
        let (buy, buy_calls) = rotate_provider("buy-venue", false);

        let report = rotate_inventory(&sell, &buy, rotate_params(true)).await.unwrap();

        assert_eq!(report.sell_order.order_id, "dry-run");
        assert_eq!(report.buy_order.order_id, "dry-run");
        assert_eq!(sell_calls.load(Ordering::SeqCst), 0);
        assert_eq!(buy_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn rotate_inventory_happy_path_fills_both_legs() {
        let (sell, sell_calls) = rotate_provider("sell-venue", false);
        let (buy, buy_calls) = rotate_provider("buy-venue", false);

        let report = rotate_inventory(&sell, &buy, rotate_params(false)).await.unwrap();

        assert_eq!(report.sell_order.order_id, "sell-venue-BTC/USDT");
        assert_eq!(report.buy_order.order_id, "buy-venue-BTC/USDT");
        assert_eq!(sell_calls.load(Ordering::SeqCst), 1);
        assert_eq!(buy_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rotate_inventory_sell_failure_still_places_buy_leg() {
        let (sell, _sell_calls) = rotate_provider("sell-venue", true);
        let (buy, buy_calls) = rotate_provider("buy-venue", false);

        let err = rotate_inventory(&sell, &buy, rotate_params(false)).await.unwrap_err();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("buy-venue-BTC/USDT"));
        // 并发下单：卖出腿失败不应该让买入腿被跳过。
        assert_eq!(buy_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rotate_inventory_buy_failure_still_places_sell_leg() {
        let (sell, sell_calls) = rotate_provider("sell-venue", false);
        let (buy, _buy_calls) = rotate_provider("buy-venue", true);

        let err = rotate_inventory(&sell, &buy, rotate_params(false)).await.unwrap_err();

        assert!(err.to_string().contains("manual reconciliation needed"));
        assert!(err.to_string().contains("sell-venue-BTC/USDT"));
        assert_eq!(sell_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rotate_inventory_both_legs_fail() {
        let (sell, _sell_calls) = rotate_provider("sell-venue", true);
        let (buy, _buy_calls) = rotate_provider("buy-venue", true);

        let err = rotate_inventory(&sell, &buy, rotate_params(false)).await.unwrap_err();

        assert!(err.to_string().contains("simulated sell-venue failure"));
        assert!(err.to_string().contains("simulated buy-venue failure"));
    }
}
