use std::collections::{HashMap, HashSet};

use futures_util::StreamExt;
use futures_util::stream;

use crate::exchange_info::ExchangeInfoProvider;
use crate::types::Symbol;
use crate::wallet::WalletProvider;
use crate::wallet::binance::BinanceWalletProvider;
use crate::wallet::kraken::KrakenWalletProvider;
use crate::wallet::types::AssetInfo;

/// 一个通过粗筛的候选币种：币安有 USDT 永续合约、Kraken 有 USDT 现货，且
/// (归一化后)是同一个币种。是否真的"有交集"还要看 [`common_chains`]。
#[derive(Debug, Clone, PartialEq)]
struct Candidate {
    /// 标准币名(以币安命名为准，如 "BTC")。
    coin: String,
    binance_symbol: Symbol,
    kraken_symbol: Symbol,
}

/// 两个交易所都能交易、且共享至少一条可转账链的币种基本信息。
#[derive(Debug, Clone, PartialEq)]
pub struct SymbolOverlap {
    pub coin: String,
    pub binance_perp_symbol: Symbol,
    pub kraken_spot_symbol: Symbol,
    /// 两边钱包信息里都支持的标准链名，已排序。
    pub common_chains: Vec<String>,
}

/// 从币安 USDT 永续合约列表和 Kraken USDT 现货列表算出候选币种。两边的 `base`
/// 已经是标准(币安)命名——币安本身就是标准，Kraken 一侧已经在
/// `exchange_info::kraken::usdt_spot_symbols` 里翻译过——这里只需要大小写
/// 不敏感的精确匹配，不用再单独处理命名差异。
fn build_candidates(binance_perp: &[Symbol], kraken_spot: &[Symbol]) -> Vec<Candidate> {
    let kraken_by_coin: HashMap<String, &Symbol> =
        kraken_spot.iter().map(|s| (s.base.to_ascii_uppercase(), s)).collect();

    binance_perp
        .iter()
        .filter_map(|b| {
            let coin = b.base.to_ascii_uppercase();
            let kraken_symbol = kraken_by_coin.get(&coin)?;
            Some(Candidate {
                coin,
                binance_symbol: b.clone(),
                kraken_symbol: (*kraken_symbol).clone(),
            })
        })
        .collect()
}

/// 求两份钱包资产信息里标准链名(`ChainInfo::network`)的交集，已排序。
fn common_chains(binance: &AssetInfo, kraken: &AssetInfo) -> Vec<String> {
    let binance_networks: HashSet<String> = binance
        .networks
        .iter()
        .map(|n| n.network.to_ascii_uppercase())
        .collect();
    let mut chains: Vec<String> = kraken
        .networks
        .iter()
        .map(|n| n.network.to_ascii_uppercase())
        .filter(|n| binance_networks.contains(n))
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    chains.sort();
    chains
}

/// Kraken 私有钱包接口(`DepositMethods`)的并发上限，避免对候选币逐个查询时
/// 触发 Kraken 的限流。
const KRAKEN_WALLET_CONCURRENCY: usize = 4;

/// 算出 Binance / Kraken 有交集的币种：币安有 USDT 永续合约、Kraken 有 USDT 现货、
/// 且两边钱包信息里至少共享一条标准链(可转账)。结果按币名排序。
pub async fn find_overlap(
    binance_info: &dyn ExchangeInfoProvider,
    kraken_info: &dyn ExchangeInfoProvider,
    binance_wallet: &BinanceWalletProvider,
    kraken_wallet: &KrakenWalletProvider,
) -> anyhow::Result<Vec<SymbolOverlap>> {
    let (binance_perp, kraken_spot) =
        tokio::try_join!(binance_info.usdt_perpetual_symbols(), kraken_info.usdt_spot_symbols())?;

    let candidates = build_candidates(&binance_perp, &kraken_spot);
    log::info!(
        "scan: binance_perp_symbols={} kraken_spot_symbols={} candidates={}",
        binance_perp.len(),
        kraken_spot.len(),
        candidates.len()
    );
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let binance_assets: HashMap<String, AssetInfo> = binance_wallet
        .all_asset_info()
        .await?
        .into_iter()
        .map(|a| (a.asset.to_ascii_uppercase(), a))
        .collect();

    let kraken_results: Vec<(Candidate, anyhow::Result<AssetInfo>)> = stream::iter(candidates)
        .map(|candidate| async move {
            let result = kraken_wallet.asset_info(&candidate.kraken_symbol.base).await;
            (candidate, result)
        })
        .buffer_unordered(KRAKEN_WALLET_CONCURRENCY)
        .collect()
        .await;

    let mut overlaps = Vec::new();
    for (candidate, kraken_asset) in kraken_results {
        let Some(binance_asset) = binance_assets.get(&candidate.coin) else {
            log::warn!("scan: {} not found in binance wallet asset list, skipping", candidate.coin);
            continue;
        };
        let kraken_asset = match kraken_asset {
            Ok(info) => info,
            Err(err) => {
                log::warn!("scan: failed to fetch kraken wallet asset info for {}: {err:#}", candidate.coin);
                continue;
            }
        };

        let chains = common_chains(binance_asset, &kraken_asset);
        if chains.is_empty() {
            continue;
        }
        overlaps.push(SymbolOverlap {
            coin: candidate.coin,
            binance_perp_symbol: candidate.binance_symbol,
            kraken_spot_symbol: candidate.kraken_symbol,
            common_chains: chains,
        });
    }

    overlaps.sort_by(|a, b| a.coin.cmp(&b.coin));
    Ok(overlaps)
}

/// 把交集结果拼成一个简单的等宽对齐文本表，供 `scan` 子命令打印。
pub fn format_overlap_table(rows: &[SymbolOverlap]) -> String {
    if rows.is_empty() {
        return "(no overlapping symbols found)".to_string();
    }

    let coin_width = rows.iter().map(|r| r.coin.len()).max().unwrap_or(4).max(4);
    let binance_width = rows
        .iter()
        .map(|r| r.binance_perp_symbol.to_string().len())
        .max()
        .unwrap_or(12)
        .max(12);
    let kraken_width = rows
        .iter()
        .map(|r| r.kraken_spot_symbol.to_string().len())
        .max()
        .unwrap_or(11)
        .max(11);

    let mut out = format!(
        "{:<coin_width$}  {:<binance_width$}  {:<kraken_width$}  COMMON_CHAINS\n",
        "COIN", "BINANCE_PERP", "KRAKEN_SPOT"
    );
    for row in rows {
        out.push_str(&format!(
            "{:<coin_width$}  {:<binance_width$}  {:<kraken_width$}  {}\n",
            row.coin,
            row.binance_perp_symbol.to_string(),
            row.kraken_spot_symbol.to_string(),
            row.common_chains.join(",")
        ));
    }
    out.pop();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn build_candidates_matches_case_insensitively() {
        let binance_perp = vec![Symbol::new("BTC", "USDT"), Symbol::new("ETH", "USDT")];
        let kraken_spot = vec![Symbol::new("btc", "USDT")];
        let candidates = build_candidates(&binance_perp, &kraken_spot);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].coin, "BTC");
        assert_eq!(candidates[0].kraken_symbol, Symbol::new("btc", "USDT"));
    }

    #[test]
    fn build_candidates_excludes_symbols_missing_on_either_side() {
        let binance_perp = vec![Symbol::new("SOL", "USDT")];
        let kraken_spot = vec![Symbol::new("ADA", "USDT")];
        assert!(build_candidates(&binance_perp, &kraken_spot).is_empty());
    }

    fn chain(network: &str) -> crate::wallet::types::ChainInfo {
        crate::wallet::types::ChainInfo {
            network: network.to_string(),
            name: network.to_string(),
            deposit_enabled: true,
            withdraw_enabled: true,
            withdraw_fee: Decimal::ZERO,
            withdraw_min: Decimal::ZERO,
            min_confirm: 0,
            contract_address: None,
        }
    }

    #[test]
    fn common_chains_returns_sorted_intersection() {
        let binance = AssetInfo {
            asset: "BTC".to_string(),
            networks: vec![chain("BTC"), chain("BSC")],
        };
        let kraken = AssetInfo {
            asset: "XBT".to_string(),
            networks: vec![chain("BSC"), chain("BTC"), chain("ETH")],
        };
        assert_eq!(common_chains(&binance, &kraken), vec!["BSC".to_string(), "BTC".to_string()]);
    }

    #[test]
    fn common_chains_empty_when_no_shared_network() {
        let binance = AssetInfo {
            asset: "FOO".to_string(),
            networks: vec![chain("ETH")],
        };
        let kraken = AssetInfo {
            asset: "FOO".to_string(),
            networks: vec![chain("SOL")],
        };
        assert!(common_chains(&binance, &kraken).is_empty());
    }

    #[test]
    fn format_overlap_table_handles_empty_input() {
        assert_eq!(format_overlap_table(&[]), "(no overlapping symbols found)");
    }

    #[test]
    fn format_overlap_table_includes_all_rows() {
        let rows = vec![SymbolOverlap {
            coin: "BTC".to_string(),
            binance_perp_symbol: Symbol::new("BTC", "USDT"),
            kraken_spot_symbol: Symbol::new("BTC", "USDT"),
            common_chains: vec!["BSC".to_string(), "BTC".to_string()],
        }];
        let table = format_overlap_table(&rows);
        assert!(table.contains("BTC"));
        assert!(table.contains("BTC/USDT"));
        assert!(table.contains("BSC,BTC"));
    }
}
