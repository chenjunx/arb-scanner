use std::collections::{HashMap, HashSet};

use futures_util::StreamExt;
use futures_util::stream;

use crate::exchange_info::ExchangeInfoProvider;
use crate::exchange_info::binance::BinanceExchangeInfoProvider;
use crate::exchange_info::types::SpotPerpPair;
use crate::types::Symbol;
use crate::wallet::WalletProvider;
use crate::wallet::binance::BinanceWalletProvider;
use crate::wallet::types::AssetInfo;

/// 一个通过粗筛的候选币种：币安有 USDT 现货+永续(用
/// [`BinanceExchangeInfoProvider::spot_perpetual_pairs`] 配好对，已经处理了
/// "1000PEPE" 这类合约乘数前缀)、副交易所有 USDT 现货，且是同一个币种。是否
/// 真的"有交集"还要看 [`common_chains`]。
#[derive(Debug, Clone, PartialEq)]
struct Candidate {
    /// 标准币名(以币安现货 base 为准，如 "BTC"、"PEPE")。
    coin: String,
    /// 币安永续 symbol，用来对冲(如 "1000PEPE/USDT")。
    binance_symbol: Symbol,
    secondary_symbol: Symbol,
}

/// 两个交易所都能交易、且共享至少一条可转账链的币种基本信息。
#[derive(Debug, Clone, PartialEq)]
pub struct SymbolOverlap {
    pub coin: String,
    pub binance_perp_symbol: Symbol,
    pub secondary_spot_symbol: Symbol,
    /// 两边钱包信息里都支持的标准链名，已排序。
    pub common_chains: Vec<String>,
}

/// 一个候选币种在求交集过程中被跳过的原因，供 `scan` 打印，避免只能靠
/// `log::warn!`(默认写到 stderr，容易在终端截图/复制粘贴时被漏掉)才能定位
/// "两边都有这个币种，为什么最终没出现在交集表里"这类问题。
#[derive(Debug, Clone, PartialEq)]
pub struct SkippedCandidate {
    pub coin: String,
    pub reason: String,
}

/// `find_overlap` 的完整结果：除了最终的交集表，也带上两边各自的完整币种列表
/// (`scan` 打印时要展示"币安都有哪些能用永续对冲的现货币"/"副交易所都有哪些
/// USDT 现货"，避免调用方为了拿这两份列表再多发一次网络请求)。
pub struct ScanResult {
    /// 币安 USDT 现货里，同一币种在币安还有 USDT 永续合约(可以用来对冲)的那些
    /// symbol——实际开仓买的是现货，永续只是用来对冲，所以这里展示的是现货
    /// symbol 而不是永续 symbol。来自 [`BinanceExchangeInfoProvider::spot_perpetual_pairs`]。
    pub binance_spot_symbols: Vec<Symbol>,
    pub secondary_spot_symbols: Vec<Symbol>,
    pub overlaps: Vec<SymbolOverlap>,
    /// 两边都有挂牌、但在求交集过程中被跳过的候选币种(钱包信息查询失败/两边
    /// 都没有共同链等)，附带原因，按币名排序。
    pub skipped: Vec<SkippedCandidate>,
    /// 两边都有挂牌、但命中黑名单被提前剔除的候选币种(未发起任何钱包/手续费
    /// 查询)，按币名排序。
    pub blacklisted: Vec<String>,
}

/// 按黑名单(大小写不敏感)把候选币种拆成"保留"和"被剔除"两组，剔除的那组只
/// 保留币名(已排序)。放在任何钱包 API 调用之前调用，保证命中黑名单的币种
/// 完全不产生网络请求。
fn partition_blacklisted(candidates: Vec<Candidate>, blacklist: &[String]) -> (Vec<Candidate>, Vec<String>) {
    let blacklist_set: HashSet<String> = blacklist.iter().map(|c| c.to_ascii_uppercase()).collect();
    let (kept, removed): (Vec<Candidate>, Vec<Candidate>) =
        candidates.into_iter().partition(|c| !blacklist_set.contains(&c.coin));
    let mut blacklisted: Vec<String> = removed.into_iter().map(|c| c.coin).collect();
    blacklisted.sort();
    (kept, blacklisted)
}

/// 从币安现货/永续配对列表和副交易所 USDT 现货列表算出候选币种。用币安现货
/// base 去匹配副交易所(而不是永续 base)，因为像 "1000PEPE" 这样带合约乘数
/// 前缀的永续 symbol 在副交易所上不存在对应 base，`spot_perpetual_pairs` 已经
/// 把它还原成真实币种 "PEPE"。副交易所一侧的命名已经在各自的
/// `exchange_info::*::usdt_spot_symbols` 实现里翻译过标准(币安)命名，这里
/// 只需要大小写不敏感的精确匹配。
fn build_candidates(binance_pairs: &[SpotPerpPair], secondary_spot: &[Symbol]) -> Vec<Candidate> {
    let secondary_by_coin: HashMap<String, &Symbol> =
        secondary_spot.iter().map(|s| (s.base.to_ascii_uppercase(), s)).collect();

    binance_pairs
        .iter()
        .filter_map(|pair| {
            let coin = pair.spot_symbol.base.to_ascii_uppercase();
            let secondary_symbol = secondary_by_coin.get(&coin)?;
            Some(Candidate {
                coin,
                binance_symbol: pair.perp_symbol.clone(),
                secondary_symbol: (*secondary_symbol).clone(),
            })
        })
        .collect()
}

/// 求两份钱包资产信息里标准链名(`ChainInfo::network`)的交集，已排序。
fn common_chains(binance: &AssetInfo, secondary: &AssetInfo) -> Vec<String> {
    let binance_networks: HashSet<String> = binance
        .networks
        .iter()
        .map(|n| n.network.to_ascii_uppercase())
        .collect();
    let mut chains: Vec<String> = secondary
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

/// 副交易所私有钱包接口的并发上限，避免对候选币逐个查询时触发对方的限流。
const SECONDARY_WALLET_CONCURRENCY: usize = 4;

/// 算出 Binance / 副交易所有交集的币种：币安有 USDT 永续合约、副交易所有 USDT
/// 现货、且两边钱包信息里至少共享一条标准链(可转账)。交集结果按币名排序。
pub async fn find_overlap(
    binance_info: &BinanceExchangeInfoProvider,
    secondary_info: &dyn ExchangeInfoProvider,
    binance_wallet: &BinanceWalletProvider,
    secondary_wallet: &dyn WalletProvider,
    blacklist: &[String],
) -> anyhow::Result<ScanResult> {
    let (binance_pairs, secondary_spot) =
        tokio::try_join!(binance_info.spot_perpetual_pairs(), secondary_info.usdt_spot_symbols())?;
    let mut binance_hedgeable_spot: Vec<Symbol> = binance_pairs.iter().map(|p| p.spot_symbol.clone()).collect();
    binance_hedgeable_spot.sort_by(|a, b| a.to_string().cmp(&b.to_string()));

    let (candidates, blacklisted) =
        partition_blacklisted(build_candidates(&binance_pairs, &secondary_spot), blacklist);
    log::info!(
        "scan: binance_spot_perp_pairs={} secondary_spot_symbols={} candidates={} blacklisted={}",
        binance_pairs.len(),
        secondary_spot.len(),
        candidates.len(),
        blacklisted.len()
    );
    if candidates.is_empty() {
        return Ok(ScanResult {
            binance_spot_symbols: binance_hedgeable_spot,
            secondary_spot_symbols: secondary_spot,
            overlaps: Vec::new(),
            skipped: Vec::new(),
            blacklisted,
        });
    }

    let binance_assets: HashMap<String, AssetInfo> = binance_wallet
        .all_asset_info()
        .await?
        .into_iter()
        .map(|a| (a.asset.to_ascii_uppercase(), a))
        .collect();

    let secondary_results: Vec<(Candidate, anyhow::Result<AssetInfo>)> = stream::iter(candidates)
        .map(|candidate| async move {
            let result = secondary_wallet.asset_info(&candidate.secondary_symbol.base).await;
            (candidate, result)
        })
        .buffer_unordered(SECONDARY_WALLET_CONCURRENCY)
        .collect()
        .await;

    let mut overlaps = Vec::new();
    let mut skipped = Vec::new();
    for (candidate, secondary_asset) in secondary_results {
        let Some(binance_asset) = binance_assets.get(&candidate.coin) else {
            let reason = "not found in binance wallet asset list".to_string();
            log::warn!("scan: {} {reason}, skipping", candidate.coin);
            skipped.push(SkippedCandidate { coin: candidate.coin, reason });
            continue;
        };
        let secondary_asset = match secondary_asset {
            Ok(info) => info,
            Err(err) => {
                let reason = format!("failed to fetch secondary wallet asset info: {err:#}");
                log::warn!("scan: {} {reason}", candidate.coin);
                skipped.push(SkippedCandidate { coin: candidate.coin, reason });
                continue;
            }
        };

        let chains = common_chains(binance_asset, &secondary_asset);
        if chains.is_empty() {
            let binance_chains: Vec<String> =
                binance_asset.networks.iter().map(|n| n.network.to_ascii_uppercase()).collect();
            let secondary_chains: Vec<String> =
                secondary_asset.networks.iter().map(|n| n.network.to_ascii_uppercase()).collect();
            let reason = format!(
                "no common chain, binance=[{}] secondary=[{}]",
                binance_chains.join(","),
                secondary_chains.join(",")
            );
            log::warn!("scan: {} {reason}", candidate.coin);
            skipped.push(SkippedCandidate { coin: candidate.coin, reason });
            continue;
        }
        overlaps.push(SymbolOverlap {
            coin: candidate.coin,
            binance_perp_symbol: candidate.binance_symbol,
            secondary_spot_symbol: candidate.secondary_symbol,
            common_chains: chains,
        });
    }

    overlaps.sort_by(|a, b| a.coin.cmp(&b.coin));
    skipped.sort_by(|a, b| a.coin.cmp(&b.coin));
    Ok(ScanResult {
        binance_spot_symbols: binance_hedgeable_spot,
        secondary_spot_symbols: secondary_spot,
        overlaps,
        skipped,
        blacklisted,
    })
}

/// 把一组 symbol 排版成多列、左对齐的清单，供 `scan` 打印币安/副交易所各自的
/// 完整币种列表。按 symbol 的展示字符串排序，固定每行 `COLUMNS` 列。
pub fn format_symbol_list(symbols: &[Symbol]) -> String {
    const COLUMNS: usize = 6;

    if symbols.is_empty() {
        return "(none)".to_string();
    }

    let mut names: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
    names.sort();

    let width = names.iter().map(|n| n.len()).max().unwrap_or(0);
    let mut out = String::new();
    for chunk in names.chunks(COLUMNS) {
        let line = chunk.iter().map(|n| format!("{n:<width$}")).collect::<Vec<_>>().join("  ");
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out.pop();
    out
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
    let secondary_width = rows
        .iter()
        .map(|r| r.secondary_spot_symbol.to_string().len())
        .max()
        .unwrap_or(11)
        .max(11);

    let header = format!(
        "{:<coin_width$}  {:<binance_width$}  {:<secondary_width$}  COMMON_CHAINS",
        "COIN", "BINANCE_PERP", "SECONDARY_SPOT"
    );
    let separator = "-".repeat(header.len());
    let mut out = format!("{header}\n{separator}\n");
    for row in rows {
        out.push_str(&format!(
            "{:<coin_width$}  {:<binance_width$}  {:<secondary_width$}  {}\n",
            row.coin,
            row.binance_perp_symbol.to_string(),
            row.secondary_spot_symbol.to_string(),
            row.common_chains.join(",")
        ));
    }
    out.pop();
    out
}

/// 把跳过原因列表拼成一行一条的文本，供 `scan` 打印，解释"两边都挂牌但没
/// 进最终交集表"的候选币种具体卡在哪一步。
pub fn format_skipped_list(skipped: &[SkippedCandidate]) -> String {
    if skipped.is_empty() {
        return "(none)".to_string();
    }
    let coin_width = skipped.iter().map(|s| s.coin.len()).max().unwrap_or(4).max(4);
    skipped
        .iter()
        .map(|s| format!("{:<coin_width$}  {}", s.coin, s.reason))
        .collect::<Vec<_>>()
        .join("\n")
}

/// 把黑名单剔除的币名列表拼成一行，供 `scan`/`monitor` 打印。
pub fn format_blacklisted_list(coins: &[String]) -> String {
    if coins.is_empty() {
        return "(none)".to_string();
    }
    coins.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn pair(spot_base: &str, perp_base: &str) -> SpotPerpPair {
        SpotPerpPair {
            spot_symbol: Symbol::new(spot_base, "USDT"),
            perp_symbol: Symbol::new(perp_base, "USDT"),
            contract_multiplier: 1,
        }
    }

    #[test]
    fn build_candidates_matches_case_insensitively() {
        let binance_pairs = vec![pair("BTC", "BTC"), pair("ETH", "ETH")];
        let secondary_spot = vec![Symbol::new("btc", "USDT")];
        let candidates = build_candidates(&binance_pairs, &secondary_spot);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].coin, "BTC");
        assert_eq!(candidates[0].secondary_symbol, Symbol::new("btc", "USDT"));
    }

    #[test]
    fn build_candidates_matches_spot_base_not_multiplier_prefixed_perp_base() {
        let binance_pairs = vec![SpotPerpPair {
            spot_symbol: Symbol::new("PEPE", "USDT"),
            perp_symbol: Symbol::new("1000PEPE", "USDT"),
            contract_multiplier: 1000,
        }];
        let secondary_spot = vec![Symbol::new("PEPE", "USDT")];
        let candidates = build_candidates(&binance_pairs, &secondary_spot);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].coin, "PEPE");
        assert_eq!(candidates[0].binance_symbol, Symbol::new("1000PEPE", "USDT"));
    }

    #[test]
    fn build_candidates_excludes_symbols_missing_on_either_side() {
        let binance_pairs = vec![pair("SOL", "SOL")];
        let secondary_spot = vec![Symbol::new("ADA", "USDT")];
        assert!(build_candidates(&binance_pairs, &secondary_spot).is_empty());
    }

    fn candidate(coin: &str) -> Candidate {
        Candidate {
            coin: coin.to_string(),
            binance_symbol: Symbol::new(coin, "USDT"),
            secondary_symbol: Symbol::new(coin, "USDT"),
        }
    }

    #[test]
    fn partition_blacklisted_removes_matching_coins_case_insensitively() {
        let candidates = vec![candidate("BTC"), candidate("ADA"), candidate("ETH")];
        let blacklist = vec!["btc".to_string(), "ETH".to_string()];
        let (kept, blacklisted) = partition_blacklisted(candidates, &blacklist);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].coin, "ADA");
        assert_eq!(blacklisted, vec!["BTC".to_string(), "ETH".to_string()]);
    }

    #[test]
    fn partition_blacklisted_keeps_everything_when_blacklist_empty() {
        let candidates = vec![candidate("BTC"), candidate("ADA")];
        let (kept, blacklisted) = partition_blacklisted(candidates, &[]);
        assert_eq!(kept.len(), 2);
        assert!(blacklisted.is_empty());
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
        let secondary = AssetInfo {
            asset: "XBT".to_string(),
            networks: vec![chain("BSC"), chain("BTC"), chain("ETH")],
        };
        assert_eq!(common_chains(&binance, &secondary), vec!["BSC".to_string(), "BTC".to_string()]);
    }

    #[test]
    fn common_chains_empty_when_no_shared_network() {
        let binance = AssetInfo {
            asset: "FOO".to_string(),
            networks: vec![chain("ETH")],
        };
        let secondary = AssetInfo {
            asset: "FOO".to_string(),
            networks: vec![chain("SOL")],
        };
        assert!(common_chains(&binance, &secondary).is_empty());
    }

    #[test]
    fn format_overlap_table_handles_empty_input() {
        assert_eq!(format_overlap_table(&[]), "(no overlapping symbols found)");
    }

    #[test]
    fn format_symbol_list_handles_empty_input() {
        assert_eq!(format_symbol_list(&[]), "(none)");
    }

    #[test]
    fn format_symbol_list_sorts_and_wraps_columns() {
        let symbols = vec![
            Symbol::new("SOL", "USDT"),
            Symbol::new("BTC", "USDT"),
            Symbol::new("ETH", "USDT"),
        ];
        let list = format_symbol_list(&symbols);
        let first_line = list.lines().next().unwrap();
        assert!(first_line.starts_with("BTC/USDT"));
        assert!(list.contains("ETH/USDT"));
        assert!(list.contains("SOL/USDT"));
    }

    #[test]
    fn format_skipped_list_handles_empty_input() {
        assert_eq!(format_skipped_list(&[]), "(none)");
    }

    #[test]
    fn format_skipped_list_includes_coin_and_reason() {
        let skipped = vec![SkippedCandidate {
            coin: "ETH".to_string(),
            reason: "no common chain, binance=[ETH] secondary=[]".to_string(),
        }];
        let list = format_skipped_list(&skipped);
        assert!(list.contains("ETH"));
        assert!(list.contains("no common chain"));
    }

    #[test]
    fn format_overlap_table_includes_all_rows() {
        let rows = vec![SymbolOverlap {
            coin: "BTC".to_string(),
            binance_perp_symbol: Symbol::new("BTC", "USDT"),
            secondary_spot_symbol: Symbol::new("BTC", "USDT"),
            common_chains: vec!["BSC".to_string(), "BTC".to_string()],
        }];
        let table = format_overlap_table(&rows);
        assert!(table.contains("BTC"));
        assert!(table.contains("BTC/USDT"));
        assert!(table.contains("BSC,BTC"));
    }
}
