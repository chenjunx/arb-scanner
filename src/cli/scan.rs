//! `scan` 子命令：只读地找出币安和副交易所"有交集"的币种——币安有 USDT 本位
//! 永续合约、副交易所有 USDT 现货、且两边钱包信息里至少共享一条可转账的标准
//! 链，打印每个币种的基本信息，作为后续 `open`/`rotate` 操作前的选币依据。
//! 不接入 engine 主循环，也不读取 `config.toml`。

use clap::Args;
use log::info;

use crate::config::ScanConfig;
use crate::exchange_info::binance::BinanceExchangeInfoProvider;
use crate::net;
use crate::types::Venue;
use crate::wallet::binance::BinanceWalletProvider;

use super::wiring;

#[derive(Args, Debug)]
pub struct ScanArgs {
    /// 使用币安测试网
    #[arg(long)]
    pub testnet: bool,

    /// 副交易所：kraken / coinex / nonkyc
    #[arg(long, default_value = "kraken")]
    pub secondary: String,
}

pub async fn run(args: ScanArgs) -> anyhow::Result<()> {
    let ScanArgs { testnet, secondary } = args;

    let proxy = net::proxy_from_env();
    let binance_info = BinanceExchangeInfoProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let secondary_info = wiring::build_secondary_exchange_info(&secondary, proxy.as_deref())?;
    let binance_wallet = BinanceWalletProvider::from_env(Venue::new("binance"), testnet, proxy.as_deref())?;
    let secondary_wallet = wiring::build_secondary_wallet_provider(&secondary, proxy.as_deref())?;

    let blacklist = ScanConfig::load_blacklist("config.toml");
    info!(
        "scan: looking for symbols overlapping between binance (usdt perpetual) and {secondary} (usdt spot) testnet={testnet} blacklist={blacklist:?}"
    );

    let result = crate::scan::find_overlap(
        &binance_info,
        secondary_info.as_ref(),
        &binance_wallet,
        secondary_wallet.as_ref(),
        &blacklist,
    )
    .await?;
    info!(
        "scan: binance_spot_symbols={} {secondary}_spot_symbols={} overlapping_symbols={} skipped={} blacklisted={}",
        result.binance_spot_symbols.len(),
        result.secondary_spot_symbols.len(),
        result.overlaps.len(),
        result.skipped.len(),
        result.blacklisted.len()
    );

    println!("== Binance USDT Spot Symbols With Perp Hedge ({}) ==", result.binance_spot_symbols.len());
    println!("{}", crate::scan::format_symbol_list(&result.binance_spot_symbols));
    println!();
    println!("== {} USDT Spot Symbols ({}) ==", secondary.to_uppercase(), result.secondary_spot_symbols.len());
    println!("{}", crate::scan::format_symbol_list(&result.secondary_spot_symbols));
    println!();
    println!("== Overlapping Symbols ({}) ==", result.overlaps.len());
    println!("{}", crate::scan::format_overlap_table(&result.overlaps));
    println!();
    println!("== Skipped Candidates ({}) ==", result.skipped.len());
    println!("{}", crate::scan::format_skipped_list(&result.skipped));
    println!();
    println!("== Blacklisted Coins (excluded, not queried) ({}) ==", result.blacklisted.len());
    println!("{}", crate::scan::format_blacklisted_list(&result.blacklisted));
    Ok(())
}
