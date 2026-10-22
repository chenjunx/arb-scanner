//! `cross-funding-kstar` 子命令：开仓等价差规则的 k* 表维护(见
//! [`crate::strategy::spread_wait`])。
//!
//! - `calibrate`：对 CSV(或 `--pair`)里每对腿拉 365 天 + 30 天窗口的 1h K 线，
//!   跑完整校准，作为新版本写进 Redis，并和生效版本比较 k*；**不会**自动生效；
//! - `approve`：人工确认后把某个版本设为生效版本，策略只用生效版本；
//! - `list`/`show`：查看已有版本。
//!
//! 只用交易所公开接口，不需要 API Key。

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Args, Subcommand};

use crate::exchange_info::ExchangeInfoProvider;
use crate::exchange_info::binance::binance_perp_contract_spec;
use crate::exchange_info::bitget::BitgetExchangeInfoProvider;
use crate::exchange_info::gate::GateExchangeInfoProvider;
use crate::exchange_info::okx::OkxExchangeInfoProvider;
use crate::exchange_info::types::PerpContractSpec;
use crate::market_data::binance_futures::BinanceFuturesKlineClient;
use crate::market_data::bitget_futures::BitgetFuturesKlineClient;
use crate::market_data::gate_futures::GateFuturesKlineClient;
use crate::market_data::okx_futures::OkxFuturesKlineClient;
use crate::market_data::{HOUR_MS, KlineInterval, KlineProvider, now_ms};
use crate::net;
use crate::strategy::cross_funding::{LegSpec, PerpExchange, parse_pair_key, parse_pairs_csv, pair_key};
use crate::strategy::kstar_store::KStarStore;
use crate::strategy::spread_wait::{
    BinAction, HourlySpread, KStarTable, LOOKBACK_HOURS, RuleParams, WINDOW_HOURS, calibrate, k_star_changes,
    latest_state,
};
use crate::utc;

use super::wiring;

/// 新表某档 k* 和生效表相差超过这个值就告警。
pub const K_STAR_ALERT_THRESHOLD: f64 = 0.5;

#[derive(Args, Debug)]
pub struct CrossFundingKStarArgs {
    #[command(subcommand)]
    pub action: KStarAction,
}

#[derive(Subcommand, Debug)]
pub enum KStarAction {
    /// 拉历史 K 线重新校准，结果作为新版本写入 Redis(不自动生效)
    Calibrate(CalibrateArgs),
    /// 人工确认后把某个版本设为生效版本
    Approve(ApproveArgs),
    /// 列出各配对的版本和生效版本
    List(ListArgs),
    /// 打印某个版本的完整 k* 表(默认生效版本)
    Show(ShowArgs),
}

#[derive(Args, Debug)]
pub struct CalibrateArgs {
    /// 合约对 CSV(和 cross-funding 同一个文件，quantity 列不用)
    #[arg(long, required_unless_present = "pair")]
    pub csv: Option<String>,

    /// 直接指定配对，可重复，如 `binance:BTCUSDT|okx:BTC-USDT-SWAP`(多腿|空腿)
    #[arg(long)]
    pub pair: Vec<String>,

    /// 数据截止时间(最后一根小时 K 线的开盘时间，UTC)，如 2026-10-20T23:00Z；
    /// 不传就用最近一根已收盘的小时。用于复现某一时点的表
    #[arg(long)]
    pub end: Option<String>,

    /// 只打印不写 Redis
    #[arg(long)]
    pub no_save: bool,

    /// 另把每对腿的表以 JSON 写到这个目录
    #[arg(long)]
    pub out: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct ApproveArgs {
    /// 配对，如 `binance:BTCUSDT|okx:BTC-USDT-SWAP`
    #[arg(long)]
    pub pair: String,

    /// 要生效的版本号(calibrate/list 打印的那个)
    #[arg(long)]
    pub version: u64,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    /// 只看这一对，不传列出全部
    #[arg(long)]
    pub pair: Option<String>,
}

#[derive(Args, Debug)]
pub struct ShowArgs {
    #[arg(long)]
    pub pair: String,

    /// 不传就是生效版本
    #[arg(long)]
    pub version: Option<u64>,
}

pub async fn run(args: CrossFundingKStarArgs) -> anyhow::Result<()> {
    match args.action {
        KStarAction::Calibrate(args) => run_calibrate(args).await,
        KStarAction::Approve(args) => run_approve(args),
        KStarAction::List(args) => run_list(args),
        KStarAction::Show(args) => run_show(args),
    }
}

/// 规范化用户输入的配对(大小写、空格)，顺便校验格式。
fn normalize_pair(key: &str) -> anyhow::Result<String> {
    let (long, short) = parse_pair_key(key)?;
    Ok(pair_key(&long, &short))
}

fn store() -> anyhow::Result<KStarStore> {
    KStarStore::new(&wiring::redis_url())
}

async fn run_calibrate(args: CalibrateArgs) -> anyhow::Result<()> {
    let mut pairs: Vec<(LegSpec, LegSpec)> = Vec::new();
    if let Some(csv) = &args.csv {
        let text = std::fs::read_to_string(csv).with_context(|| format!("failed to read csv {csv}"))?;
        let rows = parse_pairs_csv(&text).with_context(|| format!("failed to parse csv {csv}"))?;
        pairs.extend(rows.into_iter().map(|r| (r.long, r.short)));
    }
    for key in &args.pair {
        pairs.push(parse_pair_key(key)?);
    }
    let mut seen = BTreeSet::new();
    pairs.retain(|(l, s)| seen.insert(pair_key(l, s)));
    if pairs.is_empty() {
        anyhow::bail!("没有要校准的配对");
    }

    let end_ms = match &args.end {
        Some(end) => utc::parse(end)? / HOUR_MS * HOUR_MS,
        None => (now_ms() / HOUR_MS - 1) * HOUR_MS,
    };
    // 第一个样本(end − 365 天之后的那一小时)也要有完整的 720 行窗口。
    let start_ms = end_ms - (LOOKBACK_HOURS + WINDOW_HOURS as u64) * HOUR_MS;
    let store = if args.no_save { None } else { Some(store()?) };
    if let Some(dir) = &args.out {
        std::fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    }
    let proxy = net::proxy_from_env();

    let mut failures = 0;
    for (long, short) in &pairs {
        let key = pair_key(long, short);
        println!("\n==== {key} ====");
        let result = calibrate_pair(long, short, start_ms, end_ms, proxy.as_deref()).await;
        let table = match result {
            Ok(table) => table,
            Err(err) => {
                failures += 1;
                println!("校准失败：{err:#}");
                continue;
            }
        };
        print_table(&table);
        if let Some(dir) = &args.out {
            let path = dir.join(format!("kstar_{}_{}.json", key.replace([':', '|'], "_"), table.version));
            std::fs::write(&path, serde_json::to_string_pretty(&table)?)
                .with_context(|| format!("failed to write {}", path.display()))?;
            println!("JSON 已写入 {}", path.display());
        }
        let Some(store) = &store else { continue };
        let active = store.active(&key)?;
        match &active {
            None => println!("\n该配对还没有生效版本(首版)，需要人工确认后生效。"),
            Some(prev) => print_changes(prev, &table),
        }
        store.save(&table)?;
        println!("\n已写入 Redis 版本 {}(未生效)。确认无误后执行：", table.version);
        println!("  arb-scanner cross-funding-kstar approve --pair '{key}' --version {}", table.version);
    }
    if failures > 0 {
        anyhow::bail!("{failures}/{} 对校准失败", pairs.len());
    }
    Ok(())
}

async fn calibrate_pair(
    long: &LegSpec,
    short: &LegSpec,
    start_ms: u64,
    end_ms: u64,
    proxy: Option<&str>,
) -> anyhow::Result<KStarTable> {
    let (long_spec, short_spec) =
        tokio::try_join!(contract_spec(long, proxy), contract_spec(short, proxy)).context("合约规格")?;
    let (long_client, short_client) = (kline_client(long.exchange, proxy)?, kline_client(short.exchange, proxy)?);
    let (long_klines, short_klines) = tokio::try_join!(
        long_client.klines_range(&long.symbol, KlineInterval::H1, start_ms, end_ms),
        short_client.klines_range(&short.symbol, KlineInterval::H1, start_ms, end_ms),
    )
    .context("历史 K 线")?;
    for (leg, klines) in [(long, &long_klines), (short, &short_klines)] {
        match (klines.first(), klines.last()) {
            (Some(first), Some(last)) => println!(
                "{}:{} 1h K 线 {} 根，{} … {}",
                leg.exchange,
                leg.native_symbol,
                klines.len(),
                utc::format_minute(first.open_time_ms),
                utc::format_minute(last.open_time_ms)
            ),
            _ => anyhow::bail!("{}:{} 没有 K 线", leg.exchange, leg.native_symbol),
        }
    }
    let series = HourlySpread::from_klines(&long_klines, &short_klines, long_spec.price_unit, short_spec.price_unit)
        .ok_or_else(|| anyhow::anyhow!("两腿 K 线时间没有重叠"))?;
    if series.start_ms > start_ms {
        println!(
            "注意：数据从 {} 才开始(请求从 {})，样本少于 365 天",
            utc::format_minute(series.start_ms),
            utc::format_minute(start_ms)
        );
    }
    let table = calibrate(&pair_key(long, short), &series, RuleParams::default(), now_ms())?;
    if let Some(state) = latest_state(&series) {
        let fmt = |v: Option<f64>| v.map_or("缺".to_string(), |v| format!("{v:.6}"));
        println!(
            "最后一根 {}：价差 {:.6}bp，百分位 {}，σ {}bp，σ 比 {}",
            utc::format_minute(state.open_time_ms),
            state.spread_bp,
            fmt(state.pct),
            fmt(state.sigma_bp),
            fmt(state.sigma_bp.map(|s| s / table.sigma_median_bp)),
        );
    }
    Ok(table)
}

/// 合约规格只走公开接口：币安按名字推算，Gate 的公开接口不签名，凭证留空。
async fn contract_spec(leg: &LegSpec, proxy: Option<&str>) -> anyhow::Result<PerpContractSpec> {
    let venue = leg.venue();
    let provider: Box<dyn ExchangeInfoProvider> = match leg.exchange {
        PerpExchange::Binance => return Ok(binance_perp_contract_spec(&leg.symbol)),
        PerpExchange::Gate => Box::new(GateExchangeInfoProvider::new(venue, String::new(), String::new(), proxy)?),
        PerpExchange::Okx => Box::new(OkxExchangeInfoProvider::new(venue, proxy)?),
        PerpExchange::Bitget => Box::new(BitgetExchangeInfoProvider::new(venue, proxy)?),
    };
    provider.perpetual_contract_spec(&leg.symbol).await
}

fn kline_client(exchange: PerpExchange, proxy: Option<&str>) -> anyhow::Result<Arc<dyn KlineProvider>> {
    let venue = exchange.venue();
    Ok(match exchange {
        PerpExchange::Binance => Arc::new(BinanceFuturesKlineClient::new(venue, false, proxy)?),
        PerpExchange::Gate => Arc::new(GateFuturesKlineClient::new(venue, proxy)?),
        PerpExchange::Okx => Arc::new(OkxFuturesKlineClient::new(venue, proxy)?),
        PerpExchange::Bitget => Arc::new(BitgetFuturesKlineClient::new(venue, proxy)?),
    })
}

fn fmt_k(k: Option<f64>) -> String {
    k.map_or("-".to_string(), |k| format!("{k:.2}"))
}

fn print_table(table: &KStarTable) {
    println!(
        "版本 {} | 数据 {} … {} | 样本 {} | 365 天 σ 中位数 {:.4}bp | 档宽 {} 段数档宽 {} 最少段数 {}",
        table.version,
        utc::format_minute(table.data_start_ms),
        utc::format_minute(table.data_end_ms),
        table.samples,
        table.sigma_median_bp,
        table.rules.width,
        table.rules.segment_width,
        table.rules.min_segments,
    );
    if let Some(o) = &table.overall {
        println!(
            "全部样本：最优 k={:.1} 成交率 {:.1}% 期望B {:.4}bp",
            o.b_k,
            o.b_fill_rate * 100.0,
            o.b_expect_bp
        );
    }
    println!(
        "{:>9} {:>6} {:>5} {:>6} {:>5} {:>7} {:>10} {:>11}  用法",
        "档", "n", "段数", "粗段数", "最优k", "成交率", "期望B(bp)", "平台k"
    );
    for bin in &table.bins {
        let range = format!("[{:.0},{:.0})", bin.lo, bin.hi);
        let action = match &bin.action {
            BinAction::DirectOpen { reason } => format!("直接开：{reason}"),
            BinAction::Wait { k_star, note: None } => format!("等 k*={k_star:.2}"),
            BinAction::Wait { k_star, note: Some(note) } => format!("等 k*={k_star:.2}({note})"),
        };
        match &bin.stats {
            None => println!("{range:>9} {:>6} {:>5} {:>6} {:>5} {:>7} {:>10} {:>11}  {action}", 0, "-", bin.coarse_segments, "-", "-", "-", "-"),
            Some(s) => {
                let platform = s.platform.map_or("-".to_string(), |(lo, hi)| format!("{lo:.1}~{hi:.1}"));
                println!(
                    "{range:>9} {:>6} {:>5} {:>6} {:>5.1} {:>6.1}% {:>10.4} {:>11}  {action}",
                    s.n,
                    s.segments,
                    bin.coarse_segments,
                    s.b_k,
                    s.b_fill_rate * 100.0,
                    s.b_expect_bp,
                    platform
                );
            }
        }
    }
}

fn print_changes(prev: &KStarTable, next: &KStarTable) {
    let changes = k_star_changes(&prev.bins, &next.bins, K_STAR_ALERT_THRESHOLD);
    if changes.is_empty() {
        println!("\n和生效版本 {} 相比，各档 k* 变化都不超过 {K_STAR_ALERT_THRESHOLD}。", prev.version);
        return;
    }
    println!("\n⚠ 告警：和生效版本 {} 相比，以下档 k* 变化超过 {K_STAR_ALERT_THRESHOLD}(直接开按 0 算)：", prev.version);
    for c in changes {
        println!("  [{:.0},{:.0}) k* {} → {}", c.lo, c.hi, fmt_k(c.prev), fmt_k(c.next));
    }
}

fn run_approve(args: ApproveArgs) -> anyhow::Result<()> {
    let pair = normalize_pair(&args.pair)?;
    let store = store()?;
    let table = store.get(&pair, args.version)?.ok_or_else(|| anyhow::anyhow!("{pair} 没有版本 {}", args.version))?;
    print_table(&table);
    match store.active(&pair)? {
        Some(prev) if prev.version == table.version => {
            println!("\n版本 {} 已经是生效版本。", table.version);
            return Ok(());
        }
        Some(prev) => print_changes(&prev, &table),
        None => println!("\n首版生效。"),
    }
    store.approve(&pair, table.version)?;
    println!("\n{pair} 生效版本 → {}。正在运行的 cross-funding 要重启才会读到。", table.version);
    Ok(())
}

fn run_list(args: ListArgs) -> anyhow::Result<()> {
    let store = store()?;
    let pairs = match &args.pair {
        Some(pair) => vec![normalize_pair(pair)?],
        None => store.pairs()?,
    };
    if pairs.is_empty() {
        println!("Redis 里还没有任何 k* 表");
    }
    for pair in pairs {
        let active = store.active_version(&pair)?;
        println!("{pair}  生效版本 {}", active.map_or("无".to_string(), |v| v.to_string()));
        for version in store.versions(&pair)? {
            let marker = if Some(version) == active { "*" } else { " " };
            println!("  {marker} {version}  {}", utc::format_minute(version));
        }
    }
    Ok(())
}

fn run_show(args: ShowArgs) -> anyhow::Result<()> {
    let pair = normalize_pair(&args.pair)?;
    let store = store()?;
    let table = match args.version {
        Some(version) => store.get(&pair, version)?.ok_or_else(|| anyhow::anyhow!("{pair} 没有版本 {version}"))?,
        None => store.active(&pair)?.ok_or_else(|| anyhow::anyhow!("{pair} 没有生效版本"))?,
    };
    print_table(&table);
    Ok(())
}
