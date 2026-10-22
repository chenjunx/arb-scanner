//! `cross-funding-review`：读 `cross-funding` 写的决策/结果记录(JSON Lines)，按
//! "开仓等价差"的复盘规则出结论：攒满 `--min-orders`(默认 30)单已成交的结果后，
//! 比较平均实际节省和平均期望 B：
//! - 实际 ≤ 0：建议停用；
//! - 实际 ≥ 期望的一半：继续；
//! - 介于两者之间：拆成交率偏差和滑点，人工排查。

use std::collections::{BTreeMap, HashSet};

use anyhow::Context;
use clap::{Args, ValueEnum};

use crate::config::CrossFundingConfig;
use crate::strategy::cross_funding::{KNOWN_LIMITATIONS, OrderRecord, RecordEvent, TriggerKind};

#[derive(Args, Debug)]
pub struct CrossFundingReviewArgs {
    /// 记录文件；不传则取 --config 里 `[cross_funding].record_file`
    #[arg(long)]
    pub records: Option<String>,

    #[arg(long, default_value = "config.toml")]
    pub config: String,

    /// 只统计哪种模式的记录
    #[arg(long, value_enum, default_value_t = ModeFilter::Live)]
    pub mode: ModeFilter,

    /// 只统计某个配对(如 binance:BTCUSDT|okx:BTC-USDT-SWAP)
    #[arg(long)]
    pub pair: Option<String>,

    /// 至少多少单已成交才下结论
    #[arg(long, default_value_t = 30)]
    pub min_orders: usize,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModeFilter {
    Live,
    Paper,
    All,
}

impl ModeFilter {
    fn accepts(self, mode: &str) -> bool {
        match self {
            Self::Live => mode == "live",
            Self::Paper => mode == "paper",
            Self::All => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Verdict {
    /// 实际 ≥ 期望的一半。
    Continue,
    /// 0 < 实际 < 期望的一半。
    Investigate,
    /// 实际 ≤ 0。
    Stop,
    /// 期望 B 平均 ≤ 0(全是立即开)，没有可比的期望。
    NoExpectation,
}

fn mean(values: impl IntoIterator<Item = f64>) -> Option<f64> {
    let (sum, n) = values.into_iter().fold((0.0, 0usize), |(s, n), v| (s + v, n + 1));
    (n > 0).then(|| sum / n as f64)
}

/// 一组已成交结果的汇总。
#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    pub filled: usize,
    pub immediate: usize,
    pub target: usize,
    pub expiry: usize,
    pub mean_savings_bp: Option<f64>,
    pub mean_expected_b_bp: Option<f64>,
    /// 等待单里实际在目标价成交的比例 vs 校准的平均成交率。
    pub actual_fill_rate: Option<f64>,
    pub expected_fill_rate: Option<f64>,
    /// 每单两腿滑点之和的平均。
    pub mean_slippage_bp: Option<f64>,
    pub mean_target_savings_bp: Option<f64>,
    pub mean_expiry_savings_bp: Option<f64>,
}

impl Summary {
    pub fn from_results(results: &[&OrderRecord]) -> Self {
        let kind = |k: TriggerKind| results.iter().filter(move |r| r.trigger == Some(k));
        let waits: Vec<&&OrderRecord> = results.iter().filter(|r| r.k_star.is_some()).collect();
        let target_waits = waits.iter().filter(|r| r.trigger == Some(TriggerKind::Target)).count();
        Self {
            filled: results.len(),
            immediate: kind(TriggerKind::Immediate).count(),
            target: kind(TriggerKind::Target).count(),
            expiry: kind(TriggerKind::Expiry).count(),
            mean_savings_bp: mean(results.iter().filter_map(|r| r.savings_bp)),
            mean_expected_b_bp: mean(results.iter().map(|r| r.expected_b_bp)),
            actual_fill_rate: (!waits.is_empty()).then(|| target_waits as f64 / waits.len() as f64),
            expected_fill_rate: mean(waits.iter().filter_map(|r| r.expected_fill_rate)),
            mean_slippage_bp: mean(
                results
                    .iter()
                    .filter_map(|r| Some(r.long_slippage_bp? + r.short_slippage_bp?)),
            ),
            mean_target_savings_bp: mean(kind(TriggerKind::Target).filter_map(|r| r.savings_bp)),
            mean_expiry_savings_bp: mean(kind(TriggerKind::Expiry).filter_map(|r| r.savings_bp)),
        }
    }

    pub fn verdict(&self) -> Option<Verdict> {
        let savings = self.mean_savings_bp?;
        let expected = self.mean_expected_b_bp?;
        Some(if savings <= 0.0 {
            Verdict::Stop
        } else if expected <= 0.0 {
            Verdict::NoExpectation
        } else if savings >= expected / 2.0 {
            Verdict::Continue
        } else {
            Verdict::Investigate
        })
    }
}

/// 解析记录文件。坏行(比如进程被杀时写了半行)跳过并返回行号。
pub fn parse_records(text: &str) -> (Vec<OrderRecord>, Vec<usize>) {
    let mut records = Vec::new();
    let mut bad = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(line) {
            Ok(record) => records.push(record),
            Err(_) => bad.push(idx + 1),
        }
    }
    (records, bad)
}

fn fmt(x: Option<f64>) -> String {
    x.map(|x| format!("{x:.4}")).unwrap_or_else(|| "-".to_string())
}

fn fmt_pct(x: Option<f64>) -> String {
    x.map(|x| format!("{:.1}%", x * 100.0)).unwrap_or_else(|| "-".to_string())
}

fn print_summary(title: &str, s: &Summary) {
    println!(
        "{title}: 成交 {} 单(立即开 {} / 目标价 {} / 到期 {})  平均节省 {}bp  平均期望B {}bp  成交率 实际 {} / 期望 {}  两腿滑点合计 {}bp  节省 目标价单 {}bp / 到期单 {}bp",
        s.filled,
        s.immediate,
        s.target,
        s.expiry,
        fmt(s.mean_savings_bp),
        fmt(s.mean_expected_b_bp),
        fmt_pct(s.actual_fill_rate),
        fmt_pct(s.expected_fill_rate),
        fmt(s.mean_slippage_bp),
        fmt(s.mean_target_savings_bp),
        fmt(s.mean_expiry_savings_bp),
    );
}

pub fn run(args: CrossFundingReviewArgs) -> anyhow::Result<()> {
    let path = match &args.records {
        Some(path) => path.clone(),
        None => CrossFundingConfig::load(&args.config)?.record_file,
    };
    let text = std::fs::read_to_string(&path).with_context(|| format!("failed to read records {path}"))?;
    let (records, bad) = parse_records(&text);
    if !bad.is_empty() {
        println!("警告：{path} 有 {} 行无法解析，已跳过(行号 {:?})", bad.len(), bad);
    }
    let selected: Vec<&OrderRecord> = records
        .iter()
        .filter(|r| args.mode.accepts(&r.mode))
        .filter(|r| args.pair.as_ref().is_none_or(|p| &r.pair == p))
        .collect();

    let key = |r: &OrderRecord| (r.mode.clone(), r.pair.clone(), r.t0_ms);
    let finished: HashSet<_> = selected.iter().filter(|r| r.event == RecordEvent::Result).map(|r| key(r)).collect();
    let decisions = selected.iter().filter(|r| r.event == RecordEvent::Decision).count();
    let open = selected.iter().filter(|r| r.event == RecordEvent::Decision && !finished.contains(&key(r))).count();
    let results: Vec<&OrderRecord> = selected.iter().copied().filter(|r| r.event == RecordEvent::Result).collect();
    let filled: Vec<&OrderRecord> =
        results.iter().copied().filter(|r| r.outcome.as_deref() == Some("filled")).collect();
    let skipped: Vec<&OrderRecord> =
        results.iter().copied().filter(|r| r.outcome.as_deref().is_some_and(|o| o.starts_with("skipped"))).collect();
    let halted: Vec<&OrderRecord> = results
        .iter()
        .copied()
        .filter(|r| r.outcome.as_deref().is_none_or(|o| o != "filled" && !o.starts_with("skipped")))
        .collect();

    println!("记录 {path}，模式 {:?}{}", args.mode, args.pair.as_ref().map(|p| format!("，配对 {p}")).unwrap_or_default());
    println!(
        "决策 {decisions} 次，成交 {} 单，不开仓 {} 次，停止 {} 单，无结果 {open} 次(还在等待，或进程重启前被中断)",
        filled.len(),
        skipped.len(),
        halted.len()
    );
    for r in &skipped {
        println!("  不开仓：{} t0={} {}", r.pair, r.t0_utc, r.skip_reason.as_deref().unwrap_or("-"));
    }
    for r in &halted {
        println!("  停止：{} t0={} {}", r.pair, r.t0_utc, r.outcome.as_deref().unwrap_or("-"));
    }

    let summary = Summary::from_results(&filled);
    print_summary("合计", &summary);
    let mut by_pair: BTreeMap<&str, Vec<&OrderRecord>> = BTreeMap::new();
    for r in &filled {
        by_pair.entry(r.pair.as_str()).or_default().push(r);
    }
    if by_pair.len() > 1 {
        for (pair, rows) in &by_pair {
            print_summary(&format!("  {pair}"), &Summary::from_results(rows));
        }
    }

    if filled.len() < args.min_orders {
        println!("样本不足：已成交 {}/{} 单，暂不下结论(以下仅供参考)", filled.len(), args.min_orders);
    }
    match summary.verdict() {
        None => println!("结论：没有已成交的结果"),
        Some(Verdict::Stop) => println!("结论：平均实际节省 ≤ 0，建议停用等价差规则(全部立即开)"),
        Some(Verdict::NoExpectation) => println!("结论：成交的都是立即开，期望 B 为 0，没有可比的期望"),
        Some(Verdict::Continue) => println!("结论：平均实际节省 ≥ 期望 B 的一半，继续"),
        Some(Verdict::Investigate) => {
            println!("结论：平均实际节省不到期望 B 的一半，拆解：");
            println!(
                "  成交率偏差：等待单实际在目标价成交 {}，校准期望 {}",
                fmt_pct(summary.actual_fill_rate),
                fmt_pct(summary.expected_fill_rate)
            );
            println!("  滑点：每单两腿滑点合计平均 {}bp(相对触发时对手价，正数为吃亏)", fmt(summary.mean_slippage_bp));
            println!(
                "  分触发方式的平均节省：目标价单 {}bp，到期单 {}bp",
                fmt(summary.mean_target_savings_bp),
                fmt(summary.mean_expiry_savings_bp)
            );
        }
    }
    println!("\n已知局限：\n{KNOWN_LIMITATIONS}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(trigger: TriggerKind, savings: f64, expected: f64, slippage: f64) -> OrderRecord {
        let wait = trigger != TriggerKind::Immediate;
        let json = serde_json::json!({
            "event": "result", "mode": "live", "pair": "binance:BTCUSDT|okx:BTC-USDT-SWAP", "csv_line": 2,
            "coins": "0.01", "table_version": 1, "t0_ms": 0, "t0_utc": "-", "bar_open_ms": 0, "bar_spread_bp": 0.0,
            "k_star": if wait { Some(2.0) } else { None },
            "expected_b_bp": expected, "expected_fill_rate": if wait { Some(0.5) } else { None },
            "t0_mid_bp": 0.0, "baseline_bp": 1.0, "outcome": "filled", "trigger": trigger,
            "long_slippage_bp": slippage, "short_slippage_bp": 0.0, "savings_bp": savings, "liquidity": "taker",
        });
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn summary_and_verdict() {
        let rows = [
            record(TriggerKind::Target, 4.0, 2.0, 0.5),
            record(TriggerKind::Expiry, -1.0, 2.0, 0.0),
            record(TriggerKind::Immediate, 0.0, 0.0, 0.1),
        ];
        let refs: Vec<&OrderRecord> = rows.iter().collect();
        let s = Summary::from_results(&refs);
        assert_eq!((s.filled, s.immediate, s.target, s.expiry), (3, 1, 1, 1));
        assert_eq!(s.mean_savings_bp, Some(1.0));
        assert_eq!(s.mean_expected_b_bp, Some(4.0 / 3.0));
        assert_eq!(s.actual_fill_rate, Some(0.5));
        assert_eq!(s.expected_fill_rate, Some(0.5));
        assert!((s.mean_slippage_bp.unwrap() - 0.2).abs() < 1e-12);
        // 1.0 ≥ 4/3 ÷ 2
        assert_eq!(s.verdict(), Some(Verdict::Continue));

        let low = [record(TriggerKind::Expiry, 0.5, 2.0, 0.0)];
        assert_eq!(Summary::from_results(&low.iter().collect::<Vec<_>>()).verdict(), Some(Verdict::Investigate));
        let neg = [record(TriggerKind::Expiry, -0.5, 2.0, 0.0)];
        assert_eq!(Summary::from_results(&neg.iter().collect::<Vec<_>>()).verdict(), Some(Verdict::Stop));
        let imm = [record(TriggerKind::Immediate, 0.1, 0.0, 0.0)];
        assert_eq!(Summary::from_results(&imm.iter().collect::<Vec<_>>()).verdict(), Some(Verdict::NoExpectation));
        assert_eq!(Summary::from_results(&[]).verdict(), None);
    }

    #[test]
    fn parse_skips_bad_lines() {
        let good = serde_json::to_string(&record(TriggerKind::Target, 1.0, 1.0, 0.0)).unwrap();
        let (records, bad) = parse_records(&format!("{good}\n\n{{\"event\":\n{good}\n"));
        assert_eq!(records.len(), 2);
        assert_eq!(bad, vec![3]);
    }
}
