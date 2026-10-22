//! 开仓等价差规则：研究 `research/spread_wait_core.py`(`compute`/`limit_curves`/
//! `limit_table`/`recommend`)的 Rust 复刻，只有纯计算，不碰网络和下单。
//!
//! 已经决定开一对仓(A 所做多、B 所做空)时，看当前价差在过去 30 天小时收盘价差
//! 里的百分位，按历史上同一档的最优倍数 k* 定目标：目标价差 = 当前价差 − k*·σ，
//! 最多等 24 小时。价差(bp) = (多腿每币价 − 空腿每币价) / 空腿每币价 × 10000，
//! **越低越适合开仓**。
//!
//! 各函数的口径(窗口含不含 t、NaN 怎么计数、并列名次、ddof、取等号……)都和
//! pandas 实现逐条对齐，见各自注释；改口径前先和研究确认，不要随手"改进"。
//!
//! 已知局限(研究结论，不是 bug)：研究的成交判定是"小时收盘价碰到目标"，偏乐观，
//! 中小币 K 线噪声会同时放大 σ 和收益(ONE 样本外只兑现约一半)；σ 是价差水平的
//! 标准差，拉盘或价差水平切换后会失真，只能靠 σ 告警挡掉；只验证过 BTC 和 ONE
//! 两对腿(都是 binance 多 / okx 空)。

use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::market_data::{HOUR_MS, Kline};

/// 百分位/σ 的滚动窗口：含 t 在内的 720 行(30 天)。
pub const WINDOW_HOURS: usize = 720;
/// 窗口内至少要有这么多个有效小时(ceil(720 × 0.8))，不够时百分位和 σ 记缺。
pub const MIN_VALID_HOURS: usize = 576;
/// 最多等多少小时。
pub const HORIZON_HOURS: usize = 24;
/// 校准样本区间和 σ 中位数区间：最后一根往前 365 天(不含左端点)。
pub const LOOKBACK_HOURS: u64 = 365 * 24;
/// k 网格 0, 0.1, …, 4.0 的点数减一。
pub const K_MAX_TENTHS: usize = 40;
/// 平台范围：期望 B ≥ 最大值 × 这个比例的所有 k。
pub const PLATFORM_RATIO: f64 = 0.9;

/// 价差(bp)，两边都是每币价格。
pub fn spread_bp(long_per_coin: f64, short_per_coin: f64) -> f64 {
    (long_per_coin - short_per_coin) / short_per_coin * 10_000.0
}

/// 交易所报价换算成每币价格(f64)。先按十进制字符串转 f64(和 Python 读 float
/// 一样是正确舍入)，再除以合约乘数 `price_unit`(1000PEPE 这类是 1000)。
pub fn per_coin_f64(price: Decimal, price_unit: Decimal) -> f64 {
    let price: f64 = price.to_string().parse().unwrap_or(f64::NAN);
    let unit: f64 = price_unit.to_string().parse().unwrap_or(f64::NAN);
    if unit == 1.0 { price } else { price / unit }
}

/// k 网格：`np.round(np.arange(0, 4 + 0.05, 0.1), 10)`，即 i/10 的最近 f64。
pub fn k_grid() -> Vec<f64> {
    (0..=K_MAX_TENTHS).map(|i| i as f64 / 10.0).collect()
}

/// 整点小时网格上的价差序列，第 i 个值对应开盘时间 `start_ms + i·1h`，`None` 即
/// pandas 里的 NaN。
#[derive(Debug, Clone, PartialEq)]
pub struct HourlySpread {
    pub start_ms: u64,
    pub spread_bp: Vec<Option<f64>>,
}

impl HourlySpread {
    /// 两腿小时 K 线 → 价差序列。网格覆盖两腿的重叠区间(较晚的第一根 … 较早的
    /// 最后一根)，中间任何一腿缺这一小时、成交量为 0 或价格非正都记 `None`——
    /// 缺失的小时留成空行而不是删掉，因为 pandas 的 `rolling(720)` 按行开窗。
    /// 两腿没有重叠时返回 None。
    pub fn from_klines(long: &[Kline], short: &[Kline], long_price_unit: Decimal, short_price_unit: Decimal) -> Option<Self> {
        let first = |k: &[Kline]| k.iter().map(|k| k.open_time_ms).min();
        let last = |k: &[Kline]| k.iter().map(|k| k.open_time_ms).max();
        let start = first(long)?.max(first(short)?);
        let end = last(long)?.min(last(short)?);
        if start > end {
            return None;
        }
        let valid = |klines: &[Kline], unit: Decimal| -> HashMap<u64, f64> {
            klines
                .iter()
                .filter(|k| k.volume > Decimal::ZERO && k.close > Decimal::ZERO)
                .map(|k| (k.open_time_ms, per_coin_f64(k.close, unit)))
                .collect()
        };
        let long_px = valid(long, long_price_unit);
        let short_px = valid(short, short_price_unit);
        let len = ((end - start) / HOUR_MS + 1) as usize;
        let spread_bp = (0..len)
            .map(|i| {
                let t = start + i as u64 * HOUR_MS;
                Some(spread_bp(*long_px.get(&t)?, *short_px.get(&t)?))
            })
            .collect();
        Some(Self { start_ms: start, spread_bp })
    }

    pub fn len(&self) -> usize {
        self.spread_bp.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spread_bp.is_empty()
    }

    pub fn time_ms(&self, i: usize) -> u64 {
        self.start_ms + i as u64 * HOUR_MS
    }

    /// 最后一行的开盘时间。
    pub fn last_time_ms(&self) -> Option<u64> {
        self.len().checked_sub(1).map(|i| self.time_ms(i))
    }

    /// 开盘时间 > 最后一根 − 365 天的第一行下标。
    fn lookback_start(&self) -> usize {
        self.len().saturating_sub(LOOKBACK_HOURS as usize)
    }
}

/// `x.rolling(window, min_periods).rank(pct=True) * 100`：窗口含 t，NaN 不计数，
/// 并列取平均名次，百分位 = 名次 / 有效个数 × 100(能取到 100，取不到 0)。
/// 当前值缺失或有效个数不足时为 None。
pub fn rolling_pct_with(x: &[Option<f64>], window: usize, min_valid: usize) -> Vec<Option<f64>> {
    (0..x.len())
        .map(|t| {
            let cur = x[t]?;
            let lo = (t + 1).saturating_sub(window);
            let (mut n, mut less, mut equal) = (0usize, 0usize, 0usize);
            for &v in x[lo..=t].iter().flatten() {
                n += 1;
                if v < cur {
                    less += 1;
                } else if v == cur {
                    equal += 1;
                }
            }
            if n < min_valid {
                return None;
            }
            let rank = less as f64 + (equal as f64 + 1.0) / 2.0;
            Some(rank / n as f64 * 100.0)
        })
        .collect()
}

/// `x.rolling(window, min_periods).std()`(ddof=1)：窗口同百分位；和 pandas 一样
/// 只看窗口内有效个数，不要求当前值有效。
pub fn rolling_sigma_with(x: &[Option<f64>], window: usize, min_valid: usize) -> Vec<Option<f64>> {
    (0..x.len())
        .map(|t| {
            let lo = (t + 1).saturating_sub(window);
            let values: Vec<f64> = x[lo..=t].iter().flatten().copied().collect();
            let n = values.len();
            if n < min_valid.max(2) {
                return None;
            }
            let mean = values.iter().sum::<f64>() / n as f64;
            let ss: f64 = values.iter().map(|v| (v - mean) * (v - mean)).sum();
            Some((ss / (n - 1) as f64).sqrt())
        })
        .collect()
}

pub fn rolling_pct(x: &[Option<f64>]) -> Vec<Option<f64>> {
    rolling_pct_with(x, WINDOW_HOURS, MIN_VALID_HOURS)
}

pub fn rolling_sigma(x: &[Option<f64>]) -> Vec<Option<f64>> {
    rolling_sigma_with(x, WINDOW_HOURS, MIN_VALID_HOURS)
}

/// t+1 … t+24 中有效值的最小值(缺失小时跳过)。全部缺失，或数据末端凑不满
/// 24 小时未来(t+24 超出序列)时为 None。
pub fn future_min(x: &[Option<f64>]) -> Vec<Option<f64>> {
    (0..x.len())
        .map(|t| {
            if t + HORIZON_HOURS >= x.len() {
                return None;
            }
            x[t + 1..=t + HORIZON_HOURS].iter().flatten().copied().reduce(f64::min)
        })
        .collect()
}

/// 到期价差 `ffill(limit=23).shift(-24)`：t+1 … t+24 中最后一个有效值(不会用到 t
/// 本身)。全部缺失或末端凑不满 24 小时时为 None。
pub fn future_end(x: &[Option<f64>]) -> Vec<Option<f64>> {
    (0..x.len())
        .map(|t| {
            if t + HORIZON_HOURS >= x.len() {
                return None;
            }
            x[t + 1..=t + HORIZON_HOURS].iter().rev().flatten().next().copied()
        })
        .collect()
}

/// `compute()` 的逐小时结果。
#[derive(Debug, Clone, PartialEq)]
pub struct HourlyStats {
    pub pct: Vec<Option<f64>>,
    pub sigma_bp: Vec<Option<f64>>,
    pub fut_min_bp: Vec<Option<f64>>,
    pub fut_end_bp: Vec<Option<f64>>,
}

pub fn compute(series: &HourlySpread) -> HourlyStats {
    let x = &series.spread_bp;
    HourlyStats { pct: rolling_pct(x), sigma_bp: rolling_sigma(x), fut_min_bp: future_min(x), fut_end_bp: future_end(x) }
}

/// 一个校准样本(一个小时 t)。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub time_ms: u64,
    pub spread_bp: f64,
    pub pct: f64,
    pub sigma_bp: f64,
    /// 等待收益 Δ = 价差(t) − min(价差 t+1 … t+24)。
    pub wait_bp: f64,
    /// 到期市价追的盈亏 = 价差(t) − 价差(t+24)。
    pub expiry_pnl_bp: f64,
}

/// 校准样本：开盘时间 > 最后一根 − 365 天，且价差、百分位、σ(> 0)、未来最小值、
/// 到期价差都有效的小时。末端 24 小时没有完整未来，自然被排除。
pub fn samples(series: &HourlySpread, stats: &HourlyStats) -> Vec<Sample> {
    (series.lookback_start()..series.len())
        .filter_map(|i| {
            let spread = series.spread_bp[i]?;
            let sigma = stats.sigma_bp[i].filter(|s| *s > 0.0)?;
            Some(Sample {
                time_ms: series.time_ms(i),
                spread_bp: spread,
                pct: stats.pct[i]?,
                sigma_bp: sigma,
                wait_bp: spread - stats.fut_min_bp[i]?,
                expiry_pnl_bp: spread - stats.fut_end_bp[i]?,
            })
        })
        .collect()
}

/// 365 天 σ 中位数：滚动 σ 序列里开盘时间 > 最后一根 − 365 天的那段，跳过缺失，
/// 偶数个取中间两个的平均(pandas `median`)。
pub fn sigma_median(series: &HourlySpread, sigma_bp: &[Option<f64>]) -> Option<f64> {
    let mut values: Vec<f64> = sigma_bp[series.lookback_start().min(sigma_bp.len())..].iter().flatten().copied().collect();
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) { (values[mid - 1] + values[mid]) / 2.0 } else { values[mid] })
}

/// 档宽 `width` 下的档数(10 → 10 档)。
pub fn bin_count(width: f64) -> usize {
    (100.0 / width).round() as usize
}

/// 百分位所在档的下标：边界 0, width, …, 100，左闭右开，最后一档两端都含。
pub fn bin_index(pct: f64, width: f64) -> usize {
    (1..bin_count(width)).rev().find(|&i| pct >= i as f64 * width).unwrap_or(0)
}

/// 第 i 档的 `(下界, 上界)`。
pub fn bin_bounds(i: usize, width: f64) -> (f64, f64) {
    (i as f64 * width, (i + 1) as f64 * width)
}

/// 独立段数：入选小时按时间排序，相邻间隔严格大于 24 小时就开新的一段。
pub fn segment_count(times_ms: &[u64]) -> usize {
    let mut times = times_ms.to_vec();
    times.sort_unstable();
    match times.len() {
        0 => 0,
        _ => 1 + times.windows(2).filter(|w| w[1] - w[0] > HORIZON_HOURS as u64 * HOUR_MS).count(),
    }
}

/// 某个 k 下一组样本的成交率和期望。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CurvePoint {
    pub k: f64,
    pub fill_rate: f64,
    /// 成交记 d = k·σ_t，没成交记 0。
    pub expect_a_bp: f64,
    /// 成交记 d，没成交记到期市价追的盈亏。主口径。
    pub expect_b_bp: f64,
}

/// `limit_curves` 的一档：对 k 网格逐个算成交率和期望。成交判定
/// `等待收益 ≥ k·σ_t`(取等号)，σ_t 用每个小时自己当时的 σ。样本为空时返回空。
pub fn curve(samples: &[&Sample]) -> Vec<CurvePoint> {
    if samples.is_empty() {
        return Vec::new();
    }
    let n = samples.len() as f64;
    k_grid()
        .into_iter()
        .map(|k| {
            let (mut filled, mut sum_a, mut sum_b) = (0usize, 0.0, 0.0);
            for s in samples {
                let d = k * s.sigma_bp;
                if s.wait_bp >= d {
                    filled += 1;
                    sum_a += d;
                    sum_b += d;
                } else {
                    sum_b += s.expiry_pnl_bp;
                }
            }
            CurvePoint { k, fill_rate: filled as f64 / n, expect_a_bp: sum_a / n, expect_b_bp: sum_b / n }
        })
        .collect()
}

/// `limit_table` 的一行。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BinStats {
    pub lo: f64,
    pub hi: f64,
    pub n: usize,
    pub segments: usize,
    /// 期望 B 最大的 k(并列取最小的 k)及其成交率、期望 B。
    pub b_k: f64,
    pub b_fill_rate: f64,
    pub b_expect_bp: f64,
    /// 期望 A 最大的 k 及其期望 A，只做参考。
    pub a_k: f64,
    pub a_expect_bp: f64,
    /// 期望 B 最大值 > 0 时，满足 B ≥ 0.9 × 最大值的 k 的 `(最小, 最大)`(不要求连续)；
    /// 最大值 ≤ 0 时为 None("等不如立即开")。
    pub platform: Option<(f64, f64)>,
}

/// 第一个最大值的下标(`idxmax`)。
fn argmax_first(values: impl Iterator<Item = f64>) -> usize {
    let mut best = (0, f64::NEG_INFINITY);
    for (i, v) in values.enumerate() {
        if v > best.1 {
            best = (i, v);
        }
    }
    best.0
}

/// 一组样本的汇总。样本为空返回 None。
pub fn bin_stats(lo: f64, hi: f64, samples: &[&Sample]) -> Option<BinStats> {
    let points = curve(samples);
    if points.is_empty() {
        return None;
    }
    let b = &points[argmax_first(points.iter().map(|p| p.expect_b_bp))];
    let a = &points[argmax_first(points.iter().map(|p| p.expect_a_bp))];
    let platform = (b.expect_b_bp > 0.0).then(|| {
        let floor = PLATFORM_RATIO * b.expect_b_bp;
        let ks: Vec<f64> = points.iter().filter(|p| p.expect_b_bp >= floor).map(|p| p.k).collect();
        (ks[0], ks[ks.len() - 1])
    });
    let times: Vec<u64> = samples.iter().map(|s| s.time_ms).collect();
    Some(BinStats {
        lo,
        hi,
        n: samples.len(),
        segments: segment_count(&times),
        b_k: b.k,
        b_fill_rate: b.fill_rate,
        b_expect_bp: b.expect_b_bp,
        a_k: a.k,
        a_expect_bp: a.expect_a_bp,
        platform,
    })
}

fn samples_in_bin(samples: &[Sample], i: usize, width: f64) -> Vec<&Sample> {
    samples.iter().filter(|s| bin_index(s.pct, width) == i).collect()
}

/// `limit_curves(df, width, unit="sigma")`：每档的整条 k 曲线，下标即档号。
pub fn limit_curves(samples: &[Sample], width: f64) -> Vec<Vec<CurvePoint>> {
    (0..bin_count(width)).map(|i| curve(&samples_in_bin(samples, i, width))).collect()
}

/// `limit_table`：每档汇总，下标即档号，空档为 None。
pub fn limit_table(samples: &[Sample], width: f64) -> Vec<Option<BinStats>> {
    (0..bin_count(width))
        .map(|i| {
            let (lo, hi) = bin_bounds(i, width);
            bin_stats(lo, hi, &samples_in_bin(samples, i, width))
        })
        .collect()
}

/// 校准规则参数。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RuleParams {
    /// 分档宽度(k* 表的档)。
    pub width: f64,
    /// 判断"独立段数不足"时用的档宽：档宽 10 时每档段数都在 60 以上，规则形同
    /// 虚设，所以改用更粗的档(包含这一档的那个粗档)的段数。
    pub segment_width: f64,
    /// 独立段数低于这个值就直接开。
    pub min_segments: usize,
}

impl Default for RuleParams {
    fn default() -> Self {
        Self { width: 10.0, segment_width: 20.0, min_segments: 10 }
    }
}

/// 一档最终怎么用。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum BinAction {
    /// 不等，t0 两腿立即吃单。
    DirectOpen { reason: String },
    /// 目标价差 = 当前价差 − k_star·σ。
    Wait {
        k_star: f64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
    },
}

impl BinAction {
    /// 比较新旧表用：直接开视为 k* = 0(不等)。
    pub fn k_equivalent(&self) -> f64 {
        match self {
            BinAction::DirectOpen { .. } => 0.0,
            BinAction::Wait { k_star, .. } => *k_star,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalibratedBin {
    pub lo: f64,
    pub hi: f64,
    /// 空档为 None。
    pub stats: Option<BinStats>,
    /// 包含这一档的粗档(档宽 `RuleParams::segment_width`)的独立段数。
    pub coarse_segments: usize,
    pub action: BinAction,
}

/// 按校准规则把 `limit_table` 变成可用的 k* 表：
/// 1. 空档、粗档独立段数 < `min_segments`、期望 B ≤ 0 的档直接开；
/// 2. 平台范围只有一个点的档，k* 取左右相邻有效档(不是直接开、平台也不止一个
///    点)的 k* 平均；只有一侧有效就用那一侧，两侧都没有就直接开。
pub fn apply_rules(samples: &[Sample], params: &RuleParams) -> Vec<CalibratedBin> {
    let fine = limit_table(samples, params.width);
    let coarse = limit_table(samples, params.segment_width);
    let mut bins: Vec<CalibratedBin> = fine
        .into_iter()
        .enumerate()
        .map(|(i, stats)| {
            let (lo, hi) = bin_bounds(i, params.width);
            let coarse_segments = coarse
                .get(bin_index(lo, params.segment_width))
                .and_then(|c| c.as_ref())
                .map_or(0, |c| c.segments);
            let action = match &stats {
                None => BinAction::DirectOpen { reason: "无样本".to_string() },
                Some(_) if coarse_segments < params.min_segments => BinAction::DirectOpen {
                    reason: format!(
                        "独立段数 {coarse_segments} < {}(按档宽 {} 统计)",
                        params.min_segments, params.segment_width
                    ),
                },
                Some(s) if s.b_expect_bp <= 0.0 => {
                    BinAction::DirectOpen { reason: format!("期望 B = {:.4}bp ≤ 0，等不如立即开", s.b_expect_bp) }
                }
                Some(s) => BinAction::Wait { k_star: s.b_k, note: None },
            };
            CalibratedBin { lo, hi, stats, coarse_segments, action }
        })
        .collect();

    let single_point =
        |b: &CalibratedBin| b.stats.as_ref().and_then(|s| s.platform).is_some_and(|(lo, hi)| lo == hi);
    let usable = |b: &CalibratedBin| matches!(b.action, BinAction::Wait { .. }) && !single_point(b);
    let fixes: Vec<(usize, BinAction)> = (0..bins.len())
        .filter(|&i| matches!(bins[i].action, BinAction::Wait { .. }) && single_point(&bins[i]))
        .map(|i| {
            let own_k = bins[i].action.k_equivalent();
            let neighbors: Vec<(f64, f64)> = [i.checked_sub(1), Some(i + 1)]
                .into_iter()
                .flatten()
                .filter_map(|j| bins.get(j))
                .filter(|b| usable(b))
                .map(|b| (b.lo, b.action.k_equivalent()))
                .collect();
            let action = if neighbors.is_empty() {
                BinAction::DirectOpen { reason: format!("平台范围只有一个点(k={own_k})，相邻也没有有效档") }
            } else {
                let k_star = neighbors.iter().map(|(_, k)| k).sum::<f64>() / neighbors.len() as f64;
                let from: Vec<String> = neighbors.iter().map(|(lo, k)| format!("[{lo},…) k*={k}")).collect();
                BinAction::Wait {
                    k_star,
                    note: Some(format!("平台范围只有一个点(k={own_k})，取相邻档平均：{}", from.join("、"))),
                }
            };
            (i, action)
        })
        .collect();
    for (i, action) in fixes {
        bins[i].action = action;
    }
    bins
}

/// 新旧表某一档 k* 的变化(直接开按 k* = 0 算)。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KStarChange {
    pub lo: f64,
    pub hi: f64,
    pub prev: Option<f64>,
    pub next: Option<f64>,
}

/// 变化超过 `threshold` 的档；档的划分不同(某一边没有这一档)也算变化。
pub fn k_star_changes(prev: &[CalibratedBin], next: &[CalibratedBin], threshold: f64) -> Vec<KStarChange> {
    let key = |b: &CalibratedBin| (b.lo.to_bits(), b.hi.to_bits());
    let prev_by: HashMap<_, f64> = prev.iter().map(|b| (key(b), b.action.k_equivalent())).collect();
    let next_by: HashMap<_, f64> = next.iter().map(|b| (key(b), b.action.k_equivalent())).collect();
    let mut changes: Vec<KStarChange> = next
        .iter()
        .map(|b| (b.lo, b.hi, prev_by.get(&key(b)).copied(), Some(b.action.k_equivalent())))
        .chain(prev.iter().filter(|b| !next_by.contains_key(&key(b))).map(|b| (b.lo, b.hi, Some(b.action.k_equivalent()), None)))
        .filter(|(_, _, p, n)| match (p, n) {
            (Some(p), Some(n)) => (p - n).abs() > threshold,
            _ => true,
        })
        .map(|(lo, hi, prev, next)| KStarChange { lo, hi, prev, next })
        .collect();
    changes.sort_by(|a, b| a.lo.total_cmp(&b.lo));
    changes
}

/// 持久化的 k* 表(一对腿一个版本)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KStarTable {
    /// 配对标识，见 `cross_funding::PairRow::key`。
    pub pair: String,
    /// 版本号 = 生成时间(ms)。
    pub version: u64,
    /// 数据网格首尾两根的开盘时间。
    pub data_start_ms: u64,
    pub data_end_ms: u64,
    pub rules: RuleParams,
    pub samples: usize,
    /// 365 天 σ 中位数(bp)，决策时算 σ 比用。
    pub sigma_median_bp: f64,
    /// 全部样本一起算的汇总(参考)。
    pub overall: Option<BinStats>,
    pub bins: Vec<CalibratedBin>,
}

impl KStarTable {
    /// 百分位所在档。
    pub fn bin_for(&self, pct: f64) -> Option<&CalibratedBin> {
        self.bins.get(bin_index(pct, self.rules.width))
    }
}

/// 对一对腿跑完整校准。样本为空或算不出 σ 中位数时报错。
pub fn calibrate(pair: &str, series: &HourlySpread, rules: RuleParams, version: u64) -> anyhow::Result<KStarTable> {
    let stats = compute(series);
    let samples = samples(series, &stats);
    if samples.is_empty() {
        anyhow::bail!("{pair}: 没有有效样本(数据 {} 小时)", series.len());
    }
    let sigma_median_bp =
        sigma_median(series, &stats.sigma_bp).ok_or_else(|| anyhow::anyhow!("{pair}: 算不出 365 天 σ 中位数"))?;
    let all: Vec<&Sample> = samples.iter().collect();
    Ok(KStarTable {
        pair: pair.to_string(),
        version,
        data_start_ms: series.start_ms,
        data_end_ms: series.last_time_ms().unwrap_or(series.start_ms),
        rules,
        samples: samples.len(),
        sigma_median_bp,
        overall: bin_stats(0.0, 100.0, &all),
        bins: apply_rules(&samples, &rules),
    })
}

/// 最新一根有效小时的状态(`recommend()` 用的那一行)。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HourState {
    pub open_time_ms: u64,
    pub spread_bp: f64,
    /// 有效小时不足 80% 时为 None。
    pub pct: Option<f64>,
    pub sigma_bp: Option<f64>,
}

/// 序列里最后一个价差有效的小时及其百分位、σ。整段都无效时返回 None。
pub fn latest_state(series: &HourlySpread) -> Option<HourState> {
    let i = series.spread_bp.iter().rposition(|v| v.is_some())?;
    let x = &series.spread_bp[..=i];
    Some(HourState {
        open_time_ms: series.time_ms(i),
        spread_bp: x[i]?,
        pct: rolling_pct(x)[i],
        sigma_bp: rolling_sigma(x)[i],
    })
}

/// 决策阈值。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecisionParams {
    /// 最新一根小时 K 线收盘(开盘 + 1h)距离 t0 的上限。
    pub max_bar_age_ms: u64,
    /// σ ÷ 365 天 σ 中位数超过这个值就不开仓。
    pub max_sigma_ratio: f64,
}

impl Default for DecisionParams {
    fn default() -> Self {
        Self { max_bar_age_ms: 2 * HOUR_MS, max_sigma_ratio: 3.0 }
    }
}

/// 不等、两腿立即吃单的原因。
#[derive(Debug, Clone, PartialEq)]
pub enum ImmediateReason {
    /// 窗口内有效小时不足 80%，百分位/σ 记缺。
    MissingStats,
    /// 当前档被校准标成直接开。
    DirectBin { reason: String },
}

impl std::fmt::Display for ImmediateReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingStats => write!(f, "30 天窗口有效小时不足 80%，百分位/σ 缺失"),
            Self::DirectBin { reason } => write!(f, "当前档直接开：{reason}"),
        }
    }
}

/// 不开仓的原因。
#[derive(Debug, Clone, PartialEq)]
pub enum SkipReason {
    /// 最新一根有效小时 K 线收盘距离 t0 太久：百分位/σ 不可信，不开仓。
    StaleKline { bar_open_ms: u64, age_ms: u64, max_age_ms: u64 },
    /// σ 比超限，或 σ 中位数非正算不出 σ 比(ratio 为 NaN)：波动异常，不开仓。
    SigmaAlarm { ratio: f64, max_ratio: f64 },
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hours = |ms: u64| ms as f64 / HOUR_MS as f64;
        match self {
            Self::StaleKline { bar_open_ms, age_ms, max_age_ms } => write!(
                f,
                "K 线不新鲜：最新有效小时 K 线(开盘 {})已收盘 {:.2} 小时，超过上限 {:.2} 小时，百分位/σ 不可信",
                crate::utc::format_minute(*bar_open_ms),
                hours(*age_ms),
                hours(*max_age_ms)
            ),
            Self::SigmaAlarm { ratio, max_ratio } => {
                write!(f, "σ 告警：σ ÷ 365 天 σ 中位数 = {ratio:.4}，超过上限 {max_ratio}，波动异常")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    Immediate(ImmediateReason),
    /// 不开仓。
    Skip(SkipReason),
    /// 目标价差在整个 24 小时窗口内固定不变。
    Wait { k_star: f64, target_bp: f64 },
}

/// 决策结果连同中间量，全部要进日志。
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionReport {
    pub state: HourState,
    pub sigma_ratio: Option<f64>,
    /// 当前百分位所在档的 `(下界, 上界)`。
    pub bin: Option<(f64, f64)>,
    /// t0 实时价差(两腿中间价)。
    pub live_spread_bp: f64,
    pub decision: Decision,
}

/// t0 决策，按顺序判断：
/// 1. 最新小时 K 线收盘距离 t0 不超过 `max_bar_age_ms`，否则不开仓；百分位/σ 有效，否则立即开；
/// 2. σ ÷ σ 中位数不超过 `max_sigma_ratio`，否则不开仓；
/// 3. 当前档不是直接开，否则立即开；
/// 4. 目标价差 = 实时价差 − k*·σ。
pub fn decide(
    state: &HourState,
    table: &KStarTable,
    live_spread_bp: f64,
    now_ms: u64,
    params: &DecisionParams,
) -> DecisionReport {
    let sigma_ratio = state.sigma_bp.map(|s| s / table.sigma_median_bp);
    let bin = state.pct.and_then(|p| table.bin_for(p));
    let report = |decision| DecisionReport {
        state: *state,
        sigma_ratio,
        bin: bin.map(|b| (b.lo, b.hi)),
        live_spread_bp,
        decision,
    };
    let immediate = |reason| report(Decision::Immediate(reason));

    let age_ms = now_ms.saturating_sub(state.open_time_ms + HOUR_MS);
    if age_ms > params.max_bar_age_ms {
        return report(Decision::Skip(SkipReason::StaleKline {
            bar_open_ms: state.open_time_ms,
            age_ms,
            max_age_ms: params.max_bar_age_ms,
        }));
    }
    let (Some(sigma), Some(bin)) = (state.sigma_bp, bin) else {
        return immediate(ImmediateReason::MissingStats);
    };
    // σ 中位数非正(含 NaN)算不出 σ 比；比值是 NaN 也按超限处理。
    let ratio = if table.sigma_median_bp > 0.0 { sigma / table.sigma_median_bp } else { f64::NAN };
    if ratio.is_nan() || ratio > params.max_sigma_ratio {
        return report(Decision::Skip(SkipReason::SigmaAlarm { ratio, max_ratio: params.max_sigma_ratio }));
    }
    match &bin.action {
        BinAction::DirectOpen { reason } => immediate(ImmediateReason::DirectBin { reason: reason.clone() }),
        BinAction::Wait { k_star, .. } => {
            report(Decision::Wait { k_star: *k_star, target_bp: live_spread_bp - k_star * sigma })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn kline(hour: u64, close: &str, volume: &str) -> Kline {
        Kline { open_time_ms: hour * HOUR_MS, close: d(close), volume: d(volume) }
    }

    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "{a} != {b}");
    }

    fn sample(hour: u64, pct: f64, sigma: f64, wait: f64, expiry_pnl: f64) -> Sample {
        Sample { time_ms: hour * HOUR_MS, spread_bp: 0.0, pct, sigma_bp: sigma, wait_bp: wait, expiry_pnl_bp: expiry_pnl }
    }

    #[test]
    fn spread_is_long_minus_short_over_short() {
        assert_close(spread_bp(100.1, 100.0), 10.0);
        assert_close(spread_bp(99.9, 100.0), -10.0);
    }

    #[test]
    fn per_coin_divides_by_contract_multiplier() {
        // 1000PEPE 报价 0.01 → 每个 PEPE 0.00001
        assert_eq!(per_coin_f64(d("0.01"), d("1000")), 0.00001);
        assert_eq!(per_coin_f64(d("0.004036"), Decimal::ONE), 0.004036);
    }

    #[test]
    fn hourly_spread_uses_overlap_and_marks_missing_hours() {
        // 多腿 1000PEPE(乘数 1000)，空腿 PEPE
        let long = vec![kline(0, "10.0", "1"), kline(1, "10.1", "1"), kline(2, "10.0", "0"), kline(4, "10.0", "1"), kline(5, "10.0", "1")];
        let short = vec![kline(1, "0.01", "1"), kline(2, "0.01", "1"), kline(3, "0.01", "1"), kline(4, "0.01", "1")];
        let series = HourlySpread::from_klines(&long, &short, d("1000"), Decimal::ONE).unwrap();
        assert_eq!(series.start_ms, HOUR_MS);
        assert_eq!(series.len(), 4); // 1..=4
        assert_close(series.spread_bp[0].unwrap(), 100.0);
        assert_eq!(series.spread_bp[1], None); // 多腿成交量为 0
        assert_eq!(series.spread_bp[2], None); // 多腿缺这一小时
        assert_close(series.spread_bp[3].unwrap(), 0.0);
        assert_eq!(series.last_time_ms(), Some(4 * HOUR_MS));
        assert!(HourlySpread::from_klines(&long[..1], &short, Decimal::ONE, Decimal::ONE).is_none());
    }

    #[test]
    fn pct_uses_average_rank_over_valid_count() {
        let x = [Some(1.0), Some(2.0), Some(2.0)];
        let pct = rolling_pct_with(&x, 3, 1);
        assert_close(pct[0].unwrap(), 100.0);
        assert_close(pct[1].unwrap(), 100.0);
        assert_close(pct[2].unwrap(), 2.5 / 3.0 * 100.0);
        // 最小值取不到 0；NaN 不计数；当前值 NaN 时为 NaN
        let x = [Some(3.0), None, Some(2.0), Some(1.0), None];
        let pct = rolling_pct_with(&x, 5, 1);
        assert_close(pct[3].unwrap(), 1.0 / 3.0 * 100.0);
        assert_eq!(pct[4], None);
        // 窗口只看最近 window 行
        let pct = rolling_pct_with(&[Some(0.0), Some(5.0), Some(4.0)], 2, 1);
        assert_close(pct[2].unwrap(), 50.0);
    }

    #[test]
    fn pct_and_sigma_require_576_valid_hours_in_720() {
        let mut x: Vec<Option<f64>> = (0..720).map(|i| Some(i as f64)).collect();
        for v in x.iter_mut().take(144) {
            *v = None;
        }
        assert_eq!(x.iter().flatten().count(), 576);
        assert_close(rolling_pct(&x)[719].unwrap(), 100.0);
        assert!(rolling_sigma(&x)[719].is_some());
        x[144] = None;
        assert_eq!(rolling_pct(&x)[719], None);
        assert_eq!(rolling_sigma(&x)[719], None);
    }

    #[test]
    fn sigma_is_sample_std() {
        let x = [Some(1.0), Some(2.0), None, Some(3.0), Some(4.0)];
        let sigma = rolling_sigma_with(&x, 5, 1);
        assert_close(sigma[4].unwrap(), (5.0_f64 / 3.0).sqrt());
        assert_eq!(sigma[0], None); // 只有一个值，ddof=1 算不出
        assert!(rolling_sigma_with(&x, 5, 1)[2].is_some()); // 当前值缺失也照算
    }

    #[test]
    fn future_min_and_end_skip_missing_and_drop_tail() {
        let mut x: Vec<Option<f64>> = vec![Some(5.0); 30];
        x[3] = Some(1.0);
        x[24] = None;
        x[25] = Some(7.0);
        let fut_min = future_min(&x);
        let fut_end = future_end(&x);
        assert_eq!(fut_min[0], Some(1.0));
        assert_eq!(fut_min[3], Some(5.0)); // 不含 t 本身
        assert_eq!(fut_end[0], Some(5.0)); // t+24 缺失，用 t+23
        assert_eq!(fut_end[1], Some(7.0));
        assert_eq!(fut_min[5], Some(5.0));
        assert_eq!(fut_min[6], None); // 6+24 = 30 超出末端
        assert_eq!(fut_end[6], None);
        let all_missing: Vec<Option<f64>> = std::iter::once(Some(1.0)).chain(std::iter::repeat_n(None, 24)).collect();
        assert_eq!(future_min(&all_missing)[0], None);
        assert_eq!(future_end(&all_missing)[0], None);
    }

    #[test]
    fn bins_are_left_closed_and_last_bin_includes_100() {
        assert_eq!(bin_index(0.0, 10.0), 0);
        assert_eq!(bin_index(9.999, 10.0), 0);
        assert_eq!(bin_index(10.0, 10.0), 1);
        assert_eq!(bin_index(99.99, 10.0), 9);
        assert_eq!(bin_index(100.0, 10.0), 9);
        assert_eq!(bin_index(100.0, 20.0), 4);
        assert_eq!(bin_index(39.9, 20.0), 1);
        assert_eq!(bin_count(10.0), 10);
        assert_eq!(bin_bounds(9, 10.0), (90.0, 100.0));
    }

    #[test]
    fn k_grid_has_41_points() {
        let grid = k_grid();
        assert_eq!(grid.len(), 41);
        assert_eq!(grid[3], 0.3);
        assert_eq!(grid[40], 4.0);
    }

    #[test]
    fn segments_split_on_gaps_strictly_over_24h() {
        assert_eq!(segment_count(&[]), 0);
        assert_eq!(segment_count(&[0]), 1);
        assert_eq!(segment_count(&[24 * HOUR_MS, 0]), 1); // 恰好 24h 不切
        assert_eq!(segment_count(&[0, 25 * HOUR_MS, 26 * HOUR_MS, 60 * HOUR_MS]), 3);
    }

    #[test]
    fn curve_fill_is_inclusive_and_expectations_match_definition() {
        let a = sample(0, 50.0, 1.0, 0.5, -2.0);
        let b = sample(1, 50.0, 2.0, 0.0, 1.0);
        let points = curve(&[&a, &b]);
        // k=0：两个都成交(wait ≥ 0)
        assert_eq!(points[0].fill_rate, 1.0);
        assert_eq!(points[0].expect_b_bp, 0.0);
        // k=0.5：a 恰好成交(0.5 ≥ 0.5)，b 没成交记到期盈亏 1.0
        let p = points[5];
        assert_eq!(p.k, 0.5);
        assert_eq!(p.fill_rate, 0.5);
        assert_close(p.expect_a_bp, 0.25);
        assert_close(p.expect_b_bp, (0.5 + 1.0) / 2.0);
        // k=0.6：都没成交
        assert_close(points[6].expect_b_bp, (-2.0 + 1.0) / 2.0);
        assert_eq!(points[6].expect_a_bp, 0.0);
    }

    #[test]
    fn bin_stats_picks_first_max_and_platform() {
        // 单个样本 σ=1、wait=1.05、到期盈亏 0：B 在 k ≤ 1.0 时等于 k，之后为 0
        let s = sample(0, 50.0, 1.0, 1.05, 0.0);
        let stats = bin_stats(50.0, 60.0, &[&s]).unwrap();
        assert_eq!(stats.b_k, 1.0);
        assert_close(stats.b_expect_bp, 1.0);
        assert_eq!(stats.b_fill_rate, 1.0);
        assert_eq!(stats.platform, Some((0.9, 1.0)));
        assert_eq!((stats.n, stats.segments), (1, 1));
        // 期望 B 处处 ≤ 0：k* 取并列最大里最小的 k=0，无平台
        let s = sample(0, 50.0, 1.0, 0.0, -1.0);
        let stats = bin_stats(50.0, 60.0, &[&s]).unwrap();
        assert_eq!(stats.b_k, 0.0);
        assert_eq!(stats.platform, None);
        assert!(bin_stats(0.0, 10.0, &[]).is_none());
    }

    /// 造一组落在指定档、段数够多、最优 k 在 `k` 的样本：每个样本 σ=1、
    /// wait 恰好等于 k，所以 B(k') = k'(k' ≤ k)，之后取到期盈亏 `miss`。
    fn bin_samples(pct: f64, k: f64, miss: f64, start_hour: u64) -> Vec<Sample> {
        (0..12).map(|i| sample(start_hour + i * 48, pct, 1.0, k, miss)).collect()
    }

    #[test]
    fn rules_mark_direct_open_and_average_single_point_platform() {
        let mut samples = Vec::new();
        samples.extend(bin_samples(5.0, 1.0, -5.0, 0)); // [0,10)：k*=1.0，平台 0.9–1.0
        samples.extend(bin_samples(15.0, 0.0, -1.0, 10_000)); // [10,20)：B ≤ 0
        samples.extend(bin_samples(25.0, 2.0, -5.0, 20_000)); // [20,30)
        samples.extend(bin_samples(45.0, 0.2, -50.0, 30_000)); // [40,50)：平台 0.2–0.2
        samples.extend(bin_samples(55.0, 3.0, -5.0, 40_000)); // [50,60)
        samples.push(sample(50_000, 95.0, 1.0, 1.0, -1.0)); // [90,100]：段数不足(粗档 [80,100] 只有 1 段)
        let bins = apply_rules(&samples, &RuleParams::default());
        assert_eq!(bins.len(), 10);
        assert_eq!(bins[0].action, BinAction::Wait { k_star: 1.0, note: None });
        assert!(matches!(&bins[1].action, BinAction::DirectOpen { reason } if reason.contains("期望 B")));
        assert!(matches!(&bins[3].action, BinAction::DirectOpen { reason } if reason == "无样本"));
        // [40,50) 平台只有 0.2 一个点：相邻 [30,40) 无样本、[50,60) k*=3.0 → 取 3.0
        assert_eq!(bins[4].stats.as_ref().unwrap().platform, Some((0.2, 0.2)));
        assert!(matches!(bins[4].action, BinAction::Wait { k_star, note: Some(_) } if k_star == 3.0));
        assert!(matches!(&bins[9].action, BinAction::DirectOpen { reason } if reason.contains("独立段数 1 < 10")));
        assert_eq!(bins[9].coarse_segments, 1);
    }

    #[test]
    fn single_point_platform_without_usable_neighbors_opens_directly() {
        let samples = bin_samples(45.0, 0.2, -50.0, 0);
        let bins = apply_rules(&samples, &RuleParams::default());
        assert!(matches!(&bins[4].action, BinAction::DirectOpen { reason } if reason.contains("相邻也没有有效档")));
    }

    #[test]
    fn single_point_platform_averages_both_neighbors() {
        let mut samples = bin_samples(35.0, 1.0, -5.0, 0);
        samples.extend(bin_samples(45.0, 0.2, -50.0, 10_000));
        samples.extend(bin_samples(55.0, 2.0, -5.0, 20_000));
        let bins = apply_rules(&samples, &RuleParams::default());
        assert!(matches!(bins[4].action, BinAction::Wait { k_star, .. } if (k_star - 1.5).abs() < 1e-12));
    }

    fn calibrated(lo: f64, action: BinAction) -> CalibratedBin {
        CalibratedBin { lo, hi: lo + 10.0, stats: None, coarse_segments: 0, action }
    }

    #[test]
    fn k_star_changes_flag_moves_over_threshold() {
        let wait = |k| BinAction::Wait { k_star: k, note: None };
        let direct = || BinAction::DirectOpen { reason: String::new() };
        let prev = vec![calibrated(0.0, wait(1.0)), calibrated(10.0, wait(1.0)), calibrated(20.0, direct()), calibrated(30.0, wait(0.4))];
        let next = vec![calibrated(0.0, wait(1.5)), calibrated(10.0, wait(1.6)), calibrated(20.0, wait(0.8)), calibrated(30.0, direct())];
        let changes = k_star_changes(&prev, &next, 0.5);
        let los: Vec<f64> = changes.iter().map(|c| c.lo).collect();
        assert_eq!(los, vec![10.0, 20.0]); // 0.5 恰好不算；0.4 → 直接开(0) 不算
        assert_eq!((changes[1].prev, changes[1].next), (Some(0.0), Some(0.8)));
    }

    #[test]
    fn sigma_median_uses_last_365_days() {
        let n = LOOKBACK_HOURS as usize + 2;
        let series = HourlySpread { start_ms: 0, spread_bp: vec![Some(0.0); n] };
        let mut sigma: Vec<Option<f64>> = vec![Some(1.0); n];
        sigma[0] = Some(1000.0); // 窗口外
        sigma[1] = Some(1000.0); // 窗口外(开盘时间 = 最后一根 − 365 天，不含)
        sigma[2] = None;
        sigma[n - 1] = Some(3.0);
        sigma[n - 2] = Some(3.0);
        // 窗口内 8759 个有效值：8757 个 1.0、2 个 3.0 → 中位数 1.0
        assert_eq!(sigma_median(&series, &sigma), Some(1.0));
        let short = HourlySpread { start_ms: 0, spread_bp: vec![Some(0.0); 4] };
        assert_eq!(sigma_median(&short, &[Some(1.0), Some(4.0), None, Some(2.0)]), Some(2.0));
        assert_eq!(sigma_median(&short, &[Some(1.0), Some(4.0), Some(3.0), Some(2.0)]), Some(2.5));
    }

    /// 简单线性同余发生器，测试里造确定性的"随机"价差。
    fn lcg_series(n: usize, seed: u64) -> Vec<Option<f64>> {
        let mut state = seed;
        (0..n)
            .map(|i| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let noise = (state >> 11) as f64 / (1u64 << 53) as f64 - 0.5;
                if i % 97 == 13 { None } else { Some((i as f64 / 50.0).sin() * 3.0 + noise * 2.0) }
            })
            .collect()
    }

    #[test]
    fn calibration_sample_count_matches_research() {
        // 395 天数据：365 天样本减去末端 24 小时没有未来的 = 8736 行候选
        let n = 395 * 24;
        let series = HourlySpread { start_ms: 0, spread_bp: vec![Some(1.0); n] };
        let mut x = lcg_series(n, 7);
        for v in x.iter_mut() {
            *v = Some(v.unwrap_or(0.0));
        }
        let series = HourlySpread { spread_bp: x, ..series };
        let stats = compute(&series);
        let samples = samples(&series, &stats);
        assert_eq!(samples.len(), 8736);
        assert_eq!(samples[0].time_ms, series.time_ms(n - 8760));
        assert_eq!(samples.last().unwrap().time_ms, series.time_ms(n - 25));
        let table = calibrate("test", &series, RuleParams::default(), 1).unwrap();
        assert_eq!(table.samples, 8736);
        assert_eq!(table.overall.as_ref().unwrap().segments, 1);
        assert_eq!(table.bins.iter().filter_map(|b| b.stats.as_ref()).map(|s| s.n).sum::<usize>(), 8736);
        // 序列化往返
        let json = serde_json::to_string(&table).unwrap();
        assert_eq!(serde_json::from_str::<KStarTable>(&json).unwrap(), table);
    }

    #[test]
    fn latest_state_uses_last_valid_hour() {
        let mut x = lcg_series(800, 3);
        x[799] = None;
        x[798] = Some(10.0);
        let series = HourlySpread { start_ms: 0, spread_bp: x };
        let state = latest_state(&series).unwrap();
        assert_eq!(state.open_time_ms, 798 * HOUR_MS);
        assert_eq!(state.spread_bp, 10.0);
        assert_close(state.pct.unwrap(), 100.0);
        assert!(state.sigma_bp.unwrap() > 0.0);
    }

    fn table_with(bins: Vec<CalibratedBin>, sigma_median_bp: f64) -> KStarTable {
        KStarTable {
            pair: "p".to_string(),
            version: 1,
            data_start_ms: 0,
            data_end_ms: 0,
            rules: RuleParams::default(),
            samples: 0,
            sigma_median_bp,
            overall: None,
            bins,
        }
    }

    fn decision_table() -> KStarTable {
        let bins = (0..10)
            .map(|i| {
                let action = if i < 2 {
                    BinAction::DirectOpen { reason: "期望 B ≤ 0".to_string() }
                } else {
                    BinAction::Wait { k_star: i as f64 / 4.0, note: None }
                };
                calibrated(i as f64 * 10.0, action)
            })
            .collect();
        table_with(bins, 1.0)
    }

    fn state(pct: Option<f64>, sigma: Option<f64>) -> HourState {
        HourState { open_time_ms: 10 * HOUR_MS, spread_bp: 0.5, pct, sigma_bp: sigma }
    }

    #[test]
    fn decide_covers_every_branch() {
        let table = decision_table();
        let params = DecisionParams::default();
        let fresh_now = 11 * HOUR_MS + 2 * HOUR_MS; // 收盘后恰好 2 小时：仍算新鲜
        // 4. 都通过：目标 = 实时价差 − k*·σ，[40,50) 档 k* = 1.0
        let r = decide(&state(Some(45.0), Some(0.8)), &table, 1.2, fresh_now, &params);
        assert_eq!(r.bin, Some((40.0, 50.0)));
        assert_eq!(r.sigma_ratio, Some(0.8));
        match r.decision {
            Decision::Wait { k_star, target_bp } => {
                assert_eq!(k_star, 1.0);
                assert_close(target_bp, 1.2 - 0.8);
            }
            other => panic!("{other:?}"),
        }
        // 1. 不新鲜：不开仓
        let r = decide(&state(Some(45.0), Some(0.8)), &table, 1.2, fresh_now + 1, &params);
        let stale = SkipReason::StaleKline { bar_open_ms: 10 * HOUR_MS, age_ms: 2 * HOUR_MS + 1, max_age_ms: 2 * HOUR_MS };
        assert_eq!(r.decision, Decision::Skip(stale.clone()));
        assert!(stale.to_string().contains("开盘 1970-01-01 10:00"), "{stale}");
        // 1'. 百分位/σ 缺失
        let r = decide(&state(None, Some(0.8)), &table, 1.2, fresh_now, &params);
        assert_eq!(r.decision, Decision::Immediate(ImmediateReason::MissingStats));
        // 2. σ 告警不开仓：恰好 3 不告警，超过 3 告警
        let r = decide(&state(Some(45.0), Some(3.0)), &table, 1.2, fresh_now, &params);
        assert!(matches!(r.decision, Decision::Wait { .. }));
        let r = decide(&state(Some(45.0), Some(3.01)), &table, 1.2, fresh_now, &params);
        assert!(matches!(r.decision, Decision::Skip(SkipReason::SigmaAlarm { ratio, max_ratio: 3.0 }) if (ratio - 3.01).abs() < 1e-12));
        let r = decide(&state(Some(45.0), Some(0.8)), &table_with(decision_table().bins, 0.0), 1.2, fresh_now, &params);
        assert!(matches!(r.decision, Decision::Skip(SkipReason::SigmaAlarm { ratio, .. }) if ratio.is_nan()));
        // 3. 当前档直接开(含百分位 0 附近的档)
        let r = decide(&state(Some(0.139), Some(0.8)), &table, 1.2, fresh_now, &params);
        assert!(matches!(r.decision, Decision::Immediate(ImmediateReason::DirectBin { .. })));
        // 百分位 100 落最后一档
        let r = decide(&state(Some(100.0), Some(0.8)), &table, 1.2, fresh_now, &params);
        assert_eq!(r.bin, Some((90.0, 100.0)));
        assert!(matches!(r.decision, Decision::Wait { k_star, .. } if k_star == 9.0 / 4.0));
    }
}
