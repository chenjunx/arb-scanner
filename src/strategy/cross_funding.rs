//! 跨所永续价差开仓策略(`cross_funding`)："开仓等价差"一期。
//!
//! 从 CSV 读入若干"在 A 所做多、在 B 所做空"的永续合约对。价差
//! `bp = (多腿每币价 − 空腿每币价) / 空腿每币价 × 10000`，越低开仓越划算。
//!
//! 启动时每行拉最近 [`HISTORY_HOURS`] 小时的小时 K 线，算出最新一根的 30 天
//! 百分位和 σ，配合 Redis 里人工确认过的 k* 表([`KStarTable`])。t0 = 启动后两腿
//! 第一次都有新鲜报价的时刻，按 [`decide`] 判断：
//! - 立即开(百分位/σ 缺失 / 当前档直接开)：两腿马上市价；
//! - 不开仓(最新小时 K 线不新鲜 / σ 告警)：warn 打出原因，这一行停在 `Skipped`；
//! - 等待：目标 = t0 实时中间价价差 − k*·σ，24 小时内固定。可成交价差(多腿 ask、
//!   空腿 bid)≤ 目标就两腿市价；到期还没等到也两腿市价。
//!
//! 每一行只开一次仓；任意一腿被拒/超时未成交都只打 error 日志并把这一行停在
//! `Halted`，等人工处理，不做自动补救，也不平仓。持仓模式(单向)、保证金模式、杠杆
//! 由用户事先在交易所设置好，程序不修改。
//!
//! 风控：单行名义上限(启动和触发时各查一次)；已结束各行的未对冲敞口之和上限(超过
//! 就停掉所有行)；单腿成交滑点告警；kill switch 文件(存在时不决策、不下单，删掉
//! 恢复)。dry run 即模拟盘：照常决策，按触发时的对手价模拟全量成交。
//!
//! 每次 t0 决策和每一行的最终结果都往 `record_file` 追加一行 JSON
//! ([`OrderRecord`])，供 `cross-funding-review` 复盘。
//!
//! 数量/价格单位：CSV 里的 `quantity` 是实际币数；各腿下单量是合约 base 单位
//! (= 币数 / `PerpContractSpec::price_unit`，只有币安 `1000PEPEUSDT` 这类带前缀
//! 的合约才不是 1:1，张数换算在 Gate/OKX 下单网关里做)，每币价格 = 报价 /
//! `PerpContractSpec::price_unit`。

use std::collections::HashMap;
use std::fmt;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use log::{debug, error, info, warn};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

use crate::exchange_info::types::{PerpContractSpec, PrecisionKind};
use crate::exchange_info::{ExchangeInfoProvider, PrecisionCache};
use crate::market_data::orderbook::{
    ORDER_BOOK_LOG_DEPTH, ORDER_BOOK_LOG_INTERVAL_MS, OrderBookLevels, OrderBookLogThrottle, describe_order_book,
};
use crate::market_data::{HOUR_MS, KlineInterval, KlineProvider, now_ms};
use crate::order::OrderProvider;
use crate::order::types::{OrderAmount, OrderSide, OrderStatus};
use crate::order_manager::OrderManager;
use crate::order_manager::types::{OrderEvent, OrderId};
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};
use crate::utc;

use super::spread_wait::{
    Decision, DecisionParams, DecisionReport, HORIZON_HOURS, HourState, HourlySpread, KStarTable, WINDOW_HOURS, decide,
    latest_state, per_coin_f64, spread_bp,
};
use super::{Strategy, StrategySubscriber};

pub const STRATEGY_NAME: &str = "cross_funding";
const CSV_HEADER: [&str; 5] = ["long_venue", "long_symbol", "short_venue", "short_symbol", "quantity"];
/// 启动时拉多少小时的小时 K 线：30 天窗口再多一天余量，最近几小时缺数据时窗口
/// 仍然够长。
pub const HISTORY_HOURS: u64 = WINDOW_HOURS as u64 + 24;
/// 等待窗口：t0 起 24 小时。
const WAIT_HORIZON_MS: u64 = HORIZON_HOURS as u64 * HOUR_MS;
/// 生效 k* 表的数据截止时间超过这么久就打 warn(应按周校准)。
const TABLE_STALE_MS: u64 = 8 * 24 * HOUR_MS;
/// kill switch 文件多久查一次。
const KILL_SWITCH_POLL_MS: u64 = 1_000;

/// 一期已知局限，启动日志和复盘报告都会原样打印。
pub const KNOWN_LIMITATIONS: &str = "\
1. 两腿都是市价单(taker)，没有限价/挂单优化；成交回报不带手续费，节省按\"等\"和\"直接开\"两边同为 taker 费相互抵消计算，资金费不计入。
2. t0 = 启动后两腿第一次都有新鲜报价的时刻；进程重启会重新决策、重新计 24 小时，重启前还在等待的行只留下 decision 记录。K 线不新鲜、σ 告警而不开仓的行本进程内不再重新判断，要重启才会重新拉 K 线、重新决策。
3. 目标/到期只在收到行情时判断，到期单在到期后第一笔两腿都新鲜的报价时下单。
4. 百分位/σ 用启动时拉的小时 K 线，等待期间不更新(按设计 24 小时内目标固定)。
5. 校准不剔除 σ 告警时段(剔除开关待确认)。
6. 节省 = t0 可成交价差 − 实际成交价差；研究里的期望 B 按收盘价(近似中间价)算，两者口径差约半个买卖价差。
7. 不做平仓；任一腿失败只停这一行，需人工处理。
8. 模拟盘按触发时的对手价全量成交，不模拟滑点和部分成交。";

/// 支持的永续交易所。CSV 里写 `binance`/`gate`/`okx`/`bitget`，内部 venue 是
/// `binance_futures`/`gate_futures`/`okx_futures`/`bitget_futures`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PerpExchange {
    Binance,
    Gate,
    Okx,
    Bitget,
}

impl PerpExchange {
    pub fn parse(name: &str) -> anyhow::Result<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "binance" => Ok(Self::Binance),
            "gate" => Ok(Self::Gate),
            "okx" => Ok(Self::Okx),
            "bitget" => Ok(Self::Bitget),
            other => anyhow::bail!("unsupported venue '{other}', expected one of binance/gate/okx/bitget"),
        }
    }

    pub fn venue(self) -> Venue {
        match self {
            Self::Binance => Venue::new("binance_futures"),
            Self::Gate => Venue::new("gate_futures"),
            Self::Okx => Venue::new("okx_futures"),
            Self::Bitget => Venue::new("bitget_futures"),
        }
    }

    /// 把交易所原生合约名还原成内部 `Symbol`：币安/Bitget `1000PEPEUSDT` →
    /// `1000PEPE/USDT`，Gate `PEPE_USDT` → `PEPE/USDT`，OKX `PEPE-USDT-SWAP` →
    /// `PEPE/USDT`。只支持 USDT 本位。
    pub fn parse_symbol(self, native: &str) -> anyhow::Result<Symbol> {
        let native = native.trim().to_ascii_uppercase();
        let base = match self {
            Self::Binance | Self::Bitget => native.strip_suffix("USDT"),
            Self::Gate => native.strip_suffix("_USDT"),
            Self::Okx => native.strip_suffix("-USDT-SWAP"),
        };
        match base {
            Some(base) if !base.is_empty() && !base.contains(['_', '-']) => Ok(Symbol::new(base, "USDT")),
            _ => anyhow::bail!("invalid {self} USDT perpetual symbol '{native}'"),
        }
    }
}

impl fmt::Display for PerpExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Binance => "binance",
            Self::Gate => "gate",
            Self::Okx => "okx",
            Self::Bitget => "bitget",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LegSpec {
    pub exchange: PerpExchange,
    /// CSV 里的原生合约名，只用于日志。
    pub native_symbol: String,
    pub symbol: Symbol,
}

impl LegSpec {
    pub fn new(venue: &str, native_symbol: &str) -> anyhow::Result<Self> {
        let exchange = PerpExchange::parse(venue)?;
        Ok(Self { exchange, native_symbol: native_symbol.trim().to_string(), symbol: exchange.parse_symbol(native_symbol)? })
    }

    pub fn venue(&self) -> Venue {
        self.exchange.venue()
    }
}

/// [`pair_key`] 的逆运算：`binance:BTCUSDT|okx:BTC-USDT-SWAP` → (多腿, 空腿)。
pub fn parse_pair_key(key: &str) -> anyhow::Result<(LegSpec, LegSpec)> {
    let leg = |s: &str| -> anyhow::Result<LegSpec> {
        let (venue, symbol) =
            s.split_once(':').ok_or_else(|| anyhow::anyhow!("invalid leg '{s}', expected venue:SYMBOL"))?;
        LegSpec::new(venue, symbol)
    };
    let (long, short) =
        key.split_once('|').ok_or_else(|| anyhow::anyhow!("invalid pair '{key}', expected long_venue:SYMBOL|short_venue:SYMBOL"))?;
    let (long, short) = (leg(long)?, leg(short)?);
    if long.exchange == short.exchange {
        anyhow::bail!("pair '{key}': long and short legs must be on different venues");
    }
    Ok((long, short))
}

/// CSV 的一行。
#[derive(Debug, Clone, PartialEq)]
pub struct PairRow {
    /// CSV 行号(从 1 开始，含表头)，用于日志定位。
    pub line: usize,
    pub long: LegSpec,
    pub short: LegSpec,
    /// 实际币数。
    pub quantity: Decimal,
}

impl PairRow {
    pub fn label(&self) -> String {
        format!(
            "#{} 多 {}:{} / 空 {}:{}",
            self.line, self.long.exchange, self.long.native_symbol, self.short.exchange, self.short.native_symbol
        )
    }

    /// 配对标识(k* 表在 Redis 里按它存)，如 `binance:BTCUSDT|okx:BTC-USDT-SWAP`。
    /// 和行号、数量无关，CSV 调整顺序或改数量不影响已校准的表。
    pub fn key(&self) -> String {
        pair_key(&self.long, &self.short)
    }
}

pub fn pair_key(long: &LegSpec, short: &LegSpec) -> String {
    let leg = |l: &LegSpec| format!("{}:{}", l.exchange, l.native_symbol.trim().to_ascii_uppercase());
    format!("{}|{}", leg(long), leg(short))
}

/// 解析 CSV(不引第三方 crate，手动按逗号切)：第一行必须是表头
/// `long_venue,long_symbol,short_venue,short_symbol,quantity`，空行和 `#` 开头
/// 的行忽略。格式错误直接报错(带行号)，不做"跳过坏行"——CSV 是人手写的，
/// 写错了应该当场改。
pub fn parse_pairs_csv(text: &str) -> anyhow::Result<Vec<PairRow>> {
    let mut rows = Vec::new();
    let mut header_seen = false;
    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw.trim().trim_start_matches('\u{feff}');
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split(',').map(str::trim).collect();
        if !header_seen {
            let header: Vec<String> = fields.iter().map(|f| f.to_ascii_lowercase()).collect();
            if header != CSV_HEADER {
                anyhow::bail!("line {line_no}: expected header '{}', got '{line}'", CSV_HEADER.join(","));
            }
            header_seen = true;
            continue;
        }
        if fields.len() != CSV_HEADER.len() {
            anyhow::bail!("line {line_no}: expected {} fields, got {}: '{line}'", CSV_HEADER.len(), fields.len());
        }
        let long = LegSpec::new(fields[0], fields[1]).map_err(|e| anyhow::anyhow!("line {line_no}: {e}"))?;
        let short = LegSpec::new(fields[2], fields[3]).map_err(|e| anyhow::anyhow!("line {line_no}: {e}"))?;
        if long.exchange == short.exchange {
            anyhow::bail!("line {line_no}: long and short legs must be on different venues");
        }
        let quantity: Decimal = fields[4]
            .parse()
            .map_err(|e| anyhow::anyhow!("line {line_no}: invalid quantity '{}': {e}", fields[4]))?;
        if quantity <= Decimal::ZERO {
            anyhow::bail!("line {line_no}: quantity must be positive, got {quantity}");
        }
        rows.push(PairRow { line: line_no, long, short, quantity });
    }
    if !header_seen {
        anyhow::bail!("csv is empty, expected header '{}'", CSV_HEADER.join(","));
    }
    Ok(rows)
}

/// 每个 venue 需要的三类客户端：合约规格/精度、K 线、持仓查询。
#[derive(Clone)]
pub struct VenueClients {
    pub exchange_info: Arc<dyn ExchangeInfoProvider>,
    pub klines: Arc<dyn KlineProvider>,
    pub orders: Arc<dyn OrderProvider>,
}

/// 策略运行参数(来自 `config.toml` 的 `[cross_funding]` 段和命令行 `--live`)。
#[derive(Debug, Clone)]
pub struct CrossFundingParams {
    /// 启动时两边每币价格相差超过这个百分比就跳过这一行(多半是配错对/乘数)。
    pub max_price_diff_pct: Decimal,
    /// 下单后多久两腿还没全部成交就判定异常。
    pub fill_timeout_ms: u64,
    /// 两边报价都必须是最近这么久以内的才参与决策/触发。
    pub quote_max_age_ms: u64,
    /// false 时为模拟盘：照常决策，按对手价模拟成交，不下单。
    pub live: bool,
    pub decision: DecisionParams,
    /// 单行名义金额上限(USDT，币数 × 多腿每币价)，启动时和触发时各查一次。
    pub max_notional_usdt: Decimal,
    /// 已结束(开完/停止)各行未对冲敞口之和的上限(USDT)，超过就停掉所有行。
    pub max_unhedged_usdt: Decimal,
    /// 单腿成交价相对触发时对手价的滑点超过这个值(bp)打 warn。
    pub max_slippage_bp: f64,
    /// 这个文件存在时不决策、不下单；删掉后恢复。
    pub kill_switch_file: PathBuf,
    /// 决策/结果记录(JSON Lines)。
    pub record_file: PathBuf,
}

impl Default for CrossFundingParams {
    fn default() -> Self {
        Self {
            max_price_diff_pct: Decimal::from(5),
            fill_timeout_ms: 30_000,
            quote_max_age_ms: 5_000,
            live: false,
            decision: DecisionParams::default(),
            max_notional_usdt: Decimal::from(1_000),
            max_unhedged_usdt: Decimal::from(50),
            max_slippage_bp: 10.0,
            kill_switch_file: PathBuf::from("cross_funding.stop"),
            record_file: PathBuf::from("logs/cross_funding_orders.jsonl"),
        }
    }
}

/// 每币价格。
fn per_coin(price: Decimal, spec: &PerpContractSpec) -> Decimal {
    price / spec.price_unit
}

/// 两个价格的相对差，百分比，以均值为分母。
pub fn price_diff_pct(a: Decimal, b: Decimal) -> Decimal {
    let mean = (a + b) / Decimal::TWO;
    if mean <= Decimal::ZERO {
        return Decimal::MAX;
    }
    (a - b).abs() / mean * Decimal::ONE_HUNDRED
}

/// 一条腿的合法下单量规则，换算成币数：币数必须是 `coin_step` 的整数倍且
/// 不低于 `coin_min`。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CoinLotRule {
    pub coin_step: Decimal,
    pub coin_min: Decimal,
}

impl CoinLotRule {
    pub fn new(qty_step: Decimal, min_qty: Decimal, spec: &PerpContractSpec) -> Self {
        Self { coin_step: qty_step * spec.qty_unit, coin_min: min_qty * spec.qty_unit }
    }

    pub fn accepts(&self, coins: Decimal) -> bool {
        coins >= self.coin_min && (self.coin_step <= Decimal::ZERO || (coins % self.coin_step).is_zero())
    }
}

fn gcd(a: u128, b: u128) -> u128 {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// 两条腿都合法的最小币数：步进取最小公倍数，再取不低于两边最小量的第一个倍数。
/// 数值溢出(步进小数位太多)时返回 None。
pub fn min_common_coins(a: &CoinLotRule, b: &CoinLotRule) -> Option<Decimal> {
    if a.coin_step <= Decimal::ZERO || b.coin_step <= Decimal::ZERO {
        return Some(a.coin_min.max(b.coin_min));
    }
    let scale = a.coin_step.scale().max(b.coin_step.scale());
    let factor = Decimal::from_i128_with_scale(10_i128.checked_pow(scale)?, 0);
    let to_int = |d: Decimal| (d * factor).trunc().to_u128();
    let (sa, sb) = (to_int(a.coin_step)?, to_int(b.coin_step)?);
    if sa == 0 || sb == 0 {
        return None;
    }
    let lcm = (sa / gcd(sa, sb)).checked_mul(sb)?;
    let step = Decimal::from_i128_with_scale(i128::try_from(lcm).ok()?, 0) / factor;
    let need = a.coin_min.max(b.coin_min);
    let multiples = (need / step).ceil().max(Decimal::ONE);
    Some(multiples * step)
}

/// 当前报价下的两种价差(bp)。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LiveSpreads {
    /// 可成交价差：多腿 ask、空腿 bid，即此刻两腿吃单开仓的价差。
    pub exec_bp: f64,
    /// 两腿中间价价差，t0 决策用。
    pub mid_bp: f64,
}

/// 两边报价都新鲜、bid/ask 都为正时返回当前价差，否则 None。
pub fn live_spreads(
    long_quote: &Quote,
    short_quote: &Quote,
    long_spec: &PerpContractSpec,
    short_spec: &PerpContractSpec,
    now_ms: u64,
    max_age_ms: u64,
) -> Option<LiveSpreads> {
    let usable = |q: &Quote| {
        now_ms.saturating_sub(q.ts_ms) <= max_age_ms && q.bid > Decimal::ZERO && q.ask > Decimal::ZERO
    };
    if !usable(long_quote) || !usable(short_quote) {
        return None;
    }
    let mid = |q: &Quote| (q.bid + q.ask) / Decimal::TWO;
    let exec_bp = spread_bp(
        per_coin_f64(long_quote.ask, long_spec.price_unit),
        per_coin_f64(short_quote.bid, short_spec.price_unit),
    );
    let mid_bp = spread_bp(
        per_coin_f64(mid(long_quote), long_spec.price_unit),
        per_coin_f64(mid(short_quote), short_spec.price_unit),
    );
    (exec_bp.is_finite() && mid_bp.is_finite()).then_some(LiveSpreads { exec_bp, mid_bp })
}

/// 成交价相对参考价的滑点(bp)，正数表示成交得更差：买单成交价高于参考价、
/// 卖单成交价低于参考价。
pub fn slippage_bp(side: OrderSide, reference: f64, fill: f64) -> f64 {
    let diff = match side {
        OrderSide::Buy => fill - reference,
        OrderSide::Sell => reference - fill,
    };
    diff / reference * 10_000.0
}

fn finite(x: f64) -> Option<f64> {
    x.is_finite().then_some(x)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordEvent {
    /// t0 决策完成。
    Decision,
    /// 这一行结束(开完或停止)。
    Result,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    /// t0 决策为立即开。
    Immediate,
    /// 等待期内可成交价差 ≤ 目标。
    Target,
    /// 等满 24 小时没等到，到期开。
    Expiry,
}

/// `record_file` 的一行。`decision` 只有决策字段；`result` 在此基础上补齐触发、
/// 成交和节省。所有价格都是每币价格，价差/滑点单位 bp。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRecord {
    pub event: RecordEvent,
    /// `live` / `paper`。
    pub mode: String,
    pub pair: String,
    pub csv_line: usize,
    pub coins: Decimal,
    pub table_version: u64,
    pub t0_ms: u64,
    pub t0_utc: String,
    /// 决策用的最新小时 K 线(开盘时间)及其收盘价差/百分位/σ/σ 比。
    pub bar_open_ms: u64,
    pub bar_spread_bp: f64,
    pub pct: Option<f64>,
    pub sigma_bp: Option<f64>,
    pub sigma_ratio: Option<f64>,
    pub bin: Option<(f64, f64)>,
    /// 立即开的原因；等待时为 None。
    pub immediate_reason: Option<String>,
    /// 不开仓的原因(K 线不新鲜 / σ 告警)；开仓时为 None。
    #[serde(default)]
    pub skip_reason: Option<String>,
    pub k_star: Option<f64>,
    pub target_bp: Option<f64>,
    pub deadline_ms: Option<u64>,
    /// 当前档校准出的期望 B 和成交率；立即开时期望 B 记 0、成交率 None。
    pub expected_b_bp: f64,
    pub expected_fill_rate: Option<f64>,
    /// t0 两腿中间价价差。
    pub t0_mid_bp: f64,
    /// t0 可成交价差，即"不等、直接开"的成本，节省以它为基准。
    pub baseline_bp: f64,
    /// `filled` 或 `halted: 原因`。
    pub outcome: Option<String>,
    pub trigger: Option<TriggerKind>,
    pub trigger_ms: Option<u64>,
    pub trigger_exec_bp: Option<f64>,
    pub trigger_long_ask: Option<f64>,
    pub trigger_short_bid: Option<f64>,
    pub long_filled_coins: Option<Decimal>,
    pub long_avg_price: Option<f64>,
    pub short_filled_coins: Option<Decimal>,
    pub short_avg_price: Option<f64>,
    pub long_slippage_bp: Option<f64>,
    pub short_slippage_bp: Option<f64>,
    /// 多腿成交币数 − 空腿成交币数。
    pub residual_coins: Option<Decimal>,
    /// 两腿成交均价算出的价差。
    pub actual_bp: Option<f64>,
    /// `baseline_bp − actual_bp`，正数是等出来的好处。
    pub savings_bp: Option<f64>,
    /// 触发到这一行结束的耗时。
    pub fill_latency_ms: Option<u64>,
    /// 一期两腿都是市价单，固定 `taker`。
    pub liquidity: String,
}

/// 追加一行记录；写失败只打 error，不影响交易。
fn append_record(path: &Path, record: &OrderRecord) {
    let result = (|| -> anyhow::Result<()> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let mut line = serde_json::to_string(record)?;
        line.push('\n');
        std::fs::OpenOptions::new().create(true).append(true).open(path)?.write_all(line.as_bytes())?;
        Ok(())
    })();
    if let Err(err) = result {
        error!("cross_funding: 写记录 {} 失败: {err:#}；记录内容 {record:?}", path.display());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegRole {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    /// 还没有两腿都新鲜的报价，没做 t0 决策。
    AwaitingT0,
    /// 等可成交价差 ≤ `target_bp`，最晚到 `deadline_ms`。
    Waiting { target_bp: f64, deadline_ms: u64 },
    Submitted { at_ms: u64 },
    Done,
    Halted,
    /// t0 决策为不开仓。
    Skipped,
}

impl Phase {
    fn is_finished(self) -> bool {
        matches!(self, Phase::Done | Phase::Halted | Phase::Skipped)
    }
}

/// 等待期内这一笔报价该不该触发。
fn waiting_trigger(phase: Phase, exec_bp: f64, now_ms: u64) -> Option<TriggerKind> {
    match phase {
        Phase::Waiting { deadline_ms, .. } if now_ms >= deadline_ms => Some(TriggerKind::Expiry),
        Phase::Waiting { target_bp, .. } if exec_bp <= target_bp => Some(TriggerKind::Target),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct LegState {
    venue: Venue,
    symbol: Symbol,
    spec: PerpContractSpec,
    /// 下单量，合约 base 单位。
    order_qty: Decimal,
    quote: Option<Quote>,
    client_order_id: Option<String>,
    /// `(filled_qty, avg_price)`，合约 base 单位和原始报价。
    filled: Option<(Decimal, Decimal)>,
}

impl LegState {
    fn filled_coins(&self) -> Decimal {
        self.filled.map(|(qty, _)| self.spec.base_to_coins(qty)).unwrap_or(Decimal::ZERO)
    }

    /// 成交币数和每币成交均价。
    fn fill(&self) -> Option<(Decimal, f64)> {
        self.filled.map(|(qty, price)| (self.spec.base_to_coins(qty), per_coin_f64(price, self.spec.price_unit)))
    }

    /// 估值用的每币价格：有成交用成交均价，否则用最新报价中间价。
    fn mark_per_coin(&self) -> Option<Decimal> {
        let price = match (self.filled, self.quote) {
            (Some((_, price)), _) if price > Decimal::ZERO => price,
            (_, Some(q)) => (q.bid + q.ask) / Decimal::TWO,
            _ => return None,
        };
        Some(per_coin(price, &self.spec))
    }
}

/// 触发时刻的行情，算滑点用。
#[derive(Debug, Clone, Copy)]
struct TriggerInfo {
    kind: TriggerKind,
    at_ms: u64,
    exec_bp: f64,
    long_ask: f64,
    short_bid: f64,
}

#[derive(Debug, Clone)]
struct RowState {
    label: String,
    line: usize,
    pair: String,
    coins: Decimal,
    long: LegState,
    short: LegState,
    table: KStarTable,
    /// 启动时算出的最新小时状态。
    hour: HourState,
    phase: Phase,
    /// t0 决策记录，结果记录在它基础上补齐。
    record: Option<OrderRecord>,
    trigger: Option<TriggerInfo>,
}

impl RowState {
    fn leg_mut(&mut self, role: LegRole) -> &mut LegState {
        match role {
            LegRole::Long => &mut self.long,
            LegRole::Short => &mut self.short,
        }
    }

    /// 未对冲敞口(USDT)：|多腿成交币数 − 空腿成交币数| × 每币价格。
    fn unhedged_usdt(&self) -> Decimal {
        let residual = (self.long.filled_coins() - self.short.filled_coins()).abs();
        if residual.is_zero() {
            return Decimal::ZERO;
        }
        let price = self.long.mark_per_coin().or_else(|| self.short.mark_per_coin()).unwrap_or(Decimal::ZERO);
        residual * price
    }
}

#[derive(Default)]
struct State {
    rows: Vec<RowState>,
    quote_index: HashMap<(Venue, Symbol), Vec<(usize, LegRole)>>,
    order_index: HashMap<String, (usize, LegRole)>,
}

/// 一次下单动作，在锁外发布。
struct PendingSubmit {
    venue: Venue,
    symbol: Symbol,
    side: OrderSide,
    qty: Decimal,
    client_order_id: String,
    group_id: String,
}

#[derive(Default)]
struct KillSwitch {
    checked_ms: u64,
    active: bool,
}

pub struct CrossFundingStrategy {
    bus: Arc<TopicBus>,
    params: CrossFundingParams,
    rows: Vec<PairRow>,
    /// 按 [`PairRow::key`] 索引的生效 k* 表。
    tables: HashMap<String, KStarTable>,
    clients: HashMap<Venue, VenueClients>,
    /// live 模式才有；收到拒单事件时用它查订单的权威状态。
    order_manager: Option<Arc<OrderManager>>,
    state: Mutex<State>,
    /// 各 leg (venue, symbol) 最新订单簿快照，`on_order_book` 维护，独立于
    /// `state` 里的触发逻辑，暂不接入价差/下单判断。
    latest_order_books: Mutex<HashMap<(Venue, Symbol), OrderBookLevels>>,
    order_book_log: OrderBookLogThrottle,
    /// 未对冲敞口超限后置位，所有行不再下单。
    halted: AtomicBool,
    kill_switch: Mutex<KillSwitch>,
}

impl CrossFundingStrategy {
    pub fn new(
        bus: Arc<TopicBus>,
        rows: Vec<PairRow>,
        tables: HashMap<String, KStarTable>,
        clients: HashMap<Venue, VenueClients>,
        params: CrossFundingParams,
        order_manager: Option<Arc<OrderManager>>,
    ) -> Self {
        Self {
            bus,
            params,
            rows,
            tables,
            clients,
            order_manager,
            state: Mutex::new(State::default()),
            latest_order_books: Mutex::new(HashMap::new()),
            order_book_log: OrderBookLogThrottle::new(ORDER_BOOK_LOG_INTERVAL_MS),
            halted: AtomicBool::new(false),
            kill_switch: Mutex::new(KillSwitch::default()),
        }
    }

    fn mode(&self) -> &'static str {
        if self.params.live { "live" } else { "paper" }
    }

    fn clients(&self, venue: &Venue) -> anyhow::Result<&VenueClients> {
        self.clients.get(venue).ok_or_else(|| anyhow::anyhow!("no clients configured for venue {venue}"))
    }

    /// 启动时对一行做完整检查，通过后返回运行时状态。任何一步失败都返回 Err，
    /// 由调用方打日志跳过这一行。
    async fn prepare_row(
        &self,
        row: &PairRow,
        table: &KStarTable,
        precisions: &HashMap<Venue, PrecisionCache>,
    ) -> anyhow::Result<RowState> {
        let long_venue = row.long.venue();
        let short_venue = row.short.venue();
        let long_clients = self.clients(&long_venue)?;
        let short_clients = self.clients(&short_venue)?;

        // 1. 合约规格 + 数量换算/精度校验
        let long_spec = long_clients.exchange_info.perpetual_contract_spec(&row.long.symbol).await?;
        let short_spec = short_clients.exchange_info.perpetual_contract_spec(&row.short.symbol).await?;
        let lot_rule = |venue: &Venue, symbol: &Symbol, spec: &PerpContractSpec| -> anyhow::Result<CoinLotRule> {
            let cache = precisions
                .get(venue)
                .ok_or_else(|| anyhow::anyhow!("precision cache for {venue} not loaded"))?;
            let step = cache.qty_step(symbol, PrecisionKind::Market)?;
            let min = cache.min_qty(symbol, PrecisionKind::Market)?;
            Ok(CoinLotRule::new(step, min, spec))
        };
        let long_rule = lot_rule(&long_venue, &row.long.symbol, &long_spec)?;
        let short_rule = lot_rule(&short_venue, &row.short.symbol, &short_spec)?;
        if !long_rule.accepts(row.quantity) || !short_rule.accepts(row.quantity) {
            let hint = match min_common_coins(&long_rule, &short_rule) {
                Some(min_coins) => {
                    let usdt = self.approx_usdt(&row.long, &long_spec, min_coins).await;
                    match usdt {
                        Some(usdt) => format!("最小合法 quantity={} (≈{} USDT)", min_coins.normalize(), usdt.round_dp(2)),
                        None => format!("最小合法 quantity={}", min_coins.normalize()),
                    }
                }
                None => "无法计算最小合法 quantity".to_string(),
            };
            anyhow::bail!(
                "quantity={} 不满足下单精度(多腿每 {} 币一档、最少 {} 币；空腿每 {} 币一档、最少 {} 币)，{hint}",
                row.quantity.normalize(),
                long_rule.coin_step.normalize(),
                long_rule.coin_min.normalize(),
                short_rule.coin_step.normalize(),
                short_rule.coin_min.normalize(),
            );
        }
        let long_qty = long_spec.coins_to_base(row.quantity);
        let short_qty = short_spec.coins_to_base(row.quantity);

        // 2. 两边都不能已有持仓
        let long_pos = long_clients.orders.perp_position(&row.long.symbol).await?;
        let short_pos = short_clients.orders.perp_position(&row.short.symbol).await?;
        if !long_pos.is_zero() || !short_pos.is_zero() {
            anyhow::bail!(
                "已有持仓(多腿 {}={}, 空腿 {}={})，不重复开仓",
                row.long.native_symbol,
                long_pos.normalize(),
                row.short.native_symbol,
                short_pos.normalize()
            );
        }

        // 3. 小时 K 线 → 最新一根的价差/百分位/σ
        let end_ms = (now_ms() / HOUR_MS - 1) * HOUR_MS;
        let start_ms = end_ms - (HISTORY_HOURS - 1) * HOUR_MS;
        let (long_klines, short_klines) = tokio::try_join!(
            long_clients.klines.klines_range(&row.long.symbol, KlineInterval::H1, start_ms, end_ms),
            short_clients.klines.klines_range(&row.short.symbol, KlineInterval::H1, start_ms, end_ms),
        )?;
        let series = HourlySpread::from_klines(&long_klines, &short_klines, long_spec.price_unit, short_spec.price_unit)
            .ok_or_else(|| {
                anyhow::anyhow!("两腿小时 K 线没有重叠(多腿 {} 根，空腿 {} 根)", long_klines.len(), short_klines.len())
            })?;
        let hour = latest_state(&series).ok_or_else(|| anyhow::anyhow!("最近 {HISTORY_HOURS} 小时价差全部无效"))?;

        // 4. 价格合理性检查
        let (Some(last_long), Some(last_short)) = (long_klines.last(), short_klines.last()) else {
            anyhow::bail!("没有小时 K 线");
        };
        let last_long = per_coin(last_long.close, &long_spec);
        let last_short = per_coin(last_short.close, &short_spec);
        let diff_pct = price_diff_pct(last_long, last_short);
        if diff_pct > self.params.max_price_diff_pct {
            anyhow::bail!(
                "两边每币价格相差过大：多腿 {} / 空腿 {}，相差 {}% > {}%，检查是否配错合约或乘数",
                last_long.normalize(),
                last_short.normalize(),
                diff_pct.round_dp(2),
                self.params.max_price_diff_pct
            );
        }

        // 5. 名义金额上限
        let notional = row.quantity * last_long;
        if notional > self.params.max_notional_usdt {
            anyhow::bail!(
                "名义金额 ≈{} USDT 超过上限 max_notional_usdt={}",
                notional.round_dp(2),
                self.params.max_notional_usdt
            );
        }

        // 6. 24h 成交额，只打日志
        let long_volume = long_clients.klines.quote_volume_24h(&row.long.symbol).await;
        let short_volume = short_clients.klines.quote_volume_24h(&row.short.symbol).await;
        let fmt_volume = |v: anyhow::Result<Decimal>| match v {
            Ok(v) => format!("{} USDT", v.round_dp(0)),
            Err(err) => format!("查询失败({err:#})"),
        };
        let sigma_ratio = hour.sigma_bp.map(|s| s / table.sigma_median_bp);
        info!(
            "cross_funding {}: 通过检查 quantity={} 币(下单量 多腿 {} / 空腿 {}, ≈{} USDT) k*表 v{}(数据截止 {}) 最新小时 {} 价差={:.4}bp 百分位={} σ={} σ比={} 24h成交额 多={} 空={}",
            row.label(),
            row.quantity.normalize(),
            long_qty.normalize(),
            short_qty.normalize(),
            notional.round_dp(2),
            table.version,
            utc::format_minute(table.data_end_ms),
            utc::format_minute(hour.open_time_ms),
            hour.spread_bp,
            fmt_opt(hour.pct),
            fmt_opt(hour.sigma_bp),
            fmt_opt(sigma_ratio),
            fmt_volume(long_volume),
            fmt_volume(short_volume),
        );

        let leg = |venue: Venue, symbol: &Symbol, spec: PerpContractSpec, order_qty: Decimal| LegState {
            venue,
            symbol: symbol.clone(),
            spec,
            order_qty,
            quote: None,
            client_order_id: None,
            filled: None,
        };
        Ok(RowState {
            label: row.label(),
            line: row.line,
            pair: row.key(),
            coins: row.quantity,
            long: leg(long_venue, &row.long.symbol, long_spec, long_qty),
            short: leg(short_venue, &row.short.symbol, short_spec, short_qty),
            table: table.clone(),
            hour,
            phase: Phase::AwaitingT0,
            record: None,
            trigger: None,
        })
    }

    /// 精度不合法时给提示用的 USDT 估值：拿多腿最近一根小时 K 线收盘价估算，失败返回 None。
    async fn approx_usdt(&self, leg: &LegSpec, spec: &PerpContractSpec, coins: Decimal) -> Option<Decimal> {
        let clients = self.clients.get(&leg.venue())?;
        let klines = clients.klines.klines(&leg.symbol, KlineInterval::H1, 1).await.ok()?;
        Some(per_coin(klines.last()?.close, spec) * coins)
    }

    /// kill switch 文件是否存在(每 [`KILL_SWITCH_POLL_MS`] 查一次)，状态变化时打日志。
    fn kill_switch_active(&self, now: u64) -> bool {
        let mut ks = self.kill_switch.lock().unwrap();
        if now.saturating_sub(ks.checked_ms) >= KILL_SWITCH_POLL_MS {
            ks.checked_ms = now;
            let active = self.params.kill_switch_file.exists();
            if active != ks.active {
                if active {
                    warn!(
                        "cross_funding: 检测到 kill switch 文件 {}，暂停决策和下单(已提交的订单照常跟踪)，删除该文件恢复",
                        self.params.kill_switch_file.display()
                    );
                } else {
                    info!("cross_funding: kill switch 文件已删除，恢复决策和下单");
                }
                ks.active = active;
            }
        }
        ks.active
    }

    /// t0 决策：写 decision 记录；立即开返回 `Some(Immediate)`，等待则把这一行切到
    /// `Waiting` 并返回 None。
    fn decide_t0(&self, row: &mut RowState, spreads: LiveSpreads, now: u64) -> Option<TriggerKind> {
        let report = decide(&row.hour, &row.table, spreads.mid_bp, now, &self.params.decision);
        let record = self.decision_record(row, &report, spreads, now);
        match &report.decision {
            Decision::Immediate(reason) => info!(
                "cross_funding {}: t0 决策 立即开({reason})；最新小时 {} 收盘价差={:.4}bp 百分位={} σ={} σ比={} 档={} t0 中间价价差={:.4}bp 可成交价差={:.4}bp",
                row.label,
                utc::format_minute(row.hour.open_time_ms),
                row.hour.spread_bp,
                fmt_opt(row.hour.pct),
                fmt_opt(row.hour.sigma_bp),
                fmt_opt(report.sigma_ratio),
                fmt_bin(report.bin),
                spreads.mid_bp,
                spreads.exec_bp,
            ),
            Decision::Skip(reason) => warn!(
                "cross_funding {}: t0 决策 不开仓，原因：{reason}；最新小时 {} 收盘价差={:.4}bp 百分位={} σ={} σ比={} 档={} t0 中间价价差={:.4}bp 可成交价差={:.4}bp。这一行本进程内不再开仓",
                row.label,
                utc::format_minute(row.hour.open_time_ms),
                row.hour.spread_bp,
                fmt_opt(row.hour.pct),
                fmt_opt(row.hour.sigma_bp),
                fmt_opt(report.sigma_ratio),
                fmt_bin(report.bin),
                spreads.mid_bp,
                spreads.exec_bp,
            ),
            Decision::Wait { k_star, target_bp } => info!(
                "cross_funding {}: t0 决策 等待；最新小时 {} 收盘价差={:.4}bp 百分位={} σ={} σ比={} 档={} k*={k_star} 期望B={:.4}bp 成交率={} t0 中间价价差={:.4}bp 可成交价差={:.4}bp → 目标={target_bp:.4}bp，截止 {}",
                row.label,
                utc::format_minute(row.hour.open_time_ms),
                row.hour.spread_bp,
                fmt_opt(row.hour.pct),
                fmt_opt(row.hour.sigma_bp),
                fmt_opt(report.sigma_ratio),
                fmt_bin(report.bin),
                record.expected_b_bp,
                fmt_opt(record.expected_fill_rate),
                spreads.mid_bp,
                spreads.exec_bp,
                utc::format_minute(now + WAIT_HORIZON_MS),
            ),
        }
        append_record(&self.params.record_file, &record);
        row.record = Some(record);
        match report.decision {
            Decision::Immediate(_) => Some(TriggerKind::Immediate),
            Decision::Skip(reason) => {
                row.phase = Phase::Skipped;
                if let Some(result) = self.result_record(row, &format!("skipped: {reason}"), now) {
                    append_record(&self.params.record_file, &result);
                }
                None
            }
            Decision::Wait { target_bp, .. } => {
                row.phase = Phase::Waiting { target_bp, deadline_ms: now + WAIT_HORIZON_MS };
                None
            }
        }
    }

    fn decision_record(&self, row: &RowState, report: &DecisionReport, spreads: LiveSpreads, now: u64) -> OrderRecord {
        let skip_reason = match &report.decision {
            Decision::Skip(reason) => Some(reason.to_string()),
            _ => None,
        };
        let (immediate_reason, k_star, target_bp, deadline_ms, expected_b_bp, expected_fill_rate) = match &report.decision
        {
            Decision::Immediate(reason) => (Some(reason.to_string()), None, None, None, 0.0, None),
            Decision::Skip(_) => (None, None, None, None, 0.0, None),
            Decision::Wait { k_star, target_bp } => {
                let stats = report.state.pct.and_then(|p| row.table.bin_for(p)).and_then(|b| b.stats.as_ref());
                (
                    None,
                    Some(*k_star),
                    Some(*target_bp),
                    Some(now + WAIT_HORIZON_MS),
                    stats.map(|s| s.b_expect_bp).unwrap_or(0.0),
                    stats.map(|s| s.b_fill_rate),
                )
            }
        };
        OrderRecord {
            event: RecordEvent::Decision,
            mode: self.mode().to_string(),
            pair: row.pair.clone(),
            csv_line: row.line,
            coins: row.coins,
            table_version: row.table.version,
            t0_ms: now,
            t0_utc: utc::format_minute(now),
            bar_open_ms: report.state.open_time_ms,
            bar_spread_bp: report.state.spread_bp,
            pct: report.state.pct,
            sigma_bp: report.state.sigma_bp,
            sigma_ratio: report.sigma_ratio.and_then(finite),
            bin: report.bin,
            immediate_reason,
            skip_reason,
            k_star,
            target_bp,
            deadline_ms,
            expected_b_bp,
            expected_fill_rate,
            t0_mid_bp: spreads.mid_bp,
            baseline_bp: spreads.exec_bp,
            outcome: None,
            trigger: None,
            trigger_ms: None,
            trigger_exec_bp: None,
            trigger_long_ask: None,
            trigger_short_bid: None,
            long_filled_coins: None,
            long_avg_price: None,
            short_filled_coins: None,
            short_avg_price: None,
            long_slippage_bp: None,
            short_slippage_bp: None,
            residual_coins: None,
            actual_bp: None,
            savings_bp: None,
            fill_latency_ms: None,
            liquidity: "taker".to_string(),
        }
    }

    /// 在 decision 记录基础上补齐触发、成交、节省。没有 decision 记录(没做过 t0
    /// 决策)返回 None。
    fn result_record(&self, row: &RowState, outcome: &str, now: u64) -> Option<OrderRecord> {
        let mut record = row.record.clone()?;
        record.event = RecordEvent::Result;
        record.outcome = Some(outcome.to_string());
        let long = row.long.fill();
        let short = row.short.fill();
        if let Some(trigger) = row.trigger {
            record.trigger = Some(trigger.kind);
            record.trigger_ms = Some(trigger.at_ms);
            record.trigger_exec_bp = Some(trigger.exec_bp);
            record.trigger_long_ask = Some(trigger.long_ask);
            record.trigger_short_bid = Some(trigger.short_bid);
            record.fill_latency_ms = Some(now.saturating_sub(trigger.at_ms));
            record.long_slippage_bp = long.and_then(|(_, px)| finite(slippage_bp(OrderSide::Buy, trigger.long_ask, px)));
            record.short_slippage_bp =
                short.and_then(|(_, px)| finite(slippage_bp(OrderSide::Sell, trigger.short_bid, px)));
        }
        record.long_filled_coins = long.map(|(coins, _)| coins);
        record.long_avg_price = long.map(|(_, px)| px);
        record.short_filled_coins = short.map(|(coins, _)| coins);
        record.short_avg_price = short.map(|(_, px)| px);
        record.residual_coins = Some(row.long.filled_coins() - row.short.filled_coins());
        if let (Some((_, long_px)), Some((_, short_px))) = (long, short) {
            let actual = spread_bp(long_px, short_px);
            record.actual_bp = finite(actual);
            record.savings_bp = finite(record.baseline_bp - actual);
        }
        Some(record)
    }

    /// 触发：再查一次名义上限，live 下单，模拟盘直接按对手价成交。返回这一行
    /// 是否已经结束。
    #[allow(clippy::too_many_arguments)]
    fn trigger(
        &self,
        idx: usize,
        row: &mut RowState,
        kind: TriggerKind,
        spreads: LiveSpreads,
        long_quote: Quote,
        short_quote: Quote,
        now: u64,
        order_index: &mut HashMap<String, (usize, LegRole)>,
        submits: &mut Vec<PendingSubmit>,
    ) -> bool {
        let long_ask = per_coin_f64(long_quote.ask, row.long.spec.price_unit);
        let short_bid = per_coin_f64(short_quote.bid, row.short.spec.price_unit);
        row.trigger = Some(TriggerInfo { kind, at_ms: now, exec_bp: spreads.exec_bp, long_ask, short_bid });
        let target = match row.phase {
            Phase::Waiting { target_bp, .. } => format!("{target_bp:.4}bp"),
            _ => "-".to_string(),
        };
        info!(
            "cross_funding {}: 触发({kind:?}) 可成交价差={:.4}bp 目标={target} 基准={:.4}bp，多腿 ask={} 空腿 bid={}",
            row.label,
            spreads.exec_bp,
            row.record.as_ref().map(|r| r.baseline_bp).unwrap_or(f64::NAN),
            long_quote.ask,
            short_quote.bid
        );

        let notional = row.coins * per_coin(long_quote.ask, &row.long.spec);
        if notional > self.params.max_notional_usdt {
            error!(
                "cross_funding {}: 触发时名义金额 ≈{} USDT 超过上限 max_notional_usdt={}，不下单，这一行停止",
                row.label,
                notional.round_dp(2),
                self.params.max_notional_usdt
            );
            self.halt(row, "名义金额超限", now);
            return true;
        }

        if !self.params.live {
            row.long.filled = Some((row.long.order_qty, long_quote.ask));
            row.short.filled = Some((row.short.order_qty, short_quote.bid));
            info!("cross_funding {}: [模拟盘] 按对手价模拟成交，不下单", row.label);
            self.complete(row, now);
            return true;
        }

        let long_id = new_client_order_id();
        let mut short_id = new_client_order_id();
        while short_id == long_id {
            short_id = new_client_order_id();
        }
        let group_id = format!("cf-row{}-{now}", row.line);
        row.long.client_order_id = Some(long_id.clone());
        row.short.client_order_id = Some(short_id.clone());
        row.phase = Phase::Submitted { at_ms: now };
        for (leg, side, id) in [(&row.long, OrderSide::Buy, long_id.clone()), (&row.short, OrderSide::Sell, short_id.clone())] {
            submits.push(PendingSubmit {
                venue: leg.venue.clone(),
                symbol: leg.symbol.clone(),
                side,
                qty: leg.order_qty,
                client_order_id: id,
                group_id: group_id.clone(),
            });
        }
        order_index.insert(long_id, (idx, LegRole::Long));
        order_index.insert(short_id, (idx, LegRole::Short));
        false
    }

    /// 两腿都已成交：写结果、检查滑点，这一行结束。
    fn complete(&self, row: &mut RowState, now: u64) {
        row.phase = Phase::Done;
        let Some(record) = self.result_record(row, "filled", now) else {
            return;
        };
        info!(
            "cross_funding {}: 开仓完成({:?})，多腿 {}@{} 空腿 {}@{}，成交价差={}bp 基准={:.4}bp 节省={}bp 期望B={:.4}bp 滑点 多={}bp 空={}bp 残余={} 币，耗时 {}ms",
            row.label,
            record.trigger,
            fmt_opt_dec(record.long_filled_coins),
            fmt_opt(record.long_avg_price),
            fmt_opt_dec(record.short_filled_coins),
            fmt_opt(record.short_avg_price),
            fmt_opt(record.actual_bp),
            record.baseline_bp,
            fmt_opt(record.savings_bp),
            record.expected_b_bp,
            fmt_opt(record.long_slippage_bp),
            fmt_opt(record.short_slippage_bp),
            fmt_opt_dec(record.residual_coins),
            record.fill_latency_ms.unwrap_or(0),
        );
        for (leg, slippage) in [("多腿", record.long_slippage_bp), ("空腿", record.short_slippage_bp)] {
            if let Some(slippage) = slippage
                && slippage > self.params.max_slippage_bp
            {
                warn!(
                    "cross_funding {}: {leg}成交滑点 {slippage:.4}bp 超过 max_slippage_bp={}",
                    row.label, self.params.max_slippage_bp
                );
            }
        }
        if record.residual_coins.is_some_and(|r| !r.is_zero()) {
            warn!("cross_funding {}: 两腿成交币数不一致，残余 {} 币，请人工核对", row.label, fmt_opt_dec(record.residual_coins));
        }
        append_record(&self.params.record_file, &record);
    }

    /// 把这一行停在 `Halted` 并写结果记录(只在第一次停止时写)。
    fn halt(&self, row: &mut RowState, reason: &str, now: u64) {
        if row.phase == Phase::Halted {
            return;
        }
        row.phase = Phase::Halted;
        if let Some(record) = self.result_record(row, &format!("halted: {reason}"), now) {
            append_record(&self.params.record_file, &record);
        }
    }

    /// 已结束各行的未对冲敞口之和超过上限就停掉所有行。
    fn check_unhedged(&self, rows: &[RowState]) {
        let total: Decimal = rows.iter().filter(|r| r.phase.is_finished()).map(RowState::unhedged_usdt).sum();
        if total > self.params.max_unhedged_usdt && !self.halted.swap(true, Ordering::SeqCst) {
            let detail: Vec<String> = rows
                .iter()
                .filter(|r| r.phase.is_finished() && !r.unhedged_usdt().is_zero())
                .map(|r| {
                    format!(
                        "{}: 多 {} 币 / 空 {} 币 ≈{} USDT",
                        r.label,
                        r.long.filled_coins().normalize(),
                        r.short.filled_coins().normalize(),
                        r.unhedged_usdt().round_dp(2)
                    )
                })
                .collect();
            error!(
                "cross_funding: 未对冲敞口合计 ≈{} USDT 超过上限 max_unhedged_usdt={}，所有行停止下单，请人工处理：{}",
                total.round_dp(2),
                self.params.max_unhedged_usdt,
                detail.join("；")
            );
        }
    }

    /// 处理一条腿的终态失败(拒单/撤单/过期)：先查 OrderManager 里的权威状态，
    /// 确实已全部成交就按成交处理，否则记下已成交部分并停掉这一行。
    fn handle_leg_failure(&self, order_id: &OrderId, client_order_id: &str, kind: &str, reason: &str) {
        let authoritative = self.order_manager.as_ref().and_then(|om| om.get_order(order_id));
        if let Some(order) = &authoritative
            && order.status == OrderStatus::Filled
        {
            warn!("cross_funding: 订单 {client_order_id} 收到 {kind}({reason})，但权威状态已是 Filled，按成交处理");
            self.record_fill(client_order_id, order.filled_qty, order.avg_price.unwrap_or(Decimal::ZERO));
            return;
        }
        let now = now_ms();
        let mut guard = self.state.lock().unwrap();
        let State { rows, order_index, .. } = &mut *guard;
        let Some(&(idx, role)) = order_index.get(client_order_id) else {
            return;
        };
        let row = &mut rows[idx];
        if let Some(order) = &authoritative
            && order.filled_qty > Decimal::ZERO
        {
            row.leg_mut(role).filled = Some((order.filled_qty, order.avg_price.unwrap_or(Decimal::ZERO)));
        }
        let status = authoritative
            .as_ref()
            .map(|o| format!("status={:?} filled_qty={}", o.status, o.filled_qty))
            .unwrap_or_else(|| "订单记录不存在".to_string());
        error!(
            "cross_funding {}: {:?} 腿订单 {client_order_id} {kind}: {reason}；权威状态 {status}。这一行停止交易，请人工处理",
            row.label, role
        );
        self.halt(row, &format!("{role:?} 腿{kind}: {reason}"), now);
        self.check_unhedged(rows);
    }

    fn record_fill(&self, client_order_id: &str, filled_qty: Decimal, avg_price: Decimal) {
        let now = now_ms();
        let mut guard = self.state.lock().unwrap();
        let State { rows, order_index, .. } = &mut *guard;
        let Some(&(idx, role)) = order_index.get(client_order_id) else {
            return;
        };
        let row = &mut rows[idx];
        row.leg_mut(role).filled = Some((filled_qty, avg_price));
        info!(
            "cross_funding {}: {:?} 腿 {client_order_id} 成交 qty={} avg_price={}",
            row.label,
            role,
            filled_qty.normalize(),
            avg_price.normalize()
        );
        match row.phase {
            Phase::Submitted { .. } if row.long.filled.is_some() && row.short.filled.is_some() => self.complete(row, now),
            Phase::Halted => {
                warn!("cross_funding {}: 这一行已停止，{:?} 腿仍然成交了，请人工核对两边持仓", row.label, role)
            }
            _ => {}
        }
        self.check_unhedged(rows);
    }
}

fn fmt_opt(x: Option<f64>) -> String {
    x.map(|x| format!("{x:.4}")).unwrap_or_else(|| "-".to_string())
}

fn fmt_opt_dec(x: Option<Decimal>) -> String {
    x.map(|x| x.normalize().to_string()).unwrap_or_else(|| "-".to_string())
}

fn fmt_bin(bin: Option<(f64, f64)>) -> String {
    bin.map(|(lo, hi)| format!("[{lo},{hi})")).unwrap_or_else(|| "-".to_string())
}

fn new_client_order_id() -> String {
    // 刻意不用 `Strategy::generate_client_order_id`(带完整策略名)：Gate `text`
    // 前缀后最多 28 字节、OKX `clOrdId` 只能是 1~32 位字母数字，短的纯字母数字
    // id 才能在各所原样回传、按 client_order_id 关联。
    format!("cf{}{:05}", now_ms(), rand::random::<u32>() % 100_000)
}

#[async_trait]
impl Strategy for CrossFundingStrategy {
    fn name(&self) -> &str {
        STRATEGY_NAME
    }

    fn bus(&self) -> &Arc<TopicBus> {
        &self.bus
    }

    async fn init(&self, subscriber: &mut StrategySubscriber) -> anyhow::Result<()> {
        info!("cross_funding: 一期已知局限(记录在案，暂不修)：\n{KNOWN_LIMITATIONS}");
        let mut precisions = HashMap::new();
        for (venue, clients) in &self.clients {
            match PrecisionCache::load_perpetual(clients.exchange_info.as_ref()).await {
                Ok(cache) => {
                    precisions.insert(venue.clone(), cache);
                }
                Err(err) => warn!("cross_funding: 加载 {venue} 永续下单精度失败，相关行都会跳过: {err:#}"),
            }
        }

        let now = now_ms();
        let mut prepared = Vec::new();
        for row in &self.rows {
            let key = row.key();
            let Some(table) = self.tables.get(&key) else {
                error!(
                    "cross_funding {}: Redis 里没有生效的 k* 表({key})，跳过；先 cross-funding-kstar calibrate 再 approve",
                    row.label()
                );
                continue;
            };
            if table.pair != key {
                error!("cross_funding {}: k* 表属于 {}，和本行 {key} 不一致，跳过", row.label(), table.pair);
                continue;
            }
            let age_ms = now.saturating_sub(table.data_end_ms);
            if age_ms > TABLE_STALE_MS {
                warn!(
                    "cross_funding {}: k* 表 v{} 数据截止 {}，已 {:.1} 天没有重新校准(应每周校准)",
                    row.label(),
                    table.version,
                    utc::format_minute(table.data_end_ms),
                    age_ms as f64 / (24 * HOUR_MS) as f64
                );
            }
            match self.prepare_row(row, table, &precisions).await {
                Ok(state) => prepared.push(state),
                Err(err) => warn!("cross_funding {}: 跳过: {err:#}", row.label()),
            }
        }
        if prepared.is_empty() {
            anyhow::bail!("cross_funding: CSV 里 {} 行全部未通过启动检查，没有可交易的行", self.rows.len());
        }
        info!(
            "cross_funding: {}/{} 行通过检查，模式={}，记录写到 {}，kill switch 文件 {}",
            prepared.len(),
            self.rows.len(),
            self.mode(),
            self.params.record_file.display(),
            self.params.kill_switch_file.display()
        );
        self.kill_switch_active(now);

        let mut state = self.state.lock().unwrap();
        for (idx, row) in prepared.into_iter().enumerate() {
            for (role, leg) in [(LegRole::Long, &row.long), (LegRole::Short, &row.short)] {
                state.quote_index.entry((leg.venue.clone(), leg.symbol.clone())).or_default().push((idx, role));
                subscriber.subscribe_quote(Topic::quote(leg.venue.clone(), leg.symbol.clone()));
                subscriber.subscribe_order_book(Topic::order_book(leg.venue.clone(), leg.symbol.clone()));
            }
            state.rows.push(row);
        }
        Ok(())
    }

    fn on_order_book(&self, topic: &Topic, book: &OrderBookLevels) {
        let Topic::OrderBook { venue, symbol } = topic else {
            return;
        };
        self.latest_order_books.lock().unwrap().insert((venue.clone(), symbol.clone()), book.clone());

        if self.order_book_log.should_log(venue, symbol, now_ms()) {
            let quote = {
                let state = self.state.lock().unwrap();
                state.quote_index.get(&(venue.clone(), symbol.clone())).and_then(|targets| {
                    let &(idx, role) = targets.first()?;
                    let row = &state.rows[idx];
                    match role {
                        LegRole::Long => row.long.quote,
                        LegRole::Short => row.short.quote,
                    }
                })
            };
            info!(
                "cross_funding orderbook {venue} {symbol} {}",
                describe_order_book(book, quote.as_ref(), ORDER_BOOK_LOG_DEPTH)
            );
        }
    }

    fn on_quote(&self, topic: &Topic, quote: &Quote) {
        let Topic::Quote { venue, symbol } = topic else {
            return;
        };
        let now = now_ms();
        let paused = self.kill_switch_active(now);
        let mut submits = Vec::new();
        {
            let mut guard = self.state.lock().unwrap();
            let State { rows, quote_index, order_index } = &mut *guard;
            let mut finished = false;

            for row in rows.iter_mut() {
                if let Phase::Submitted { at_ms } = row.phase
                    && now.saturating_sub(at_ms) > self.params.fill_timeout_ms
                {
                    error!(
                        "cross_funding {}: 下单 {}ms 后两腿仍未全部成交(多腿 {:?} 空腿 {:?})，这一行停止交易，请人工处理",
                        row.label,
                        now - at_ms,
                        row.long.filled,
                        row.short.filled
                    );
                    self.halt(row, "成交超时", now);
                    finished = true;
                }
            }

            if let Some(targets) = quote_index.get(&(venue.clone(), symbol.clone())) {
                for &(idx, role) in targets {
                    let row = &mut rows[idx];
                    row.leg_mut(role).quote = Some(*quote);
                    if paused || self.halted.load(Ordering::SeqCst) {
                        continue;
                    }
                    if !matches!(row.phase, Phase::AwaitingT0 | Phase::Waiting { .. }) {
                        continue;
                    }
                    let (Some(long_quote), Some(short_quote)) = (row.long.quote, row.short.quote) else {
                        continue;
                    };
                    let Some(spreads) = live_spreads(
                        &long_quote,
                        &short_quote,
                        &row.long.spec,
                        &row.short.spec,
                        now,
                        self.params.quote_max_age_ms,
                    ) else {
                        continue;
                    };
                    let kind = if row.phase == Phase::AwaitingT0 { self.decide_t0(row, spreads, now) } else { None };
                    let Some(kind) = kind.or_else(|| waiting_trigger(row.phase, spreads.exec_bp, now)) else {
                        continue;
                    };
                    finished |= self.trigger(
                        idx,
                        row,
                        kind,
                        spreads,
                        long_quote,
                        short_quote,
                        now,
                        order_index,
                        &mut submits,
                    );
                }
            }
            if finished {
                self.check_unhedged(rows);
            }
        }

        for submit in submits {
            info!(
                "cross_funding: 提交市价单 {} {} {:?} qty={} client_order_id={}",
                submit.venue, submit.symbol, submit.side, submit.qty, submit.client_order_id
            );
            self.submit_order(
                submit.venue,
                submit.symbol,
                submit.side,
                OrderAmount::Base(submit.qty),
                Some(submit.client_order_id),
                Some(submit.group_id),
                None,
            );
        }
    }

    fn on_order_event(&self, event: &OrderEvent) {
        let Some(client_order_id) = event.client_order_id() else {
            return;
        };
        if !self.state.lock().unwrap().order_index.contains_key(client_order_id) {
            return;
        }
        match event {
            OrderEvent::Filled { filled_qty, avg_price, .. } => self.record_fill(client_order_id, *filled_qty, *avg_price),
            OrderEvent::RejectedByRisk { order_id, reason, .. } => {
                self.handle_leg_failure(order_id, client_order_id, "被风控拒绝", reason)
            }
            OrderEvent::RejectedByExchange { order_id, reason, .. } => {
                self.handle_leg_failure(order_id, client_order_id, "被交易所拒绝/失效", reason)
            }
            OrderEvent::Cancelled { order_id, filled_qty, .. } => self.handle_leg_failure(
                order_id,
                client_order_id,
                "被撤销",
                &format!("filled_qty={filled_qty}"),
            ),
            OrderEvent::PartiallyFilled { filled_qty, avg_price, .. } => {
                info!("cross_funding: 订单 {client_order_id} 部分成交 qty={filled_qty} avg_price={avg_price}")
            }
            other => debug!("cross_funding: 订单事件 {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::spread_wait::{BinAction, BinStats, CalibratedBin, RuleParams};

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn spec(price_unit: i64, qty_unit: i64) -> PerpContractSpec {
        PerpContractSpec { price_unit: Decimal::from(price_unit), qty_unit: Decimal::from(qty_unit) }
    }

    fn quote(bid: &str, ask: &str, ts_ms: u64) -> Quote {
        Quote { bid: d(bid), bid_size: Decimal::ONE, ask: d(ask), ask_size: Decimal::ONE, ts_ms }
    }

    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-6, "{a} != {b}");
    }

    #[test]
    fn parses_csv_rows_and_maps_symbols() {
        let text = "long_venue,long_symbol,short_venue,short_symbol,quantity\n\
                    \n# comment\n\
                    gate,PEPE_USDT,binance,1000PEPEUSDT,10000000\n\
                    Binance , BTCUSDT , gate , btc_usdt , 0.01\n\
                    okx,PEPE-USDT-SWAP,bitget,1000PEPEUSDT,10000000\n";
        let rows = parse_pairs_csv(text).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].line, 4);
        assert_eq!(rows[0].long.exchange, PerpExchange::Gate);
        assert_eq!(rows[0].long.symbol, Symbol::new("PEPE", "USDT"));
        assert_eq!(rows[0].short.symbol, Symbol::new("1000PEPE", "USDT"));
        assert_eq!(rows[0].short.venue(), Venue::new("binance_futures"));
        assert_eq!(rows[0].quantity, Decimal::from(10_000_000));
        assert_eq!(rows[1].short.symbol, Symbol::new("BTC", "USDT"));
        assert_eq!(rows[1].quantity, d("0.01"));
        assert_eq!(rows[2].long.venue(), Venue::new("okx_futures"));
        assert_eq!(rows[2].long.symbol, Symbol::new("PEPE", "USDT"));
        assert_eq!(rows[2].short.venue(), Venue::new("bitget_futures"));
        assert_eq!(rows[2].short.symbol, Symbol::new("1000PEPE", "USDT"));
    }

    #[test]
    fn csv_rejects_bad_header_fields_and_values() {
        assert!(parse_pairs_csv("").is_err());
        assert!(parse_pairs_csv("a,b,c,d,e\n").is_err());
        let header = "long_venue,long_symbol,short_venue,short_symbol,quantity\n";
        for bad in [
            "gate,PEPE_USDT,binance,1000PEPEUSDT\n",
            "kraken,PEPE_USDT,binance,1000PEPEUSDT,1\n",
            "okx,PEPE_USDT,binance,1000PEPEUSDT,1\n",
            "okx,PEPE-USD-SWAP,binance,1000PEPEUSDT,1\n",
            "gate,PEPE_USDT,bitget,PEPE-USDT-SWAP,1\n",
            "gate,PEPEUSDT,binance,1000PEPEUSDT,1\n",
            "gate,PEPE_USDT,binance,1000PEPE_USDT,1\n",
            "gate,PEPE_USDT,gate,BTC_USDT,1\n",
            "gate,PEPE_USDT,binance,1000PEPEUSDT,0\n",
            "gate,PEPE_USDT,binance,1000PEPEUSDT,abc\n",
        ] {
            let err = parse_pairs_csv(&format!("{header}{bad}")).unwrap_err();
            assert!(err.to_string().contains("line 2"), "{bad}: {err}");
        }
    }
    #[test]
    fn price_diff_pct_uses_mean_as_denominator() {
        assert_eq!(price_diff_pct(d("100"), d("100")), Decimal::ZERO);
        assert_eq!(price_diff_pct(d("95"), d("105")), Decimal::from(10));
        // 乘数配错 1000 倍时远远超过 5%
        assert!(price_diff_pct(d("0.000004"), d("0.004")) > Decimal::from(5));
    }

    #[test]
    fn lot_rule_and_min_common_coins() {
        // Gate PEPE：1 张 = 1e7 币，最少 1 张
        let gate = CoinLotRule::new(Decimal::ONE, Decimal::ONE, &spec(1, 10_000_000));
        // 币安 1000PEPE：步进 1(=1000 币)，最少 1
        let binance = CoinLotRule::new(Decimal::ONE, Decimal::ONE, &spec(1000, 1000));
        assert!(gate.accepts(Decimal::from(20_000_000)));
        assert!(!gate.accepts(Decimal::from(15_000_000)));
        assert!(binance.accepts(Decimal::from(15_000_000)));
        assert!(!binance.accepts(Decimal::from(1500)));
        assert_eq!(min_common_coins(&gate, &binance), Some(Decimal::from(10_000_000)));

        // BTC：币安步进 0.001 最少 0.001；Gate 1 张 = 0.0001 币
        let binance_btc = CoinLotRule::new(d("0.001"), d("0.001"), &spec(1, 1));
        let gate_btc = CoinLotRule::new(Decimal::ONE, Decimal::ONE, &PerpContractSpec {
            price_unit: Decimal::ONE,
            qty_unit: d("0.0001"),
        });
        assert!(binance_btc.accepts(d("0.01")) && gate_btc.accepts(d("0.01")));
        assert!(!binance_btc.accepts(d("0.0105")));
        assert_eq!(min_common_coins(&binance_btc, &gate_btc), Some(d("0.001")));

        // 步进 0.004 和 0.006 的最小公倍数 0.012，最少量 0.02 → 0.024
        let a = CoinLotRule { coin_step: d("0.004"), coin_min: d("0.02") };
        let b = CoinLotRule { coin_step: d("0.006"), coin_min: d("0.006") };
        assert_eq!(min_common_coins(&a, &b), Some(d("0.024")));
    }

    #[test]
    fn live_spreads_use_per_coin_prices_and_require_fresh_quotes() {
        // 多腿 Gate PEPE 每币报价，空腿币安 1000PEPE 报价 = 1000 币的价格
        let long_spec = spec(1, 10_000_000);
        let short_spec = spec(1000, 1000);
        let now = 100_000;
        let long = quote("0.0000039", "0.000004", now - 1_000);
        let short = quote("0.00398", "0.004", now - 2_000);
        let spreads = live_spreads(&long, &short, &long_spec, &short_spec, now, 5_000).unwrap();
        // exec：多 ask 0.000004 vs 空 bid 0.00000398 → (4 − 3.98) / 3.98 × 1e4
        assert_close(spreads.exec_bp, 0.02 / 3.98 * 10_000.0);
        // mid：0.00000395 vs 0.00000399
        assert_close(spreads.mid_bp, -0.04 / 3.99 * 10_000.0);

        let stale = quote("0.00398", "0.004", now - 6_000);
        assert!(live_spreads(&long, &stale, &long_spec, &short_spec, now, 5_000).is_none());
        let empty = quote("0", "0.004", now);
        assert!(live_spreads(&long, &empty, &long_spec, &short_spec, now, 5_000).is_none());
    }

    #[test]
    fn slippage_is_positive_when_fill_is_worse() {
        assert_close(slippage_bp(OrderSide::Buy, 100.0, 100.01), 1.0);
        assert_close(slippage_bp(OrderSide::Buy, 100.0, 99.99), -1.0);
        assert_close(slippage_bp(OrderSide::Sell, 100.0, 99.99), 1.0);
    }

    #[test]
    fn waiting_triggers_on_target_or_expiry() {
        let phase = Phase::Waiting { target_bp: -2.0, deadline_ms: 1_000 };
        assert_eq!(waiting_trigger(phase, -1.9, 999), None);
        assert_eq!(waiting_trigger(phase, -2.0, 999), Some(TriggerKind::Target));
        assert_eq!(waiting_trigger(phase, 5.0, 1_000), Some(TriggerKind::Expiry));
        assert_eq!(waiting_trigger(Phase::AwaitingT0, -10.0, 0), None);
    }

    #[test]
    fn client_order_id_fits_gate_text_limit() {
        let id = new_client_order_id();
        assert!(id.starts_with("cf"));
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric()), "{id}");
        assert!(id.len() <= 28, "{id}");
    }

    // ---- 状态机：直接往策略里塞一行，喂报价/订单事件 ----

    const PAIR: &str = "binance:BTCUSDT|okx:BTC-USDT-SWAP";

    fn table(action: BinAction) -> KStarTable {
        let bins = (0..10)
            .map(|i| {
                let lo = i as f64 * 10.0;
                CalibratedBin {
                    lo,
                    hi: lo + 10.0,
                    stats: Some(BinStats {
                        lo,
                        hi: lo + 10.0,
                        n: 500,
                        segments: 50,
                        b_k: 2.0,
                        b_fill_rate: 0.6,
                        b_expect_bp: 1.5,
                        a_k: 2.0,
                        a_expect_bp: 1.6,
                        platform: Some((1.5, 2.5)),
                    }),
                    coarse_segments: 100,
                    action: action.clone(),
                }
            })
            .collect();
        KStarTable {
            pair: PAIR.to_string(),
            version: 7,
            data_start_ms: 0,
            data_end_ms: 0,
            rules: RuleParams::default(),
            samples: 8736,
            sigma_median_bp: 1.0,
            overall: None,
            bins,
        }
    }

    fn long_venue() -> Venue {
        Venue::new("binance_futures")
    }

    fn short_venue() -> Venue {
        Venue::new("okx_futures")
    }

    fn btc() -> Symbol {
        Symbol::new("BTC", "USDT")
    }

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("cf_test_{name}_{}_{}", std::process::id(), rand::random::<u32>()))
    }

    /// 1 币 BTC，币安多 / OKX 空，最新小时刚收盘，σ = `sigma`(σ 中位数 1)。
    fn strategy(params: CrossFundingParams, action: BinAction, sigma: f64) -> CrossFundingStrategy {
        let strategy = CrossFundingStrategy::new(
            Arc::new(TopicBus::new()),
            Vec::new(),
            HashMap::new(),
            HashMap::new(),
            params,
            None,
        );
        let leg = |venue: Venue| LegState {
            venue,
            symbol: btc(),
            spec: spec(1, 1),
            order_qty: Decimal::ONE,
            quote: None,
            client_order_id: None,
            filled: None,
        };
        let row = RowState {
            label: "#2 test".to_string(),
            line: 2,
            pair: PAIR.to_string(),
            coins: Decimal::ONE,
            long: leg(long_venue()),
            short: leg(short_venue()),
            table: table(action),
            hour: HourState {
                open_time_ms: (now_ms() / HOUR_MS - 1) * HOUR_MS,
                spread_bp: 0.0,
                pct: Some(55.0),
                sigma_bp: Some(sigma),
            },
            phase: Phase::AwaitingT0,
            record: None,
            trigger: None,
        };
        let mut state = strategy.state.lock().unwrap();
        state.quote_index.insert((long_venue(), btc()), vec![(0, LegRole::Long)]);
        state.quote_index.insert((short_venue(), btc()), vec![(0, LegRole::Short)]);
        state.rows.push(row);
        drop(state);
        strategy
    }

    fn params(name: &str) -> CrossFundingParams {
        CrossFundingParams {
            record_file: temp_path(&format!("{name}.jsonl")),
            kill_switch_file: temp_path(&format!("{name}.stop")),
            ..CrossFundingParams::default()
        }
    }

    fn feed(strategy: &CrossFundingStrategy, venue: Venue, bid: &str, ask: &str) {
        strategy.on_quote(&Topic::quote(venue, btc()), &quote(bid, ask, now_ms()));
    }

    /// 立即开的档。
    fn direct() -> BinAction {
        BinAction::DirectOpen { reason: "期望 B ≤ 0".to_string() }
    }

    fn phase(strategy: &CrossFundingStrategy) -> Phase {
        strategy.state.lock().unwrap().rows[0].phase
    }

    fn records(strategy: &CrossFundingStrategy) -> Vec<OrderRecord> {
        let text = std::fs::read_to_string(&strategy.params.record_file).unwrap_or_default();
        let records = text.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        let _ = std::fs::remove_file(&strategy.params.record_file);
        records
    }

    #[test]
    fn on_order_book_records_latest_snapshot_per_leg() {
        let s = strategy(params("order_book"), BinAction::Wait { k_star: 2.0, note: None }, 1.0);

        let book_long = OrderBookLevels { bids: Arc::new(vec![(d("100"), d("1"))]), asks: Arc::new(vec![]), ts_ms: 1 };
        let book_short = OrderBookLevels { bids: Arc::new(vec![]), asks: Arc::new(vec![(d("101"), d("2"))]), ts_ms: 2 };
        s.on_order_book(&Topic::order_book(long_venue(), btc()), &book_long);
        s.on_order_book(&Topic::order_book(short_venue(), btc()), &book_short);

        let latest = s.latest_order_books.lock().unwrap();
        assert_eq!(latest[&(long_venue(), btc())], book_long);
        assert_eq!(latest[&(short_venue(), btc())], book_short);
    }

    #[test]
    fn paper_waits_for_target_and_records_savings() {
        let s = strategy(params("target"), BinAction::Wait { k_star: 2.0, note: None }, 1.0);
        feed(&s, long_venue(), "100", "100.01");
        assert_eq!(phase(&s), Phase::AwaitingT0);
        // 两腿中间价相同 → mid 0bp，可成交 1bp；目标 = 0 − 2 × 1 = −2bp
        feed(&s, short_venue(), "100", "100.01");
        let Phase::Waiting { target_bp, deadline_ms } = phase(&s) else { panic!("{:?}", phase(&s)) };
        assert_close(target_bp, -2.0);
        assert!(deadline_ms > now_ms() + 23 * HOUR_MS);
        // −1bp 还不够
        feed(&s, long_venue(), "99.98", "99.99");
        assert!(matches!(phase(&s), Phase::Waiting { .. }));
        // 多腿 ask 99.97 → −3bp ≤ −2bp 触发，模拟盘直接成交
        feed(&s, long_venue(), "99.96", "99.97");
        assert_eq!(phase(&s), Phase::Done);

        let records = records(&s);
        assert_eq!(records.len(), 2);
        let (decision, result) = (&records[0], &records[1]);
        assert_eq!(decision.event, RecordEvent::Decision);
        assert_eq!(decision.mode, "paper");
        assert_eq!(decision.k_star, Some(2.0));
        assert_eq!(decision.expected_b_bp, 1.5);
        assert_eq!(decision.expected_fill_rate, Some(0.6));
        assert_eq!(decision.bin, Some((50.0, 60.0)));
        assert_close(decision.baseline_bp, 1.0);
        assert_eq!(result.event, RecordEvent::Result);
        assert_eq!(result.outcome.as_deref(), Some("filled"));
        assert_eq!(result.trigger, Some(TriggerKind::Target));
        assert_close(result.actual_bp.unwrap(), -3.0);
        assert_close(result.savings_bp.unwrap(), 4.0);
        assert_close(result.long_slippage_bp.unwrap(), 0.0);
        assert_eq!(result.residual_coins, Some(Decimal::ZERO));
    }

    #[test]
    fn sigma_alarm_skips_row() {
        let s = strategy(params("sigma"), BinAction::Wait { k_star: 2.0, note: None }, 4.0);
        feed(&s, long_venue(), "100", "100.01");
        feed(&s, short_venue(), "100", "100.01");
        assert_eq!(phase(&s), Phase::Skipped);
        // 之后价差再好也不开
        feed(&s, long_venue(), "90", "90.01");
        assert_eq!(phase(&s), Phase::Skipped);
        let records = records(&s);
        assert_eq!(records.len(), 2);
        let (decision, result) = (&records[0], &records[1]);
        assert_eq!(decision.event, RecordEvent::Decision);
        assert!(decision.skip_reason.as_deref().unwrap().contains("σ"), "{decision:?}");
        assert_eq!(decision.immediate_reason, None);
        assert_eq!(result.event, RecordEvent::Result);
        assert!(result.outcome.as_deref().unwrap().starts_with("skipped: σ"), "{result:?}");
        assert_eq!(result.trigger, None);
        assert_eq!(result.long_filled_coins, None);
        assert_eq!(result.savings_bp, None);
    }

    #[test]
    fn stale_kline_skips_row_with_reason() {
        let s = strategy(params("stale"), BinAction::Wait { k_star: 2.0, note: None }, 1.0);
        // 最新有效小时退到 4 小时前开盘：收盘已超过 2 小时
        s.state.lock().unwrap().rows[0].hour.open_time_ms -= 3 * HOUR_MS;
        feed(&s, long_venue(), "100", "100.01");
        feed(&s, short_venue(), "100", "100.01");
        assert_eq!(phase(&s), Phase::Skipped);
        let records = records(&s);
        assert_eq!(records.len(), 2);
        let reason = records[0].skip_reason.as_deref().unwrap();
        assert!(reason.starts_with("K 线不新鲜") && reason.contains("超过上限 2.00 小时"), "{reason}");
        assert_eq!(records[1].outcome.as_deref(), Some(format!("skipped: {reason}").as_str()));
        assert_eq!(records[1].trigger, None);
    }

    #[test]
    fn direct_bin_opens_immediately() {
        let s = strategy(params("direct"), BinAction::DirectOpen { reason: "期望 B ≤ 0".to_string() }, 1.0);
        feed(&s, long_venue(), "100", "100.01");
        feed(&s, short_venue(), "100", "100.01");
        assert_eq!(phase(&s), Phase::Done);
        assert_eq!(records(&s)[1].trigger, Some(TriggerKind::Immediate));
    }

    #[test]
    fn expiry_opens_at_market() {
        let s = strategy(params("expiry"), BinAction::Wait { k_star: 2.0, note: None }, 1.0);
        feed(&s, long_venue(), "100", "100.01");
        feed(&s, short_venue(), "100", "100.01");
        s.state.lock().unwrap().rows[0].phase = Phase::Waiting { target_bp: -2.0, deadline_ms: 0 };
        feed(&s, long_venue(), "100", "100.01");
        assert_eq!(phase(&s), Phase::Done);
        let records = records(&s);
        assert_eq!(records[1].trigger, Some(TriggerKind::Expiry));
    }

    #[test]
    fn kill_switch_pauses_decisions() {
        let p = params("kill");
        std::fs::write(&p.kill_switch_file, "").unwrap();
        let s = strategy(p, direct(), 1.0);
        feed(&s, long_venue(), "100", "100.01");
        feed(&s, short_venue(), "100", "100.01");
        assert_eq!(phase(&s), Phase::AwaitingT0);
        std::fs::remove_file(&s.params.kill_switch_file).unwrap();
        s.kill_switch.lock().unwrap().checked_ms = 0;
        feed(&s, short_venue(), "100", "100.01");
        assert_eq!(phase(&s), Phase::Done);
        records(&s);
    }

    #[test]
    fn notional_limit_is_rechecked_at_trigger() {
        let p = CrossFundingParams { max_notional_usdt: Decimal::from(50), ..params("notional") };
        let s = strategy(p, direct(), 1.0);
        feed(&s, long_venue(), "100", "100.01");
        feed(&s, short_venue(), "100", "100.01");
        assert_eq!(phase(&s), Phase::Halted);
        let records = records(&s);
        assert!(records[1].outcome.as_deref().unwrap().contains("名义"), "{:?}", records[1]);
        assert_eq!(records[1].actual_bp, None);
    }

    #[test]
    fn live_leg_failure_halts_and_unhedged_limit_stops_everything() {
        let p = CrossFundingParams { live: true, ..params("live") };
        let s = strategy(p, direct(), 1.0);
        feed(&s, long_venue(), "100", "100.01");
        feed(&s, short_venue(), "100", "100.01");
        assert!(matches!(phase(&s), Phase::Submitted { .. }));
        let (long_id, short_id) = {
            let state = s.state.lock().unwrap();
            (state.rows[0].long.client_order_id.clone().unwrap(), state.rows[0].short.client_order_id.clone().unwrap())
        };
        s.on_order_event(&OrderEvent::Filled {
            order_id: OrderId::new("o1"),
            client_order_id: Some(long_id),
            filled_qty: Decimal::ONE,
            avg_price: d("100.02"),
        });
        assert!(matches!(phase(&s), Phase::Submitted { .. }));
        assert!(!s.halted.load(Ordering::SeqCst));
        s.on_order_event(&OrderEvent::RejectedByExchange {
            order_id: OrderId::new("o2"),
            client_order_id: Some(short_id),
            reason: "insufficient margin".to_string(),
        });
        assert_eq!(phase(&s), Phase::Halted);
        // 多腿 1 币 × 100 USDT > 上限 50
        assert!(s.halted.load(Ordering::SeqCst));
        let records = records(&s);
        let result = &records[1];
        assert!(result.outcome.as_deref().unwrap().starts_with("halted"), "{result:?}");
        assert_eq!(result.residual_coins, Some(Decimal::ONE));
        assert_close(result.long_slippage_bp.unwrap(), 1.0 / 100.01 * 10_000.0 / 100.0);
    }

    #[test]
    fn live_fills_complete_the_row() {
        let p = CrossFundingParams { live: true, ..params("live_ok") };
        let s = strategy(p, direct(), 1.0);
        feed(&s, long_venue(), "100", "100.01");
        feed(&s, short_venue(), "100", "100.01");
        let (long_id, short_id) = {
            let state = s.state.lock().unwrap();
            (state.rows[0].long.client_order_id.clone().unwrap(), state.rows[0].short.client_order_id.clone().unwrap())
        };
        for (id, price) in [(long_id, "100.01"), (short_id, "100")] {
            s.on_order_event(&OrderEvent::Filled {
                order_id: OrderId::new(id.clone()),
                client_order_id: Some(id),
                filled_qty: Decimal::ONE,
                avg_price: d(price),
            });
        }
        assert_eq!(phase(&s), Phase::Done);
        assert!(!s.halted.load(Ordering::SeqCst));
        let records = records(&s);
        assert_eq!(records[1].mode, "live");
        assert_close(records[1].actual_bp.unwrap(), 1.0);
        assert_close(records[1].savings_bp.unwrap(), 0.0);
    }
}
