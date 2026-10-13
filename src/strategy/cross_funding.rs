//! 跨所永续价差开仓策略(`cross_funding`)：从 CSV 读入若干"在 A 所做多、在 B 所
//! 做空"的永续合约对，启动时用过去一段时间的 5m K 线收盘价算出价差
//! (`空腿每币价格 − 多腿每币价格`)的 EWMA 均值 μ 和标准差 σ，盘中盯两边
//! bookTicker，一旦 `空腿 bid − 多腿 ask ≥ μ + k·σ` 就两边同时市价开仓。
//!
//! 每一行只开一次仓；任意一腿被拒/超时未成交都只打 error 日志并把这一行停在
//! `Halted`，等人工处理，不做自动补救。持仓模式(单向)、保证金模式、杠杆由用户
//! 事先在交易所设置好，程序不修改。
//!
//! 数量/价格单位：CSV 里的 `quantity` 是实际币数；各所下单量 = 币数 /
//! `PerpContractSpec::qty_unit`，每币价格 = 报价 / `PerpContractSpec::price_unit`。

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, error, info, warn};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use crate::exchange_info::types::{PerpContractSpec, PrecisionKind};
use crate::exchange_info::{ExchangeInfoProvider, PrecisionCache};
use crate::market_data::{Kline, KlineInterval, KlineProvider, now_ms};
use crate::order::OrderProvider;
use crate::order::types::{OrderAmount, OrderSide, OrderStatus};
use crate::order_manager::OrderManager;
use crate::order_manager::types::{OrderEvent, OrderId};
use crate::topic::{Topic, TopicBus};
use crate::types::{Quote, Symbol, Venue};

use super::{Strategy, StrategySubscriber};

pub const STRATEGY_NAME: &str = "cross_funding";
const CSV_HEADER: [&str; 5] = ["long_venue", "long_symbol", "short_venue", "short_symbol", "quantity"];
/// 算价差统计用的 K 线周期，也是 μ/σ 的刷新周期。
pub const KLINE_INTERVAL: KlineInterval = KlineInterval::M5;
/// 每根 K 线收盘后多久去拉新 K 线(给交易所一点时间把上一根收盘)。
const REFRESH_DELAY_AFTER_BAR_MS: u64 = 10_000;
/// 刷新时最后一根还不是刚收盘的那根的话，隔多久重试、最多重试几次。
const REFRESH_RETRY_DELAY: Duration = Duration::from_secs(5);
const REFRESH_MAX_RETRIES: usize = 3;

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
    pub fn venue(&self) -> Venue {
        self.exchange.venue()
    }
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
        let parse_leg = |venue: &str, symbol: &str| -> anyhow::Result<LegSpec> {
            let exchange = PerpExchange::parse(venue)?;
            Ok(LegSpec { exchange, native_symbol: symbol.to_string(), symbol: exchange.parse_symbol(symbol)? })
        };
        let long = parse_leg(fields[0], fields[1]).map_err(|e| anyhow::anyhow!("line {line_no}: {e}"))?;
        let short = parse_leg(fields[2], fields[3]).map_err(|e| anyhow::anyhow!("line {line_no}: {e}"))?;
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

/// 策略运行参数(命令行传入)。
#[derive(Debug, Clone)]
pub struct CrossFundingParams {
    /// 拉多少根 [`KLINE_INTERVAL`] K 线。
    pub kline_bars: usize,
    /// 两边对齐后至少要有多少根才算数。
    pub min_samples: usize,
    /// EWMA 半衰期(根数)。
    pub half_life: f64,
    /// 触发阈值 = μ + `sigma_k`·σ。
    pub sigma_k: f64,
    /// 启动时两边每币价格相差超过这个百分比就跳过这一行(多半是配错对/乘数)。
    pub max_price_diff_pct: Decimal,
    /// 下单后多久两腿还没全部成交就判定异常。
    pub fill_timeout_ms: u64,
    /// 两边报价都必须是最近这么久以内的才参与触发判断。
    pub quote_max_age_ms: u64,
    /// false 时只打日志、不下单。
    pub live: bool,
}

impl Default for CrossFundingParams {
    fn default() -> Self {
        Self {
            // 24h / 16h / 6h 的 5m 根数
            kline_bars: 288,
            min_samples: 192,
            half_life: 72.0,
            sigma_k: 1.0,
            max_price_diff_pct: Decimal::from(5),
            fill_timeout_ms: 30_000,
            quote_max_age_ms: 5_000,
            live: false,
        }
    }
}

/// 价差的 EWMA 统计量，单位是 USDT/币。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpreadStats {
    pub mean: f64,
    pub std: f64,
    pub samples: usize,
    /// 参与计算的最后一根 K 线的开盘时间。
    pub last_open_time_ms: u64,
}

impl SpreadStats {
    pub fn trigger_level(&self, sigma_k: f64) -> f64 {
        self.mean + sigma_k * self.std
    }
}

/// 两边 K 线按开盘时间对齐(任意一边缺的时间点直接丢掉)，返回
/// `(open_time_ms, long_close, short_close)`，按时间升序。
pub fn align_closes(long: &[Kline], short: &[Kline]) -> Vec<(u64, Decimal, Decimal)> {
    let short_by_time: HashMap<u64, Decimal> = short.iter().map(|k| (k.open_time_ms, k.close)).collect();
    let mut aligned: Vec<(u64, Decimal, Decimal)> = long
        .iter()
        .filter_map(|k| short_by_time.get(&k.open_time_ms).map(|s| (k.open_time_ms, k.close, *s)))
        .collect();
    aligned.sort_by_key(|(t, _, _)| *t);
    aligned.dedup_by_key(|(t, _, _)| *t);
    aligned
}

/// 指数加权均值和方差(West 递推)：`alpha = 1 − 0.5^(1/half_life)`，
/// 第一项作为初值。返回 `(mean, std)`；序列为空返回 None。
pub fn ewma_mean_std(series: &[f64], half_life: f64) -> Option<(f64, f64)> {
    let (&first, rest) = series.split_first()?;
    let alpha = 1.0 - 0.5_f64.powf(1.0 / half_life);
    let mut mean = first;
    let mut var = 0.0;
    for &x in rest {
        let diff = x - mean;
        let incr = alpha * diff;
        mean += incr;
        var = (1.0 - alpha) * (var + diff * incr);
    }
    Some((mean, var.max(0.0).sqrt()))
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

/// 由两腿 K 线算价差统计量。同时返回最后一根对齐 K 线两边的每币收盘价
/// `(long, short)`，供启动时的价格合理性检查使用。
pub fn compute_spread_stats(
    long: &[Kline],
    short: &[Kline],
    long_spec: &PerpContractSpec,
    short_spec: &PerpContractSpec,
    min_samples: usize,
    half_life: f64,
) -> anyhow::Result<(SpreadStats, Decimal, Decimal)> {
    let aligned = align_closes(long, short);
    if aligned.len() < min_samples {
        anyhow::bail!(
            "only {} aligned klines (long {}, short {}), need at least {min_samples}",
            aligned.len(),
            long.len(),
            short.len()
        );
    }
    let series: Vec<f64> = aligned
        .iter()
        .map(|(_, l, s)| (per_coin(*s, short_spec) - per_coin(*l, long_spec)).to_f64().unwrap_or(f64::NAN))
        .collect();
    if series.iter().any(|x| !x.is_finite()) {
        anyhow::bail!("spread series contains non-finite values");
    }
    let (mean, std) = ewma_mean_std(&series, half_life).expect("non-empty series");
    let (last_time, last_long, last_short) = *aligned.last().expect("non-empty");
    Ok((
        SpreadStats { mean, std, samples: aligned.len(), last_open_time_ms: last_time },
        per_coin(last_long, long_spec),
        per_coin(last_short, short_spec),
    ))
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

/// 触发判断：两边报价都新鲜、价格为正时，返回当前可成交价差
/// `空腿 bid/price_unit − 多腿 ask/price_unit`(USDT/币)，否则 None。
pub fn executable_spread(
    long_quote: &Quote,
    short_quote: &Quote,
    long_spec: &PerpContractSpec,
    short_spec: &PerpContractSpec,
    now_ms: u64,
    max_age_ms: u64,
) -> Option<f64> {
    let fresh = |q: &Quote| now_ms.saturating_sub(q.ts_ms) <= max_age_ms;
    if !fresh(long_quote) || !fresh(short_quote) || long_quote.ask <= Decimal::ZERO || short_quote.bid <= Decimal::ZERO {
        return None;
    }
    (per_coin(short_quote.bid, short_spec) - per_coin(long_quote.ask, long_spec)).to_f64()
}

/// 日志用：f64 保留 6 位有效数字按十进制打印(避免 PEPE 这种价差打成 1e-9)。
fn fmt_num(x: f64) -> String {
    Decimal::from_f64(x)
        .and_then(|d| d.round_sf(6))
        .map(|d| d.normalize().to_string())
        .unwrap_or_else(|| format!("{x:e}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegRole {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    Watching,
    /// dry run 下已经触发过一次，不再重复打日志。
    DryRunTriggered,
    Submitted { at_ms: u64 },
    Opened,
    Halted,
}

#[derive(Debug, Clone)]
struct LegState {
    venue: Venue,
    symbol: Symbol,
    spec: PerpContractSpec,
    /// 交易所单位的下单量。
    order_qty: Decimal,
    quote: Option<Quote>,
    client_order_id: Option<String>,
    /// `(filled_qty, avg_price)`，交易所单位。
    filled: Option<(Decimal, Decimal)>,
}

#[derive(Debug, Clone)]
struct RowState {
    label: String,
    line: usize,
    coins: Decimal,
    long: LegState,
    short: LegState,
    stats: SpreadStats,
    phase: Phase,
}

impl RowState {
    fn leg_mut(&mut self, role: LegRole) -> &mut LegState {
        match role {
            LegRole::Long => &mut self.long,
            LegRole::Short => &mut self.short,
        }
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

pub struct CrossFundingStrategy {
    bus: Arc<TopicBus>,
    params: CrossFundingParams,
    rows: Vec<PairRow>,
    clients: HashMap<Venue, VenueClients>,
    /// live 模式才有；收到拒单事件时用它查订单的权威状态。
    order_manager: Option<Arc<OrderManager>>,
    state: Arc<Mutex<State>>,
    refresher: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl CrossFundingStrategy {
    pub fn new(
        bus: Arc<TopicBus>,
        rows: Vec<PairRow>,
        clients: HashMap<Venue, VenueClients>,
        params: CrossFundingParams,
        order_manager: Option<Arc<OrderManager>>,
    ) -> Self {
        Self {
            bus,
            params,
            rows,
            clients,
            order_manager,
            state: Arc::new(Mutex::new(State::default())),
            refresher: Mutex::new(None),
        }
    }

    fn clients(&self, venue: &Venue) -> anyhow::Result<&VenueClients> {
        self.clients.get(venue).ok_or_else(|| anyhow::anyhow!("no clients configured for venue {venue}"))
    }

    /// 启动时对一行做完整检查，通过后返回运行时状态。任何一步失败都返回 Err，
    /// 由调用方打日志跳过这一行。
    async fn prepare_row(
        &self,
        row: &PairRow,
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
        let long_qty = row.quantity / long_spec.qty_unit;
        let short_qty = row.quantity / short_spec.qty_unit;

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

        // 3. K 线 + 统计量
        let long_klines =
            long_clients.klines.klines(&row.long.symbol, KLINE_INTERVAL, self.params.kline_bars).await?;
        let short_klines =
            short_clients.klines.klines(&row.short.symbol, KLINE_INTERVAL, self.params.kline_bars).await?;
        let (stats, last_long, last_short) = compute_spread_stats(
            &long_klines,
            &short_klines,
            &long_spec,
            &short_spec,
            self.params.min_samples,
            self.params.half_life,
        )?;

        // 4. 价格合理性检查
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

        // 5. 24h 成交额，只打日志
        let long_volume = long_clients.klines.quote_volume_24h(&row.long.symbol).await;
        let short_volume = short_clients.klines.quote_volume_24h(&row.short.symbol).await;
        let fmt_volume = |v: anyhow::Result<Decimal>| match v {
            Ok(v) => format!("{} USDT", v.round_dp(0)),
            Err(err) => format!("查询失败({err:#})"),
        };

        let trigger = stats.trigger_level(self.params.sigma_k);
        let price = last_long.to_f64().unwrap_or(f64::NAN);
        let notional = row.quantity * last_long;
        info!(
            "cross_funding {}: 通过检查 quantity={} 币(多腿 {} / 空腿 {} 交易所单位, ≈{} USDT) samples={} μ={} σ={} 触发线={} ({} bps) 最新每币价 多={} 空={} 24h成交额 多={} 空={}",
            row.label(),
            row.quantity.normalize(),
            long_qty.normalize(),
            short_qty.normalize(),
            notional.round_dp(2),
            stats.samples,
            fmt_num(stats.mean),
            fmt_num(stats.std),
            fmt_num(trigger),
            fmt_num(trigger / price * 10_000.0),
            last_long.normalize(),
            last_short.normalize(),
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
            coins: row.quantity,
            long: leg(long_venue, &row.long.symbol, long_spec, long_qty),
            short: leg(short_venue, &row.short.symbol, short_spec, short_qty),
            stats,
            phase: Phase::Watching,
        })
    }

    /// 精度不合法时给提示用的 USDT 估值：拿多腿最近一根 K 线收盘价估算，失败返回 None。
    async fn approx_usdt(&self, leg: &LegSpec, spec: &PerpContractSpec, coins: Decimal) -> Option<Decimal> {
        let clients = self.clients.get(&leg.venue())?;
        let klines = clients.klines.klines(&leg.symbol, KLINE_INTERVAL, 1).await.ok()?;
        Some(per_coin(klines.last()?.close, spec) * coins)
    }

    fn spawn_refresher(&self) {
        let state = self.state.clone();
        let klines: HashMap<Venue, Arc<dyn KlineProvider>> =
            self.clients.iter().map(|(v, c)| (v.clone(), c.klines.clone())).collect();
        let params = self.params.clone();
        let handle = tokio::spawn(async move {
            loop {
                let now = now_ms();
                let bar_ms = KLINE_INTERVAL.millis();
                let next = (now / bar_ms + 1) * bar_ms + REFRESH_DELAY_AFTER_BAR_MS;
                tokio::time::sleep(Duration::from_millis(next - now)).await;
                refresh_all(&state, &klines, &params).await;
            }
        });
        *self.refresher.lock().unwrap() = Some(handle);
    }

    /// 处理一条腿的终态失败(拒单/撤单/过期)：先查 OrderManager 里的权威状态，
    /// 确实已全部成交就按成交处理，否则停掉这一行。
    fn handle_leg_failure(&self, order_id: &OrderId, client_order_id: &str, kind: &str, reason: &str) {
        let authoritative = self.order_manager.as_ref().and_then(|om| om.get_order(order_id));
        if let Some(order) = &authoritative
            && order.status == OrderStatus::Filled
        {
            warn!("cross_funding: 订单 {client_order_id} 收到 {kind}({reason})，但权威状态已是 Filled，按成交处理");
            self.record_fill(client_order_id, order.filled_qty, order.avg_price.unwrap_or(Decimal::ZERO));
            return;
        }
        let mut state = self.state.lock().unwrap();
        let Some(&(idx, role)) = state.order_index.get(client_order_id) else {
            return;
        };
        let row = &mut state.rows[idx];
        let status = authoritative
            .as_ref()
            .map(|o| format!("status={:?} filled_qty={}", o.status, o.filled_qty))
            .unwrap_or_else(|| "订单记录不存在".to_string());
        error!(
            "cross_funding {}: {:?} 腿订单 {client_order_id} {kind}: {reason}；权威状态 {status}。这一行停止交易，请人工处理",
            row.label, role
        );
        row.phase = Phase::Halted;
    }

    fn record_fill(&self, client_order_id: &str, filled_qty: Decimal, avg_price: Decimal) {
        let mut state = self.state.lock().unwrap();
        let Some(&(idx, role)) = state.order_index.get(client_order_id) else {
            return;
        };
        let row = &mut state.rows[idx];
        row.leg_mut(role).filled = Some((filled_qty, avg_price));
        info!(
            "cross_funding {}: {:?} 腿 {client_order_id} 成交 qty={} avg_price={}",
            row.label, role, filled_qty.normalize(), avg_price.normalize()
        );
        match row.phase {
            Phase::Submitted { at_ms } => {
                let (Some((long_qty, long_px)), Some((short_qty, short_px))) = (row.long.filled, row.short.filled) else {
                    return;
                };
                let spread = per_coin(short_px, &row.short.spec) - per_coin(long_px, &row.long.spec);
                let long_pc = per_coin(long_px, &row.long.spec);
                let spread_bps = if long_pc > Decimal::ZERO { spread / long_pc * Decimal::from(10_000) } else { Decimal::ZERO };
                info!(
                    "cross_funding {}: 开仓完成，耗时 {}ms，多腿 {}@{} 空腿 {}@{}，成交价差 {} USDT/币 ({} bps)，μ={} 触发线={}，名义 ≈{} USDT",
                    row.label,
                    now_ms().saturating_sub(at_ms),
                    long_qty.normalize(),
                    long_px.normalize(),
                    short_qty.normalize(),
                    short_px.normalize(),
                    spread.normalize(),
                    spread_bps.round_dp(2),
                    fmt_num(row.stats.mean),
                    fmt_num(row.stats.trigger_level(self.params.sigma_k)),
                    (row.coins * long_pc).round_dp(2),
                );
                row.phase = Phase::Opened;
            }
            Phase::Halted => {
                warn!("cross_funding {}: 这一行已停止，{:?} 腿仍然成交了，请人工核对两边持仓", row.label, role);
            }
            _ => {}
        }
    }
}

impl Drop for CrossFundingStrategy {
    fn drop(&mut self) {
        if let Some(handle) = self.refresher.lock().unwrap().take() {
            handle.abort();
        }
    }
}

/// 每根 K 线收盘后刷新：对所有还在 Watching 的行重新拉 K 线、重算 μ/σ。最后
/// 一根不是刚收盘的那根就隔几秒重试，重试用完或请求失败就保留旧值并打 warn。
async fn refresh_all(
    state: &Arc<Mutex<State>>,
    klines: &HashMap<Venue, Arc<dyn KlineProvider>>,
    params: &CrossFundingParams,
) {
    let targets: Vec<(usize, RowState)> = {
        let state = state.lock().unwrap();
        state
            .rows
            .iter()
            .enumerate()
            .filter(|(_, r)| r.phase == Phase::Watching)
            .map(|(i, r)| (i, r.clone()))
            .collect()
    };
    for (idx, row) in targets {
        let bar_ms = KLINE_INTERVAL.millis();
        let expected_last = (now_ms() / bar_ms) * bar_ms - bar_ms;
        let mut result = Err(anyhow::anyhow!("not attempted"));
        for attempt in 0..=REFRESH_MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(REFRESH_RETRY_DELAY).await;
            }
            result = refresh_row(&row, klines, params).await.and_then(|stats| {
                if stats.last_open_time_ms < expected_last {
                    anyhow::bail!("最后一根 K 线开盘时间 {} 还不是刚收盘的那根 {expected_last}", stats.last_open_time_ms)
                }
                Ok(stats)
            });
            if result.is_ok() {
                break;
            }
        }
        match result {
            Ok(stats) => {
                let mut state = state.lock().unwrap();
                let row = &mut state.rows[idx];
                if row.phase != Phase::Watching {
                    continue;
                }
                info!(
                    "cross_funding {}: 定时更新 samples={} μ={} → {} σ={} → {} 触发线={}",
                    row.label,
                    stats.samples,
                    fmt_num(row.stats.mean),
                    fmt_num(stats.mean),
                    fmt_num(row.stats.std),
                    fmt_num(stats.std),
                    fmt_num(stats.trigger_level(params.sigma_k)),
                );
                row.stats = stats;
            }
            Err(err) => warn!("cross_funding {}: 定时更新失败，沿用旧的 μ/σ: {err:#}", row.label),
        }
    }
}

async fn refresh_row(
    row: &RowState,
    klines: &HashMap<Venue, Arc<dyn KlineProvider>>,
    params: &CrossFundingParams,
) -> anyhow::Result<SpreadStats> {
    let provider = |venue: &Venue| klines.get(venue).ok_or_else(|| anyhow::anyhow!("no kline provider for {venue}"));
    let long = provider(&row.long.venue)?.klines(&row.long.symbol, KLINE_INTERVAL, params.kline_bars).await?;
    let short = provider(&row.short.venue)?.klines(&row.short.symbol, KLINE_INTERVAL, params.kline_bars).await?;
    let (stats, _, _) =
        compute_spread_stats(&long, &short, &row.long.spec, &row.short.spec, params.min_samples, params.half_life)?;
    Ok(stats)
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
        let mut precisions = HashMap::new();
        for (venue, clients) in &self.clients {
            match PrecisionCache::load_perpetual(clients.exchange_info.as_ref()).await {
                Ok(cache) => {
                    precisions.insert(venue.clone(), cache);
                }
                Err(err) => warn!("cross_funding: 加载 {venue} 永续下单精度失败，相关行都会跳过: {err:#}"),
            }
        }

        let mut prepared = Vec::new();
        for row in &self.rows {
            match self.prepare_row(row, &precisions).await {
                Ok(state) => prepared.push(state),
                Err(err) => warn!("cross_funding {}: 跳过: {err:#}", row.label()),
            }
        }
        if prepared.is_empty() {
            anyhow::bail!("cross_funding: CSV 里 {} 行全部未通过启动检查，没有可交易的行", self.rows.len());
        }
        info!(
            "cross_funding: {}/{} 行通过检查，live={}，开始监控",
            prepared.len(),
            self.rows.len(),
            self.params.live
        );

        {
            let mut state = self.state.lock().unwrap();
            for (idx, row) in prepared.into_iter().enumerate() {
                for (role, leg) in [(LegRole::Long, &row.long), (LegRole::Short, &row.short)] {
                    state.quote_index.entry((leg.venue.clone(), leg.symbol.clone())).or_default().push((idx, role));
                    subscriber.subscribe_quote(Topic::quote(leg.venue.clone(), leg.symbol.clone()));
                }
                state.rows.push(row);
            }
        }
        self.spawn_refresher();
        Ok(())
    }

    fn on_quote(&self, topic: &Topic, quote: &Quote) {
        let Topic::Quote { venue, symbol } = topic else {
            return;
        };
        let now = now_ms();
        let mut submits = Vec::new();
        {
            let mut guard = self.state.lock().unwrap();
            let state = &mut *guard;

            for row in state.rows.iter_mut() {
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
                    row.phase = Phase::Halted;
                }
            }

            let Some(targets) = state.quote_index.get(&(venue.clone(), symbol.clone())) else {
                return;
            };
            for &(idx, role) in targets {
                let row = &mut state.rows[idx];
                row.leg_mut(role).quote = Some(*quote);
                if row.phase != Phase::Watching {
                    continue;
                }
                let (Some(long_quote), Some(short_quote)) = (row.long.quote, row.short.quote) else {
                    continue;
                };
                let Some(spread) = executable_spread(
                    &long_quote,
                    &short_quote,
                    &row.long.spec,
                    &row.short.spec,
                    now,
                    self.params.quote_max_age_ms,
                ) else {
                    continue;
                };
                let trigger = row.stats.trigger_level(self.params.sigma_k);
                if spread < trigger {
                    continue;
                }
                info!(
                    "cross_funding {}: 触发 可成交价差={} ≥ 触发线={} (μ={} σ={})，多腿 ask={} 空腿 bid={}",
                    row.label,
                    fmt_num(spread),
                    fmt_num(trigger),
                    fmt_num(row.stats.mean),
                    fmt_num(row.stats.std),
                    long_quote.ask,
                    short_quote.bid
                );
                if !self.params.live {
                    info!(
                        "cross_funding {}: [dry-run] 本应市价 买入 {} {} / 卖出 {} {}，不下单",
                        row.label,
                        row.long.order_qty.normalize(),
                        row.long.symbol,
                        row.short.order_qty.normalize(),
                        row.short.symbol
                    );
                    row.phase = Phase::DryRunTriggered;
                    continue;
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
                for (leg, side, id) in
                    [(&row.long, OrderSide::Buy, long_id.clone()), (&row.short, OrderSide::Sell, short_id.clone())]
                {
                    submits.push(PendingSubmit {
                        venue: leg.venue.clone(),
                        symbol: leg.symbol.clone(),
                        side,
                        qty: leg.order_qty,
                        client_order_id: id,
                        group_id: group_id.clone(),
                    });
                }
                state.order_index.insert(long_id, (idx, LegRole::Long));
                state.order_index.insert(short_id, (idx, LegRole::Short));
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
    use crate::market_data::HOUR_MS;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn spec(price_unit: i64, qty_unit: i64) -> PerpContractSpec {
        PerpContractSpec { price_unit: Decimal::from(price_unit), qty_unit: Decimal::from(qty_unit) }
    }

    fn kline(hour: u64, close: &str) -> Kline {
        Kline { open_time_ms: hour * HOUR_MS, close: d(close) }
    }

    fn quote(bid: &str, ask: &str, ts_ms: u64) -> Quote {
        Quote { bid: d(bid), bid_size: Decimal::ONE, ask: d(ask), ask_size: Decimal::ONE, ts_ms }
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
    fn align_closes_drops_missing_timestamps() {
        let long = vec![kline(1, "1"), kline(2, "2"), kline(4, "4")];
        let short = vec![kline(2, "20"), kline(3, "30"), kline(4, "40")];
        let aligned = align_closes(&long, &short);
        assert_eq!(aligned, vec![(2 * HOUR_MS, d("2"), d("20")), (4 * HOUR_MS, d("4"), d("40"))]);
    }

    #[test]
    fn ewma_of_constant_series_has_zero_std() {
        let (mean, std) = ewma_mean_std(&[3.0; 10], 36.0).unwrap();
        assert!((mean - 3.0).abs() < 1e-12);
        assert!(std.abs() < 1e-12);
        assert!(ewma_mean_std(&[], 36.0).is_none());
    }

    #[test]
    fn ewma_weights_recent_values_more() {
        // 半衰期 1：alpha = 0.5
        let (mean, std) = ewma_mean_std(&[0.0, 2.0], 1.0).unwrap();
        assert!((mean - 1.0).abs() < 1e-12);
        // var = (1-0.5) * (0 + 2 * 1) = 1
        assert!((std - 1.0).abs() < 1e-12);
        let (mean, _) = ewma_mean_std(&[0.0, 0.0, 0.0, 10.0], 1.0).unwrap();
        assert!((mean - 5.0).abs() < 1e-12);
    }

    #[test]
    fn spread_stats_normalize_by_price_unit() {
        // 多腿 Gate PEPE 每币报价，空腿币安 1000PEPE 报价 = 1000 币的价格
        let long: Vec<Kline> = (0..5).map(|h| kline(h, "0.000004")).collect();
        let short: Vec<Kline> = (0..5).map(|h| kline(h, "0.004010")).collect();
        let (stats, last_long, last_short) =
            compute_spread_stats(&long, &short, &spec(1, 10_000_000), &spec(1000, 1000), 5, 36.0).unwrap();
        assert_eq!(stats.samples, 5);
        assert_eq!(stats.last_open_time_ms, 4 * HOUR_MS);
        assert!((stats.mean - 0.00000001).abs() < 1e-15);
        assert_eq!(last_long, d("0.000004"));
        assert_eq!(last_short, d("0.00000401"));
        assert!(compute_spread_stats(&long, &short, &spec(1, 1), &spec(1000, 1000), 6, 36.0).is_err());
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
    fn executable_spread_requires_fresh_positive_quotes() {
        let long_spec = spec(1, 10_000_000);
        let short_spec = spec(1000, 1000);
        let now = 100_000;
        let long = quote("0.0000039", "0.000004", now - 1_000);
        let short = quote("0.00402", "0.00403", now - 2_000);
        let spread = executable_spread(&long, &short, &long_spec, &short_spec, now, 5_000).unwrap();
        assert!((spread - 0.00000002).abs() < 1e-15);

        let stale = quote("0.00402", "0.00403", now - 6_000);
        assert!(executable_spread(&long, &stale, &long_spec, &short_spec, now, 5_000).is_none());
        let empty = quote("0", "0.00403", now);
        assert!(executable_spread(&long, &empty, &long_spec, &short_spec, now, 5_000).is_none());
    }

    #[test]
    fn trigger_level_is_mean_plus_k_sigma() {
        let stats = SpreadStats { mean: 1.0, std: 0.5, samples: 48, last_open_time_ms: 0 };
        assert_eq!(stats.trigger_level(1.0), 1.5);
        assert_eq!(stats.trigger_level(2.0), 2.0);
    }

    #[test]
    fn client_order_id_fits_gate_text_limit() {
        let id = new_client_order_id();
        assert!(id.starts_with("cf"));
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric()), "{id}");
        assert!(id.len() <= 28, "{id}");
    }
}
