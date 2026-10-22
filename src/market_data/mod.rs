pub mod binance;
pub mod binance_futures;
pub mod bitget_futures;
pub mod cache;
pub mod coinex;
pub mod gate;
pub mod gate_futures;
pub mod kraken;
pub mod link_health;
pub mod mock;
pub mod okx_futures;

use std::sync::Arc;

use async_trait::async_trait;
use rust_decimal::Decimal;
use tokio::task::JoinHandle;

use crate::topic::TopicBus;
use crate::types::{Symbol, Venue};

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// 行情数据源扩展点：每个交易所/场所实现一个 `MarketDataSource`，
/// 将行情发布到 `TopicBus` 上按 (数据类型, venue, symbol) 区分的 topic。
///
/// 接入真实交易所时，只需新增一个实现该 trait 的类型（如 WS 客户端），
/// 在 main.rs 中注册即可，无需改动 engine/strategy 代码。
pub trait MarketDataSource: Send + 'static {
    fn venue(&self) -> Venue;

    /// 消费 self 并在后台任务中运行，持续把行情发布到 bus 上。
    fn spawn(self: Box<Self>, bus: Arc<TopicBus>) -> JoinHandle<()>;
}

pub const HOUR_MS: u64 = 3_600_000;

/// [`KlineProvider::klines`] 支持的 K 线周期。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KlineInterval {
    M5,
    H1,
}

impl KlineInterval {
    pub fn millis(self) -> u64 {
        match self {
            KlineInterval::M5 => 5 * 60_000,
            KlineInterval::H1 => HOUR_MS,
        }
    }
}

/// 一根 K 线里策略用到的字段：开盘时间(毫秒)、收盘价(交易所原始报价，
/// 没有按合约乘数换算)和成交量。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Kline {
    pub open_time_ms: u64,
    pub close: Decimal,
    /// 成交量，单位因交易所而异(币数或张数)，只用来判断这根 K 线有没有成交。
    pub volume: Decimal,
}

/// 历史 K 线 / 24h 成交额的 REST 查询扩展点。和 [`MarketDataSource`] 不同，
/// 这是按需调用的请求/响应接口，不往 bus 上发布任何东西。
#[async_trait]
pub trait KlineProvider: Send + Sync {
    fn venue(&self) -> Venue;

    /// 最近 `limit` 根**已收盘**的 `interval` 周期 K 线，按开盘时间升序。交易所
    /// 返回的最后一根通常是当前还没走完的那根，由实现负责剔除(见 [`closed_klines`])。
    async fn klines(&self, symbol: &Symbol, interval: KlineInterval, limit: usize) -> anyhow::Result<Vec<Kline>>;

    /// 开盘时间落在 `[start_ms, end_ms]` 内的**已收盘** K 线，按开盘时间升序、
    /// 去重。用于拉一年这种远超单次请求上限的历史，实现负责翻页(见
    /// [`closed_klines_in_range`])。
    async fn klines_range(
        &self,
        symbol: &Symbol,
        interval: KlineInterval,
        start_ms: u64,
        end_ms: u64,
    ) -> anyhow::Result<Vec<Kline>>;

    /// 最近 24h 的计价币(USDT)成交额。
    async fn quote_volume_24h(&self, symbol: &Symbol) -> anyhow::Result<Decimal>;
}

/// 按开盘时间升序去重，剔除 `open_time + 周期 > now` 的未收盘 K 线，最后只保留
/// 最近 `limit` 根。实现 [`KlineProvider::klines`] 时向交易所多要 1 根，
/// 再交给这个函数处理。
pub(crate) fn closed_klines(mut klines: Vec<Kline>, interval: KlineInterval, now_ms: u64, limit: usize) -> Vec<Kline> {
    klines.sort_by_key(|k| k.open_time_ms);
    klines.dedup_by_key(|k| k.open_time_ms);
    klines.retain(|k| k.open_time_ms + interval.millis() <= now_ms);
    let skip = klines.len().saturating_sub(limit);
    klines.split_off(skip)
}

/// [`KlineProvider::klines_range`] 的收尾：按开盘时间升序去重，只保留
/// `[start_ms, end_ms]` 内且 `open_time + 周期 <= now` 的已收盘 K 线。
pub(crate) fn closed_klines_in_range(
    mut klines: Vec<Kline>,
    interval: KlineInterval,
    now_ms: u64,
    start_ms: u64,
    end_ms: u64,
) -> Vec<Kline> {
    klines.sort_by_key(|k| k.open_time_ms);
    klines.dedup_by_key(|k| k.open_time_ms);
    klines.retain(|k| k.open_time_ms >= start_ms && k.open_time_ms <= end_ms && k.open_time_ms + interval.millis() <= now_ms);
    klines
}

/// 翻页拉区间 K 线时的页数上限，防止交易所一直返回满页时死循环。按最小的
/// 单页 100 根(OKX history-candles)算，400 页 = 40000 根 1h，够 4 年多。
pub(crate) const RANGE_MAX_PAGES: usize = 400;

#[cfg(test)]
mod tests {
    use super::*;

    fn kline(open_time_ms: u64) -> Kline {
        Kline { open_time_ms, close: Decimal::ONE, volume: Decimal::ONE }
    }

    #[test]
    fn closed_klines_drops_unclosed_bar_and_keeps_latest() {
        let now = 10 * HOUR_MS + 5_000;
        let klines = vec![kline(10 * HOUR_MS), kline(7 * HOUR_MS), kline(9 * HOUR_MS), kline(8 * HOUR_MS), kline(9 * HOUR_MS)];
        let closed = closed_klines(klines, KlineInterval::H1, now, 2);
        assert_eq!(closed, vec![kline(8 * HOUR_MS), kline(9 * HOUR_MS)]);
    }

    #[test]
    fn closed_klines_keeps_bar_closing_exactly_now() {
        let closed = closed_klines(vec![kline(0), kline(HOUR_MS)], KlineInterval::H1, HOUR_MS, 10);
        assert_eq!(closed, vec![kline(0)]);
    }

    #[test]
    fn closed_klines_uses_interval_length() {
        let m5 = KlineInterval::M5.millis();
        let closed = closed_klines(vec![kline(0), kline(m5), kline(2 * m5)], KlineInterval::M5, 2 * m5 + 1, 10);
        assert_eq!(closed, vec![kline(0), kline(m5)]);
    }

    #[test]
    fn closed_klines_in_range_filters_both_ends_and_open_bar() {
        let klines = vec![kline(3 * HOUR_MS), kline(0), kline(HOUR_MS), kline(2 * HOUR_MS), kline(HOUR_MS), kline(4 * HOUR_MS)];
        let closed = closed_klines_in_range(klines, KlineInterval::H1, 4 * HOUR_MS + 1, HOUR_MS, 10 * HOUR_MS);
        assert_eq!(closed, vec![kline(HOUR_MS), kline(2 * HOUR_MS), kline(3 * HOUR_MS)]);
    }
}
