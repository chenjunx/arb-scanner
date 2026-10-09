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

/// 一根 K 线里策略用到的字段：开盘时间(毫秒)和收盘价(交易所原始报价，
/// 没有按合约乘数换算)。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Kline {
    pub open_time_ms: u64,
    pub close: Decimal,
}

/// 历史 K 线 / 24h 成交额的 REST 查询扩展点。和 [`MarketDataSource`] 不同，
/// 这是按需调用的请求/响应接口，不往 bus 上发布任何东西。
#[async_trait]
pub trait KlineProvider: Send + Sync {
    fn venue(&self) -> Venue;

    /// 最近 `limit` 根**已收盘**的 1h K 线，按开盘时间升序。交易所返回的最后一根
    /// 通常是当前还没走完的小时，由实现负责剔除(见 [`closed_hourly_klines`])。
    async fn hourly_klines(&self, symbol: &Symbol, limit: usize) -> anyhow::Result<Vec<Kline>>;

    /// 最近 24h 的计价币(USDT)成交额。
    async fn quote_volume_24h(&self, symbol: &Symbol) -> anyhow::Result<Decimal>;
}

/// 按开盘时间升序去重，剔除 `open_time + 1h > now` 的未收盘 K 线，最后只保留
/// 最近 `limit` 根。实现 [`KlineProvider::hourly_klines`] 时向交易所多要 1 根，
/// 再交给这个函数处理。
pub(crate) fn closed_hourly_klines(mut klines: Vec<Kline>, now_ms: u64, limit: usize) -> Vec<Kline> {
    klines.sort_by_key(|k| k.open_time_ms);
    klines.dedup_by_key(|k| k.open_time_ms);
    klines.retain(|k| k.open_time_ms + HOUR_MS <= now_ms);
    let skip = klines.len().saturating_sub(limit);
    klines.split_off(skip)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kline(open_time_ms: u64) -> Kline {
        Kline { open_time_ms, close: Decimal::ONE }
    }

    #[test]
    fn closed_hourly_klines_drops_unclosed_bar_and_keeps_latest() {
        let now = 10 * HOUR_MS + 5_000;
        let klines = vec![kline(10 * HOUR_MS), kline(7 * HOUR_MS), kline(9 * HOUR_MS), kline(8 * HOUR_MS), kline(9 * HOUR_MS)];
        let closed = closed_hourly_klines(klines, now, 2);
        assert_eq!(closed, vec![kline(8 * HOUR_MS), kline(9 * HOUR_MS)]);
    }

    #[test]
    fn closed_hourly_klines_keeps_bar_closing_exactly_now() {
        let closed = closed_hourly_klines(vec![kline(0), kline(HOUR_MS)], HOUR_MS, 10);
        assert_eq!(closed, vec![kline(0)]);
    }
}
