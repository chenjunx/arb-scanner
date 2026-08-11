use std::time::Duration;

use rand::Rng;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::types::{MarketEvent, Quote, Symbol, Venue};

use super::{MarketDataSource, now_ms};

/// 单个模拟交易对的初始状态与波动参数。
#[derive(Debug, Clone)]
pub struct MockSymbolConfig {
    pub symbol: Symbol,
    pub initial_mid: Decimal,
    /// 每个 tick 中间价随机漂移的最大幅度（相对中间价的比例，如 0.001 = 0.1%）。
    pub volatility: f64,
    /// 买卖价差（相对中间价的比例，如 0.0005 = 0.05%）。
    pub spread: f64,
}

/// 占位数据源：在真实交易所行情接入之前，用随机游走生成行情，
/// 用于跑通引擎/策略/sink 的完整链路，也便于编写确定性较高的测试。
pub struct MockSource {
    venue: Venue,
    symbols: Vec<MockSymbolConfig>,
    tick_interval: Duration,
}

impl MockSource {
    pub fn new(venue: Venue, symbols: Vec<MockSymbolConfig>, tick_interval: Duration) -> Self {
        Self {
            venue,
            symbols,
            tick_interval,
        }
    }

    fn next_quote(mid: &mut Decimal, cfg: &MockSymbolConfig, ts_ms: u64) -> Quote {
        let mut rng = rand::thread_rng();
        let drift: f64 = rng.gen_range(-cfg.volatility..=cfg.volatility);
        let drift_factor = Decimal::from_f64(1.0 + drift).unwrap_or(Decimal::ONE);
        *mid *= drift_factor;

        let half_spread = Decimal::from_f64(cfg.spread / 2.0).unwrap_or_default();
        let spread_amount = *mid * half_spread;
        Quote {
            bid: *mid - spread_amount,
            bid_size: Decimal::from(1),
            ask: *mid + spread_amount,
            ask_size: Decimal::from(1),
            ts_ms,
        }
    }
}

impl MarketDataSource for MockSource {
    fn venue(&self) -> Venue {
        self.venue.clone()
    }

    fn spawn(self: Box<Self>, tx: mpsc::Sender<MarketEvent>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut mids: Vec<Decimal> =
                self.symbols.iter().map(|cfg| cfg.initial_mid).collect();
            let mut interval = tokio::time::interval(self.tick_interval);
            loop {
                interval.tick().await;
                let ts_ms = now_ms();
                for (idx, cfg) in self.symbols.iter().enumerate() {
                    let quote = Self::next_quote(&mut mids[idx], cfg, ts_ms);
                    let event = MarketEvent {
                        venue: self.venue.clone(),
                        symbol: cfg.symbol.clone(),
                        quote,
                    };
                    if tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
        })
    }
}
