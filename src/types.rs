use std::sync::Arc;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// 交易所/交易场所标识，如 "binance"、"okx"。用字符串封装而非枚举，
/// 便于在不改动核心代码的情况下接入任意新的场所。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Venue(pub Arc<str>);

impl Venue {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 交易对，如 base=BTC quote=USDT。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Symbol {
    pub base: Arc<str>,
    pub quote: Arc<str>,
}

impl Symbol {
    pub fn new(base: impl Into<Arc<str>>, quote: impl Into<Arc<str>>) -> Self {
        Self {
            base: base.into(),
            quote: quote.into(),
        }
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

/// 某个 (venue, symbol) 上的最优买卖一档快照。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quote {
    pub bid: Decimal,
    pub bid_size: Decimal,
    pub ask: Decimal,
    pub ask_size: Decimal,
    pub ts_ms: u64,
}

