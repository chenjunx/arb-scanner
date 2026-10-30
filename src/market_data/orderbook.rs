use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::sync::{Arc, Mutex};

use rust_decimal::Decimal;

use crate::types::{Quote, Symbol, Venue};

/// 订单簿观测日志里每侧打印的档位数。
pub const ORDER_BOOK_LOG_DEPTH: usize = 5;
/// 同一个 (venue, symbol) 两次订单簿观测日志之间的最小间隔。
pub const ORDER_BOOK_LOG_INTERVAL_MS: u64 = 30_000;

/// 按 (venue, symbol) 节流：每个 key 在 `interval_ms` 内最多放行一次。
pub struct OrderBookLogThrottle {
    interval_ms: u64,
    last_ms: Mutex<HashMap<(Venue, Symbol), u64>>,
}

impl OrderBookLogThrottle {
    pub fn new(interval_ms: u64) -> Self {
        Self { interval_ms, last_ms: Mutex::new(HashMap::new()) }
    }

    pub fn should_log(&self, venue: &Venue, symbol: &Symbol, now_ms: u64) -> bool {
        let mut last_ms = self.last_ms.lock().unwrap();
        match last_ms.get(&(venue.clone(), symbol.clone())) {
            Some(&last) if now_ms.saturating_sub(last) < self.interval_ms => false,
            _ => {
                last_ms.insert((venue.clone(), symbol.clone()), now_ms);
                true
            }
        }
    }
}

/// 一行订单簿观测日志：前 `depth` 档 + 同一 (venue, symbol) 最新 `Quote`，
/// 以及两条解析路径的买一/卖一是否一致(两条流推送时刻不同，偶发不一致属正常)。
pub fn describe_order_book(book: &OrderBookLevels, quote: Option<&Quote>, depth: usize) -> String {
    let mut out = String::new();
    let levels = |out: &mut String, side: &[(Decimal, Decimal)]| {
        out.push('[');
        for (i, (price, qty)) in side.iter().take(depth).enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = write!(out, "{price}x{qty}");
        }
        out.push(']');
    };
    let _ = write!(out, "levels={}/{} bids=", book.bids.len(), book.asks.len());
    levels(&mut out, &book.bids);
    out.push_str(" asks=");
    levels(&mut out, &book.asks);
    let _ = write!(out, " book_ts={}", book.ts_ms);
    match quote {
        Some(quote) => {
            let top_match = book.bids.first().map(|l| l.0) == Some(quote.bid)
                && book.asks.first().map(|l| l.0) == Some(quote.ask);
            let _ = write!(
                out,
                " | quote bid={} ask={} quote_ts={} top_match={top_match}",
                quote.bid, quote.ask, quote.ts_ms
            );
        }
        None => out.push_str(" | quote=none"),
    }
    out
}

/// 某个 (venue, symbol) 上的多档订单簿快照：bids 按价格降序、asks 按价格升序。
/// 用 `Arc<Vec<_>>` 包装档位数据，让 bus 的多订阅者 `Clone` 时只增加引用计数，
/// 不深拷贝整本订单簿。
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBookLevels {
    pub bids: Arc<Vec<(Decimal, Decimal)>>,
    pub asks: Arc<Vec<(Decimal, Decimal)>>,
    pub ts_ms: u64,
}

/// 单个 symbol 的本地订单簿：按价格维护 price -> qty，由各交易所的 WS 任务在收到
/// 快照/增量帧后调用 `apply_bid`/`apply_ask` 合并，再用 `snapshot` 导出成
/// [`OrderBookLevels`] 发布到 bus。纯内存结构，不碰网络，方便脱离真实连接单测。
#[derive(Debug, Default)]
pub struct LocalOrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl LocalOrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    /// 清空本地簿，重新同步前(比如校验和失败、定期强制重同步)调用。
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// 合并一条 bid 档位：`qty` 为 0 表示该价位已被吃空/撤单，从簿里删除。
    pub fn apply_bid(&mut self, price: Decimal, qty: Decimal) {
        if qty.is_zero() {
            self.bids.remove(&price);
        } else {
            self.bids.insert(price, qty);
        }
    }

    /// 合并一条 ask 档位：`qty` 为 0 表示该价位已被吃空/撤单，从簿里删除。
    pub fn apply_ask(&mut self, price: Decimal, qty: Decimal) {
        if qty.is_zero() {
            self.asks.remove(&price);
        } else {
            self.asks.insert(price, qty);
        }
    }

    /// 导出当前本地簿的快照：bids 按价格降序、asks 按价格升序。
    pub fn snapshot(&self, ts_ms: u64) -> OrderBookLevels {
        OrderBookLevels {
            bids: Arc::new(self.bids.iter().rev().map(|(p, q)| (*p, *q)).collect()),
            asks: Arc::new(self.asks.iter().map(|(p, q)| (*p, *q)).collect()),
            ts_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn apply_bid_inserts_and_removes_by_zero_qty() {
        let mut book = LocalOrderBook::new();
        book.apply_bid(d("100"), d("1"));
        book.apply_bid(d("99"), d("2"));
        assert_eq!(book.snapshot(0).bids.as_ref(), &vec![(d("100"), d("1")), (d("99"), d("2"))]);

        book.apply_bid(d("100"), d("0"));
        assert_eq!(book.snapshot(0).bids.as_ref(), &vec![(d("99"), d("2"))]);
    }

    #[test]
    fn apply_ask_inserts_and_removes_by_zero_qty() {
        let mut book = LocalOrderBook::new();
        book.apply_ask(d("101"), d("1"));
        book.apply_ask(d("102"), d("2"));
        assert_eq!(book.snapshot(0).asks.as_ref(), &vec![(d("101"), d("1")), (d("102"), d("2"))]);

        book.apply_ask(d("101"), d("0"));
        assert_eq!(book.snapshot(0).asks.as_ref(), &vec![(d("102"), d("2"))]);
    }

    #[test]
    fn bids_sorted_descending_asks_ascending_regardless_of_insert_order() {
        let mut book = LocalOrderBook::new();
        for p in ["99", "101", "100"] {
            book.apply_bid(d(p), d("1"));
            book.apply_ask(d(p), d("1"));
        }
        let snap = book.snapshot(123);
        assert_eq!(
            snap.bids.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
            vec![d("101"), d("100"), d("99")]
        );
        assert_eq!(
            snap.asks.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
            vec![d("99"), d("100"), d("101")]
        );
        assert_eq!(snap.ts_ms, 123);
    }

    #[test]
    fn throttle_allows_once_per_interval_per_key() {
        let throttle = OrderBookLogThrottle::new(1000);
        let (a, b, btc) = (Venue::new("a"), Venue::new("b"), Symbol::new("BTC", "USDT"));
        assert!(throttle.should_log(&a, &btc, 10_000));
        assert!(!throttle.should_log(&a, &btc, 10_999));
        assert!(throttle.should_log(&b, &btc, 10_999));
        assert!(throttle.should_log(&a, &btc, 11_000));
    }

    #[test]
    fn describes_top_levels_and_compares_with_quote() {
        let book = OrderBookLevels {
            bids: Arc::new(vec![(d("100"), d("1")), (d("99"), d("2")), (d("98"), d("3"))]),
            asks: Arc::new(vec![(d("101"), d("4"))]),
            ts_ms: 7,
        };
        let quote = Quote { bid: d("100"), bid_size: d("1"), ask: d("101"), ask_size: d("4"), ts_ms: 8 };
        assert_eq!(
            describe_order_book(&book, Some(&quote), 2),
            "levels=3/1 bids=[100x1, 99x2] asks=[101x4] book_ts=7 | quote bid=100 ask=101 quote_ts=8 top_match=true"
        );
        let stale = Quote { bid: d("99.5"), ..quote };
        assert!(describe_order_book(&book, Some(&stale), 2).ends_with("top_match=false"));
        assert!(describe_order_book(&book, None, 2).ends_with("quote=none"));
    }

    #[test]
    fn clear_empties_both_sides() {
        let mut book = LocalOrderBook::new();
        book.apply_bid(d("1"), d("1"));
        book.apply_ask(d("2"), d("1"));
        book.clear();
        assert!(book.is_empty());
        let snap = book.snapshot(0);
        assert!(snap.bids.is_empty());
        assert!(snap.asks.is_empty());
    }
}
