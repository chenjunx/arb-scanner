use std::collections::BTreeMap;
use std::sync::Arc;

use rust_decimal::Decimal;

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
