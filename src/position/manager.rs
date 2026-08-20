use std::sync::{Arc, Mutex};

use log::info;
use rust_decimal::prelude::Signed;
use rust_decimal::Decimal;

use crate::order::types::OrderSide;
use crate::types::{Symbol, Venue};

use super::adjustment_log::{AdjustmentLog, AdjustmentRecord, InMemoryAdjustmentLog};
use super::store::PositionStore;
use super::types::{AdjustmentReason, AssetExposure, FillOutcome, VenuePosition};

/// 跨交易所/跨产品的统一持仓视图。写入口是 `on_filled` (由 `RiskEngine`/
/// `OrderManager` 在订单成交后调用)，查询接口按 `(venue, symbol)` 或按
/// base 资产聚合两种粒度提供。见 `docs/position_manager_design.md`。
pub struct PositionManager {
    store: Arc<dyn PositionStore>,
    adjustment_log: Arc<dyn AdjustmentLog>,
}

impl PositionManager {
    pub fn new(store: Arc<dyn PositionStore>) -> Self {
        Self { store, adjustment_log: Arc::new(InMemoryAdjustmentLog::new()) }
    }

    /// 接入生产用的审计日志实现(如 `RedisAdjustmentLog`)，默认是纯内存、
    /// 重启即丢的 `InMemoryAdjustmentLog`——绝大多数调用点(测试代码、
    /// Redis-free 的 dry-run 路径)不需要关心这个，只有唯一的生产接线点
    /// (`main.rs::build_portfolio_stack`)需要显式调用。
    pub fn with_adjustment_log(mut self, log: Arc<dyn AdjustmentLog>) -> Self {
        self.adjustment_log = log;
        self
    }

    /// 订单成交后调用，按增量成交量 (不是累计值) + 本次成交价更新净仓位和
    /// 加权平均建仓价，把这笔成交对旧仓位实现的盈亏累加进
    /// `VenuePosition::realized_pnl`（唯一账本，`PortfolioManager` 直接读这里），
    /// 同时通过 `FillOutcome` 把同一笔盈亏带出去供调用方(如 `OrderManager`)
    /// 使用。`fill_price` 为 `None` 时 (极少数场景下交易所推送没带价格) 只更新
    /// 数量，均价保持不变，也不计已实现盈亏。
    ///
    /// 已实现盈亏必须在 `store.update` 的原子闭包内部算，不能由调用方在调用
    /// 前后各查一次仓位自己算——`PositionStore::update` 的读改写原子性正是为了
    /// 防止并发成交推送互相覆盖，闭包外" before/after "两次查询之间可能被其它
    /// 并发成交插入，导致算出错误的已实现盈亏。
    pub fn on_filled(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        side: OrderSide,
        filled_qty_delta: Decimal,
        fill_price: Option<Decimal>,
        fee: Option<Decimal>,
        fee_asset: Option<String>,
        fee_usdt: Option<Decimal>,
        ts_ms: u64,
    ) -> FillOutcome {
        let venue_for_closure = venue.clone();
        let symbol_for_closure = symbol.clone();
        let fee_asset_for_closure = fee_asset.clone();
        let realized_pnl_slot = Arc::new(Mutex::new(Decimal::ZERO));
        let slot = realized_pnl_slot.clone();

        self.store.update(
            venue,
            symbol,
            Box::new(move |current: Option<VenuePosition>| {
                let mut pos =
                    current.unwrap_or_else(|| VenuePosition::flat(venue_for_closure.clone(), symbol_for_closure.clone()));

                let signed_delta = match side {
                    OrderSide::Buy => filled_qty_delta,
                    OrderSide::Sell => -filled_qty_delta,
                };
                let old_qty = pos.net_qty;
                let old_avg = pos.avg_price;
                let new_qty = old_qty + signed_delta;

                // 已实现盈亏：只有和现有仓位方向相反的成交（减仓/穿零反向）才会
                // 实现盈亏；同方向加仓或从 0 建仓恒为 0。closed_qty 是这笔成交里
                // "用来平掉旧仓位"的部分，穿零时超出 old_qty 的部分是按新方向
                // 重新建仓，不计入。
                if let Some(price) = fill_price {
                    if !old_qty.is_zero() && old_qty.signum() != signed_delta.signum() {
                        if let Some(avg) = old_avg {
                            let closed_qty = signed_delta.abs().min(old_qty.abs());
                            let realized = closed_qty * (price - avg) * old_qty.signum();
                            *slot.lock().unwrap() = realized;
                            pos.realized_pnl += realized;
                        }
                    }
                }

                pos.avg_price = match (pos.avg_price, fill_price) {
                    (_, None) => pos.avg_price,
                    (None, Some(price)) => Some(price),
                    (Some(avg), Some(price)) => {
                        if new_qty.is_zero() {
                            None
                        } else if old_qty.signum() == new_qty.signum() && new_qty.abs() >= old_qty.abs() {
                            // 加仓 (含从 0 建仓时 signum 相等的边界情况，已被上面
                            // (None, Some) 分支拦掉): 按新增部分做加权平均。
                            Some((avg * old_qty.abs() + price * filled_qty_delta) / new_qty.abs())
                        } else if old_qty.signum() == new_qty.signum() {
                            // 减仓但未穿零，均价不变。
                            Some(avg)
                        } else {
                            // 穿零反向，新方向以本次成交价重新建仓。
                            Some(price)
                        }
                    }
                };

                // 累加手续费（原始币种，独立于 realized_pnl）
                if let (Some(fee_amt), Some(asset)) = (fee, fee_asset_for_closure.as_ref()) {
                    *pos.total_fees.entry(asset.clone()).or_insert(Decimal::ZERO) += fee_amt;
                }

                pos.net_qty = new_qty;
                pos.updated_at_ms = ts_ms;
                pos
            }),
        );

        // 手续费的 USDT 等值若在这次成交里就能同步解出来，走 `apply_adjustment`
        // 冲减 realized_pnl（手续费是成本，取负）；异步才能解出的情形（如
        // BNB/KFEE 需要查价）由调用方在查到价格后另外调 `apply_adjustment`。
        // 不管同步还是异步，最终都收敛到同一个方法，审计记录也统一由它写。
        if let Some(usdt) = fee_usdt {
            self.apply_adjustment(venue, symbol, -usdt, AdjustmentReason::FeeUsdt, ts_ms);
        }

        FillOutcome {
            realized_pnl: *realized_pnl_slot.lock().unwrap(),
            fee: match (fee, fee_asset) {
                (Some(amt), Some(asset)) => Some((amt, asset)),
                _ => None,
            },
            fee_usdt,
        }
    }

    /// 记录一笔非成交导致的已实现盈亏调整（资金费结算、手续费换算成 USDT 后
    /// 冲减盈亏、人工修正等）。直接累加到 `VenuePosition::realized_pnl`，不碰
    /// net_qty/avg_price；符号由调用方决定（例如手续费传负数），这里只管做加法。
    ///
    /// 和 `on_filled` 一样，加法必须写在 `store.update` 传入闭包的内部（用闭包参数
    /// `current`，而不是在调用 `apply_adjustment` 之前由外部先查一次 `realized_pnl`
    /// 自己算好新值再传进来）——闭包内部的读改写是原子的一步，闭包外部"先查后传"
    /// 则会在并发的另一次 `on_filled`/`apply_adjustment`（同一 venue+symbol，例如
    /// 两笔手续费换算结果前后脚回调）之间留出竞态窗口，导致其中一次更新被覆盖丢失。
    pub fn apply_adjustment(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        amount: Decimal,
        reason: AdjustmentReason,
        ts_ms: u64,
    ) {
        info!("position: non-fill realized_pnl adjustment venue={venue} symbol={symbol} amount={amount} reason={reason:?}");
        let venue_for_closure = venue.clone();
        let symbol_for_closure = symbol.clone();
        let before_after_slot = Arc::new(Mutex::new((Decimal::ZERO, Decimal::ZERO)));
        let slot = before_after_slot.clone();
        self.store.update(
            venue,
            symbol,
            Box::new(move |current: Option<VenuePosition>| {
                let mut pos =
                    current.unwrap_or_else(|| VenuePosition::flat(venue_for_closure.clone(), symbol_for_closure.clone()));
                let before = pos.realized_pnl;
                pos.realized_pnl += amount;
                *slot.lock().unwrap() = (before, pos.realized_pnl);
                pos.updated_at_ms = ts_ms;
                pos
            }),
        );

        let (realized_pnl_before, realized_pnl_after) = *before_after_slot.lock().unwrap();
        self.adjustment_log.record(AdjustmentRecord {
            venue: venue.clone(),
            symbol: symbol.clone(),
            amount,
            reason,
            realized_pnl_before,
            realized_pnl_after,
            ts_ms,
        });
    }

    /// 单个 venue+symbol 的净数量 (正=多头，负=空头)。
    pub fn position(&self, venue: &Venue, symbol: &Symbol) -> Decimal {
        self.store.get(venue, symbol).map(|p| p.net_qty).unwrap_or(Decimal::ZERO)
    }

    /// 单个 venue+symbol 的完整快照 (含均价)。
    pub fn venue_position(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePosition> {
        self.store.get(venue, symbol)
    }

    /// 全量仓位，供监控/调试使用。
    pub fn all_positions(&self) -> Vec<VenuePosition> {
        self.store.all()
    }

    /// 按 base 资产聚合所有 venue/产品上的净敞口 (如 BTC 现货多头 + 合约空头
    /// 自动相抵，见 `docs/position_manager_design.md` "核心设计"一节)。
    pub fn asset_exposure(&self, asset: &str) -> AssetExposure {
        let venues: Vec<VenuePosition> = self
            .store
            .all()
            .into_iter()
            .filter(|p| p.symbol.base.as_ref().eq_ignore_ascii_case(asset))
            .collect();
        let net_qty = venues.iter().map(|p| p.net_qty).sum();
        AssetExposure {
            asset: asset.to_string(),
            net_qty,
            venues,
        }
    }

    /// 汇总所有持仓的手续费，按币种分组
    pub fn total_fees_by_asset(&self) -> std::collections::HashMap<String, Decimal> {
        let mut totals = std::collections::HashMap::new();
        for pos in self.store.all() {
            for (asset, amount) in pos.total_fees {
                *totals.entry(asset).or_insert(Decimal::ZERO) += amount;
            }
        }
        totals
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::store::InMemoryPositionStore;

    fn manager() -> PositionManager {
        PositionManager::new(Arc::new(InMemoryPositionStore::new()))
    }

    #[test]
    fn opens_position_from_flat() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), None, None, None, 1);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::new(5, 1));
        assert_eq!(pos.avg_price, Some(Decimal::new(50000, 0)));
    }

    #[test]
    fn same_direction_add_computes_weighted_average() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        // 0.5 BTC @ 50000
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), None, None, None, 1);
        // + 0.5 BTC @ 60000 -> avg = (50000*0.5 + 60000*0.5) / 1.0 = 55000
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(60000, 0)), None, None, None, 2);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::ONE);
        assert_eq!(pos.avg_price, Some(Decimal::new(55000, 0)));
    }

    #[test]
    fn same_direction_reduce_keeps_avg_price() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(4, 1), Some(Decimal::new(70000, 0)), None, None, None, 2);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::new(6, 1));
        assert_eq!(pos.avg_price, Some(Decimal::new(50000, 0)));
    }

    #[test]
    fn reduce_to_exactly_zero_clears_avg_price() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::ONE, Some(Decimal::new(70000, 0)), None, None, None, 2);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::ZERO);
        assert_eq!(pos.avg_price, None);
    }

    #[test]
    fn crossing_zero_reopens_at_new_fill_price() {
        let pm = manager();
        let venue = Venue::new("binance_futures");
        let symbol = Symbol::new("BTC", "USDT");

        // 多 1.0 BTC @ 50000
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        // 卖 1.5 BTC @ 60000 -> net_qty 从 +1.0 变成 -0.5，穿零反向
        pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(15, 1), Some(Decimal::new(60000, 0)), None, None, None, 2);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::new(-5, 1));
        assert_eq!(pos.avg_price, Some(Decimal::new(60000, 0)));
    }

    #[test]
    fn fill_without_price_only_updates_quantity() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), None, None, None, None, 2);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::new(15, 1));
        assert_eq!(pos.avg_price, Some(Decimal::new(50000, 0)));
    }

    #[test]
    fn asset_exposure_nets_spot_long_against_futures_short_across_venues() {
        let pm = manager();
        let symbol = Symbol::new("BTC", "USDT");

        // Binance 现货买入 1.0 BTC
        pm.on_filled(&Venue::new("binance_spot"), &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        // Binance 合约卖出开空 0.5 BTC
        pm.on_filled(&Venue::new("binance_futures"), &symbol, OrderSide::Sell, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), None, None, None, 2);
        // Kraken 现货买入 0.5 BTC (转仓过去的另一半)
        pm.on_filled(&Venue::new("kraken_spot"), &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), None, None, None, 3);

        let exposure = pm.asset_exposure("BTC");
        assert_eq!(exposure.net_qty, Decimal::ONE); // 1.0 + (-0.5) + 0.5 = 1.0，只有合约那条腿对冲了一半
        assert_eq!(exposure.venues.len(), 3);

        // 换一个不相关的 asset 不应该混进来
        let other = pm.asset_exposure("ETH");
        assert_eq!(other.net_qty, Decimal::ZERO);
        assert!(other.venues.is_empty());
    }

    #[test]
    fn asset_exposure_is_case_insensitive() {
        let pm = manager();
        let symbol = Symbol::new("BTC", "USDT");
        pm.on_filled(&Venue::new("binance_spot"), &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);

        assert_eq!(pm.asset_exposure("btc").net_qty, Decimal::ONE);
    }

    #[test]
    fn opening_from_flat_realizes_no_pnl() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), None, None, None, 1);
        assert_eq!(outcome.realized_pnl, Decimal::ZERO);
        assert_eq!(pm.venue_position(&venue, &symbol).unwrap().realized_pnl, Decimal::ZERO);
    }

    #[test]
    fn same_direction_add_realizes_no_pnl() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), None, None, None, 1);
        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(60000, 0)), None, None, None, 2);
        assert_eq!(outcome.realized_pnl, Decimal::ZERO);
        assert_eq!(pm.venue_position(&venue, &symbol).unwrap().realized_pnl, Decimal::ZERO);
    }

    #[test]
    fn same_direction_reduce_realizes_pnl_on_closed_qty() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        // 多 1.0 BTC @ 50000
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        // 卖 0.4 BTC @ 70000 -> 已实现盈亏 = 0.4 * (70000 - 50000) * 1 = 8000
        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(4, 1), Some(Decimal::new(70000, 0)), None, None, None, 2);
        assert_eq!(outcome.realized_pnl, Decimal::new(8000, 0));
        assert_eq!(pm.venue_position(&venue, &symbol).unwrap().realized_pnl, Decimal::new(8000, 0));
    }

    #[test]
    fn crossing_zero_only_realizes_pnl_on_the_closed_portion() {
        let pm = manager();
        let venue = Venue::new("binance_futures");
        let symbol = Symbol::new("BTC", "USDT");

        // 多 1.0 BTC @ 50000
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        // 卖 1.5 BTC @ 60000 -> 只有平掉旧仓位的 1.0 部分计已实现盈亏 = 1.0 * (60000-50000) * 1 = 10000，
        // 反向新开的 0.5 空头不计入。
        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(15, 1), Some(Decimal::new(60000, 0)), None, None, None, 2);
        assert_eq!(outcome.realized_pnl, Decimal::new(10000, 0));
        assert_eq!(pm.venue_position(&venue, &symbol).unwrap().realized_pnl, Decimal::new(10000, 0));
    }

    #[test]
    fn realized_pnl_accumulates_across_multiple_reducing_fills() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        // 多 1.0 BTC @ 50000
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        // 卖 0.4 BTC @ 70000 -> 已实现盈亏 = 0.4 * 20000 = 8000
        pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(4, 1), Some(Decimal::new(70000, 0)), None, None, None, 2);
        // 再卖 0.6 BTC @ 40000 -> 已实现盈亏 = 0.6 * -10000 = -6000，累计应为 8000 - 6000 = 2000
        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(6, 1), Some(Decimal::new(40000, 0)), None, None, None, 3);
        assert_eq!(outcome.realized_pnl, Decimal::new(-6000, 0));
        assert_eq!(pm.venue_position(&venue, &symbol).unwrap().realized_pnl, Decimal::new(2000, 0));
    }

    #[test]
    fn fill_without_price_realizes_no_pnl() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(5, 1), None, None, None, None, 2);
        assert_eq!(outcome.realized_pnl, Decimal::ZERO);
    }

    #[test]
    fn accumulates_fees_by_asset() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        // 第一笔成交，手续费 0.001 BNB
        let outcome1 = pm.on_filled(
            &venue,
            &symbol,
            OrderSide::Buy,
            Decimal::new(5, 1),
            Some(Decimal::new(50000, 0)),
            Some(Decimal::new(1, 3)),
            Some("BNB".to_string()),
            None,
            1,
        );
        assert_eq!(outcome1.fee, Some((Decimal::new(1, 3), "BNB".to_string())));

        // 第二笔成交，手续费 0.002 BNB
        let outcome2 = pm.on_filled(
            &venue,
            &symbol,
            OrderSide::Buy,
            Decimal::new(5, 1),
            Some(Decimal::new(60000, 0)),
            Some(Decimal::new(2, 3)),
            Some("BNB".to_string()),
            None,
            2,
        );
        assert_eq!(outcome2.fee, Some((Decimal::new(2, 3), "BNB".to_string())));

        // 检查持仓中累计的手续费
        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.total_fees.get("BNB"), Some(&Decimal::new(3, 3))); // 0.001 + 0.002 = 0.003
    }

    #[test]
    fn accumulates_fees_for_multiple_assets() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        // BNB 手续费
        pm.on_filled(
            &venue,
            &symbol,
            OrderSide::Buy,
            Decimal::ONE,
            Some(Decimal::new(50000, 0)),
            Some(Decimal::new(1, 3)),
            Some("BNB".to_string()),
            None,
            1,
        );

        // USDT 手续费
        pm.on_filled(
            &venue,
            &symbol,
            OrderSide::Sell,
            Decimal::new(5, 1),
            Some(Decimal::new(60000, 0)),
            Some(Decimal::new(30, 0)),
            Some("USDT".to_string()),
            None,
            2,
        );

        // 再次 BNB 手续费
        pm.on_filled(
            &venue,
            &symbol,
            OrderSide::Buy,
            Decimal::new(3, 1),
            Some(Decimal::new(55000, 0)),
            Some(Decimal::new(2, 3)),
            Some("BNB".to_string()),
            None,
            3,
        );

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.total_fees.get("BNB"), Some(&Decimal::new(3, 3))); // 0.001 + 0.002
        assert_eq!(pos.total_fees.get("USDT"), Some(&Decimal::new(30, 0))); // 30
    }

    #[test]
    fn total_fees_by_asset_aggregates_across_positions() {
        let pm = manager();
        let btc_symbol = Symbol::new("BTC", "USDT");
        let eth_symbol = Symbol::new("ETH", "USDT");

        // BTC 持仓，BNB 手续费
        pm.on_filled(
            &Venue::new("binance_spot"),
            &btc_symbol,
            OrderSide::Buy,
            Decimal::ONE,
            Some(Decimal::new(50000, 0)),
            Some(Decimal::new(1, 3)),
            Some("BNB".to_string()),
            None,
            1,
        );

        // ETH 持仓，BNB 手续费
        pm.on_filled(
            &Venue::new("binance_spot"),
            &eth_symbol,
            OrderSide::Buy,
            Decimal::new(10, 0),
            Some(Decimal::new(3000, 0)),
            Some(Decimal::new(2, 3)),
            Some("BNB".to_string()),
            None,
            2,
        );

        // BTC 持仓，USDT 手续费
        pm.on_filled(
            &Venue::new("kraken_spot"),
            &btc_symbol,
            OrderSide::Buy,
            Decimal::new(5, 1),
            Some(Decimal::new(50000, 0)),
            Some(Decimal::new(25, 0)),
            Some("USDT".to_string()),
            None,
            3,
        );

        let totals = pm.total_fees_by_asset();
        assert_eq!(totals.get("BNB"), Some(&Decimal::new(3, 3))); // 0.001 + 0.002
        assert_eq!(totals.get("USDT"), Some(&Decimal::new(25, 0))); // 25
    }

    #[test]
    fn apply_adjustment_accumulates_into_realized_pnl_without_touching_position() {
        let pm = manager();
        let venue = Venue::new("binance_futures");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);

        // 手续费换算成 USDT 后冲减盈亏 (调用方负责传负数)
        pm.apply_adjustment(&venue, &symbol, Decimal::new(-5, 0), AdjustmentReason::FeeUsdt, 2);
        // 资金费结算，正数=收到
        pm.apply_adjustment(&venue, &symbol, Decimal::new(3, 1), AdjustmentReason::Funding, 3);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.realized_pnl, Decimal::new(-47, 1)); // -5 + 0.3
        assert_eq!(pos.updated_at_ms, 3);
        // 不影响仓位数量/均价
        assert_eq!(pos.net_qty, Decimal::ONE);
        assert_eq!(pos.avg_price, Some(Decimal::new(50000, 0)));
    }

    #[test]
    fn apply_adjustment_is_created_flat_when_position_never_existed() {
        let pm = manager();
        let venue = Venue::new("binance_futures");
        let symbol = Symbol::new("ETH", "USDT");

        pm.apply_adjustment(&venue, &symbol, Decimal::new(100, 0), AdjustmentReason::Manual, 1);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.realized_pnl, Decimal::new(100, 0));
        assert_eq!(pos.net_qty, Decimal::ZERO);
        assert_eq!(pos.avg_price, None);
    }

    #[test]
    fn concurrent_fills_and_adjustments_do_not_lose_updates_to_realized_pnl() {
        let pm = Arc::new(manager());
        let venue = Venue::new("binance_futures");
        let symbol = Symbol::new("BTC", "USDT");

        // 起 1.0 BTC 多头底仓，后面并发的减仓成交才有得实现盈亏
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(1000, 0), Some(Decimal::new(50000, 0)), None, None, None, 1);

        let mut handles = Vec::new();
        // 16 个线程各自 apply_adjustment 一笔固定 delta
        for i in 0..16i64 {
            let pm = pm.clone();
            let venue = venue.clone();
            let symbol = symbol.clone();
            handles.push(std::thread::spawn(move || {
                pm.apply_adjustment(&venue, &symbol, Decimal::new(i, 0), AdjustmentReason::Manual, 10 + i as u64);
            }));
        }
        // 8 个线程各自 on_filled 一笔 0.001 BTC 减仓 @ 60000 (每笔已实现盈亏 = 0.001 * 10000 = 10)
        for i in 0..8i64 {
            let pm = pm.clone();
            let venue = venue.clone();
            let symbol = symbol.clone();
            handles.push(std::thread::spawn(move || {
                pm.on_filled(
                    &venue,
                    &symbol,
                    OrderSide::Sell,
                    Decimal::new(1, 3),
                    Some(Decimal::new(60000, 0)),
                    None,
                    None,
                    None,
                    100 + i as u64,
                );
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // sum(0..16) = 120, 8 笔减仓各实现 10 = 80
        let expected: Decimal = Decimal::new(120, 0) + Decimal::new(80, 0);
        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.realized_pnl, expected);
    }

    #[test]
    fn apply_adjustment_writes_audit_record_with_before_after_pnl() {
        let log = Arc::new(InMemoryAdjustmentLog::new());
        let pm = PositionManager::new(Arc::new(InMemoryPositionStore::new())).with_adjustment_log(log.clone());
        let venue = Venue::new("binance_futures");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        pm.apply_adjustment(&venue, &symbol, Decimal::new(-5, 0), AdjustmentReason::FeeUsdt, 2);

        let records = log.all();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.venue, venue);
        assert_eq!(record.symbol, symbol);
        assert_eq!(record.amount, Decimal::new(-5, 0));
        assert_eq!(record.reason, AdjustmentReason::FeeUsdt);
        assert_eq!(record.realized_pnl_before, Decimal::ZERO);
        assert_eq!(record.realized_pnl_after, Decimal::new(-5, 0));
        assert_eq!(record.ts_ms, 2);
    }

    #[test]
    fn on_filled_sync_fee_usdt_subtracts_and_logs_adjustment() {
        let log = Arc::new(InMemoryAdjustmentLog::new());
        let pm = PositionManager::new(Arc::new(InMemoryPositionStore::new())).with_adjustment_log(log.clone());
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(
            &venue,
            &symbol,
            OrderSide::Buy,
            Decimal::new(5, 1),
            Some(Decimal::new(50000, 0)),
            Some(Decimal::new(30, 0)),
            Some("USDT".to_string()),
            Some(Decimal::new(30, 0)),
            1,
        );

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.realized_pnl, Decimal::new(-30, 0));

        let records = log.all();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].reason, AdjustmentReason::FeeUsdt);
        assert_eq!(records[0].amount, Decimal::new(-30, 0));
    }
}
