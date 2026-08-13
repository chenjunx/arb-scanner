use std::sync::{Arc, Mutex};

use rust_decimal::prelude::Signed;
use rust_decimal::Decimal;

use crate::order::types::OrderSide;
use crate::types::{Symbol, Venue};

use super::store::PositionStore;
use super::types::{AssetExposure, FillOutcome, VenuePosition};

/// 跨交易所/跨产品的统一持仓视图。写入口是 `on_filled` (由 `RiskEngine`/
/// `OrderManager` 在订单成交后调用)，查询接口按 `(venue, symbol)` 或按
/// base 资产聚合两种粒度提供。见 `docs/position_manager_design.md`。
pub struct PositionManager {
    store: Arc<dyn PositionStore>,
}

impl PositionManager {
    pub fn new(store: Arc<dyn PositionStore>) -> Self {
        Self { store }
    }

    /// 订单成交后调用，按增量成交量 (不是累计值) + 本次成交价更新净仓位和
    /// 加权平均建仓价，并把这笔成交对旧仓位实现的盈亏通过 `FillOutcome` 带出去。
    /// `fill_price` 为 `None` 时 (极少数场景下交易所推送没带价格) 只更新数量，
    /// 均价保持不变，也不计已实现盈亏。
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
        ts_ms: u64,
    ) -> FillOutcome {
        let venue_for_closure = venue.clone();
        let symbol_for_closure = symbol.clone();
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
                            *slot.lock().unwrap() = closed_qty * (price - avg) * old_qty.signum();
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
                pos.net_qty = new_qty;
                pos.updated_at_ms = ts_ms;
                pos
            }),
        );

        FillOutcome { realized_pnl: *realized_pnl_slot.lock().unwrap() }
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

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), 1);

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
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), 1);
        // + 0.5 BTC @ 60000 -> avg = (50000*0.5 + 60000*0.5) / 1.0 = 55000
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(60000, 0)), 2);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::ONE);
        assert_eq!(pos.avg_price, Some(Decimal::new(55000, 0)));
    }

    #[test]
    fn same_direction_reduce_keeps_avg_price() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);
        pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(4, 1), Some(Decimal::new(70000, 0)), 2);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::new(6, 1));
        assert_eq!(pos.avg_price, Some(Decimal::new(50000, 0)));
    }

    #[test]
    fn reduce_to_exactly_zero_clears_avg_price() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);
        pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::ONE, Some(Decimal::new(70000, 0)), 2);

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
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);
        // 卖 1.5 BTC @ 60000 -> net_qty 从 +1.0 变成 -0.5，穿零反向
        pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(15, 1), Some(Decimal::new(60000, 0)), 2);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::new(-5, 1));
        assert_eq!(pos.avg_price, Some(Decimal::new(60000, 0)));
    }

    #[test]
    fn fill_without_price_only_updates_quantity() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), None, 2);

        let pos = pm.venue_position(&venue, &symbol).unwrap();
        assert_eq!(pos.net_qty, Decimal::new(15, 1));
        assert_eq!(pos.avg_price, Some(Decimal::new(50000, 0)));
    }

    #[test]
    fn asset_exposure_nets_spot_long_against_futures_short_across_venues() {
        let pm = manager();
        let symbol = Symbol::new("BTC", "USDT");

        // Binance 现货买入 1.0 BTC
        pm.on_filled(&Venue::new("binance_spot"), &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);
        // Binance 合约卖出开空 0.5 BTC
        pm.on_filled(&Venue::new("binance_futures"), &symbol, OrderSide::Sell, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), 2);
        // Kraken 现货买入 0.5 BTC (转仓过去的另一半)
        pm.on_filled(&Venue::new("kraken_spot"), &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), 3);

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
        pm.on_filled(&Venue::new("binance_spot"), &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);

        assert_eq!(pm.asset_exposure("btc").net_qty, Decimal::ONE);
    }

    #[test]
    fn opening_from_flat_realizes_no_pnl() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), 1);
        assert_eq!(outcome.realized_pnl, Decimal::ZERO);
    }

    #[test]
    fn same_direction_add_realizes_no_pnl() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), 1);
        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(60000, 0)), 2);
        assert_eq!(outcome.realized_pnl, Decimal::ZERO);
    }

    #[test]
    fn same_direction_reduce_realizes_pnl_on_closed_qty() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        // 多 1.0 BTC @ 50000
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);
        // 卖 0.4 BTC @ 70000 -> 已实现盈亏 = 0.4 * (70000 - 50000) * 1 = 8000
        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(4, 1), Some(Decimal::new(70000, 0)), 2);
        assert_eq!(outcome.realized_pnl, Decimal::new(8000, 0));
    }

    #[test]
    fn crossing_zero_only_realizes_pnl_on_the_closed_portion() {
        let pm = manager();
        let venue = Venue::new("binance_futures");
        let symbol = Symbol::new("BTC", "USDT");

        // 多 1.0 BTC @ 50000
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);
        // 卖 1.5 BTC @ 60000 -> 只有平掉旧仓位的 1.0 部分计已实现盈亏 = 1.0 * (60000-50000) * 1 = 10000，
        // 反向新开的 0.5 空头不计入。
        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(15, 1), Some(Decimal::new(60000, 0)), 2);
        assert_eq!(outcome.realized_pnl, Decimal::new(10000, 0));
    }

    #[test]
    fn fill_without_price_realizes_no_pnl() {
        let pm = manager();
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");

        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), 1);
        let outcome = pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::new(5, 1), None, 2);
        assert_eq!(outcome.realized_pnl, Decimal::ZERO);
    }
}
