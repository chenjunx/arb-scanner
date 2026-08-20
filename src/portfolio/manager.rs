use std::sync::Arc;

use dashmap::DashMap;
use rust_decimal::Decimal;

use crate::position::PositionManager;
use crate::types::{Quote, Symbol, Venue};

use super::store::PnlStore;
use super::types::{AssetPnlSummary, AssetValuation, VenuePnl, VenuePositionValuation};

/// Portfolio 模块：在 `PositionManager` (仓位数量的唯一真相源) 之上提供两类
/// 只读视图——mark-to-market 估值 (按最新行情算市值/浮动盈亏) 和已实现
/// 盈亏/手续费统计。自己只写一份独立的 `PnlStore` 账本，不碰
/// `PositionManager`/`PositionStore`，职责边界见 `docs/portfolio_design.md`。
pub struct PortfolioManager {
    position_manager: Arc<PositionManager>,
    pnl_store: Arc<dyn PnlStore>,
    quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
}

impl PortfolioManager {
    pub fn new(
        position_manager: Arc<PositionManager>,
        pnl_store: Arc<dyn PnlStore>,
        quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
    ) -> Self {
        Self { position_manager, pnl_store, quote_cache }
    }

    /// 成交后调用 (由 `OrderManager` 在拿到 `FillOutcome` 后转发)：把
    /// `realized_pnl` 累加进 `PnlStore`。
    pub fn record_fill(&self, venue: &Venue, symbol: &Symbol, realized_pnl: Decimal, ts_ms: u64) {
        let venue_for_closure = venue.clone();
        let symbol_for_closure = symbol.clone();
        self.pnl_store.update(
            venue,
            symbol,
            Box::new(move |current| {
                let mut pnl =
                    current.unwrap_or_else(|| VenuePnl::flat(venue_for_closure.clone(), symbol_for_closure.clone()));
                pnl.realized_pnl += realized_pnl;
                pnl.trade_count += 1;
                pnl.updated_at_ms = ts_ms;
                pnl
            }),
        );
    }

    pub fn venue_pnl(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePnl> {
        self.pnl_store.get(venue, symbol)
    }

    /// 按 base 资产聚合已实现盈亏/手续费，并拼上 `asset_valuation` 算出的浮动
    /// 盈亏。`unrealized_pnl` 缺行情时为 `None`，`net_pnl` 仍然给出不含浮动
    /// 部分的值。
    pub fn asset_pnl(&self, asset: &str) -> AssetPnlSummary {
        let realized_pnl = self
            .pnl_store
            .all()
            .into_iter()
            .filter(|p| p.symbol.base.as_ref().eq_ignore_ascii_case(asset))
            .fold(Decimal::ZERO, |realized, p| realized + p.realized_pnl);

        let unrealized_pnl = self.asset_valuation(asset).unrealized_pnl;
        let net_pnl = realized_pnl + unrealized_pnl.unwrap_or(Decimal::ZERO);

        AssetPnlSummary { asset: asset.to_string(), realized_pnl, unrealized_pnl, net_pnl }
    }

    /// mark-to-market: `mid = (bid+ask)/2`，`market_value = net_qty * mid`，
    /// `unrealized_pnl = (mid - avg_price) * net_qty`。查不到行情或
    /// `avg_price` 为 `None` (仓位为 0) 时，三个字段都返回 `None`，不用 0
    /// 兜底，避免"没有行情"和"确实不赚不赔"混淆。`None` 表示该 (venue,
    /// symbol) 从未有过成交记录。
    pub fn venue_valuation(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePositionValuation> {
        let pos = self.position_manager.venue_position(venue, symbol)?;
        Some(self.valuation_for(pos.venue, pos.symbol, pos.net_qty, pos.avg_price))
    }

    pub fn all_valuations(&self) -> Vec<VenuePositionValuation> {
        self.position_manager
            .all_positions()
            .into_iter()
            .map(|pos| self.valuation_for(pos.venue, pos.symbol, pos.net_qty, pos.avg_price))
            .collect()
    }

    /// 按 base 资产聚合估值。`market_value`/`unrealized_pnl` 只有当参与聚合的
    /// venue 全部拿到了 mark price 才是 `Some`，避免"部分 venue 缺价"时市值被
    /// 悄悄少算却看起来像是完整数字。
    pub fn asset_valuation(&self, asset: &str) -> AssetValuation {
        let venues: Vec<VenuePositionValuation> = self
            .all_valuations()
            .into_iter()
            .filter(|v| v.symbol.base.as_ref().eq_ignore_ascii_case(asset))
            .collect();

        let net_qty = venues.iter().map(|v| v.net_qty).sum();
        let all_priced = !venues.is_empty() && venues.iter().all(|v| v.market_value.is_some());
        let market_value = all_priced.then(|| venues.iter().filter_map(|v| v.market_value).sum());
        let unrealized_pnl = all_priced.then(|| venues.iter().filter_map(|v| v.unrealized_pnl).sum());

        AssetValuation {
            asset: asset.to_string(),
            net_qty,
            market_value,
            unrealized_pnl,
            venues,
        }
    }

    fn valuation_for(
        &self,
        venue: Venue,
        symbol: Symbol,
        net_qty: Decimal,
        avg_price: Option<Decimal>,
    ) -> VenuePositionValuation {
        let mark_price = avg_price.and_then(|_| {
            self.quote_cache
                .get(&(venue.clone(), symbol.clone()))
                .map(|q| (q.bid + q.ask) / Decimal::new(2, 0))
        });
        let market_value = mark_price.map(|mp| net_qty * mp);
        let unrealized_pnl = match (mark_price, avg_price) {
            (Some(mp), Some(avg)) => Some((mp - avg) * net_qty),
            _ => None,
        };

        VenuePositionValuation {
            venue,
            symbol,
            net_qty,
            avg_price,
            mark_price,
            market_value,
            unrealized_pnl,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order::types::OrderSide;
    use crate::position::InMemoryPositionStore;
    use crate::portfolio::store::InMemoryPnlStore;

    fn venue() -> Venue {
        Venue::new("binance_spot")
    }
    fn symbol() -> Symbol {
        Symbol::new("BTC", "USDT")
    }

    fn manager() -> (PortfolioManager, Arc<PositionManager>) {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let portfolio = PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), Arc::new(DashMap::new()));
        (portfolio, pm)
    }

    #[test]
    fn record_fill_accumulates_realized_pnl_and_trade_count() {
        let (portfolio, _pm) = manager();
        portfolio.record_fill(&venue(), &symbol(), Decimal::ZERO, 1);

        let pnl = portfolio.venue_pnl(&venue(), &symbol()).unwrap();
        assert_eq!(pnl.trade_count, 1);
    }

    #[test]
    fn record_fill_accumulates_realized_pnl_across_multiple_fills() {
        let (portfolio, _pm) = manager();
        portfolio.record_fill(&venue(), &symbol(), Decimal::new(100, 0), 1);
        portfolio.record_fill(&venue(), &symbol(), Decimal::new(50, 0), 2);

        let pnl = portfolio.venue_pnl(&venue(), &symbol()).unwrap();
        assert_eq!(pnl.realized_pnl, Decimal::new(150, 0));
    }

    #[test]
    fn venue_valuation_is_none_when_never_traded() {
        let (portfolio, _pm) = manager();
        assert_eq!(portfolio.venue_valuation(&venue(), &symbol()), None);
    }

    #[test]
    fn venue_valuation_marks_all_fields_none_without_quote() {
        let (portfolio, pm) = manager();
        pm.on_filled(&venue(), &symbol(), OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);

        let valuation = portfolio.venue_valuation(&venue(), &symbol()).unwrap();
        assert_eq!(valuation.net_qty, Decimal::ONE);
        assert_eq!(valuation.avg_price, Some(Decimal::new(50000, 0)));
        assert_eq!(valuation.mark_price, None);
        assert_eq!(valuation.market_value, None);
        assert_eq!(valuation.unrealized_pnl, None);
    }

    #[test]
    fn venue_valuation_computes_market_value_and_unrealized_pnl_with_quote() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let quote_cache = Arc::new(DashMap::new());
        let portfolio = PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), quote_cache.clone());

        pm.on_filled(&venue(), &symbol(), OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        quote_cache.insert(
            (venue(), symbol()),
            Quote {
                bid: Decimal::new(59000, 0),
                bid_size: Decimal::ONE,
                ask: Decimal::new(61000, 0),
                ask_size: Decimal::ONE,
                ts_ms: 1,
            },
        );

        let valuation = portfolio.venue_valuation(&venue(), &symbol()).unwrap();
        assert_eq!(valuation.mark_price, Some(Decimal::new(60000, 0)));
        assert_eq!(valuation.market_value, Some(Decimal::new(60000, 0)));
        assert_eq!(valuation.unrealized_pnl, Some(Decimal::new(10000, 0)));
    }

    #[test]
    fn venue_valuation_handles_short_position() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let quote_cache = Arc::new(DashMap::new());
        let portfolio = PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), quote_cache.clone());

        pm.on_filled(&venue(), &symbol(), OrderSide::Sell, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        quote_cache.insert(
            (venue(), symbol()),
            Quote {
                bid: Decimal::new(39000, 0),
                bid_size: Decimal::ONE,
                ask: Decimal::new(41000, 0),
                ask_size: Decimal::ONE,
                ts_ms: 1,
            },
        );

        let valuation = portfolio.venue_valuation(&venue(), &symbol()).unwrap();
        assert_eq!(valuation.net_qty, Decimal::new(-1, 0));
        assert_eq!(valuation.mark_price, Some(Decimal::new(40000, 0)));
        assert_eq!(valuation.market_value, Some(Decimal::new(-40000, 0)));
        // 空头，跌价盈利: (40000 - 50000) * -1 = 10000
        assert_eq!(valuation.unrealized_pnl, Some(Decimal::new(10000, 0)));
    }

    #[test]
    fn asset_valuation_aggregates_across_venues_when_all_priced() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let quote_cache = Arc::new(DashMap::new());
        let portfolio = PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), quote_cache.clone());

        let binance = Venue::new("binance_spot");
        let kraken = Venue::new("kraken_spot");
        pm.on_filled(&binance, &symbol(), OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        pm.on_filled(&kraken, &symbol(), OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), None, None, None, 2);
        for v in [&binance, &kraken] {
            quote_cache.insert(
                (v.clone(), symbol()),
                Quote {
                    bid: Decimal::new(59000, 0),
                    bid_size: Decimal::ONE,
                    ask: Decimal::new(61000, 0),
                    ask_size: Decimal::ONE,
                    ts_ms: 1,
                },
            );
        }

        let valuation = portfolio.asset_valuation("BTC");
        assert_eq!(valuation.net_qty, Decimal::new(15, 1));
        assert_eq!(valuation.market_value, Some(Decimal::new(90000, 0)));
        assert_eq!(valuation.venues.len(), 2);
    }

    #[test]
    fn asset_valuation_is_none_market_value_when_any_venue_missing_quote() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let quote_cache = Arc::new(DashMap::new());
        let portfolio = PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), quote_cache.clone());

        let binance = Venue::new("binance_spot");
        let kraken = Venue::new("kraken_spot");
        pm.on_filled(&binance, &symbol(), OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        pm.on_filled(&kraken, &symbol(), OrderSide::Buy, Decimal::new(5, 1), Some(Decimal::new(50000, 0)), None, None, None, 2);
        quote_cache.insert(
            (binance.clone(), symbol()),
            Quote {
                bid: Decimal::new(59000, 0),
                bid_size: Decimal::ONE,
                ask: Decimal::new(61000, 0),
                ask_size: Decimal::ONE,
                ts_ms: 1,
            },
        );
        // kraken 缺行情

        let valuation = portfolio.asset_valuation("BTC");
        assert_eq!(valuation.market_value, None);
        assert_eq!(valuation.unrealized_pnl, None);
    }

    #[test]
    fn asset_pnl_aggregates_realized_pnl_across_venues() {
        let (portfolio, _pm) = manager();
        let binance = Venue::new("binance_spot");
        let kraken = Venue::new("kraken_spot");
        portfolio.record_fill(&binance, &symbol(), Decimal::new(100, 0), 1);
        portfolio.record_fill(&kraken, &symbol(), Decimal::new(50, 0), 2);

        let summary = portfolio.asset_pnl("BTC");
        assert_eq!(summary.realized_pnl, Decimal::new(150, 0));
        assert_eq!(summary.unrealized_pnl, None);
        assert_eq!(summary.net_pnl, Decimal::new(150, 0));
    }

}
