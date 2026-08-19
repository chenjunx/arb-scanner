use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use rust_decimal::Decimal;

use crate::position::PositionManager;
use crate::types::{Quote, Symbol, Venue};

use super::store::PnlStore;
use super::types::{AssetPnlSummary, AssetValuation, FeeConfig, VenuePnl, VenuePositionValuation};

/// Portfolio 模块：在 `PositionManager` (仓位数量的唯一真相源) 之上提供两类
/// 只读视图——mark-to-market 估值 (按最新行情算市值/浮动盈亏) 和已实现
/// 盈亏/手续费统计。自己只写一份独立的 `PnlStore` 账本，不碰
/// `PositionManager`/`PositionStore`，职责边界见 `docs/portfolio_design.md`。
pub struct PortfolioManager {
    position_manager: Arc<PositionManager>,
    pnl_store: Arc<dyn PnlStore>,
    quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
    fee_config: HashMap<Venue, FeeConfig>,
    default_fee_config: FeeConfig,
}

impl PortfolioManager {
    pub fn new(
        position_manager: Arc<PositionManager>,
        pnl_store: Arc<dyn PnlStore>,
        quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
        fee_config: HashMap<Venue, FeeConfig>,
    ) -> Self {
        Self {
            position_manager,
            pnl_store,
            quote_cache,
            fee_config,
            default_fee_config: FeeConfig::default(),
        }
    }

    /// 成交后调用 (由 `OrderManager` 在拿到 `FillOutcome` 后转发)：`real_fee`
    /// 非 `None` 时直接用交易所真实手续费，否则按 venue 的
    /// `taker_fee_bps × fee_discount` 估算兜底 (需要 `fill_price` 才能估算；
    /// 两者都缺时本次不计手续费，也不标记为估算)，连同 `realized_pnl` 一起
    /// 累加进 `PnlStore`。
    pub fn record_fill(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        filled_qty_delta: Decimal,
        fill_price: Option<Decimal>,
        real_fee: Option<Decimal>,
        fee_usdt: Option<Decimal>,
        realized_pnl: Decimal,
        ts_ms: u64,
    ) {
        let (fee, is_estimated) = match real_fee {
            Some(fee) => (fee, false),
            None => match fill_price {
                Some(price) => {
                    let cfg = self.fee_config.get(venue).copied().unwrap_or(self.default_fee_config);
                    let fee = filled_qty_delta.abs() * price * cfg.taker_fee_bps / Decimal::new(10000, 0) * cfg.fee_discount;
                    (fee, true)
                }
                None => (Decimal::ZERO, false),
            },
        };

        let venue_for_closure = venue.clone();
        let symbol_for_closure = symbol.clone();
        self.pnl_store.update(
            venue,
            symbol,
            Box::new(move |current| {
                let mut pnl =
                    current.unwrap_or_else(|| VenuePnl::flat(venue_for_closure.clone(), symbol_for_closure.clone()));
                pnl.realized_pnl += realized_pnl;
                pnl.fees_paid += fee;
                if let Some(usdt) = fee_usdt {
                    pnl.fees_paid_usdt += usdt;
                }
                pnl.fee_is_estimated = pnl.fee_is_estimated || is_estimated;
                pnl.trade_count += 1;
                pnl.updated_at_ms = ts_ms;
                pnl
            }),
        );
    }

    /// 后台异步查价(`pricing::FeeUsdtConverter::query_async`)成功后调用，
    /// 把换算出的 USDT 等值补记到 `fees_paid_usdt`。与 `record_fill` 走独立的
    /// 原子 `pnl_store.update`，因为查价结果回来时这笔成交早已经处理完。
    pub fn apply_fee_usdt(&self, venue: &Venue, symbol: &Symbol, amount_usdt: Decimal) {
        let venue_for_closure = venue.clone();
        let symbol_for_closure = symbol.clone();
        self.pnl_store.update(
            venue,
            symbol,
            Box::new(move |current| {
                let mut pnl =
                    current.unwrap_or_else(|| VenuePnl::flat(venue_for_closure.clone(), symbol_for_closure.clone()));
                pnl.fees_paid_usdt += amount_usdt;
                pnl
            }),
        );
    }

    /// 后台异步查价失败，或找不到对应 venue 的 `OrderProvider` 时调用，标记
    /// 这个 (venue, symbol) 的 `fees_paid_usdt` 可能不完整，不当 0 处理。
    pub fn mark_fee_usdt_incomplete(&self, venue: &Venue, symbol: &Symbol) {
        let venue_for_closure = venue.clone();
        let symbol_for_closure = symbol.clone();
        self.pnl_store.update(
            venue,
            symbol,
            Box::new(move |current| {
                let mut pnl =
                    current.unwrap_or_else(|| VenuePnl::flat(venue_for_closure.clone(), symbol_for_closure.clone()));
                pnl.fees_usdt_incomplete = true;
                pnl
            }),
        );
    }

    pub fn venue_pnl(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePnl> {
        self.pnl_store.get(venue, symbol)
    }

    /// 永续合约资金费到账后调用(由 `accounting::FundingFeeTracker` 定期轮询交易
    /// 所资金费流水后转发)：`amount` 正=收到、负=支付，累加进 `PnlStore` 的
    /// `funding_pnl`，不影响 `trade_count`(那是成交笔数，资金费不是成交)。
    pub fn record_funding_fee(&self, venue: &Venue, symbol: &Symbol, amount: Decimal, ts_ms: u64) {
        let venue_for_closure = venue.clone();
        let symbol_for_closure = symbol.clone();
        self.pnl_store.update(
            venue,
            symbol,
            Box::new(move |current| {
                let mut pnl =
                    current.unwrap_or_else(|| VenuePnl::flat(venue_for_closure.clone(), symbol_for_closure.clone()));
                pnl.funding_pnl += amount;
                pnl.updated_at_ms = ts_ms;
                pnl
            }),
        );
    }

    /// 按 base 资产聚合已实现盈亏/手续费/资金费，并拼上 `asset_valuation` 算出
    /// 的浮动盈亏。`unrealized_pnl` 缺行情时为 `None`，`net_pnl` 仍然给出不含
    /// 浮动部分的值。
    pub fn asset_pnl(&self, asset: &str) -> AssetPnlSummary {
        let (realized_pnl, fees_paid, fees_paid_usdt, fees_usdt_incomplete, funding_pnl) = self
            .pnl_store
            .all()
            .into_iter()
            .filter(|p| p.symbol.base.as_ref().eq_ignore_ascii_case(asset))
            .fold(
                (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, false, Decimal::ZERO),
                |(realized, fees, fees_usdt, incomplete, funding), p| {
                    (
                        realized + p.realized_pnl,
                        fees + p.fees_paid,
                        fees_usdt + p.fees_paid_usdt,
                        incomplete || p.fees_usdt_incomplete,
                        funding + p.funding_pnl,
                    )
                },
            );

        let unrealized_pnl = self.asset_valuation(asset).unrealized_pnl;
        let net_pnl = realized_pnl - fees_paid + funding_pnl + unrealized_pnl.unwrap_or(Decimal::ZERO);

        AssetPnlSummary {
            asset: asset.to_string(),
            realized_pnl,
            fees_paid,
            fees_paid_usdt,
            fees_usdt_incomplete,
            funding_pnl,
            unrealized_pnl,
            net_pnl,
        }
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
        let portfolio = PortfolioManager::new(
            pm.clone(),
            Arc::new(InMemoryPnlStore::new()),
            Arc::new(DashMap::new()),
            HashMap::new(),
        );
        (portfolio, pm)
    }

    #[test]
    fn record_fill_uses_real_fee_when_present() {
        let (portfolio, _pm) = manager();
        portfolio.record_fill(
            &venue(),
            &symbol(),
            Decimal::ONE,
            Some(Decimal::new(50000, 0)),
            Some(Decimal::new(5, 0)),
            None,
            Decimal::ZERO,
            1,
        );

        let pnl = portfolio.venue_pnl(&venue(), &symbol()).unwrap();
        assert_eq!(pnl.fees_paid, Decimal::new(5, 0));
        assert!(!pnl.fee_is_estimated);
        assert_eq!(pnl.trade_count, 1);
    }

    #[test]
    fn record_fill_estimates_fee_when_real_fee_missing() {
        let mut fee_config = HashMap::new();
        fee_config.insert(venue(), FeeConfig { taker_fee_bps: Decimal::new(10, 0), fee_discount: Decimal::ONE });
        let portfolio = PortfolioManager::new(
            Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new()))),
            Arc::new(InMemoryPnlStore::new()),
            Arc::new(DashMap::new()),
            fee_config,
        );

        // 1.0 BTC @ 50000, 10 bps -> fee = 1.0 * 50000 * 10 / 10000 = 50
        portfolio.record_fill(&venue(), &symbol(), Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, Decimal::ZERO, 1);

        let pnl = portfolio.venue_pnl(&venue(), &symbol()).unwrap();
        assert_eq!(pnl.fees_paid, Decimal::new(50, 0));
        assert!(pnl.fee_is_estimated);
    }

    #[test]
    fn record_fill_estimation_stays_sticky_after_real_fee_seen_later() {
        let (portfolio, _pm) = manager();
        portfolio.record_fill(&venue(), &symbol(), Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, Decimal::ZERO, 1);
        portfolio.record_fill(&venue(), &symbol(), Decimal::ONE, Some(Decimal::new(50000, 0)), Some(Decimal::new(5, 0)), None, Decimal::ZERO, 2);

        let pnl = portfolio.venue_pnl(&venue(), &symbol()).unwrap();
        assert!(pnl.fee_is_estimated);
        assert_eq!(pnl.trade_count, 2);
    }

    #[test]
    fn record_fill_without_real_fee_or_price_records_zero_fee() {
        let (portfolio, _pm) = manager();
        portfolio.record_fill(&venue(), &symbol(), Decimal::ONE, None, None, None, Decimal::ZERO, 1);

        let pnl = portfolio.venue_pnl(&venue(), &symbol()).unwrap();
        assert_eq!(pnl.fees_paid, Decimal::ZERO);
        assert!(!pnl.fee_is_estimated);
    }

    #[test]
    fn record_fill_accumulates_realized_pnl_across_multiple_fills() {
        let (portfolio, _pm) = manager();
        portfolio.record_fill(&venue(), &symbol(), Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, Decimal::new(100, 0), 1);
        portfolio.record_fill(&venue(), &symbol(), Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, Decimal::new(50, 0), 2);

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
        let portfolio = PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), quote_cache.clone(), HashMap::new());

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
        let portfolio = PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), quote_cache.clone(), HashMap::new());

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
        let portfolio = PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), quote_cache.clone(), HashMap::new());

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
        let portfolio = PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), quote_cache.clone(), HashMap::new());

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
    fn asset_pnl_aggregates_realized_and_fees_across_venues() {
        let (portfolio, _pm) = manager();
        let binance = Venue::new("binance_spot");
        let kraken = Venue::new("kraken_spot");
        portfolio.record_fill(&binance, &symbol(), Decimal::ONE, Some(Decimal::new(50000, 0)), Some(Decimal::new(5, 0)), None, Decimal::new(100, 0), 1);
        portfolio.record_fill(&kraken, &symbol(), Decimal::ONE, Some(Decimal::new(50000, 0)), Some(Decimal::new(3, 0)), None, Decimal::new(50, 0), 2);

        let summary = portfolio.asset_pnl("BTC");
        assert_eq!(summary.realized_pnl, Decimal::new(150, 0));
        assert_eq!(summary.fees_paid, Decimal::new(8, 0));
        assert_eq!(summary.unrealized_pnl, None);
        assert_eq!(summary.net_pnl, Decimal::new(142, 0));
    }

    #[test]
    fn record_funding_fee_accumulates_and_updates_timestamp() {
        let (portfolio, _pm) = manager();
        portfolio.record_funding_fee(&venue(), &symbol(), Decimal::new(-5, 1), 1);
        portfolio.record_funding_fee(&venue(), &symbol(), Decimal::new(8, 1), 2);

        let pnl = portfolio.venue_pnl(&venue(), &symbol()).unwrap();
        assert_eq!(pnl.funding_pnl, Decimal::new(3, 1));
        assert_eq!(pnl.updated_at_ms, 2);
        // 资金费不是成交，不应该影响 trade_count/fees_paid/realized_pnl
        assert_eq!(pnl.trade_count, 0);
        assert_eq!(pnl.fees_paid, Decimal::ZERO);
        assert_eq!(pnl.realized_pnl, Decimal::ZERO);
    }

    #[test]
    fn asset_pnl_includes_funding_pnl_in_net_pnl() {
        let (portfolio, _pm) = manager();
        let binance = Venue::new("binance_spot");
        let futures = Venue::new("binance_futures");
        portfolio.record_fill(&binance, &symbol(), Decimal::ONE, Some(Decimal::new(50000, 0)), Some(Decimal::new(5, 0)), None, Decimal::new(100, 0), 1);
        portfolio.record_funding_fee(&futures, &symbol(), Decimal::new(-15, 1), 2);

        let summary = portfolio.asset_pnl("BTC");
        assert_eq!(summary.funding_pnl, Decimal::new(-15, 1));
        // net_pnl = realized_pnl(100) - fees_paid(5) + funding_pnl(-1.5) = 93.5
        assert_eq!(summary.net_pnl, Decimal::new(935, 1));
    }
}
