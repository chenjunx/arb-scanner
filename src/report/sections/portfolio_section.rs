use std::collections::BTreeSet;
use std::sync::Arc;

use crate::portfolio::PortfolioManager;
use crate::position::PositionManager;
use crate::report::section::ReportSection;

/// 按 base 资产聚合已实现盈亏/浮动盈亏。资产列表从
/// `PositionManager::all_positions()` 里出现过的 symbol.base 去重得到，
/// 不需要单独维护一份资产清单。
///
/// `market_value`/`unrealized_pnl` 依赖 `PortfolioManager` 内部的
/// `quote_cache`；本 section 所在的 report 进程不接入实时行情，`quote_cache`
/// 恒为空，因此这两项会渲染成 "N/A"（见模块设计说明）。
pub struct PortfolioSection {
    position_manager: Arc<PositionManager>,
    portfolio_manager: Arc<PortfolioManager>,
}

impl PortfolioSection {
    pub fn new(position_manager: Arc<PositionManager>, portfolio_manager: Arc<PortfolioManager>) -> Self {
        Self { position_manager, portfolio_manager }
    }
}

impl ReportSection for PortfolioSection {
    fn title(&self) -> &str {
        "投资组合盈亏"
    }

    fn render(&self) -> String {
        let assets: BTreeSet<String> =
            self.position_manager.all_positions().into_iter().map(|p| p.symbol.base.to_string()).collect();

        if assets.is_empty() {
            return "(暂无持仓/成交记录)".to_string();
        }

        let mut lines = Vec::with_capacity(assets.len());
        for asset in assets {
            let pnl = self.portfolio_manager.asset_pnl(&asset);
            let valuation = self.portfolio_manager.asset_valuation(&asset);
            lines.push(format!(
                "{asset}: net_qty={} market_value={} realized_pnl={} unrealized_pnl={} net_pnl={}",
                valuation.net_qty,
                fmt_opt(valuation.market_value),
                pnl.realized_pnl,
                fmt_opt(pnl.unrealized_pnl),
                pnl.net_pnl,
            ));
        }
        lines.join("\n")
    }
}

fn fmt_opt(value: Option<rust_decimal::Decimal>) -> String {
    value.map(|v| v.to_string()).unwrap_or_else(|| "N/A".to_string())
}

#[cfg(test)]
mod tests {
    use dashmap::DashMap;
    use rust_decimal::Decimal;

    use super::*;
    use crate::order::types::OrderSide;
    use crate::portfolio::InMemoryPnlStore;
    use crate::position::InMemoryPositionStore;
    use crate::types::{Symbol, Venue};

    #[test]
    fn renders_placeholder_when_no_positions() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let portfolio =
            Arc::new(PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), Arc::new(DashMap::new())));
        let section = PortfolioSection::new(pm, portfolio);
        assert_eq!(section.render(), "(暂无持仓/成交记录)");
    }

    #[test]
    fn renders_one_line_per_asset_with_realized_pnl_and_na_unrealized() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let portfolio =
            Arc::new(PortfolioManager::new(pm.clone(), Arc::new(InMemoryPnlStore::new()), Arc::new(DashMap::new())));
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        portfolio.record_fill(&venue, &symbol, Decimal::new(100, 0), 1);

        let section = PortfolioSection::new(pm, portfolio);
        let body = section.render();
        assert!(body.contains("BTC:"), "body was: {body}");
        assert!(body.contains("realized_pnl=100"), "body was: {body}");
        assert!(body.contains("market_value=N/A"), "body was: {body}");
        assert!(body.contains("unrealized_pnl=N/A"), "body was: {body}");
    }
}
