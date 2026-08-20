use std::collections::BTreeSet;
use std::sync::Arc;

use crate::portfolio::PortfolioManager;
use crate::report::section::ReportSection;

/// 投资组合报告：仓位明细 (每个 venue+symbol 的净数量/均价/已实现盈亏/市值/
/// 浮动盈亏) + 按 base 资产聚合的汇总。所有数字都经 `PortfolioManager`
/// 计算，不直接读 `PositionManager`——`PortfolioManager` 是仓位数量与已实现
/// 盈亏的唯一真相源 `PositionManager` 之上的只读视图。
pub struct PortfolioSection {
    portfolio_manager: Arc<PortfolioManager>,
}

impl PortfolioSection {
    pub fn new(portfolio_manager: Arc<PortfolioManager>) -> Self {
        Self { portfolio_manager }
    }
}

impl ReportSection for PortfolioSection {
    fn title(&self) -> &str {
        "投资组合盈亏"
    }

    fn render(&self) -> String {
        // 未过滤：全量 (venue, symbol)，含已平仓但仍有历史 realized_pnl 的记录，
        // 用来算资产汇总；"仓位明细"只列当前非零仓位，过滤掉的部分不会丢失，
        // 已实现盈亏依然计入下面的资产汇总。
        let all = self.portfolio_manager.all_valuations();
        if all.is_empty() {
            return "(暂无持仓/成交记录)".to_string();
        }

        let mut open: Vec<_> = all.iter().filter(|v| !v.net_qty.is_zero()).collect();
        open.sort_by(|a, b| (&a.venue, &a.symbol).cmp(&(&b.venue, &b.symbol)));

        let mut lines = vec!["仓位明细:".to_string()];
        if open.is_empty() {
            lines.push("  (当前无持仓)".to_string());
        } else {
            for v in &open {
                lines.push(format!(
                    "  {} {}: net_qty={} avg_price={} realized_pnl={} market_value={} unrealized_pnl={}",
                    v.venue,
                    v.symbol,
                    v.net_qty,
                    fmt_opt(v.avg_price),
                    v.realized_pnl,
                    fmt_opt(v.market_value),
                    fmt_opt(v.unrealized_pnl),
                ));
            }
        }

        let assets: BTreeSet<String> = all.iter().map(|v| v.symbol.base.to_string()).collect();
        lines.push("资产汇总:".to_string());
        for asset in assets {
            let pnl = self.portfolio_manager.asset_pnl(&asset);
            lines.push(format!(
                "  {asset}: realized_pnl={} unrealized_pnl={} net_pnl={}",
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
    use crate::position::{InMemoryPositionStore, PositionManager};
    use crate::types::{Symbol, Venue};

    #[test]
    fn renders_placeholder_when_no_positions() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let portfolio = Arc::new(PortfolioManager::new(pm, Arc::new(DashMap::new())));
        let section = PortfolioSection::new(portfolio);
        assert_eq!(section.render(), "(暂无持仓/成交记录)");
    }

    #[test]
    fn renders_venue_detail_and_asset_summary() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let portfolio = Arc::new(PortfolioManager::new(pm.clone(), Arc::new(DashMap::new())));
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);

        let section = PortfolioSection::new(portfolio);
        let body = section.render();
        assert!(body.contains("仓位明细:"), "body was: {body}");
        assert!(body.contains("binance_spot BTC/USDT:"), "body was: {body}");
        assert!(body.contains("realized_pnl=0"), "body was: {body}");
        assert!(body.contains("market_value=N/A"), "body was: {body}");
        assert!(body.contains("资产汇总:"), "body was: {body}");
        assert!(body.contains("BTC: realized_pnl=0"), "body was: {body}");
    }

    #[test]
    fn omits_flat_positions_from_venue_detail_but_keeps_realized_pnl_in_summary() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let portfolio = Arc::new(PortfolioManager::new(pm.clone(), Arc::new(DashMap::new())));
        let venue = Venue::new("binance_spot");
        let symbol = Symbol::new("BTC", "USDT");
        pm.on_filled(&venue, &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        pm.on_filled(&venue, &symbol, OrderSide::Sell, Decimal::ONE, Some(Decimal::new(50100, 0)), None, None, None, 2);

        let section = PortfolioSection::new(portfolio);
        let body = section.render();
        assert!(body.contains("仓位明细:\n  (当前无持仓)"), "body was: {body}");
        assert!(body.contains("BTC: realized_pnl=100"), "body was: {body}");
    }
}
