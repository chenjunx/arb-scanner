use std::sync::Arc;

use crate::position::PositionManager;
use crate::report::section::ReportSection;

/// 按 venue+symbol 列出当前非零仓位的明细（净数量、均价）。已平仓
/// （`net_qty == 0`）的记录不列出，避免报告随时间无限膨胀。
pub struct PositionSection {
    position_manager: Arc<PositionManager>,
}

impl PositionSection {
    pub fn new(position_manager: Arc<PositionManager>) -> Self {
        Self { position_manager }
    }
}

impl ReportSection for PositionSection {
    fn title(&self) -> &str {
        "仓位明细"
    }

    fn render(&self) -> String {
        let mut positions: Vec<_> =
            self.position_manager.all_positions().into_iter().filter(|p| !p.net_qty.is_zero()).collect();

        if positions.is_empty() {
            return "(当前无持仓)".to_string();
        }

        positions.sort_by(|a, b| (&a.venue, &a.symbol).cmp(&(&b.venue, &b.symbol)));
        positions
            .into_iter()
            .map(|p| {
                format!(
                    "{} {}: net_qty={} avg_price={} total_fees_usdt={}{}",
                    p.venue,
                    p.symbol,
                    p.net_qty,
                    p.avg_price.map(|v| v.to_string()).unwrap_or_else(|| "N/A".to_string()),
                    p.total_fees_usdt,
                    if p.fees_usdt_incomplete { "(部分未换算)" } else { "" }
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use crate::order::types::OrderSide;
    use crate::position::InMemoryPositionStore;
    use crate::types::{Symbol, Venue};

    #[test]
    fn renders_placeholder_when_flat() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let section = PositionSection::new(pm);
        assert_eq!(section.render(), "(当前无持仓)");
    }

    #[test]
    fn skips_flat_positions_and_lists_open_ones_sorted() {
        let pm = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
        let symbol = Symbol::new("BTC", "USDT");
        pm.on_filled(&Venue::new("kraken_spot"), &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 1);
        pm.on_filled(&Venue::new("binance_spot"), &symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(50000, 0)), None, None, None, 2);
        // 开仓又平仓，应该被过滤掉
        let flat_symbol = Symbol::new("ETH", "USDT");
        pm.on_filled(&Venue::new("binance_spot"), &flat_symbol, OrderSide::Buy, Decimal::ONE, Some(Decimal::new(3000, 0)), None, None, None, 3);
        pm.on_filled(&Venue::new("binance_spot"), &flat_symbol, OrderSide::Sell, Decimal::ONE, Some(Decimal::new(3100, 0)), None, None, None, 4);

        let section = PositionSection::new(pm);
        let body = section.render();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("binance_spot"), "body was: {body}");
        assert!(lines[1].starts_with("kraken_spot"), "body was: {body}");
        assert!(!body.contains("ETH"), "body was: {body}");
    }
}
