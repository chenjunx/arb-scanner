use log::info;

use crate::strategy::{Opportunity, OpportunityKind};

use super::OpportunitySink;

/// 最简单的 sink 实现：把发现的套利机会打到日志里。
pub struct LogSink;

impl OpportunitySink for LogSink {
    fn handle(&self, opportunity: &Opportunity) {
        match &opportunity.kind {
            OpportunityKind::CrossExchange {
                symbol,
                buy_venue,
                sell_venue,
            } => {
                info!(
                    "[{}] {} buy={} sell={} profit_bps={} detail={}",
                    opportunity.strategy,
                    symbol,
                    buy_venue,
                    sell_venue,
                    opportunity.expected_profit_bps,
                    opportunity.detail
                );
            }
            OpportunityKind::Triangular { venue, legs } => {
                info!(
                    "[{}] venue={} legs={}/{}/{} profit_bps={} detail={}",
                    opportunity.strategy,
                    venue,
                    legs[0],
                    legs[1],
                    legs[2],
                    opportunity.expected_profit_bps,
                    opportunity.detail
                );
            }
        }
    }
}
