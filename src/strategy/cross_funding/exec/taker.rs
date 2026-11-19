//! 两腿同时市价(taker)。

use rust_decimal::prelude::ToPrimitive;

use super::{BookPair, ExecAction, ExecAlgo, ExecEstimate, ExecEvent, ExecStep, market_vwap, new_client_order_id};
use crate::order::types::OrderSide;
use crate::strategy::cross_funding::{LegRole, LegState};
use crate::strategy::spread_wait::spread_bp;

#[derive(Debug, Clone)]
pub(in super::super) struct TakerTaker {
    /// 下单后多久两腿还没全部成交就判定失败。
    fill_timeout_ms: u64,
    started_ms: Option<u64>,
}

impl TakerTaker {
    pub fn new(fill_timeout_ms: u64) -> Self {
        Self { fill_timeout_ms, started_ms: None }
    }

    fn status(long: &LegState, short: &LegState) -> ExecStep {
        if long.filled.is_some() && short.filled.is_some() { ExecStep::done() } else { ExecStep::idle() }
    }
}

impl ExecAlgo for TakerTaker {
    fn liquidity(&self) -> &'static str {
        "taker"
    }

    fn clone_box(&self) -> Box<dyn ExecAlgo> {
        Box::new(self.clone())
    }

    fn estimate(&self, books: &BookPair<'_>, long: &LegState, short: &LegState) -> Option<ExecEstimate> {
        if books.now_ms.saturating_sub(books.long.ts_ms) > books.max_age_ms
            || books.now_ms.saturating_sub(books.short.ts_ms) > books.max_age_ms
        {
            return None;
        }
        let long_vwap = market_vwap(&books.long.asks, long.order_qty, &long.spec)?;
        let short_vwap = market_vwap(&books.short.bids, short.order_qty, &short.spec)?;
        let exec_bp = spread_bp(long_vwap.to_f64()?, short_vwap.to_f64()?);
        exec_bp.is_finite().then_some(ExecEstimate {
            long_px_per_coin: long_vwap,
            short_px_per_coin: short_vwap,
            exec_bp,
        })
    }

    fn start(&mut self, long: &mut LegState, short: &mut LegState, now_ms: u64) -> ExecStep {
        self.started_ms = Some(now_ms);
        let long_id = new_client_order_id();
        let mut short_id = new_client_order_id();
        while short_id == long_id {
            short_id = new_client_order_id();
        }
        long.client_order_id = Some(long_id.clone());
        short.client_order_id = Some(short_id.clone());
        ExecStep::running(vec![
            ExecAction::Market { leg: LegRole::Long, side: OrderSide::Buy, qty: long.order_qty, client_order_id: long_id },
            ExecAction::Market { leg: LegRole::Short, side: OrderSide::Sell, qty: short.order_qty, client_order_id: short_id },
        ])
    }

    fn on_event(&mut self, long: &mut LegState, short: &mut LegState, event: ExecEvent, _now_ms: u64) -> ExecStep {
        match event {
            ExecEvent::Filled { leg, filled_qty, avg_price } => {
                let state = match leg {
                    LegRole::Long => &mut *long,
                    LegRole::Short => &mut *short,
                };
                state.filled = Some((filled_qty, avg_price));
                Self::status(long, short)
            }
            ExecEvent::Failed { leg, kind, reason, partial } => {
                if let Some(partial) = partial {
                    match leg {
                        LegRole::Long => long.filled = Some(partial),
                        LegRole::Short => short.filled = Some(partial),
                    }
                }
                ExecStep::failed(format!("{leg:?} 腿{kind}: {reason}"))
            }
        }
    }

    fn on_tick(&mut self, long: &mut LegState, short: &mut LegState, now_ms: u64) -> ExecStep {
        match self.started_ms {
            Some(at) if now_ms.saturating_sub(at) > self.fill_timeout_ms => ExecStep::failed(format!(
                "成交超时({}ms 后两腿仍未全部成交，多腿 {:?} 空腿 {:?})",
                now_ms.saturating_sub(at),
                long.filled,
                short.filled
            )),
            _ => ExecStep::idle(),
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;
    use super::super::ExecStatus;
    use crate::exchange_info::types::PerpContractSpec;
    use crate::types::{Symbol, Venue};

    fn leg(venue: &str) -> LegState {
        LegState {
            venue: Venue::new(venue),
            symbol: Symbol::new("BTC", "USDT"),
            spec: PerpContractSpec { price_unit: Decimal::ONE, qty_unit: Decimal::ONE },
            order_qty: Decimal::ONE,
            client_order_id: None,
            filled: None,
        }
    }

    fn filled(leg: LegRole) -> ExecEvent {
        ExecEvent::Filled { leg, filled_qty: Decimal::ONE, avg_price: Decimal::from(100) }
    }

    #[test]
    fn start_submits_one_market_order_per_leg_with_distinct_ids() {
        let (mut long, mut short) = (leg("a"), leg("b"));
        let step = TakerTaker::new(1_000).start(&mut long, &mut short, 0);
        assert_eq!(step.status, ExecStatus::Running);
        let [ExecAction::Market { leg: l, side: ls, client_order_id: lid, .. }, ExecAction::Market { leg: s, side: ss, client_order_id: sid, .. }] =
            step.actions.as_slice()
        else {
            panic!("{step:?}");
        };
        assert_eq!((*l, *ls, *s, *ss), (LegRole::Long, OrderSide::Buy, LegRole::Short, OrderSide::Sell));
        assert_ne!(lid, sid);
        assert_eq!(long.client_order_id.as_ref(), Some(lid));
        assert_eq!(short.client_order_id.as_ref(), Some(sid));
    }

    #[test]
    fn done_only_after_both_legs_filled() {
        let (mut long, mut short) = (leg("a"), leg("b"));
        let mut algo = TakerTaker::new(1_000);
        algo.start(&mut long, &mut short, 0);
        assert_eq!(algo.on_event(&mut long, &mut short, filled(LegRole::Long), 1).status, ExecStatus::Running);
        assert_eq!(algo.on_event(&mut long, &mut short, filled(LegRole::Short), 2).status, ExecStatus::Done);
        assert!(long.filled.is_some() && short.filled.is_some());
    }

    #[test]
    fn failure_keeps_partial_fill_and_reports_reason() {
        let (mut long, mut short) = (leg("a"), leg("b"));
        let mut algo = TakerTaker::new(1_000);
        algo.start(&mut long, &mut short, 0);
        let partial = Some((Decimal::from(3), Decimal::from(99)));
        let step = algo.on_event(
            &mut long,
            &mut short,
            ExecEvent::Failed { leg: LegRole::Short, kind: "被撤销".into(), reason: "x".into(), partial },
            1,
        );
        assert_eq!(step.status, ExecStatus::Failed("Short 腿被撤销: x".to_string()));
        assert_eq!(short.filled, partial);
        assert_eq!(long.filled, None);
    }

    #[test]
    fn tick_times_out_only_after_fill_timeout() {
        let (mut long, mut short) = (leg("a"), leg("b"));
        let mut algo = TakerTaker::new(1_000);
        assert_eq!(algo.on_tick(&mut long, &mut short, 5_000).status, ExecStatus::Running);
        algo.start(&mut long, &mut short, 10_000);
        assert_eq!(algo.on_tick(&mut long, &mut short, 11_000).status, ExecStatus::Running);
        assert!(matches!(algo.on_tick(&mut long, &mut short, 11_001).status, ExecStatus::Failed(_)));
    }
}
