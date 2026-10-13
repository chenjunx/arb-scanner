use async_trait::async_trait;

use crate::order::OrderProvider;
use crate::order::gate_futures::GateFuturesOrderProvider;
use crate::types::{Symbol, Venue};

use super::provider::{FundingFeeProvider, FundingIncomeRecord};

#[async_trait]
impl FundingFeeProvider for GateFuturesOrderProvider {
    fn venue(&self) -> Venue {
        OrderProvider::venue(self)
    }

    async fn funding_income(&self, symbol: &Symbol, start_time_ms: Option<u64>) -> anyhow::Result<Vec<FundingIncomeRecord>> {
        self.funding_history(symbol, start_time_ms).await
    }
}
