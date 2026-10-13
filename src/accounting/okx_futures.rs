use async_trait::async_trait;

use crate::order::OrderProvider;
use crate::order::okx_futures::OkxFuturesOrderProvider;
use crate::types::{Symbol, Venue};

use super::provider::{FundingFeeProvider, FundingIncomeRecord};

#[async_trait]
impl FundingFeeProvider for OkxFuturesOrderProvider {
    fn venue(&self) -> Venue {
        OrderProvider::venue(self)
    }

    async fn funding_income(&self, symbol: &Symbol, start_time_ms: Option<u64>) -> anyhow::Result<Vec<FundingIncomeRecord>> {
        self.funding_history(symbol, start_time_ms).await
    }
}
