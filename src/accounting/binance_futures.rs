use async_trait::async_trait;

use crate::order::OrderProvider;
use crate::order::binance_futures::BinanceFuturesOrderProvider;
use crate::types::{Symbol, Venue};

use super::provider::{FundingFeeProvider, FundingIncomeRecord};

#[async_trait]
impl FundingFeeProvider for BinanceFuturesOrderProvider {
    fn venue(&self) -> Venue {
        OrderProvider::venue(self)
    }

    async fn funding_income(&self, symbol: &Symbol, start_time_ms: Option<u64>) -> anyhow::Result<Vec<FundingIncomeRecord>> {
        self.income_history(symbol, start_time_ms).await
    }
}
