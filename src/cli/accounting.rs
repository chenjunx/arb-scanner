//! `accounting` 子命令：独立的常驻进程，定期轮询交易所资金费流水，通过
//! `PositionManager::apply_adjustment`(`AdjustmentReason::Funding`)累加进对应
//! 仓位的 `realized_pnl`。跟踪对象是 `PositionManager`(Redis 支撑)里每次轮询时
//! 读到的当前非零仓位，而不是启动时固定的一份列表，所以
//! `open`/`close` 开平的期货仓位不需要重启这个进程就能被自动跟踪/停止跟踪。
//! 如果 `monitor` 已经在跑且没加 `--no-portfolio`，通常不需要单独起本命令；只需要
//! 资金费追踪、不想启动价差扫描和行情连接时单独使用。

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use dashmap::DashMap;
use log::info;

use crate::accounting::{FundingFeeTracker, RedisFundingCursorStore};
use crate::net;
use crate::types::Venue;

use super::wiring;

#[derive(Args, Debug)]
pub struct AccountingArgs {
    /// 使用币安测试网
    #[arg(long)]
    pub testnet: bool,

    /// 轮询资金费流水的间隔(秒)
    #[arg(long, default_value_t = 1800)]
    pub interval_secs: u64,

    /// 首次轮询时向前回溯多久(小时)，之后按 Redis 里的游标增量拉取
    #[arg(long, default_value_t = 168)]
    pub initial_lookback_hours: u64,
}

pub async fn run(args: AccountingArgs) -> anyhow::Result<()> {
    let AccountingArgs { testnet, interval_secs, initial_lookback_hours } = args;

    let proxy = net::proxy_from_env();
    let providers =
        wiring::binance_futures_funding_providers(Venue::new("binance_futures"), testnet, proxy.as_deref())?;

    let redis_url = wiring::redis_url();
    info!("accounting: connecting to redis at {redis_url}");
    let (position_manager, _portfolio_manager) = wiring::build_portfolio_stack(&redis_url, Arc::new(DashMap::new()))?;
    let cursor_store =
        RedisFundingCursorStore::new(&redis_url).context("failed to connect RedisFundingCursorStore to redis")?;

    let tracker = Arc::new(FundingFeeTracker::new(
        providers,
        position_manager,
        Arc::new(cursor_store),
        Duration::from_secs(interval_secs),
        Duration::from_secs(initial_lookback_hours * 3600),
    ));
    tracker.spawn();

    info!(
        "accounting: tracking funding fees testnet={testnet} interval_secs={interval_secs} initial_lookback_hours={initial_lookback_hours}, press ctrl-c to stop"
    );
    tokio::signal::ctrl_c().await.context("failed to listen for ctrl-c")?;
    info!("accounting: received ctrl-c, shutting down");
    Ok(())
}
