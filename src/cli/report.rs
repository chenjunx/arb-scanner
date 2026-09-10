//! `report` 子命令：独立的常驻进程，定期把投资组合盈亏/仓位明细/订单概览汇总
//! 成一份报告并分发给各个已注册的 `ReportChannel`(目前只有 `LogChannel`)。只连接
//! Redis 读取数据，不接入实时行情，所以报告里的 `market_value`/
//! `unrealized_pnl` 会显示为 "N/A"(和 `accounting` 命令同样的既有限制，见
//! `crate::report::sections::PortfolioSection` 的说明；如果通过 `monitor`
//! (未加 `--no-portfolio`)驱动，接了实时行情，会有真实数字)。如果 `monitor` 已经
//! 在跑且没加 `--no-portfolio`，通常不需要单独起本命令；只需要报告、不想启动价差
//! 扫描和行情连接时单独使用。

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use dashmap::DashMap;
use log::info;

use super::wiring;

#[derive(Args, Debug)]
pub struct ReportArgs {
    /// 出报告的间隔(秒)
    #[arg(long, default_value_t = 300)]
    pub interval_secs: u64,
}

pub async fn run(args: ReportArgs) -> anyhow::Result<()> {
    let interval_secs = args.interval_secs;

    let redis_url = wiring::redis_url();
    info!("report: connecting to redis at {redis_url}");
    let (_position_manager, portfolio_manager) = wiring::build_portfolio_stack(&redis_url, Arc::new(DashMap::new()))?;

    wiring::spawn_report_tracker(&redis_url, portfolio_manager, Duration::from_secs(interval_secs))?;

    info!("report: reporting every interval_secs={interval_secs}, press ctrl-c to stop");
    tokio::signal::ctrl_c().await.context("failed to listen for ctrl-c")?;
    info!("report: received ctrl-c, shutting down");
    Ok(())
}
