//! 命令行入口。`main.rs` 只负责进程级初始化(日志、rustls provider)，参数解析和
//! 子命令分发都在这里，各子命令的实现分散在同级的兄弟模块里。
//!
//! 放在 lib 而不是 bin 内部，是为了让 `tests/` 也能直接构造并断言这些参数结构体。

pub mod accounting;
pub mod args;
pub mod close;
pub mod engine_command;
pub mod monitor;
pub mod open;
pub mod reconcile_order;
pub mod report;
pub mod rotate;
pub mod scan;
pub mod set_position;
pub mod transfer;
pub mod wiring;

use clap::{Parser, Subcommand};

/// 不带子命令时使用的默认配置文件。
const DEFAULT_CONFIG_PATH: &str = "config.toml";

#[derive(Parser, Debug)]
#[command(
    name = "arb-scanner",
    version,
    about = "跨交易所套利扫描/执行工具",
    long_about = "不带子命令时读取配置文件(默认 config.toml)启动常驻套利引擎；\
                  各子命令是围绕它的一次性运维/手动操作工具。",
    // 默认引擎的配置文件位置是个位置参数，和子命令互斥：`arb-scanner config.toml`
    // 走引擎，`arb-scanner scan` 走子命令。
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    /// 常驻引擎的配置文件路径(不带子命令时生效)
    #[arg(value_name = "CONFIG")]
    pub config: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// 币安现货买入 + U 本位合约做空对冲开仓
    Open(open::OpenArgs),
    /// 一个交易所卖出、另一个交易所买入等量同一资产
    Rotate(rotate::RotateArgs),
    /// 平掉币安现货/Kraken 现货/币安合约中的任意几条腿
    Close(close::CloseArgs),
    /// 跨交易所划转资产
    Transfer(transfer::TransferArgs),
    /// 找出币安和副交易所都能交易且能互转的币种
    Scan(scan::ScanArgs),
    /// 持续监控币安/副交易所现货价差
    Monitor(monitor::MonitorArgs),
    /// 常驻轮询资金费流水并累加进仓位盈亏
    Accounting(accounting::AccountingArgs),
    /// 常驻定期打印组合盈亏/仓位/订单报告
    Report(report::ReportArgs),
    /// 用交易所 REST 结果核对/修正卡住的历史订单
    #[command(name = "reconcile-order")]
    ReconcileOrder(reconcile_order::ReconcileOrderArgs),
    /// 用交易所后台的真实持仓覆盖写 Redis 里的仓位记录
    #[command(name = "set-position")]
    SetPosition(set_position::SetPositionArgs),
}

/// 解析命令行并执行。参数非法时 clap 会自己打印用法并退出进程。
pub async fn run() -> anyhow::Result<()> {
    dispatch(Cli::parse()).await
}

pub async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Some(Command::Open(args)) => open::run(args).await,
        Some(Command::Rotate(args)) => rotate::run(args).await,
        Some(Command::Close(args)) => close::run(args).await,
        Some(Command::Transfer(args)) => transfer::run(args).await,
        Some(Command::Scan(args)) => scan::run(args).await,
        Some(Command::Monitor(args)) => monitor::run(args).await,
        Some(Command::Accounting(args)) => accounting::run(args).await,
        Some(Command::Report(args)) => report::run(args).await,
        Some(Command::ReconcileOrder(args)) => reconcile_order::run(args).await,
        Some(Command::SetPosition(args)) => set_position::run(args).await,
        None => engine_command::run(cli.config.as_deref().unwrap_or(DEFAULT_CONFIG_PATH)).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn clap_definition_is_valid() {
        Cli::command().debug_assert();
    }

    /// 不带子命令时的两种历史用法都要继续可用：裸跑用默认 config.toml，
    /// 或者把配置文件当第一个位置参数传进来。
    #[test]
    fn bare_invocation_falls_back_to_default_config() {
        let cli = Cli::try_parse_from(["arb-scanner"]).unwrap();
        assert!(cli.command.is_none());
        assert_eq!(cli.config, None);

        let cli = Cli::try_parse_from(["arb-scanner", "my-config.toml"]).unwrap();
        assert!(cli.command.is_none());
        assert_eq!(cli.config.as_deref(), Some("my-config.toml"));
    }

    #[test]
    fn subcommands_keep_their_hyphenated_names() {
        let cli = Cli::try_parse_from(["arb-scanner", "reconcile-order", "--order-id", "ORD-1"]).unwrap();
        assert!(matches!(cli.command, Some(Command::ReconcileOrder(_))));

        let cli = Cli::try_parse_from([
            "arb-scanner",
            "set-position",
            "--venue",
            "binance_spot",
            "--symbol",
            "APE/USDT",
            "--qty",
            "0",
        ])
        .unwrap();
        assert!(matches!(cli.command, Some(Command::SetPosition(_))));
    }

    /// `--live` 关掉 dry run，不传就是 dry run；两个一起传视为写法有歧义，直接报错。
    #[test]
    fn dry_run_defaults_on_and_live_turns_it_off() {
        let cli = Cli::try_parse_from(["arb-scanner", "open", "--symbol", "BTC/USDT", "--amount", "100"]).unwrap();
        let Some(Command::Open(args)) = cli.command else { panic!("expected open") };
        assert!(args.dry.is_dry_run());

        let cli = Cli::try_parse_from(["arb-scanner", "open", "--symbol", "BTC/USDT", "--amount", "100", "--live"])
            .unwrap();
        let Some(Command::Open(args)) = cli.command else { panic!("expected open") };
        assert!(!args.dry.is_dry_run());

        assert!(Cli::try_parse_from(["arb-scanner", "open", "--live", "--dry-run"]).is_err());
    }

    #[test]
    fn symbol_must_be_base_slash_quote() {
        assert!(Cli::try_parse_from(["arb-scanner", "close", "--symbol", "BTCUSDT"]).is_err());
        let cli = Cli::try_parse_from(["arb-scanner", "close", "--symbol", "BTC/USDT", "--futures-qty", "1"]).unwrap();
        let Some(Command::Close(args)) = cli.command else { panic!("expected close") };
        assert_eq!(args.symbol.base.as_ref(), "BTC");
        assert_eq!(args.symbol.quote.as_ref(), "USDT");
    }
}
