//! 子命令之间共享的参数片段和解析器。
//!
//! `clap` 的 `#[command(flatten)]` 让这些片段可以像 mixin 一样被各子命令复用，
//! 避免以前手写 argv 循环时每个命令都把 `--testnet/--live/--dry-run/
//! --client-order-id-prefix` 逐字抄一遍。

use std::time::Duration;

use clap::Args;

use crate::types::Symbol;

/// `--symbol BTC/USDT` 的解析器。`Symbol` 本身没有实现 `FromStr`(它是
/// base/quote 两段结构，没有唯一的字符串表示约定)，所以这里给 clap 提供一个
/// 显式的 `value_parser`，错误信息和原来手写解析时保持一致。
pub fn parse_symbol(raw: &str) -> Result<Symbol, String> {
    let (base, quote) = raw
        .split_once('/')
        .ok_or_else(|| "must be in Base/Quote format, e.g. BTC/USDT".to_string())?;
    if base.is_empty() || quote.is_empty() {
        return Err("must be in Base/Quote format, e.g. BTC/USDT".to_string());
    }
    Ok(Symbol::new(base, quote))
}

/// 当前 unix 毫秒时间戳。`transfer`/`reconcile-order`/`set-position` 都要生成
/// 一个"现在"的时间戳，以前各自抄了一遍同样的 `SystemTime` 三连。
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// 所有"会真的动钱"的子命令(`open`/`close`/`rotate`/`transfer`)共享的开关。
///
/// 默认 dry run：不传 `--live` 就只做链路校验，不下单/不提币。
///
/// **和旧版手写解析的唯一差异**：以前 `--dry-run --live` 同时传是允许的
/// (后出现的那个生效)，现在 clap 会直接报冲突。这个组合本来就是有歧义的写法，
/// 明确报错比"看谁写在后面"更安全。
#[derive(Args, Debug, Clone)]
pub struct DryRunArgs {
    /// 使用交易所测试网(只影响 binance 一侧，Kraken 客户端不支持 testnet)
    #[arg(long)]
    pub testnet: bool,

    /// 真实下单/提币。不传则为 dry run，只做链路校验
    #[arg(long)]
    pub live: bool,

    /// 显式声明本次只做 dry run(默认行为，保留以兼容既有脚本)
    #[arg(long, conflicts_with = "live")]
    pub dry_run: bool,

    /// client_order_id 前缀，便于在交易所后台按前缀筛出本次操作的订单
    #[arg(long)]
    pub client_order_id_prefix: Option<String>,
}

impl DryRunArgs {
    pub fn is_dry_run(&self) -> bool {
        !self.live
    }
}

/// `open`/`close`/`rotate` 在 [`DryRunArgs`] 之外还共享的等待成交超时。
#[derive(Args, Debug, Clone)]
pub struct FillTimeoutArgs {
    /// 等待市价单成交的超时(秒)
    #[arg(long, default_value_t = 60)]
    pub fill_timeout_secs: u64,
}

impl FillTimeoutArgs {
    pub fn fill_timeout(&self) -> Duration {
        Duration::from_secs(self.fill_timeout_secs)
    }
}
