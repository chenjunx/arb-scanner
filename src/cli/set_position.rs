//! `set-position` 子命令：用交易所后台核对出的真实持仓，覆盖写 Redis 里
//! `PositionManager` 记的 `net_qty`/`avg_price`——用于修正历史 bug(WS 成交被
//! REST 轮询覆盖、跨进程订单号碰撞导致重复计成交等)遗留下来的脏数据，这些
//! 数据是纯增量累加出来的，代码修好之后也不会自动纠正；也用于手动补单后把
//! 交易所上的真实持仓补录进来。只支持 [`VENUES`] 里列出的、真实下过单的
//! venue，和 `reconcile-order` 一样默认只读、加 `--confirm` 才真正覆盖写入。
//!
//! `--qty` 填交易所界面显示的持仓数量(合约 base 单位)：所有 venue 都一样，
//! `gate_futures`/`okx_futures` 也是币数而不是张数——张数只在下单网关里换算。

use clap::Args;
use log::info;
use rust_decimal::Decimal;

use crate::position::{PositionStore, RedisPositionStore, VenuePosition};
use crate::types::{Symbol, Venue};

use anyhow::Context;

use super::args::{now_ms, parse_symbol};
use super::wiring;

/// 各下单链路实际写进仓位记录的 venue 名：`binance`/`kraken` 是常驻引擎按
/// config.toml 里的 venue 名下单，其余是各子命令写死的名字。限制住可写范围，
/// 避免手滑把 venue 名打错后凭空写出一条无人认领的仓位记录。
const VENUES: [&str; 9] = [
    "binance",
    "binance_spot",
    "binance_futures",
    "kraken",
    "kraken_spot",
    "gate_spot",
    "gate_futures",
    "okx_futures",
    "bitget_futures",
];

fn parse_venue(raw: &str) -> Result<Venue, String> {
    if VENUES.contains(&raw) {
        Ok(Venue::new(raw))
    } else {
        Err(format!("只支持 {}，收到 '{raw}'", VENUES.join("/")))
    }
}

#[derive(Args, Debug)]
pub struct SetPositionArgs {
    /// 要覆盖写的场所：binance / binance_spot / binance_futures / kraken /
    /// kraken_spot / gate_spot / gate_futures / okx_futures / bitget_futures
    #[arg(long, value_parser = parse_venue)]
    pub venue: Venue,

    /// 交易对，如 APE/USDT
    #[arg(long, value_parser = parse_symbol)]
    pub symbol: Symbol,

    /// 交易所后台查到的真实净持仓量(正=多头/净持有，负=空头)；
    /// 填交易所界面显示的数量(合约 base 单位，Gate/OKX 也填币数不填张数)
    #[arg(long)]
    pub qty: Decimal,

    /// 交易所后台查到的真实持仓均价，`--qty` 非 0 时必填
    #[arg(long)]
    pub avg_price: Option<Decimal>,

    /// 真正覆盖写入 Redis(默认只读打印)
    #[arg(long)]
    pub confirm: bool,
}

pub async fn run(args: SetPositionArgs) -> anyhow::Result<()> {
    let SetPositionArgs { venue, symbol, qty, avg_price, confirm } = args;

    if !qty.is_zero() && avg_price.is_none() {
        anyhow::bail!("--qty 非 0 时必须提供 --avg-price (交易所后台查到的真实持仓均价)");
    }
    let avg_price = if qty.is_zero() { None } else { avg_price };

    let redis_url = wiring::redis_url();
    info!("set-position: connecting to redis at {redis_url}");
    let store = RedisPositionStore::new(&redis_url).context("failed to connect RedisPositionStore to redis")?;

    let current = store.get(&venue, &symbol);
    println!("当前 Redis 里的记录: {current:#?}");
    println!(
        "将要写入: venue={venue} symbol={symbol} net_qty={qty} avg_price={}",
        avg_price.map(|p| p.to_string()).unwrap_or_else(|| "None".to_string())
    );

    if !confirm {
        println!("只读模式(默认)：尚未写入 Redis。确认以上数字和交易所后台一致后，加 --confirm 重新执行以落库。");
        return Ok(());
    }

    info!("set-position: --confirm 已指定，覆盖写入 Redis");
    let ts_ms = now_ms();
    let venue_for_write = venue.clone();
    let symbol_for_write = symbol.clone();
    store.update(
        &venue,
        &symbol,
        Box::new(move |_current| VenuePosition {
            venue: venue_for_write,
            symbol: symbol_for_write,
            net_qty: qty,
            avg_price,
            total_fees: std::collections::HashMap::new(),
            realized_pnl: Decimal::ZERO,
            pending_qty: Decimal::ZERO,
            updated_at_ms: ts_ms,
        }),
    );

    let updated = store.get(&venue, &symbol);
    println!("写入后的记录: {updated:#?}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_venue_accepts_known_venues_and_rejects_typos() {
        for venue in VENUES {
            assert_eq!(parse_venue(venue).unwrap(), Venue::new(venue));
        }
        assert!(parse_venue("bitget").is_err());
        assert!(parse_venue("gate_future").is_err());
    }
}
