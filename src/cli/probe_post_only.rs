//! `probe-post-only` 临时子命令：对一家交易所下一笔 post-only 限价单，打印下单返回、
//! 延迟后的查单状态和撤单结果，用来核实各所对"会吃单的 post-only 单"的真实表现。
//! 排查完即可删除(本文件 + `cli/mod.rs` 里的两处引用)。
//!
//! 不传 `--live` 只打印将要发送的请求，不下单。
//!
//! 风险：post-only 若没被交易所当 post-only 处理，穿价单会按 taker 真实成交。
//! 用最小下单量、小币种。永续(gate_futures/okx)的 `--qty` 要是 1 张的整数倍。

use std::sync::Arc;
use std::time::Duration;

use clap::Args;
use rust_decimal::Decimal;

use crate::exchange_info::ExchangeInfoProvider;
use crate::exchange_info::gate::GateExchangeInfoProvider;
use crate::exchange_info::okx::OkxExchangeInfoProvider;
use crate::net;
use crate::order::binance::BinanceOrderProvider;
use crate::order::binance_futures::BinanceFuturesOrderProvider;
use crate::order::bitget_futures::BitgetFuturesOrderProvider;
use crate::order::contract_size::ContractSizeCache;
use crate::order::gate::GateOrderProvider;
use crate::order::gate_futures::GateFuturesOrderProvider;
use crate::order::kraken::KrakenOrderProvider;
use crate::order::okx_futures::OkxFuturesOrderProvider;
use crate::order::types::{LimitOrderRequest, MarginMode, OrderResult, OrderSide, OrderStatus};
use crate::order::{OrderProvider, order_outcome_unknown};
use crate::types::{Symbol, Venue};

use super::args::{now_ms, parse_symbol};
use super::cross_funding::MarginModeArg;

const VENUES: &str = "binance | binance_futures | gate | gate_futures | okx | bitget | kraken";

#[derive(Args, Debug)]
pub struct ProbePostOnlyArgs {
    /// 交易所：binance | binance_futures | gate | gate_futures | okx | bitget | kraken
    #[arg(long)]
    pub venue: String,

    /// 交易对 Base/Quote，如 BTC/USDT(Kraken 用 BTC/USD)
    #[arg(long, value_parser = parse_symbol)]
    pub symbol: Symbol,

    /// buy | sell。buy 时价格高于卖一会穿价，sell 时价格低于买一会穿价
    #[arg(long)]
    pub side: String,

    /// 限价
    #[arg(long)]
    pub price: Decimal,

    /// 数量(币数)
    #[arg(long)]
    pub qty: Decimal,

    /// 真实下单；不传只打印请求
    #[arg(long)]
    pub live: bool,

    /// 币安测试网(只影响 binance/binance_futures)
    #[arg(long)]
    pub testnet: bool,

    /// OKX/Bitget 保证金模式
    #[arg(long, value_enum, default_value_t = MarginModeArg::Cross)]
    pub margin_mode: MarginModeArg,
}

async fn build_provider(args: &ProbePostOnlyArgs) -> anyhow::Result<Arc<dyn OrderProvider>> {
    let proxy = net::proxy_from_env();
    let proxy = proxy.as_deref();
    let margin_mode = MarginMode::from(args.margin_mode);
    Ok(match args.venue.as_str() {
        "binance" => Arc::new(BinanceOrderProvider::from_env(Venue::new("binance_spot"), args.testnet, proxy)?),
        "binance_futures" => {
            Arc::new(BinanceFuturesOrderProvider::from_env(Venue::new("binance_futures"), args.testnet, proxy)?)
        }
        "gate" => Arc::new(GateOrderProvider::from_env(Venue::new("gate_spot"), proxy)?),
        "gate_futures" => {
            let venue = Venue::new("gate_futures");
            let info: Arc<dyn ExchangeInfoProvider> = Arc::new(GateExchangeInfoProvider::from_env(venue.clone(), proxy)?);
            Arc::new(GateFuturesOrderProvider::from_env(venue, Arc::new(ContractSizeCache::new(info)), proxy)?)
        }
        "okx" => {
            let venue = Venue::new("okx_futures");
            let info: Arc<dyn ExchangeInfoProvider> = Arc::new(OkxExchangeInfoProvider::new(venue.clone(), proxy)?);
            Arc::new(OkxFuturesOrderProvider::from_env(venue, margin_mode, Arc::new(ContractSizeCache::new(info)), proxy)?)
        }
        "bitget" => Arc::new(BitgetFuturesOrderProvider::from_env(Venue::new("bitget_futures"), margin_mode, proxy)?),
        "kraken" => Arc::new(KrakenOrderProvider::from_env(Venue::new("kraken_spot"), proxy)?),
        other => anyhow::bail!("unknown venue '{other}', expected {VENUES}"),
    })
}

fn is_terminal(status: OrderStatus) -> bool {
    !matches!(status, OrderStatus::New | OrderStatus::PartiallyFilled)
}

fn print_result(label: &str, result: &anyhow::Result<OrderResult>) {
    match result {
        Ok(r) => println!(
            "[{label}] OK status={:?} filled_qty={} avg_price={:?} order_id={}",
            r.status, r.filled_qty, r.avg_price, r.order_id
        ),
        Err(e) => println!("[{label}] ERR outcome_unknown={} err={e:#}", order_outcome_unknown(e)),
    }
}

pub async fn run(args: ProbePostOnlyArgs) -> anyhow::Result<()> {
    let side = match args.side.to_ascii_lowercase().as_str() {
        "buy" => OrderSide::Buy,
        "sell" => OrderSide::Sell,
        other => anyhow::bail!("--side must be buy or sell, got '{other}'"),
    };
    let client_order_id = format!("probe{}", now_ms());
    let req = LimitOrderRequest {
        symbol: args.symbol.clone(),
        side,
        quantity: args.qty,
        price: args.price,
        client_order_id: Some(client_order_id),
        post_only: true,
        dry_run: false,
    };
    println!("[request] venue={} {req:?}", args.venue);
    if !args.live {
        println!("[request] dry run, pass --live to actually place the order");
        return Ok(());
    }

    let provider = build_provider(&args).await?;
    let placed = provider.place_limit_order(req).await;
    print_result("place", &placed);

    let Ok(placed) = placed else {
        println!("[summary] place failed synchronously -> would be marked Rejected/RejectedByExchange");
        return Ok(());
    };

    tokio::time::sleep(Duration::from_secs(2)).await;
    let queried = provider.query_order(&args.symbol, &placed.order_id).await;
    print_result("query_after_2s", &queried);

    let live_on_exchange = match &queried {
        Ok(r) => !is_terminal(r.status),
        Err(_) => !is_terminal(placed.status),
    };
    if live_on_exchange {
        let cancelled = provider.cancel_order(&args.symbol, &placed.order_id).await;
        match &cancelled {
            Ok(()) => println!("[cancel] OK"),
            Err(e) => println!("[cancel] ERR {e:#}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let queried = provider.query_order(&args.symbol, &placed.order_id).await;
        print_result("query_after_cancel", &queried);
    } else {
        println!("[cancel] skipped, order already terminal");
    }
    Ok(())
}
