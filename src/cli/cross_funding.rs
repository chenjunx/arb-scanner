//! `cross-funding` 子命令：读 CSV 里的跨所永续合约对，常驻运行
//! [`CrossFundingStrategy`]，价差达到 EWMA 均值 + k·σ 时两边市价开仓。
//!
//! 默认 dry run：只打日志、不连 Redis、不建订单流水线(启动检查仍然要查持仓，
//! 所以 API Key 照样需要)。`--live` 才真的下单，并默认带上两个后台服务：每 5 分钟
//! 打印一次组合盈亏/仓位/订单报告(`--report-interval-secs`)，每 30 分钟把四家的
//! 资金费流水累加进仓位盈亏(`--funding-interval-secs`，按交易所抢 Redis 租约，
//! 和 `accounting`/`monitor` 同时跑也不会重复记账)。

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, ValueEnum};
use log::info;
use rust_decimal::Decimal;

use crate::accounting::FundingFeeProvider;
use crate::engine::ArbitrageEngine;
use crate::exchange_info::binance::BinanceExchangeInfoProvider;
use crate::exchange_info::bitget::BitgetExchangeInfoProvider;
use crate::exchange_info::gate::GateExchangeInfoProvider;
use crate::exchange_info::okx::OkxExchangeInfoProvider;
use crate::market_data::MarketDataSource;
use crate::market_data::binance_futures::{BinanceFuturesKlineClient, BinanceFuturesSource};
use crate::market_data::bitget_futures::{BitgetFuturesKlineClient, BitgetFuturesSource};
use crate::market_data::gate_futures::{GateFuturesKlineClient, GateFuturesSource};
use crate::market_data::okx_futures::{OkxFuturesKlineClient, OkxFuturesSource};
use crate::net;
use crate::order::OrderProvider;
use crate::order::binance_futures::{BinanceFuturesOrderProvider, BinanceFuturesUserDataStream};
use crate::order::bitget_futures::{BitgetFuturesOrderProvider, BitgetFuturesPrivateOrderStream};
use crate::order::gate_futures::{GateFuturesOrderProvider, GateFuturesPrivateOrderStream};
use crate::order::okx_futures::{OkxFuturesOrderProvider, OkxFuturesPrivateOrderStream};
use crate::order::types::MarginMode;
use crate::order_manager::OrderStreamSource;
use crate::strategy::cross_funding::{
    CrossFundingParams, CrossFundingStrategy, PairRow, PerpExchange, VenueClients, parse_pairs_csv,
};
use crate::topic::TopicBus;
use crate::types::{Symbol, Venue};

use super::wiring::{self, ManualLeg};

#[derive(Args, Debug)]
pub struct CrossFundingArgs {
    /// 合约对 CSV，表头 long_venue,long_symbol,short_venue,short_symbol,quantity；
    /// venue 填 binance/gate/okx/bitget，symbol 填交易所原生合约名(如 1000PEPEUSDT、
    /// PEPE_USDT、PEPE-USDT-SWAP)，quantity 是实际币数
    #[arg(long)]
    pub csv: String,

    /// 真实下单。不传则为 dry run，触发时只打日志
    #[arg(long)]
    pub live: bool,

    /// 使用币安测试网(只影响 binance 一侧)
    #[arg(long)]
    pub testnet: bool,

    /// 启动/整点刷新时拉多少根 1h K 线
    #[arg(long, default_value_t = 72)]
    pub kline_hours: usize,

    /// 两边对齐后至少要有多少根 K 线
    #[arg(long, default_value_t = 48)]
    pub min_samples: usize,

    /// EWMA 半衰期(根数)
    #[arg(long, default_value_t = 36.0)]
    pub half_life: f64,

    /// 触发阈值 = μ + sigma_k·σ
    #[arg(long, default_value_t = 1.0)]
    pub sigma_k: f64,

    /// 启动时两边每币价格相差超过多少百分比就跳过这一行
    #[arg(long, default_value_t = Decimal::from(5))]
    pub max_price_diff_pct: Decimal,

    /// 下单后多少秒两腿还没全部成交就停止这一行
    #[arg(long, default_value_t = 30)]
    pub fill_timeout_secs: u64,

    /// OKX/Bitget 下单带的保证金模式，必须和交易所里该合约的设置一致
    /// (币安/Gate 不需要，沿用账户设置)
    #[arg(long, value_enum, default_value_t = MarginModeArg::Cross)]
    pub margin_mode: MarginModeArg,

    /// --live 时定期打印组合盈亏/仓位/订单报告的间隔(秒)，0 表示不打印；
    /// dry run 不连 Redis，不打印报告
    #[arg(long, default_value_t = 300)]
    pub report_interval_secs: u64,

    /// --live 时轮询资金费流水、累加进仓位盈亏的间隔(秒)，0 表示不记账。
    /// 多个进程同时记账时按交易所抢 Redis 租约，同一交易所只有一个进程在记
    #[arg(long, default_value_t = 1800)]
    pub funding_interval_secs: u64,

    /// 某个仓位第一次记资金费时向前回溯多久(小时)，之后按 Redis 里的游标增量拉取
    #[arg(long, default_value_t = 168)]
    pub funding_initial_lookback_hours: u64,
}

/// 同一个下单 provider 同时当下单客户端和资金费查询客户端用。
fn order_and_funding<P>(provider: P) -> (Arc<dyn OrderProvider>, Arc<dyn FundingFeeProvider>)
where
    P: OrderProvider + FundingFeeProvider + 'static,
{
    let provider = Arc::new(provider);
    (provider.clone(), provider)
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarginModeArg {
    /// 全仓
    Cross,
    /// 逐仓
    Isolated,
}

impl From<MarginModeArg> for MarginMode {
    fn from(arg: MarginModeArg) -> Self {
        match arg {
            MarginModeArg::Cross => MarginMode::Cross,
            MarginModeArg::Isolated => MarginMode::Isolated,
        }
    }
}

impl CrossFundingArgs {
    fn params(&self) -> anyhow::Result<CrossFundingParams> {
        if self.kline_hours == 0 || self.min_samples == 0 || self.min_samples > self.kline_hours {
            anyhow::bail!(
                "--min-samples({}) 必须在 1..=--kline-hours({}) 之间",
                self.min_samples,
                self.kline_hours
            );
        }
        if !(self.half_life.is_finite() && self.half_life > 0.0) {
            anyhow::bail!("--half-life 必须为正数");
        }
        if !self.sigma_k.is_finite() {
            anyhow::bail!("--sigma-k 必须是有限数");
        }
        Ok(CrossFundingParams {
            kline_hours: self.kline_hours,
            min_samples: self.min_samples,
            half_life: self.half_life,
            sigma_k: self.sigma_k,
            max_price_diff_pct: self.max_price_diff_pct,
            fill_timeout_ms: self.fill_timeout_secs * 1000,
            live: self.live,
            ..CrossFundingParams::default()
        })
    }
}

/// 每个用到的交易所各自的 symbol 列表(去重)。
fn symbols_by_exchange(rows: &[PairRow]) -> HashMap<PerpExchange, Vec<Symbol>> {
    let mut sets: HashMap<PerpExchange, BTreeSet<(String, String)>> = HashMap::new();
    for row in rows {
        for leg in [&row.long, &row.short] {
            sets.entry(leg.exchange)
                .or_default()
                .insert((leg.symbol.base.to_string(), leg.symbol.quote.to_string()));
        }
    }
    sets.into_iter()
        .map(|(exchange, set)| (exchange, set.into_iter().map(|(b, q)| Symbol::new(b, q)).collect()))
        .collect()
}

pub async fn run(args: CrossFundingArgs) -> anyhow::Result<()> {
    let params = args.params()?;
    let text = std::fs::read_to_string(&args.csv).with_context(|| format!("failed to read csv {}", args.csv))?;
    let rows = parse_pairs_csv(&text).with_context(|| format!("failed to parse csv {}", args.csv))?;
    if rows.is_empty() {
        anyhow::bail!("csv {} 里没有任何合约对", args.csv);
    }
    let symbols = symbols_by_exchange(&rows);
    let all_symbols: Vec<Symbol> = symbols.values().flatten().cloned().collect();

    let proxy = net::proxy_from_env();
    let testnet = args.testnet;
    let margin_mode = MarginMode::from(args.margin_mode);
    let bus = Arc::new(TopicBus::new());

    let mut clients = HashMap::new();
    let mut legs = Vec::new();
    let mut funding_providers: HashMap<Venue, Arc<dyn FundingFeeProvider>> = HashMap::new();
    let mut sources: Vec<Box<dyn MarketDataSource>> = Vec::new();
    for (&exchange, exchange_symbols) in &symbols {
        let venue = exchange.venue();
        let (client, stream, source, funding): (
            VenueClients,
            Box<dyn OrderStreamSource>,
            Box<dyn MarketDataSource>,
            Arc<dyn FundingFeeProvider>,
        ) = match exchange {
            PerpExchange::Binance => {
                let (orders, funding) =
                    order_and_funding(BinanceFuturesOrderProvider::from_env(venue.clone(), testnet, proxy.as_deref())?);
                (
                    VenueClients {
                        exchange_info: Arc::new(BinanceExchangeInfoProvider::from_env(
                            venue.clone(),
                            testnet,
                            proxy.as_deref(),
                        )?),
                        klines: Arc::new(BinanceFuturesKlineClient::new(venue.clone(), testnet, proxy.as_deref())?),
                        orders,
                    },
                    Box::new(
                        BinanceFuturesUserDataStream::from_env(
                            venue.clone(),
                            testnet,
                            proxy.as_deref(),
                            exchange_symbols.clone(),
                        )
                        .context("failed to build binance futures user data stream")?,
                    ),
                    Box::new(BinanceFuturesSource::new(venue.clone(), exchange_symbols.clone(), testnet, proxy.clone())),
                    funding,
                )
            }
            PerpExchange::Gate => {
                let (orders, funding) =
                    order_and_funding(GateFuturesOrderProvider::from_env(venue.clone(), proxy.as_deref())?);
                (
                    VenueClients {
                        exchange_info: Arc::new(GateExchangeInfoProvider::from_env(venue.clone(), proxy.as_deref())?),
                        klines: Arc::new(GateFuturesKlineClient::new(venue.clone(), proxy.as_deref())?),
                        orders,
                    },
                    Box::new(
                        GateFuturesPrivateOrderStream::from_env(venue.clone(), proxy.as_deref())
                            .context("failed to build gate futures order stream")?,
                    ),
                    Box::new(GateFuturesSource::new(venue.clone(), exchange_symbols.clone(), proxy.clone())),
                    funding,
                )
            }
            PerpExchange::Okx => {
                let (orders, funding) = order_and_funding(OkxFuturesOrderProvider::from_env(
                    venue.clone(),
                    margin_mode,
                    proxy.as_deref(),
                )?);
                (
                    VenueClients {
                        exchange_info: Arc::new(OkxExchangeInfoProvider::new(venue.clone(), proxy.as_deref())?),
                        klines: Arc::new(OkxFuturesKlineClient::new(venue.clone(), proxy.as_deref())?),
                        orders,
                    },
                    Box::new(
                        OkxFuturesPrivateOrderStream::from_env(venue.clone(), proxy.as_deref())
                            .context("failed to build okx order stream")?,
                    ),
                    Box::new(OkxFuturesSource::new(venue.clone(), exchange_symbols.clone(), proxy.clone())),
                    funding,
                )
            }
            PerpExchange::Bitget => {
                let (orders, funding) = order_and_funding(BitgetFuturesOrderProvider::from_env(
                    venue.clone(),
                    margin_mode,
                    proxy.as_deref(),
                )?);
                (
                    VenueClients {
                        exchange_info: Arc::new(BitgetExchangeInfoProvider::new(venue.clone(), proxy.as_deref())?),
                        klines: Arc::new(BitgetFuturesKlineClient::new(venue.clone(), proxy.as_deref())?),
                        orders,
                    },
                    Box::new(
                        BitgetFuturesPrivateOrderStream::from_env(venue.clone(), proxy.as_deref())
                            .context("failed to build bitget order stream")?,
                    ),
                    Box::new(BitgetFuturesSource::new(venue.clone(), exchange_symbols.clone(), proxy.clone())),
                    funding,
                )
            }
        };
        legs.push(ManualLeg {
            venue: venue.clone(),
            provider: client.orders.clone(),
            stream,
            limits: wiring::manual_risk_limits(),
        });
        funding_providers.insert(venue.clone(), funding);
        clients.insert(venue, client);
        sources.push(source);
    }

    let (order_manager, stream_handles) = if args.live {
        let services = wiring::PipelineServices {
            report_interval: (args.report_interval_secs > 0).then(|| Duration::from_secs(args.report_interval_secs)),
            funding: (args.funding_interval_secs > 0).then(|| wiring::FundingSettings {
                providers: funding_providers,
                poll_interval: Duration::from_secs(args.funding_interval_secs),
                initial_lookback: Duration::from_secs(args.funding_initial_lookback_hours * 3600),
            }),
        };
        let pipeline = wiring::build_manual_pipeline(
            &wiring::redis_url(),
            bus.clone(),
            &all_symbols,
            legs,
            HashMap::new(),
            services,
        )
        .await?;
        (Some(pipeline.order_manager), pipeline.stream_handles)
    } else {
        info!("cross_funding: dry run 模式，不连 Redis、不下单；加 --live 才会真实下单");
        (None, Vec::new())
    };

    let source_handles: Vec<_> = sources.into_iter().map(|s| s.spawn(bus.clone())).collect();

    let strategy = CrossFundingStrategy::new(bus.clone(), rows, clients, params, order_manager);
    let result = ArbitrageEngine::new(vec![Box::new(strategy)]).run(bus).await;

    for handle in stream_handles.into_iter().chain(source_handles) {
        handle.abort();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command};
    use clap::Parser;

    #[test]
    fn parses_defaults_and_validates_params() {
        let cli = Cli::try_parse_from(["arb-scanner", "cross-funding", "--csv", "pairs.csv"]).unwrap();
        let Some(Command::CrossFunding(args)) = cli.command else { panic!("expected cross-funding") };
        assert!(!args.live);
        let params = args.params().unwrap();
        assert_eq!(params.kline_hours, 72);
        assert_eq!(params.min_samples, 48);
        assert_eq!(params.half_life, 36.0);
        assert_eq!(params.fill_timeout_ms, 30_000);
        assert_eq!(params.max_price_diff_pct, Decimal::from(5));
        assert_eq!(args.margin_mode, MarginModeArg::Cross);
        assert_eq!(args.report_interval_secs, 300);
        assert_eq!(args.funding_interval_secs, 1800);
        assert_eq!(args.funding_initial_lookback_hours, 168);

        let cli =
            Cli::try_parse_from(["arb-scanner", "cross-funding", "--csv", "pairs.csv", "--margin-mode", "isolated"]).unwrap();
        let Some(Command::CrossFunding(args)) = cli.command else { panic!("expected cross-funding") };
        assert_eq!(MarginMode::from(args.margin_mode), MarginMode::Isolated);

        let cli = Cli::try_parse_from([
            "arb-scanner",
            "cross-funding",
            "--csv",
            "pairs.csv",
            "--kline-hours",
            "24",
            "--min-samples",
            "48",
        ])
        .unwrap();
        let Some(Command::CrossFunding(args)) = cli.command else { panic!("expected cross-funding") };
        assert!(args.params().is_err());
    }

    #[test]
    fn groups_symbols_by_exchange() {
        let rows = parse_pairs_csv(
            "long_venue,long_symbol,short_venue,short_symbol,quantity\n\
             gate,PEPE_USDT,binance,1000PEPEUSDT,10000000\n\
             binance,BTCUSDT,gate,PEPE_USDT,0.01\n",
        )
        .unwrap();
        let symbols = symbols_by_exchange(&rows);
        assert_eq!(symbols[&PerpExchange::Gate], vec![Symbol::new("PEPE", "USDT")]);
        assert_eq!(
            symbols[&PerpExchange::Binance],
            vec![Symbol::new("1000PEPE", "USDT"), Symbol::new("BTC", "USDT")]
        );
    }
}
