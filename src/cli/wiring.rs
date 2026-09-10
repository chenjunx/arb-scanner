//! 子命令共享的依赖装配代码：Redis 连接、订单流水线、按名字构造各交易所的
//! provider/行情源。
//!
//! 这里只放**两个以上**子命令用得到的东西；只被单个命令用到的装配逻辑留在那个
//! 命令自己的模块里(例如 `cross_exchange_execution` 的执行依赖只有默认引擎会
//! 用，就留在 [`super::engine_command`])。

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use dashmap::DashMap;
use log::info;
use rust_decimal::Decimal;

use crate::accounting::FundingFeeProvider;
use crate::exchange_info::ExchangeInfoProvider;
use crate::exchange_info::coinex::CoinexExchangeInfoProvider;
use crate::exchange_info::kraken::KrakenExchangeInfoProvider;
use crate::exchange_info::nonkyc::NonkycExchangeInfoProvider;
use crate::market_data::MarketDataSource;
use crate::market_data::kraken::KrakenSpotSource;
use crate::order::OrderProvider;
use crate::order::binance::{BinanceOrderProvider, BinanceUserDataStream};
use crate::order::binance_futures::{BinanceFuturesOrderProvider, BinanceFuturesUserDataStream};
use crate::order::gate::{GateOrderProvider, GatePrivateOrderStream};
use crate::order::kraken::{KrakenOrderProvider, KrakenPrivateOrderStream};
use crate::order_manager::risk_service::{AssetImbalanceLimit, RiskLimits};
use crate::order_manager::{
    ExchangeAdapter, ExecutionService, InMemoryOrderStore, OrderManager, OrderStore, OrderStreamSource,
    RedisOrderIdAllocator, RedisOrderStore, RiskService,
};
use crate::portfolio::PortfolioManager;
use crate::position::{InMemoryPositionStore, PositionManager, RedisAdjustmentLog, RedisPositionStore};
use crate::pricing::FeeUsdtConverter;
use crate::strategy::manual::ManualStrategy;
use crate::topic::TopicBus;
use crate::types::{Quote, Symbol, Venue};
use crate::wallet::WalletProvider;
use crate::wallet::binance::BinanceWalletProvider;
use crate::wallet::coinex::CoinexWalletProvider;
use crate::wallet::kraken::KrakenWalletProvider;
use crate::wallet::nonkyc::NonkycWalletProvider;

/// 等每条私有 WS 建连+鉴权/订阅完成的超时。
const STREAM_READY_TIMEOUT: Duration = Duration::from_secs(20);

/// 所有需要 Redis 的子命令统一从这里取连接串，避免各处重复抄同一行 fallback。
pub fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

/// 手动子命令(`open`/`rotate`/`close`)给每条腿用的默认风控上限：金额/仓位不设限
/// (数量是调用方在命令行里显式算好传进来的)，只保留每窗口最多 3 单的频率闸，
/// 防手滑重复执行。
pub fn manual_risk_limits() -> RiskLimits {
    RiskLimits {
        max_order_amount: Decimal::MAX,
        max_position: Decimal::MAX,
        max_orders_per_window: 3,
    }
}

/// 连 Redis 建出 `PositionManager`/`PortfolioManager` 这套仓位/组合盈亏技术栈，
/// 供 `open --live`/`monitor`/`accounting`/`report` 共用，避免各自重复一遍
/// "连 Redis -> RedisPositionStore -> PositionManager/PortfolioManager"
/// 的引导代码。`quote_cache` 由调用方决定：不需要浮动盈亏(纯记账场景)传空
/// `Arc::new(DashMap::new())`；手动开平仓/轮转这几个一次性命令不接实时行情，
/// 同样传空 cache。
pub fn build_portfolio_stack(
    redis_url: &str,
    quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
) -> anyhow::Result<(Arc<PositionManager>, Arc<PortfolioManager>)> {
    let position_store =
        RedisPositionStore::new(redis_url).context("failed to connect RedisPositionStore to redis")?;
    let adjustment_log =
        RedisAdjustmentLog::new(redis_url).context("failed to connect RedisAdjustmentLog to redis")?;

    let position_manager = Arc::new(
        PositionManager::new(Arc::new(position_store)).with_adjustment_log(Arc::new(adjustment_log)),
    );
    let portfolio_manager = Arc::new(PortfolioManager::new(position_manager.clone(), quote_cache));
    Ok((position_manager, portfolio_manager))
}

/// 交给 [`build_manual_pipeline`] 的一条交易腿：下单用的 provider、成交确认用的
/// 私有 WS 流、以及这条腿的风控上限。`venue` 单独带一份而不是从
/// `provider.venue()` 取，是因为调用方(如 `open`)会用比 provider 更细的 venue
/// 名(`binance_spot`/`binance_futures`)来区分同一套凭证下的两个市场。
pub struct ManualLeg {
    pub venue: Venue,
    pub provider: Arc<dyn OrderProvider>,
    pub stream: Box<dyn OrderStreamSource>,
    pub limits: RiskLimits,
}

/// `open`/`rotate`/`close` 三个手动命令共用的 live 流水线：搭好
/// RiskService/ExecutionService/OrderManager，为每条腿起对应的交易所私有
/// WS 流并等它就绪，返回喂给 `ManualStrategy::new` 的 `order_manager`。
/// `stream_handles` 只是为了在调用方 `.join.abort()`，`_risk_handle`/
/// `_execution_handle` 不需要保留——`tokio::spawn` 返回的 `JoinHandle` drop
/// 不会取消任务，这两个后台循环会随进程退出而结束，和现有 `open --live`
/// 分支的行为一致。
pub struct ManualPipeline {
    pub order_manager: Arc<OrderManager>,
    pub stream_handles: Vec<tokio::task::JoinHandle<()>>,
}

pub async fn build_manual_pipeline(
    redis_url: &str,
    bus: Arc<TopicBus>,
    symbols: &[Symbol],
    legs: Vec<ManualLeg>,
    asset_imbalance_limits: HashMap<String, AssetImbalanceLimit>,
) -> anyhow::Result<ManualPipeline> {
    let order_store = Arc::new(RedisOrderStore::new(redis_url).context("failed to connect RedisOrderStore to redis")?);
    let order_id_allocator =
        RedisOrderIdAllocator::new(redis_url).context("failed to connect RedisOrderIdAllocator to redis")?;
    let (position_manager, _portfolio_manager) = build_portfolio_stack(redis_url, Arc::new(DashMap::new()))?;

    let mut risk_limits = HashMap::new();
    let mut adapters = HashMap::new();
    let mut fee_providers = HashMap::new();
    for leg in &legs {
        for symbol in symbols {
            risk_limits.insert((leg.venue.clone(), symbol.clone()), leg.limits.clone());
        }
        adapters.insert(
            leg.venue.clone(),
            Arc::new(ExchangeAdapter::new(leg.venue.clone(), leg.provider.clone())),
        );
        fee_providers.insert(leg.venue.clone(), leg.provider.clone());
    }
    let fee_converter = Some(Arc::new(FeeUsdtConverter::new(fee_providers)));

    let risk_service = Arc::new(
        RiskService::new(
            bus.clone(),
            Arc::new(order_id_allocator),
            order_store.clone(),
            risk_limits,
            position_manager.clone(),
        )
        .with_asset_imbalance_limits(asset_imbalance_limits),
    );
    let execution_service = Arc::new(ExecutionService::new(bus.clone(), adapters, order_store.clone()));
    let order_manager = Arc::new(OrderManager::new(bus.clone(), position_manager, order_store, fee_converter));

    let _risk_handle = risk_service.clone().start();
    let _execution_handle = execution_service.clone().start();
    let _cancel_handle = execution_service.clone().start_cancel_listener();

    // 等每条私有流真正建连+鉴权/订阅完成，再让调用方开始下单——否则市价单
    // 可能在 WS 就绪前就已成交，导致成交推送被永久错过（WS API 不重放）。
    let mut stream_handles = Vec::new();
    for leg in legs {
        let venue = leg.venue;
        let handle = leg.stream.spawn(order_manager.clone());
        tokio::time::timeout(STREAM_READY_TIMEOUT, handle.ready)
            .await
            .with_context(|| format!("等待 {venue} 私有 WS 就绪超时"))?
            .with_context(|| format!("{venue} 私有 WS 未能就绪就退出了(检查 API Key/网络)"))?;
        stream_handles.push(handle.join);
    }

    Ok(ManualPipeline { order_manager, stream_handles })
}

/// `open --live`/`rotate --live`/`close --live` 的公共收尾：连 Redis -> 建流水线
/// -> 用它构造 `ManualStrategy` 交给 `action` 执行 -> **无论成败**都先 abort 掉
/// 私有 WS 任务再把结果抛出去。
///
/// 抽出来的关键理由是最后那步：这三个命令以前各自手写
/// `let r = ...await; for h in handles { h.abort(); } r?`，一旦有人图省事写成
/// `...await?` 就会在报错路径上漏掉 abort，把 WS 任务连同进程一起挂住。
pub async fn run_manual_live<T, F, Fut>(
    label: &str,
    symbol: &Symbol,
    legs: Vec<ManualLeg>,
    asset_imbalance_limits: HashMap<String, AssetImbalanceLimit>,
    action: F,
) -> anyhow::Result<T>
where
    F: FnOnce(ManualStrategy) -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let redis_url = redis_url();
    info!("{label} --live: connecting to redis at {redis_url}");
    let bus = Arc::new(TopicBus::new());

    let pipeline = build_manual_pipeline(
        &redis_url,
        bus.clone(),
        std::slice::from_ref(symbol),
        legs,
        asset_imbalance_limits,
    )
    .await?;

    let strategy = ManualStrategy::new(bus, pipeline.order_manager);
    let result = action(strategy).await;

    for handle in pipeline.stream_handles {
        handle.abort();
    }

    result
}

/// `transfer` 子命令专用的 live 流水线：只需要 RiskService/ExecutionService/
/// OrderManager 三件套，不涉及任何 `OrderStreamSource`/`ExchangeAdapter`——
/// 划转不是交易单，走的是 `ExecutionService::with_wallet_providers` 而不是
/// 交易所私有 WS。`order_store` 单独保留一份，供调用方直接构造
/// `TransferMonitor`(它需要 `Arc<dyn OrderStore>`，不经过 `OrderManager`)。
pub struct TransferPipeline {
    pub order_manager: Arc<OrderManager>,
    pub order_store: Arc<dyn OrderStore>,
}

pub async fn build_transfer_pipeline(
    redis_url: &str,
    bus: Arc<TopicBus>,
    wallet_providers: HashMap<Venue, Arc<dyn WalletProvider>>,
) -> anyhow::Result<TransferPipeline> {
    let order_store: Arc<dyn OrderStore> =
        Arc::new(RedisOrderStore::new(redis_url).context("failed to connect RedisOrderStore to redis")?);
    let order_id_allocator =
        RedisOrderIdAllocator::new(redis_url).context("failed to connect RedisOrderIdAllocator to redis")?;
    let (position_manager, _portfolio_manager) = build_portfolio_stack(redis_url, Arc::new(DashMap::new()))?;

    // 空 risk_limits：RiskService 对 Transfer 分支恒 Approved，不查这个 map。
    let risk_service = Arc::new(RiskService::new(
        bus.clone(),
        Arc::new(order_id_allocator),
        order_store.clone(),
        HashMap::new(),
        position_manager.clone(),
    ));
    // 空 adapters：本命令不发 Trade 请求。
    let execution_service = Arc::new(
        ExecutionService::new(bus.clone(), HashMap::new(), order_store.clone())
            .with_wallet_providers(wallet_providers, position_manager.clone()),
    );
    let order_manager = Arc::new(OrderManager::new(bus.clone(), position_manager, order_store.clone(), None));

    let _risk_handle = risk_service.clone().start();
    let _execution_handle = execution_service.clone().start();
    let _cancel_handle = execution_service.clone().start_cancel_listener();

    Ok(TransferPipeline { order_manager, order_store })
}

/// 把 `"binance"`/`"kraken"` 映射到 `transfer` 子命令使用的划转 venue——刻意
/// 和 `open`/`close` 交易腿用的 `binance_spot`/`kraken_spot` 完全一致，保证
/// `ExecutionService::handle_transfer` 对 `PositionManager` 的乐观记账落到和
/// 真实交易同一个仓位桶里，而不是另开一条无关记录。
pub fn transfer_venue(name: &str) -> anyhow::Result<Venue> {
    match name {
        "binance" => Ok(Venue::new("binance_spot")),
        "kraken" => Ok(Venue::new("kraken_spot")),
        other => anyhow::bail!("unknown venue '{other}' for 'transfer' subcommand, expected 'binance' or 'kraken'"),
    }
}

/// 和 [`transfer_venue`] 按同样的名字映射构造对应的 `WalletProvider`，用
/// [`transfer_venue`] 算出的 venue 标注(而非 `scan`/`monitor` 等命令用的裸
/// `"binance"`/`"kraken"`)，因为这个 venue 会被用作
/// `ExecutionService::wallet_providers` 这个 HashMap 的 key，必须和
/// `TransferRequest.from_venue`/`to_venue` 完全一致才能查到。
pub fn build_transfer_wallet_provider(
    name: &str,
    testnet: bool,
    proxy: Option<&str>,
) -> anyhow::Result<Arc<dyn WalletProvider>> {
    let venue = transfer_venue(name)?;
    match name {
        "binance" => Ok(Arc::new(BinanceWalletProvider::from_env(venue, testnet, proxy)?)),
        "kraken" => Ok(Arc::new(KrakenWalletProvider::from_env(venue, proxy)?)),
        _ => unreachable!("transfer_venue already validated name"),
    }
}

/// 把 `"binance"` / `"kraken"` / `"gate"` 映射到对应的现货 `OrderProvider`，供
/// `rotate` 子命令按名字选择交易所。
pub fn build_order_provider(name: &str, testnet: bool, proxy: Option<&str>) -> anyhow::Result<Arc<dyn OrderProvider>> {
    match name {
        "binance" => Ok(Arc::new(BinanceOrderProvider::from_env(
            Venue::new("binance_spot"),
            testnet,
            proxy,
        )?)),
        "kraken" => Ok(Arc::new(KrakenOrderProvider::from_env(Venue::new("kraken_spot"), proxy)?)),
        "gate" => Ok(Arc::new(GateOrderProvider::from_env(Venue::new("gate_spot"), proxy)?)),
        other => anyhow::bail!("unknown venue '{other}' for 'rotate' subcommand, expected 'binance', 'kraken' or 'gate'"),
    }
}

/// 和 [`build_order_provider`] 按同样的 venue 名字映射对应的私有 WS 订单流，
/// 供 `rotate --live`/`close --live` 建 [`build_manual_pipeline`] 需要的
/// `OrderStreamSource`。
pub fn build_order_stream_source(
    name: &str,
    testnet: bool,
    proxy: Option<&str>,
    symbol: &Symbol,
) -> anyhow::Result<Box<dyn OrderStreamSource>> {
    match name {
        "binance" => Ok(Box::new(BinanceUserDataStream::from_env(
            Venue::new("binance_spot"),
            testnet,
            proxy,
            vec![symbol.clone()],
        )?)),
        "kraken" => Ok(Box::new(KrakenPrivateOrderStream::from_env(Venue::new("kraken_spot"), proxy)?)),
        "gate" => Ok(Box::new(GatePrivateOrderStream::from_env(Venue::new("gate_spot"), proxy, vec![symbol.clone()])?)),
        other => anyhow::bail!("unknown venue '{other}' for 'rotate' subcommand, expected 'binance', 'kraken' or 'gate'"),
    }
}

/// `binance_spot` 腿的私有 WS 流，`open --live`/`close --live` 共用。
pub fn binance_spot_stream(
    venue: Venue,
    testnet: bool,
    proxy: Option<&str>,
    symbol: &Symbol,
) -> anyhow::Result<Box<dyn OrderStreamSource>> {
    Ok(Box::new(
        BinanceUserDataStream::from_env(venue, testnet, proxy, vec![symbol.clone()])
            .context("failed to start binance spot user data stream")?,
    ))
}

/// `binance_futures` 腿的私有 WS 流，`open --live`/`close --live` 共用。
pub fn binance_futures_stream(
    venue: Venue,
    testnet: bool,
    proxy: Option<&str>,
    symbol: &Symbol,
) -> anyhow::Result<Box<dyn OrderStreamSource>> {
    Ok(Box::new(
        BinanceFuturesUserDataStream::from_env(venue, testnet, proxy, vec![symbol.clone()])
            .context("failed to start binance futures user data stream")?,
    ))
}

/// 把 `--secondary <name>` 映射到对应的 `ExchangeInfoProvider`，供 `scan`/
/// `monitor` 子命令选择"副交易所"。Kraken 有完整实现（含下单）；CoinEx 目前
/// 只按 `scan` 需要的接口实现（无下单支持）——接入新交易所只需要在这里和
/// [`build_secondary_wallet_provider`] 加一个分支。
pub fn build_secondary_exchange_info(name: &str, proxy: Option<&str>) -> anyhow::Result<Box<dyn ExchangeInfoProvider>> {
    match name {
        "kraken" => Ok(Box::new(KrakenExchangeInfoProvider::from_env(Venue::new(name), proxy)?)),
        "coinex" => Ok(Box::new(CoinexExchangeInfoProvider::from_env(Venue::new(name), proxy)?)),
        "nonkyc" => Ok(Box::new(NonkycExchangeInfoProvider::from_env(Venue::new(name), proxy)?)),
        other => anyhow::bail!(
            "unknown --secondary venue '{other}', only 'kraken'/'coinex'/'nonkyc' are currently supported"
        ),
    }
}

/// 和 [`build_secondary_exchange_info`] 按同样的名字映射构造对应的
/// `WalletProvider`，供 `scan::find_overlap` 查询副交易所的钱包链信息。
pub fn build_secondary_wallet_provider(name: &str, proxy: Option<&str>) -> anyhow::Result<Box<dyn WalletProvider>> {
    match name {
        "kraken" => Ok(Box::new(KrakenWalletProvider::from_env(Venue::new(name), proxy)?)),
        "coinex" => Ok(Box::new(CoinexWalletProvider::from_env(Venue::new(name), proxy)?)),
        "nonkyc" => Ok(Box::new(NonkycWalletProvider::from_env(Venue::new(name), proxy)?)),
        other => anyhow::bail!(
            "unknown --secondary venue '{other}', only 'kraken'/'coinex'/'nonkyc' are currently supported"
        ),
    }
}

/// 和上面两个函数按同样的名字映射构造 `monitor` 子命令用的行情源和心跳探针
/// symbol——探针 symbol 是副交易所的原生报价资产（Kraken 报价用 BTC/USD），
/// 用来判断该行情链路是否健康，见 `LinkHealthMonitor`。
pub fn build_secondary_market_data_source(
    name: &str,
    venue: Venue,
    symbols: Vec<Symbol>,
    proxy: Option<String>,
) -> anyhow::Result<Box<dyn MarketDataSource>> {
    match name {
        "kraken" => Ok(Box::new(KrakenSpotSource::new(venue, symbols, proxy))),
        other => anyhow::bail!("unknown --secondary venue '{other}', only 'kraken' is currently supported"),
    }
}

pub fn secondary_probe_symbol(name: &str) -> Symbol {
    match name {
        "kraken" => Symbol::new("BTC", "USD"),
        _ => Symbol::new("BTC", "USDT"),
    }
}

/// 为 `rotate`/`close` 的 dry_run 路径构造一个不连 Redis 的"轻量"
/// `ManualStrategy`：`rotate_inventory`/`close_hedged_position` 的 dry_run
/// 分支完全不会碰 `self.bus`/`self.order_manager`（直接调
/// `provider.place_market_order(dry_run: true)`），这里用纯内存实现垫背即可，
/// 不需要 dry_run 也要求本地起 Redis。
pub fn bare_manual_strategy() -> ManualStrategy {
    let bus = Arc::new(TopicBus::new());
    let position_manager = Arc::new(PositionManager::new(Arc::new(InMemoryPositionStore::new())));
    let order_manager = Arc::new(OrderManager::new(
        bus.clone(),
        position_manager,
        Arc::new(InMemoryOrderStore::new()),
        None,
    ));
    ManualStrategy::new(bus, order_manager)
}

/// 起定期报告任务：`PortfolioSection` + `OrderSection` 两个板块，输出到
/// `LogChannel`。`report` 子命令和 `monitor`(未加 `--no-portfolio`)装的是完全
/// 同一套，以前两边各抄了一遍。
pub fn spawn_report_tracker(
    redis_url: &str,
    portfolio_manager: Arc<PortfolioManager>,
    interval: Duration,
) -> anyhow::Result<()> {
    let order_store: Arc<dyn OrderStore> =
        Arc::new(RedisOrderStore::new(redis_url).context("failed to connect RedisOrderStore to redis")?);

    let sections: Vec<Arc<dyn crate::report::ReportSection>> = vec![
        Arc::new(crate::report::PortfolioSection::new(portfolio_manager)),
        Arc::new(crate::report::OrderSection::new(order_store)),
    ];
    let channels: Vec<Arc<dyn crate::report::ReportChannel>> = vec![Arc::new(crate::report::channels::LogChannel)];

    Arc::new(crate::report::ReportTracker::new(sections, channels, interval)).spawn();
    Ok(())
}

/// `monitor`(未加 `--no-portfolio`)和 `accounting` 都要建的币安 U 本位合约资金费
/// provider map，两处以前各自抄了一遍同样的三行。
pub fn binance_futures_funding_providers(
    venue: Venue,
    testnet: bool,
    proxy: Option<&str>,
) -> anyhow::Result<HashMap<Venue, Arc<dyn FundingFeeProvider>>> {
    let provider: Arc<dyn FundingFeeProvider> =
        Arc::new(BinanceFuturesOrderProvider::from_env(venue.clone(), testnet, proxy)?);
    Ok(HashMap::from([(venue, provider)]))
}
