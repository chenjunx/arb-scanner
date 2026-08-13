# Portfolio 模块设计文档

## 概述

Portfolio 模块在 [`position`](../src/position) 模块之上提供**估值 + 盈亏**视图：按最新行情
把 `PositionManager` 的净仓位折算成市值和浮动盈亏 (unrealized PnL)，同时在每笔成交发生时
记一笔已实现盈亏 (realized PnL) 和估算手续费，按 `(venue, symbol)` 和 base 资产两种粒度
查询。

**动机**：`position_manager_design.md` 的"非目标"里明确排除了 PnL —— `PositionManager`
只回答"我在哪个 venue 持有多少、成本价多少"，不回答"这笔套利到底赚了多少"、"现在打平的话
值多少钱"。这两个问题是 Portfolio 模块要补上的缺口。经和用户确认，本版范围是：

- ✅ 按 asset 估值持仓 (mark-to-market)：结合 `PositionManager` 的净仓位/均价和最新行情，
  算出市值和浮动盈亏
- ✅ 已实现盈亏 / 手续费统计：每笔成交记一笔 realized PnL + 手续费，可累计查询。手续费
  **优先用交易所真实返还值，拿不到时才退化为估算**（见"核心组件 §2"）
- ❌ quote 资产/现金余额追踪、跨 venue 总账户权益 (一个数字)、持久化 —— 均不在本版范围，
  见"非目标"

## 架构设计

### 为什么放在 PositionManager 之上而不是并入其中

`PositionManager` 的职责是"仓位数量的唯一真相源"，`position_manager_design.md` 已经明确
把 PnL 排除在外，是为了让它保持单一职责、不需要行情依赖。如果直接把估值/盈亏字段加进
`VenuePosition`，会让 `PositionManager` 同时依赖行情缓存、又要在 `on_filled` 里做和仓位
无关的盈亏结算，职责重新混在一起。所以 Portfolio 作为独立的只读消费方 + 自己的盈亏账本：

- 读 `PositionManager` 拿净仓位/均价（不写，`PositionManager` 还是仓位状态唯一入口）
- 读 `ArbitrageEngine::shared_cache()` 拿最新行情做 mark-to-market（见"核心组件 §4"）
- 自己的 `PnlStore` 存 realized PnL/手续费累计值（结构和 `PositionStore` 同构）

### 数据流

```
                 交易所私有 WS (成交回报)              行情 WS (Quote)
                          │                                  │
                          ▼                                  ▼
┌─────────────────────────────────────────┐   ┌───────────────────────────────┐
│              OrderManager                 │   │         ArbitrageEngine        │
│  handle_exchange_update():                │   │  run(): 收到 MarketEvent 时     │
│    - risk_engine.on_filled(...)           │   │    顺手写入 cache（已有实现，    │
│      -> position_manager.on_filled(...)   │   │    无需改动)                    │
│         返回 FillOutcome{realized_pnl}     │   └───────────────┬───────────────┘
│    - portfolio.record_fill(               │                   │ shared_cache()
│        venue, symbol, filled_qty_delta,   │                   │ (Arc<DashMap<(Venue,Symbol),Quote>>)
│        fill_price, update.fee,            │                   │
│        outcome.realized_pnl, ts_ms)       │                   │
└───────────────┬───────────────────────────┘                   │
                 ▼                                               │
┌─────────────────────────────────────────────────────────────────┐
│                        PortfolioManager                           │
│  record_fill(): real_fee 非 None 直接用，否则按 FeeConfig 估算，   │
│    连同 realized_pnl 一起更新 PnlStore                            │
│  venue_valuation()/asset_valuation(): position_manager 净仓位/均价  │
│    × 从 shared_cache 查到的最新 mid 价 -> 市值 + 浮动盈亏           │
│  venue_pnl()/asset_pnl(): 查 PnlStore 的已实现盈亏/手续费累计值，    │
│    asset_pnl() 顺带把 asset_valuation() 的浮动盈亏拼进汇总          │
└───────┬───────────────────────────────────┬───────────────────────┘
        │ 委托读写 (原子 update)                │ 只读查询，不写
        ▼                                     ▼
┌───────────────────────┐           ┌─────────────────────────┐
│      dyn PnlStore      │           │      PositionManager      │
│  InMemoryPnlStore       │           │  (已有实现，见 §"对已有   │
│  ← 本次实现              │           │   模块的改动"）            │
└───────────────────────┘           └─────────────────────────┘
```

## 对已有模块的改动 (尽量最小化)

两处改动：`PositionManager::on_filled` 把已实现盈亏带出来；`OrderResult`/`ExchangeOrderUpdate`
新增手续费字段，把交易所已经返回、但目前被丢弃的真实手续费接进来。

### 1. FillOutcome：把已实现盈亏带出来

`PositionManager::on_filled` 把"这笔成交对旧仓位实现了多少盈亏"作为
返回值带出来。之所以必须由 `PositionManager` 自己算，而不是 Portfolio 在调用前后各查一次
`venue_position()` 自己算，是因为 `PositionStore::update` 的原子读改写正是为了防止并发
成交推送互相覆盖 (见 `position_manager_design.md` "核心组件 §2")——如果 Portfolio 在
`store.update` 之外单独读"旧快照"，两次读写之间可能被另一个并发成交插入，读到的旧快照就
不是这笔成交实际基于的状态，会算错。所以已实现盈亏必须在同一个原子 `update` 闭包内，用
闭包能看到的"更新前状态"计算。

```rust
/// on_filled 的返回值，把这次调用在 PositionStore 原子更新内部算出来的
/// 已实现盈亏带给调用方，PositionManager 本身不存储/累计它。
pub struct FillOutcome {
    pub realized_pnl: Decimal,
}

impl PositionManager {
    pub fn on_filled(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        side: OrderSide,
        filled_qty_delta: Decimal,
        fill_price: Option<Decimal>,
        ts_ms: u64,
    ) -> FillOutcome {
        let realized_pnl_slot = Arc::new(Mutex::new(Decimal::ZERO));
        let slot = realized_pnl_slot.clone();

        self.store.update(venue, symbol, Box::new(move |current| {
            let mut pos = current.unwrap_or_else(|| VenuePosition::flat(venue.clone(), symbol.clone()));
            let old_qty = pos.net_qty;
            let old_avg = pos.avg_price;

            let signed_delta = match side {
                OrderSide::Buy => filled_qty_delta,
                OrderSide::Sell => -filled_qty_delta,
            };

            // 已实现盈亏：只有和现有仓位方向相反的成交（减仓/穿零反向）才会实现
            // 盈亏；同方向加仓或从 0 建仓恒为 0。closed_qty 是这笔成交里"用来平掉
            // 旧仓位"的部分，穿零时超出 old_qty 的部分是按新方向重新建仓，不计入。
            if let Some(price) = fill_price {
                if !old_qty.is_zero() && old_qty.signum() != signed_delta.signum() {
                    if let Some(avg) = old_avg {
                        let closed_qty = signed_delta.abs().min(old_qty.abs());
                        *slot.lock().unwrap() = closed_qty * (price - avg) * old_qty.signum();
                    }
                }
            }

            // ... 原有 avg_price/net_qty 更新逻辑不变 ...
            pos.net_qty = old_qty + signed_delta;
            pos.updated_at_ms = ts_ms;
            pos
        }));

        FillOutcome { realized_pnl: *realized_pnl_slot.lock().unwrap() }
    }
}
```

`RiskEngine::on_filled` 只需要把这个返回值转发出去：

```rust
pub fn on_filled(&self, venue: &Venue, symbol: &Symbol, side: OrderSide,
                  filled_qty: Decimal, fill_price: Option<Decimal>, ts_ms: u64) -> FillOutcome {
    self.position_manager.on_filled(venue, symbol, side, filled_qty, fill_price, ts_ms)
}
```

`OrderManager::handle_exchange_update` 在现有调用 `risk_engine.on_filled(...)` 的地方
(`src/order_manager/manager.rs:274` 附近) 拿到 `FillOutcome`，多转发一步给新增的
`portfolio: Arc<PortfolioManager>` 字段：

```rust
if fill_delta > Decimal::ZERO {
    let outcome = self.risk_engine.on_filled(&venue, &symbol, side, fill_delta, avg_price, update.ts_ms);
    self.portfolio.record_fill(&venue, &symbol, fill_delta, avg_price, update.fee, outcome.realized_pnl, update.ts_ms);
}
```

除此之外不涉及 `PositionStore`/`RiskEngine::check`/`RiskLimits` 等其它逻辑，`position/store.rs`
和风控规则完全不用动。

### 2. OrderResult / ExchangeOrderUpdate：接入交易所真实手续费

调查现有三个交易所的下单/成交推送实现后发现：**手续费其实已经在交易所返回的原始响应里，
只是现有解析代码没接它**，之前认为"拿不到真实手续费"的判断是错的，只有 Binance 合约这一
条路径目前确实拿不到，Kraken REST 下单路径受接口本身限制也拿不到，其余(Binance 现货
REST/WS、Kraken WS)都能接真实值。逐个交易所的情况：

- **Binance 现货 REST** (`place_market_order_raw`)：MARKET 单默认 `newOrderRespType=FULL`，
  响应体自带 `fills` 数组，每笔成交都有 `commission`/`commissionAsset`，但
  [`OrderResponse`](../src/order/binance.rs#L262-L271) 没声明这个字段，serde 默认忽略未知
  字段，数据其实一直都在，只是被静默丢弃。
- **Binance 现货 WS** (`executionReport`)：带 `n`(本次成交手续费，增量)/`N`(手续费币种)，
  和 `OrderManager` 按 `fill_delta` 处理增量成交的模型天然契合，比解析 REST 的 `fills`
  数组更直接。[`ExecutionReport`](../src/order/binance.rs#L464-L480) 目前没取这两个字段。
- **Binance 合约**：`newOrderRespType` 只支持 `ACK`/`RESULT`，没有 `FULL`，REST 下单响应
  拿不到手续费；要拿真实值得靠合约 User Data Stream 的 `ORDER_TRADE_UPDATE` 事件（同样带
  `n`/`N`），但目前代码里没有任何合约 `OrderStreamSource` 实现，是一块新工作量，本版不做
  （见"后期扩展计划"）。
- **Kraken REST** (`AddOrder`)：本来就不同步返回成交信息，这条路径确实拿不到，维持现状。
- **Kraken WS** (`executions` channel)：已核对
  [Kraken 官方 WS v2 文档](https://docs.kraken.com/api/docs/websocket-v2/executions/)——
  `exec_type: "trade"` 的推送带一个 `fees` 数组，每项是 `{asset, qty}`(该笔成交的手续费，
  按 quote 币种计价)，语义上和 `cum_qty`/`cum_cost` 同一条消息里出现，是**这一次成交事件**
  的手续费而不是订单累计值，和 Binance WS 的 `n`/`N` 同一模型。
  [`KrakenExecutionData`](../src/order/kraken.rs#L427-L439) 目前没解析这个字段。

因此 `OrderResult`/`ExchangeOrderUpdate` 各新增两个可选字段，各交易所实现按上面的结论
填或不填：

```rust
// src/order/types.rs
pub struct OrderResult {
    pub order_id: String,
    pub status: OrderStatus,
    pub filled_qty: Decimal,
    pub avg_price: Option<Decimal>,
    /// 交易所真实返还的手续费，拿不到时为 None (如 Kraken REST AddOrder 本身
    /// 不同步返回成交信息、Binance 合约缺私有流)，由 Portfolio 退化为估算。
    pub fee: Option<Decimal>,
    pub fee_asset: Option<String>,
}

// src/order_manager/stream.rs
pub struct ExchangeOrderUpdate {
    pub venue: Venue,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub status: OrderStatus,
    pub filled_qty: Decimal,
    pub avg_price: Option<Decimal>,
    /// 本次推送(增量)对应的手续费，语义上对齐 Binance `executionReport` 的
    /// `n`/`N`——是这一次 fill_delta 的手续费，不是订单累计值，和 filled_qty/
    /// avg_price(累计值)刻意不同，调用方(OrderManager::handle_exchange_update)
    /// 直接把它转发给 `portfolio.record_fill` 即可，不需要自己再做增量计算。
    pub fee: Option<Decimal>,
    pub fee_asset: Option<String>,
    pub ts_ms: u64,
}
```

`Binance` 现货一侧的解析改动：REST 端把 `fills` 里的 `commission` 按 `commissionAsset`
分组求和，只有单一币种时才写入 `fee`/`fee_asset`(多币种混合是 BNB 抵扣额度中途用完等
边缘情况，概率很低，出现时保留 `None` 交给 Portfolio 估算兜底，不做加权处理)；WS 端直接
把每条 `executionReport` 的 `n`/`N` 透传进 `ExchangeOrderUpdate`。

`Kraken` WS 一侧的解析改动：`KrakenExecutionData` 新增

```rust
#[derive(Debug, Deserialize)]
struct KrakenFee {
    asset: String,
    qty: Decimal,
}

#[derive(Debug, Deserialize)]
struct KrakenExecutionData {
    order_id: String,
    #[serde(default)]
    cl_ord_id: Option<String>,
    order_status: String,
    #[serde(default)]
    cum_qty: Decimal,
    #[serde(default)]
    cum_cost: Decimal,
    /// 只有 exec_type == "trade" 的推送才带这个字段，其它状态更新(pending_new/
    /// canceled 等)天然缺失，靠 serde(default) 落到空 Vec，不用额外判断 exec_type。
    #[serde(default)]
    fees: Vec<KrakenFee>,
}
```

和 Binance 一样按 `asset` 分组求和，单一币种才写入 `fee`/`fee_asset`；`fees` 为空(非
`trade` 类型的推送，或者 Kraken 少见地不带手续费的成交)时传 `None`，交给 Portfolio 按
`FeeConfig` 估算兜底。

Binance 合约在私有流补齐前统一传 `None`。

## 核心组件

### 1. PortfolioManager

```rust
pub struct PortfolioManager {
    position_manager: Arc<PositionManager>,
    pnl_store: Arc<dyn PnlStore>,
    /// 直接复用 ArbitrageEngine::shared_cache()，不再单独维护一份行情缓存。
    quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
    fee_config: HashMap<Venue, FeeConfig>,
    default_fee_config: FeeConfig,
}

impl PortfolioManager {
    pub fn new(
        position_manager: Arc<PositionManager>,
        pnl_store: Arc<dyn PnlStore>,
        quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
        fee_config: HashMap<Venue, FeeConfig>,
    ) -> Self;

    // 成交后调用 (由 OrderManager 在拿到 FillOutcome 后转发)：real_fee 非 None
    // 时直接用交易所真实手续费，否则按 venue 的 taker_fee_bps × fee_discount
    // 估算兜底，连同 realized_pnl 一起累加进 PnlStore。
    pub fn record_fill(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        filled_qty_delta: Decimal,
        fill_price: Option<Decimal>,
        real_fee: Option<Decimal>,
        realized_pnl: Decimal,
        ts_ms: u64,
    );

    // 单个 venue+symbol 的已实现盈亏/手续费累计。
    pub fn venue_pnl(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePnl>;

    // 按 base 资产聚合已实现盈亏/手续费，并把 asset_valuation() 的浮动盈亏拼进来。
    pub fn asset_pnl(&self, asset: &str) -> AssetPnlSummary;

    // 单个 venue+symbol 的市值/浮动盈亏 (无最新行情时 mark_price 及后续字段为 None)。
    pub fn venue_valuation(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePositionValuation>;

    // 按 base 资产聚合市值/浮动盈亏。
    pub fn asset_valuation(&self, asset: &str) -> AssetValuation;

    // 全量估值，供监控/CLI 展示。
    pub fn all_valuations(&self) -> Vec<VenuePositionValuation>;
}
```

**mark-to-market 算法** (`venue_valuation`)：从 `quote_cache` 按 `(venue, symbol)` 查
`Quote`，取 `mid = (bid + ask) / 2` 作为 mark price；`market_value = net_qty * mid`；
`unrealized_pnl = (mid - avg_price) * net_qty` —— 这个公式对多空都成立，不需要额外按
方向取反 (空头 `net_qty` 为负，跌价时 `(mid - avg_price)` 也为负，两个负数相乘得正数
利润)。查不到行情或 `avg_price` 为 `None` (仓位为 0) 时，`mark_price`/`market_value`/
`unrealized_pnl` 都返回 `None`，不用 0 兜底，避免"没有行情"和"确实不赚不赔"混淆。

### 2. 手续费来源：真实值优先，FeeConfig 估算兜底

`record_fill` 收到的 `real_fee`（来自"对已有模块的改动 §2"新增的 `OrderResult.fee`/
`ExchangeOrderUpdate.fee`）非 `None` 时直接用它记账；只有交易所侧确实拿不到真实值时
(目前是 Binance 合约、Kraken REST `AddOrder` 路径、Binance 现货多币种手续费的边缘情况)
才退化为按 `taker_fee_bps`/`fee_discount` 估算——复用 `config.rs` 里 `VenueConfig` 已有的
概念 (和 `monitor` 子命令算实际成本用的是同一套数字，见最近一次提交 "switch binance fee
basis to taker")，和 `RiskLimits` 一样按 venue 建表 + 默认值兜底：

```rust
#[derive(Debug, Clone, Default)]
pub struct FeeConfig {
    pub taker_fee_bps: Decimal,
    pub fee_discount: Decimal,
}
```

估算公式：`fee = filled_qty_delta.abs() * fill_price * taker_fee_bps / 10000 * fee_discount`，
仅当 `real_fee` 为 `None` 且 `fill_price` 非 `None` 时使用。`VenuePnl` 因此新增一个
`fee_is_estimated: bool` 标记 (只要这个 venue+symbol 上有过一次估算兜底就置 `true`，见"数据
类型")，避免估算值和真实值混在一起却让调用方误以为全是精确数字。`fill_price` 为 `None`
时这笔成交跳过手续费和已实现盈亏统计 (只有 `PositionManager` 那边的数量记账不受影响)。

### 3. PnlStore (持久化接口，和 PositionStore 同构)

```rust
pub trait PnlStore: Send + Sync {
    fn all(&self) -> Vec<VenuePnl>;
    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePnl>;
    fn update(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        f: Box<dyn FnOnce(Option<VenuePnl>) -> VenuePnl + Send>,
    );
}

pub struct InMemoryPnlStore {
    entries: Mutex<HashMap<(Venue, Symbol), VenuePnl>>,
}
```

原子 `update` 的理由和 `PositionStore` 一致：避免同一 `(venue, symbol)` 上并发成交的
盈亏/手续费累加互相覆盖。

### 4. Mark price 来源：直接复用 ArbitrageEngine::shared_cache()

`engine.rs` 里的 `ArbitrageEngine` 已经维护了一份 `Arc<DashMap<(Venue, Symbol), Quote>>`
并通过 `shared_cache()` 对外暴露 (`monitor --mode periodic` 已经在用这个接口)。Portfolio
不需要再造一个价格缓存/再订阅一遍行情流，直接把这个 `Arc` 传给 `PortfolioManager` 即可，
按 `(venue, symbol)` 精确查询该 venue 自己的最新报价，而不是笼统按 symbol 取一个跨
venue 混用的价格。

## 数据类型

```rust
/// PositionManager::on_filled 的返回值。
pub struct FillOutcome {
    pub realized_pnl: Decimal,
}

/// 单个 (venue, symbol) 的已实现盈亏/手续费累计。
pub struct VenuePnl {
    pub venue: Venue,
    pub symbol: Symbol,
    pub realized_pnl: Decimal,
    pub fees_paid: Decimal,
    /// 只要 fees_paid 里累加过一次 FeeConfig 估算值(而非交易所真实手续费)就
    /// 置 true，提示调用方这个累计数不是全部来自交易所真实返还值。
    pub fee_is_estimated: bool,
    pub trade_count: u64,
    pub updated_at_ms: u64,
}

/// 按 base 资产聚合的已实现盈亏汇总，含浮动盈亏拼接。
pub struct AssetPnlSummary {
    pub asset: String,
    pub realized_pnl: Decimal,
    pub fees_paid: Decimal,
    /// 来自 asset_valuation() 的浮动盈亏；缺行情时为 None，不当 0 处理。
    pub unrealized_pnl: Option<Decimal>,
    /// realized_pnl - fees_paid + unrealized_pnl.unwrap_or(0)；缺行情时仍然
    /// 给出这个值(只是不含浮动部分)，并靠 unrealized_pnl=None 提示调用方"这不是
    /// 全量"。
    pub net_pnl: Decimal,
}

/// 单个 venue+symbol 的估值快照。
pub struct VenuePositionValuation {
    pub venue: Venue,
    pub symbol: Symbol,
    pub net_qty: Decimal,
    pub avg_price: Option<Decimal>,
    pub mark_price: Option<Decimal>,
    pub market_value: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
}

/// 按 base 资产聚合的估值。
pub struct AssetValuation {
    pub asset: String,
    pub net_qty: Decimal,
    /// 只有当参与聚合的 venue 全部拿到了 mark price 才是 Some，避免"部分 venue
    /// 缺价"时的市值被悄悄少算却看起来像是完整数字。
    pub market_value: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub venues: Vec<VenuePositionValuation>,
}
```

## 使用示例

### 1. 初始化 (接在 PositionManager/RiskEngine/ArbitrageEngine 之后)

```rust
// PositionManager/RiskEngine 初始化和 position_manager_design.md 一致
let position_store = Arc::new(InMemoryPositionStore::new());
let position_manager = Arc::new(PositionManager::new(position_store));
let risk_engine = Arc::new(RiskEngine::new(risk_limits, position_manager.clone()));

// ArbitrageEngine 已有初始化流程，拿到共享行情缓存
let engine = ArbitrageEngine::new(strategies, sinks);
let quote_cache = engine.shared_cache();

// 手续费配置：从 config.toml 的 [[venues]] 里已有的 taker_fee_bps/fee_discount 转换
let mut fee_config = HashMap::new();
fee_config.insert(
    Venue::new("binance_spot"),
    FeeConfig { taker_fee_bps: Decimal::new(10, 2), fee_discount: Decimal::ONE }, // 0.1%
);

let pnl_store = Arc::new(InMemoryPnlStore::new());
let portfolio = Arc::new(PortfolioManager::new(position_manager.clone(), pnl_store, quote_cache, fee_config));

// OrderManager 新增 portfolio 依赖
let order_manager = Arc::new(OrderManager::new(risk_engine, execution_engine, event_tx, portfolio.clone()));
```

### 2. 查询已实现盈亏

```rust
let summary = portfolio.asset_pnl("BTC");
println!(
    "BTC 已实现盈亏: {} 手续费: {} 净盈亏: {}",
    summary.realized_pnl, summary.fees_paid, summary.net_pnl
);
```

### 3. 查询市值/浮动盈亏 (对冲组合当前值多少钱)

```rust
let valuation = portfolio.asset_valuation("BTC");
match valuation.market_value {
    Some(mv) => println!("BTC 敞口市值: {mv}, 浮动盈亏: {:?}", valuation.unrealized_pnl),
    None => println!("部分 venue 缺最新行情，暂时算不出完整市值"),
}
for v in &valuation.venues {
    println!("  {} {}: qty={} avg={:?} mark={:?}", v.venue, v.symbol, v.net_qty, v.avg_price, v.mark_price);
}
```

## 非目标 (本版明确不做)

- **不追踪 quote 资产/现金余额**：和 `position_manager_design.md` 的既有简化一致，只按
  base 资产估值，不管 USDT 等 quote 资产的余额变化。
- **不提供单一"总账户权益"数字**：因为不含现金余额，"仅 base 资产市值"加总意义有限，
  容易被误读成账户总资产，本版不做，见"后期扩展计划"。
- **Binance 合约手续费本版仍是估算值**：拿真实值需要新增合约 User Data Stream
  (`ORDER_TRADE_UPDATE`)，目前代码里合约只有下单没有私有流，是一块独立工作量，本版不做，
  见"后期扩展计划"。
- **Kraken REST (`AddOrder`) 路径下单仍是估算值**：接口本身不同步返回成交/手续费信息，
  只能靠这条路径下单时退化为估算；Kraken **WS** (`executions` channel) 已确认真实值可用，
  见"对已有模块的改动 §2"。
- **不做持久化**：`InMemoryPnlStore` 重启即丢，`PnlStore` trait 是为了让后续接
  Redis/sled 时不用改 `PortfolioManager` 的调用方，和 `PositionStore` 现状一致。
- **不区分策略/group_id**：`asset_pnl`/`venue_pnl` 是全局累计，不按 `OrderRequest.strategy_name`
  或 `group_id` 拆分单个套利动作的盈亏。

## 测试

计划覆盖 (`cargo test --lib position` 里新增 `on_filled` 返回值相关用例 + `cargo test --lib portfolio`):

- `PositionManager::on_filled` 返回的 `FillOutcome.realized_pnl`：
  - 从 0 建仓、同向加仓 → 0
  - 同向减仓未穿零 → `closed_qty * (fill_price - avg_price) * sign`
  - 穿零反向 → 只对被平掉的旧仓位部分计已实现盈亏，新方向部分为 0
  - `fill_price = None` → 0 (不计算)
- `PortfolioManager::record_fill`：
  - `real_fee` 非 `None` 时直接记账，不走 `FeeConfig` 估算，`fee_is_estimated` 保持 `false`
  - `real_fee` 为 `None` 时按 venue 配置估算，未配置的 venue 用默认 `FeeConfig`，
    `fee_is_estimated` 置为 `true` 且后续调用不会被真实值覆盖回 `false`
  - 多笔成交的 realized_pnl/fees_paid/trade_count 正确累加，真实值和估算值混合累加时
    `fees_paid` 数值正确、`fee_is_estimated` 仍然是 `true`（只要出现过一次估算）
- `asset_valuation`/`venue_valuation`：
  - mark price 命中时市值/浮动盈亏计算正确 (含空头方向验证)
  - 缺行情时对应字段是 `None` 而不是 0
  - 多 venue 聚合时只要有一个 venue 缺价，`AssetValuation.market_value` 整体为 `None`
- `asset_pnl` 正确拼接 realized (来自 PnlStore) 和 unrealized (来自 asset_valuation)

## 后期扩展计划

### 1. 补齐剩余的真实手续费缺口
本版已经把 Binance 现货 (REST `fills`/WS `n`+`N`) 和 Kraken WS (`executions` channel 的
`fees` 数组) 接成真实手续费，只剩一块没做：
- **Binance 合约**：新增合约 User Data Stream，接 `ORDER_TRADE_UPDATE` 事件的 `n`/`N`
  (或退一步定期拉取 `GET /fapi/v1/userTrades` 核对/补齐)。Kraken REST `AddOrder` 路径
  仍会退化为估算，这是接口本身的同步限制，不是待办项。

### 2. quote 资产/现金余额追踪 → 总账户权益
需要先有余额查询能力 (`wallet::WalletProvider` 目前只有转账相关接口，没有余额查询)，
之后才能把 quote 资产余额和 base 资产市值加总成一个"总权益"数字，是本版被排除的两个
选项，留给后续统一设计。

### 3. Redis/sql 持久化 PnlStore
和 `position_manager_design.md` "后期扩展计划 §1" 一起做，两个 store 用同一套原子更新
机制 (`WATCH`/`MULTI` 或 Lua 脚本)。

### 4. 按策略/group_id 拆分盈亏
`VenuePnl` 目前是 `(venue, symbol)` 粒度的全局累计，后续可以在 key 里加上
`strategy_name`/`group_id`，单独核算每个套利策略/每组订单的表现。

### 5. `main.rs` 增加 `portfolio` 展示子命令
输出 `all_valuations()`/`asset_pnl()` 的表格视图，供人工核对当前对冲组合的市值和盈亏，
和 `position_manager_design.md` "后期扩展计划 §5" (`main.rs` 接入 `OrderManager`) 是
同一批工作的一部分。

## 总结

Portfolio 模块提供：
- ✅ 按 `(venue, symbol)` / base 资产的市值 + 浮动盈亏 (复用 `ArbitrageEngine` 已有的
  行情缓存，不新增行情订阅)
- ✅ 每笔成交的已实现盈亏 (由 `PositionManager::on_filled` 在原子更新内算出，避免并发
  竞态) + 手续费累计统计，**真实值优先**(Binance 现货 REST/WS、Kraken WS)，拿不到时才
  退化为估算(Binance 合约、Kraken REST `AddOrder`)，并用 `fee_is_estimated` 标记区分
- ✅ 对已有模块的改动降到最低：`PositionManager::on_filled`/`RiskEngine::on_filled`
  返回值从 `()` 变成 `FillOutcome`；`OrderResult`/`ExchangeOrderUpdate` 各新增
  `fee`/`fee_asset` 两个可选字段；`PositionStore`/风控规则完全不动
- ✅ `PnlStore` trait 占位，后续可无痛切换 Redis/sql，和 `PositionStore` 同一套设计语言
- 🔄 不含现金余额、总账户权益、持久化、按策略拆分，以及 Binance 合约的真实手续费接入
  (见"非目标"，均已列入"后期扩展计划")
