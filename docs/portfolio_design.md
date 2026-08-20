# Portfolio 模块设计文档

## 概述

Portfolio 模块在 [`position`](../src/position) 模块之上提供**估值 + 盈亏**视图：按最新行情
把 `PositionManager` 的净仓位折算成市值和浮动盈亏 (unrealized PnL)；已实现盈亏 (realized PnL)
不再由 Portfolio 自己记账，而是直接读 `PositionManager::VenuePosition.realized_pnl`——这是
成交平仓、手续费换算、资金费结算共同维护的唯一真相源，见"架构设计"，按 `(venue, symbol)`
和 base 资产两种粒度查询。

**动机**：`position_manager_design.md` 的"非目标"里明确排除了 PnL —— `PositionManager`
只回答"我在哪个 venue 持有多少、成本价多少"，不回答"这笔套利到底赚了多少"、"现在打平的话
值多少钱"。这两个问题是 Portfolio 模块要补上的缺口。经和用户确认，本版范围是：

- ✅ 按 asset 估值持仓 (mark-to-market)：结合 `PositionManager` 的净仓位/均价和最新行情，
  算出市值和浮动盈亏
- ✅ 已实现盈亏统计：每笔成交记一笔 realized PnL，可累计查询
- ❌ 手续费统计（`fees_paid`/`fee_is_estimated` 等字段已移除，手续费只用于冲减
  `PositionManager` 侧的已实现盈亏，见"对已有模块的改动 §2"）
- ❌ quote 资产/现金余额追踪、跨 venue 总账户权益 (一个数字)、持久化 —— 均不在本版范围，
  见"非目标"

## 架构设计

### 为什么放在 PositionManager 之上而不是并入其中

`PositionManager` 的职责是"仓位数量 + 已实现盈亏的唯一真相源"（`VenuePosition.realized_pnl`
由 `on_filled` 的成交平仓结算和 `apply_adjustment` 的手续费/资金费调整共同维护，见
`position_manager_design.md`）。Portfolio 只做行情相关的那部分：mark-to-market 估值和浮动
盈亏 (unrealized PnL)，不需要也不应该自己再维护一份 realized PnL 账本——曾经的 `PnlStore`
只在成交时由 `OrderManager::record_fill` 累加，完全绕开了后续的手续费/资金费调整，导致这份
账本和 `PositionManager` 长期不一致，已删除。所以 Portfolio 是纯粹的只读消费方：

- 读 `PositionManager` 拿净仓位/均价/已实现盈亏（不写，`PositionManager` 还是仓位和已实现
  盈亏状态的唯一入口）
- 读 `ArbitrageEngine::shared_cache()` 拿最新行情做 mark-to-market（见"核心组件 §4"）
- 自己不持有任何状态（`quote_cache` 只是外部传入的共享缓存引用）

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
│         写入 VenuePosition.realized_pnl    │   └───────────────┬───────────────┘
│    - apply_adjustment(...) 冲减手续费/      │                   │ shared_cache()
│      结算资金费，同样写 realized_pnl        │                   │ (Arc<DashMap<(Venue,Symbol),Quote>>)
└───────────────┬───────────────────────────┘                   │
                 ▼                                               │
┌─────────────────────────────────────────────────────────────────┐
│                        PortfolioManager                           │
│  venue_valuation()/asset_valuation(): position_manager 净仓位/均价/ │
│    realized_pnl × 从 shared_cache 查到的最新 mid 价 -> 市值 + 浮动  │
│    盈亏                                                            │
│  asset_pnl(): 基于 asset_valuation() 聚合 realized_pnl，拼上         │
│    unrealized_pnl                                                  │
└───────────────────────────────┬───────────────────────────────────┘
                                 │ 只读查询，不写
                                 ▼
                       ┌─────────────────────────┐
                       │      PositionManager      │
                       │  (已有实现，见 §"对已有   │
                       │   模块的改动"）            │
                       └─────────────────────────┘
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
(`src/order_manager/manager.rs`) 触发 `position_manager.on_filled(...)`，`realized_pnl`
直接写进 `VenuePosition`，`OrderManager` 不再需要转发给单独的 Portfolio 账本——`portfolio`
字段和这一步转发已经删除，`PortfolioManager` 查询时直接读 `PositionManager` 即可。

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
    /// 不同步返回成交信息、Binance 合约缺私有流)，缺失时这笔成交不冲减
    /// 已实现盈亏。
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
    /// 用它换算 USDT 等值去冲减 `PositionManager` 的已实现盈亏。
    pub fee: Option<Decimal>,
    pub fee_asset: Option<String>,
    pub ts_ms: u64,
}
```

`Binance` 现货一侧的解析改动：REST 端把 `fills` 里的 `commission` 按 `commissionAsset`
分组求和，只有单一币种时才写入 `fee`/`fee_asset`(多币种混合是 BNB 抵扣额度中途用完等
边缘情况，概率很低，出现时保留 `None`，不做加权处理)；WS 端直接
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
`trade` 类型的推送，或者 Kraken 少见地不带手续费的成交)时传 `None`。

Binance 合约在私有流补齐前统一传 `None`。

## 核心组件

### 1. PortfolioManager

```rust
pub struct PortfolioManager {
    position_manager: Arc<PositionManager>,
    /// 直接复用 ArbitrageEngine::shared_cache()，不再单独维护一份行情缓存。
    quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
}

impl PortfolioManager {
    pub fn new(
        position_manager: Arc<PositionManager>,
        quote_cache: Arc<DashMap<(Venue, Symbol), Quote>>,
    ) -> Self;

    // 按 base 资产聚合已实现盈亏 (直接来自 PositionManager，不经过任何中间账本)，
    // 并把 asset_valuation() 的浮动盈亏拼进来。
    pub fn asset_pnl(&self, asset: &str) -> AssetPnlSummary;

    // 单个 venue+symbol 的市值/浮动盈亏/已实现盈亏 (无最新行情时 mark_price 及
    // 后续字段为 None，realized_pnl 始终有值)。
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

### 2. 手续费：只用于冲减已实现盈亏，Portfolio 不再单独记账

Portfolio 曾经维护 `fees_paid`/`fees_paid_usdt`/`fees_usdt_incomplete`/`fee_is_estimated`
四个字段，按交易所真实手续费优先、拿不到时用 `FeeConfig`(`taker_fee_bps`/`fee_discount`)
估算兜底。这套估算逻辑已经移除：`OrderResult.fee`/`ExchangeOrderUpdate.fee` 拿到的手续费
现在只用于换算 USDT 等值、冲减 `PositionManager` 侧的已实现盈亏 (`AdjustmentReason::FeeUsdt`，
见 `order_manager/manager.rs`)，Portfolio 自己不再单独统计手续费金额。

### 3. 已实现盈亏账本：已删除，直接读 PositionManager

早期版本这里是一个独立的 `PnlStore` trait (`InMemoryPnlStore`/`RedisPnlStore`，结构和
`PositionStore` 同构)，由 `OrderManager` 在每次成交后调用 `record_fill()` 累加。这份账本
只覆盖"成交平仓"这一种已实现盈亏来源，后来新增的手续费换算 (`AdjustmentReason::FeeUsdt`)
和资金费结算 (`AdjustmentReason::Funding`) 都是直接写 `PositionManager::VenuePosition
.realized_pnl`，完全绕开了 `PnlStore`，导致两份账本长期不一致 (`PnlStore` 偏小)。现已把
`PnlStore` 整条链路 (trait、`InMemoryPnlStore`、`RedisPnlStore`、`VenuePnl` 类型) 删除，
`PortfolioManager` 的 `venue_valuation`/`asset_valuation`/`asset_pnl` 都直接从
`PositionManager::VenuePosition.realized_pnl` 读取，不再有单独的持久化/一致性问题。

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

/// 按 base 资产聚合的已实现盈亏汇总，含浮动盈亏拼接。
pub struct AssetPnlSummary {
    pub asset: String,
    pub realized_pnl: Decimal,
    /// 来自 asset_valuation() 的浮动盈亏；缺行情时为 None，不当 0 处理。
    pub unrealized_pnl: Option<Decimal>,
    /// realized_pnl + unrealized_pnl.unwrap_or(0)；缺行情时仍然
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
    /// 直接来自 PositionManager::VenuePosition.realized_pnl：成交平仓 + 手续费 +
    /// 资金费的完整已实现盈亏，始终有值 (不依赖行情)。
    pub realized_pnl: Decimal,
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

let portfolio = Arc::new(PortfolioManager::new(position_manager.clone(), quote_cache));

// OrderManager 不依赖 PortfolioManager，PortfolioManager 单向读 PositionManager
let order_manager = Arc::new(OrderManager::new(bus, position_manager.clone(), order_store, fee_converter));
```

### 2. 查询已实现盈亏

```rust
let summary = portfolio.asset_pnl("BTC");
println!(
    "BTC 已实现盈亏: {} 净盈亏: {}",
    summary.realized_pnl, summary.net_pnl
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
- **不做持久化**：已实现盈亏的持久化完全交给 `PositionManager`/`PositionStore`
  (Redis)，`PortfolioManager` 自身不持有任何需要持久化的状态。
- **不区分策略/group_id**：`asset_pnl` 是全局累计，不按 `OrderRequest.strategy_name`
  或 `group_id` 拆分单个套利动作的盈亏。

## 测试

计划覆盖 (`cargo test --lib position` 里新增 `on_filled` 返回值相关用例 + `cargo test --lib portfolio`):

- `PositionManager::on_filled` 返回的 `FillOutcome.realized_pnl`：
  - 从 0 建仓、同向加仓 → 0
  - 同向减仓未穿零 → `closed_qty * (fill_price - avg_price) * sign`
  - 穿零反向 → 只对被平掉的旧仓位部分计已实现盈亏，新方向部分为 0
  - `fill_price = None` → 0 (不计算)
- `venue_valuation`/`all_valuations` 的 `realized_pnl` 字段：直接反映
  `PositionManager::VenuePosition.realized_pnl`，包括成交平仓、手续费换算
  (`AdjustmentReason::FeeUsdt`)、资金费结算 (`AdjustmentReason::Funding`)
- `asset_valuation`/`venue_valuation`：
  - mark price 命中时市值/浮动盈亏计算正确 (含空头方向验证)
  - 缺行情时对应字段是 `None` 而不是 0
  - 多 venue 聚合时只要有一个 venue 缺价，`AssetValuation.market_value` 整体为 `None`
- `asset_pnl` 正确聚合已平仓资产的历史 realized_pnl (不因当前 net_qty=0 而丢失)，并拼接
  unrealized (来自 asset_valuation)

## 后期扩展计划

### 1. 补齐剩余的真实手续费缺口
本版已经把 Binance 现货 (REST `fills`/WS `n`+`N`) 和 Kraken WS (`executions` channel 的
`fees` 数组) 接成真实手续费，只剩一块没做：
- **Binance 合约**：新增合约 User Data Stream，接 `ORDER_TRADE_UPDATE` 事件的 `n`/`N`
  (或退一步定期拉取 `GET /fapi/v1/userTrades` 核对/补齐)。Kraken REST `AddOrder` 路径
  拿不到真实手续费时，这笔成交就没有 fee 数据可用于冲减 `PositionManager` 的已实现
  盈亏(不再有估算兜底)，这是接口本身的同步限制，不是待办项。

### 2. quote 资产/现金余额追踪 → 总账户权益
需要先有余额查询能力 (`wallet::WalletProvider` 目前只有转账相关接口，没有余额查询)，
之后才能把 quote 资产余额和 base 资产市值加总成一个"总权益"数字，是本版被排除的两个
选项，留给后续统一设计。

### 3. 按策略/group_id 拆分盈亏
`PositionManager::VenuePosition.realized_pnl` 目前是 `(venue, symbol)` 粒度的全局累计，
后续可以在 key 里加上 `strategy_name`/`group_id`，单独核算每个套利策略/每组订单的表现。

### 4. `main.rs` 增加 `portfolio` 展示子命令
输出 `all_valuations()`/`asset_pnl()` 的表格视图，供人工核对当前对冲组合的市值和盈亏，
和 `position_manager_design.md` "后期扩展计划 §5" (`main.rs` 接入 `OrderManager`) 是
同一批工作的一部分。

## 总结

Portfolio 模块提供：
- ✅ 按 `(venue, symbol)` / base 资产的市值 + 浮动盈亏 (复用 `ArbitrageEngine` 已有的
  行情缓存，不新增行情订阅)
- ✅ 每笔成交的已实现盈亏 (由 `PositionManager::on_filled` 在原子更新内算出，避免并发
  竞态)；手续费不在 Portfolio 单独记账，而是换算成 USDT 等值后直接冲减
  `PositionManager` 的已实现盈亏(见"手续费：只用于冲减已实现盈亏")
- ✅ 对已有模块的改动降到最低：`PositionManager::on_filled`/`RiskEngine::on_filled`
  返回值从 `()` 变成 `FillOutcome`；`OrderResult`/`ExchangeOrderUpdate` 各新增
  `fee`/`fee_asset` 两个可选字段；`PositionStore`/风控规则完全不动
- ✅ 已实现盈亏不再单独记账：`PortfolioManager` 是 `PositionManager` 之上的纯只读视图，
  `realized_pnl` 直接读 `PositionManager::VenuePosition.realized_pnl`（成交平仓 + 手续费 +
  资金费的完整口径），不存在两份账本互相漂移的问题
- 🔄 不含现金余额、总账户权益、按策略拆分，以及 Binance 合约的真实手续费接入
  (见"非目标"，均已列入"后期扩展计划")
