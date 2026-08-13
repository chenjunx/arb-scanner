# 仓位管理模块设计文档

## 概述

仓位管理模块 (`position`) 提供跨交易所/跨产品的统一持仓视图：以 **资产 (base 币种)**
为单位聚合净敞口 (现货多头 + 合约空头自动相抵)，同时保留每个 `(venue, symbol)` 上的
明细仓位 (数量 + 加权平均建仓价)，供风控限额检查、`close` 等命令按 venue 拆解出
"该平多少"使用。

**动机**：当前 `RiskEngine` (`src/order_manager/risk.rs`) 里已经有一份很基础的持仓
状态 (`positions: Mutex<HashMap<(Venue, Symbol), Decimal>>`)，但它只是风控检查的副产
品——没有均价、不能跨 venue 聚合、也不打算对外暴露给别的模块查询。而 `close` 命令
(`src/main.rs:413-419`) 的注释里明确写着："这个代码库里没有余额/持仓查询接口，没法
自动算出'全部'是多少，需要调用方自己核对仓位后传入"。仓位管理模块就是补上这个缺口，
并把 `RiskEngine` 的持仓存储收敛成对它的委托，避免出现两份会漂移的状态。

## 架构设计

### 数据流

```
                 交易所私有 WS (成交回报)
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                       OrderManager                                │
│  handle_exchange_update():                                        │
│    - 按增量成交量 fill_delta 记账 (幂等/防倒退，见已有实现)         │
│    - 调用 risk_engine.on_filled(venue, symbol, side,               │
│                                  fill_delta, avg_price, ts_ms)     │
└────────────┬────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        RiskEngine                                 │
│  - 限额/频率检查逻辑不变 (max_order_amount / max_orders_per_window)│
│  - 不再自己存 positions，check() 里的持仓检查和 on_filled()/       │
│    position() 全部委托给 PositionManager                          │
└────────────┬────────────────────────────────────────────────────┘
             │ 委托
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      PositionManager                              │
│  on_filled(): 按 (venue, symbol) 原子读改写:                       │
│    - net_qty += (Buy:+delta / Sell:-delta)                        │
│    - avg_price 按加权平均成本法更新 (加仓/减仓/穿零反向分别处理)     │
│  position()/venue_position(): 查询单个 (venue, symbol)             │
│  asset_exposure(asset): 按 base 币种跨 venue/产品聚合净敞口          │
│  all_positions(): 全量仓位，供监控/CLI 展示                        │
└────────────┬────────────────────────────────────────────────────┘
             │ 委托读写 (原子 update)
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     dyn PositionStore                             │
│  InMemoryPositionStore   ← 本次实现                                │
│  RedisPositionStore      ← 后期扩展 (见"后期扩展计划")              │
└─────────────────────────────────────────────────────────────────┘

查询方: RiskEngine.check() (单 venue+symbol 限额)
       close/未来的 CLI 工具 (asset_exposure 按 venue 拆解平仓数量)
       监控/展示 (all_positions)
```

### 核心设计：为什么资产级聚合不需要区分"现货/合约"

`open_hedged_position` 的对冲逻辑是：Binance 现货 **买入** (Buy) + Binance 合约
**卖出开空** (Sell)。`OrderSide::Buy` 记 `+delta`、`Sell` 记 `-delta` 是仓位记账
的既有约定 (`RiskEngine::on_filled` 现在就是这么做的)，对合约同样成立——开空就是
一笔 `Sell`，天然记为负数。于是把 `binance_spot`/`binance_futures`/`kraken_spot`
三个 venue 上、`symbol.base` 相同的仓位直接相加，现货多头和合约空头就会自动相抵，
不需要额外的"产品类型"字段或特殊处理。

## 核心组件

### 1. PositionManager

**职责**
- 持仓变更的唯一写入口 (由 `OrderManager`/`RiskEngine` 在成交后调用)
- 维护每个 `(venue, symbol)` 的净数量 + 加权平均建仓价
- 按 base 资产跨 venue/产品聚合出全局净敞口

**主要接口**
```rust
impl PositionManager {
    pub fn new(store: Arc<dyn PositionStore>) -> Self;

    // 订单成交后调用；fill_price 拿不到时(极少数场景)只更新数量，均价不变
    pub fn on_filled(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        side: OrderSide,
        filled_qty_delta: Decimal,
        fill_price: Option<Decimal>,
        ts_ms: u64,
    );

    // 单个 venue+symbol 的净数量 (正=多头，负=空头)，替代 RiskEngine 原来的实现
    pub fn position(&self, venue: &Venue, symbol: &Symbol) -> Decimal;

    // 单个 venue+symbol 的完整快照 (含均价)
    pub fn venue_position(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePosition>;

    // 按 base 资产聚合的全局净敞口 + 明细拆分
    pub fn asset_exposure(&self, asset: &str) -> AssetExposure;

    // 全量仓位，供监控/调试
    pub fn all_positions(&self) -> Vec<VenuePosition>;
}
```

**加权平均建仓价算法** (`on_filled` 内部，针对单个 `(venue, symbol)`):
1. 从 0 建仓：`avg_price = fill_price`
2. 同方向加仓 (`new_qty` 与 `net_qty` 同号且 `|new_qty| >= |net_qty|`)：
   `avg_price = (avg_price*|net_qty| + fill_price*delta) / |new_qty|`
3. 同方向减仓但未穿零：`avg_price` 不变 (只有卖出会实现盈亏，但本版不计算 PnL，
   见"非目标")
4. 穿零反向：新方向以本次成交价重新建仓
5. 减到恰好 0：`avg_price = None`

`fill_price` 取 `ExchangeOrderUpdate.avg_price`——即该笔订单截至当前的累计均价，
不是本次增量成交的精确价格。多笔部分成交价差较大时会有近似误差，这是已知的简化
(和现有 `max_orders_per_window` 用简单计数代替真正滑动窗口是同一类"先能用、后
精确化"的取舍)。

### 2. PositionStore (持久化接口)

**职责**
- 抽象仓位的存储后端，`PositionManager` 只依赖这个 trait，不关心具体实现
- 本次只提供 `InMemoryPositionStore`；`update()` 设计成原子读改写，是为了让
  未来的 Redis 实现能用 `WATCH`/`MULTI` 或 Lua 脚本保证并发安全，而不用改
  `PositionManager` 的调用方式

```rust
pub trait PositionStore: Send + Sync {
    fn all(&self) -> Vec<VenuePosition>;
    fn get(&self, venue: &Venue, symbol: &Symbol) -> Option<VenuePosition>;

    /// 原子地对单个 (venue, symbol) 做读-改-写，避免并发成交推送互相覆盖
    /// (例如同一 venue+symbol 上两个策略的订单同时成交)。
    fn update(
        &self,
        venue: &Venue,
        symbol: &Symbol,
        f: Box<dyn FnOnce(Option<VenuePosition>) -> VenuePosition + Send>,
    );
}

pub struct InMemoryPositionStore {
    positions: Mutex<HashMap<(Venue, Symbol), VenuePosition>>,
}
```

这个 trait 形状是对 `order_manager_design.md` 里"后期扩展计划 §1"草拟的
`PositionStore { get_position, update_position }` 的落地和细化——因为要存均价和
时间戳，不只是一个裸 `Decimal`，所以改成了存取整个 `VenuePosition`，并把
"读-改-写"收敛成一个原子 `update`。

### 3. RiskEngine (改造)

**职责变化**
- 限额/频率检查逻辑不变
- 不再自己维护 `positions: Mutex<HashMap<...>>`，改为持有
  `Arc<PositionManager>`，`check()` 里的持仓检查、`on_filled()`、`position()`
  全部委托过去，消除两份持仓状态漂移的风险

```rust
pub struct RiskEngine {
    limits: HashMap<(Venue, Symbol), RiskLimits>,
    default_limits: RiskLimits,
    position_manager: Arc<PositionManager>,   // 替代原来的 positions 字段
    order_counts: Mutex<HashMap<(Venue, Symbol), u32>>,
}

impl RiskEngine {
    pub fn new(limits: HashMap<(Venue, Symbol), RiskLimits>, position_manager: Arc<PositionManager>) -> Self;

    // 签名新增 fill_price/ts_ms，内部直接转发给 position_manager.on_filled
    pub fn on_filled(
        &self, venue: &Venue, symbol: &Symbol, side: OrderSide,
        filled_qty: Decimal, fill_price: Option<Decimal>, ts_ms: u64,
    );

    pub fn position(&self, venue: &Venue, symbol: &Symbol) -> Decimal {
        self.position_manager.position(venue, symbol)
    }
}
```

`OrderManager::handle_exchange_update` 里唯一需要改的地方：调用
`risk_engine.on_filled(...)` 时把已经拿到的 `avg_price` 和 `update.ts_ms` 一并
传过去 (`src/order_manager/manager.rs:273-275` 附近)。

## 数据类型

```rust
/// 单个 (venue, symbol) 上的净仓位快照。
pub struct VenuePosition {
    pub venue: Venue,
    pub symbol: Symbol,
    /// 净数量，base 币种单位。正=净多头，负=净空头(含合约空头)。
    pub net_qty: Decimal,
    /// 当前净仓位的加权平均建仓价；net_qty 为 0 时是 None。
    pub avg_price: Option<Decimal>,
    pub updated_at_ms: u64,
}

/// 按 base 资产跨 venue/产品聚合后的全局敞口。
pub struct AssetExposure {
    pub asset: String,
    /// 所有相关 venue+symbol 净仓位之和；接近 0 视为已对冲。
    pub net_qty: Decimal,
    /// 参与聚合的明细，用于按 venue 拆解出"该平多少"。
    pub venues: Vec<VenuePosition>,
}
```

## 使用示例

### 1. 初始化 (接在现有 order_manager 初始化流程之前)

```rust
let position_store = Arc::new(InMemoryPositionStore::new());
let position_manager = Arc::new(PositionManager::new(position_store));

let mut risk_limits = HashMap::new();
risk_limits.insert(
    (Venue::new("binance_spot"), Symbol::new("BTC", "USDT")),
    RiskLimits { max_order_amount: Decimal::new(1, 0), max_position: Decimal::new(10, 0), max_orders_per_window: 100 },
);
let risk_engine = Arc::new(RiskEngine::new(risk_limits, position_manager.clone()));

// ExecutionEngine/OrderManager 初始化和现有文档一致，不受影响
```

### 2. 查询全局敞口 (对冲是否已经打平)

```rust
let exposure = position_manager.asset_exposure("BTC");
println!("BTC 全局净敞口: {}", exposure.net_qty); // 接近 0 说明现货多头/合约空头已对冲
for v in &exposure.venues {
    println!("  {} {}: {} @ {:?}", v.venue, v.symbol, v.net_qty, v.avg_price);
}
```

### 3. 未来集成方向：`close --all` 自动算平仓数量

当前 `close` 命令要求手动传 `--binance-spot-qty`/`--kraken-spot-qty`/`--futures-qty`
(`src/main.rs:413-419`)。接入 `PositionManager` 之后可以新增 `--all` 模式：

```rust
let exposure = position_manager.asset_exposure(&symbol.base);
for v in &exposure.venues {
    if v.net_qty.is_zero() { continue; }
    let side = if v.net_qty.is_sign_positive() { OrderSide::Sell } else { OrderSide::Buy };
    // 用 v.venue / v.net_qty.abs() / side 拼出对应的平仓请求
}
```

这一步涉及把 `main.rs` 的 `open`/`rotate`/`close` 从直接调用 `execution::` 里的
裸函数，改成走 `OrderManager` (目前完全没有接入)，工作量不小，本次设计文档不包含
具体改造，列入"后期扩展计划"。

## 非目标 (本版明确不做)

- **不算已实现/未实现盈亏 (PnL)**：只有数量和加权平均建仓价，没有 mark price、
  没有手续费扣减、没有已实现盈亏结转。
- **不追踪 quote 资产敞口**：只按 `symbol.base` 聚合，USDT 之类的 quote 资产
  余额变化不计入 (和现有 `RiskEngine` 的既有简化一致)。
- **不做持久化**：`InMemoryPositionStore` 重启即丢，`PositionStore` trait 是为
  了让后续接 Redis/sled 时不用改 `PositionManager` 的调用方。

## 测试

计划覆盖 (`cargo test --lib position`):
- 从 0 建仓、同向加仓的加权均价计算
- 同向减仓均价不变、减到 0 时 `avg_price` 变 `None`
- 穿零反向后按新成交价重新建仓
- 多个 venue/product 的同资产仓位聚合 (`asset_exposure`)，包括现货多头 + 合约
  空头基本相抵的场景
- `fill_price = None` 时只更新数量、均价不变
- 并发 `on_filled` 调用不丢更新 (针对 `InMemoryPositionStore::update` 的原子性)

## 后期扩展计划

### 1. Redis/sled 持久化 `PositionStore`
重启不丢仓位，需要用 `WATCH`/`MULTI` 或 Lua 脚本保证 `update()` 的原子读改写
语义。这一项也会替代/落地 `order_manager_design.md` 里"后期扩展计划 §1"的
Redis 持仓存储计划。

### 2. 已实现盈亏 / 手续费统计
在"非目标"里明确排除，需要额外记录每笔平仓的实现盈亏和手续费，属于更完整的
交易统计功能，建议单独设计。

### 3. quote 资产敞口追踪
当前只按 base 资产聚合，如果要监控 USDT 等 quote 资产的整体余额变化，需要在
`on_filled` 里再记一条 quote 侧的反向记账。

### 4. 跨资产的 RiskEngine 限额
现有 `RiskLimits.max_position` 仍是按单一 `(venue, symbol)`。有了
`asset_exposure` 之后，可以在 `RiskEngine::check` 里增加一层"资产级全局敞口
上限"检查 (如"BTC 全局净敞口任何时候不能超过 X")。

### 5. `main.rs` 命令接入 `OrderManager`/`PositionManager`
`open`/`rotate`/`close` 目前完全绕开 `order_manager`，直接调用 `execution::` 里
的裸函数。接入之后 `close` 才能真正支持"自动查仓位、不用手动传 qty"。

## 总结

仓位管理模块提供：
- ✅ 按 `(venue, symbol)` 的净数量 + 加权平均建仓价
- ✅ 按 base 资产跨 venue/产品的全局净敞口聚合 (现货多头/合约空头自动相抵)
- ✅ `RiskEngine` 收敛为对 `PositionManager` 的委托，消除双份状态
- ✅ `PositionStore` trait 占位，后续可无痛切换 Redis/sled
- 🔄 不含 PnL、quote 资产敞口、持久化 (见"非目标"，均已列入后期扩展计划)
