# 订单管理系统设计文档

## 概述

订单管理系统 (OrderManager) 提供了完整的订单生命周期管理，包括风控检查、执行路由、状态跟踪和事件通知。

## 架构设计

### 数据流

```
┌─────────────────────────────────────────────────────────────────┐
│                          Strategy                                │
│  - on_update() 发现机会 -> 提交 OrderRequest                     │
│  - on_order_event() 接收成交回调 -> 更新策略内部状态             │
└────────────┬────────────────────────────────────────────▲────────┘
             │ submit_order()                              │ OrderEvent
             ▼                                             │
┌─────────────────────────────────────────────────────────┴────────┐
│                       OrderManager                               │
│  - 生成唯一 OrderId                                              │
│  - 创建 Order 对象，存入内存状态表                                │
│  - 发送 Submitted 事件                                           │
│  - 启动后台任务执行风控+下单流程                                  │
└────────────┬─────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        RiskEngine                                │
│  check():                                                        │
│    - 单笔订单金额检查 (max_order_amount)                          │
│    - 持仓限额检查 (max_position，内存净持仓计数)                  │
│    - 下单频率检查 (max_orders_per_window，简单计数)               │
│  on_filled(): 用真实成交量更新持仓                                │
│  release(): 订单失败时释放预占用额度                              │
│  【后期扩展】: 接入 Redis 存储持仓状态                            │
└────────────┬─────────────────────────────────────────────────────┘
             │ Approved / Rejected
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     ExecutionEngine                              │
│  - 维护 venue -> ExchangeAdapter 路由表                          │
│  - execute(order) 找到对应 adapter 提交订单                       │
│  - 发送 Accepted 事件 (已路由到交易所)                            │
│  - 解析交易所返回，发送 Filled/PartiallyFilled/Rejected 事件     │
└────────────┬─────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     ExchangeAdapter                              │
│  - 持有 Arc<dyn OrderProvider>                                   │
│  - 把内部 Order 转成 MarketOrderRequest                           │
│  - 调用现有的 OrderProvider::place_market_order                   │
└────────────┬─────────────────────────────────────────────────────┘
             │
             ▼
        交易所 API (binance/kraken 现货、binance 合约)
```

## 核心组件

### 1. OrderManager

**职责**
- 订单生命周期管理的统一入口
- 生成全局唯一 `OrderId` (格式: `ORD-000000000001`)
- 维护订单状态表 (内存 `HashMap<OrderId, Order>`)
- 协调风控引擎和执行引擎
- 发布订单事件到事件总线

**主要接口**
```rust
impl OrderManager {
    // 提交订单 (异步执行，立即返回)
    pub async fn submit_order(&self, request: OrderRequest) -> OrderResponse;
    
    // 查询订单状态
    pub fn get_order(&self, order_id: &OrderId) -> Option<Order>;
    
    // 获取所有订单 (监控/调试)
    pub fn all_orders(&self) -> Vec<Order>;
}

pub struct OrderResponse {
    pub order_id: OrderId,
    // 可选等待最终结果 (测试用，生产环境建议通过事件异步处理)
    pub result_rx: oneshot::Receiver<Result<Order, String>>,
}
```

### 2. RiskEngine

**职责**
- 订单提交前的风控检查
- 维护持仓状态 (当前内存实现，预留 Redis 接口)
- 下单频率控制

**风控规则**
1. **单笔限额**: `max_order_amount` - 单个订单的最大金额
2. **持仓限额**: `max_position` - 净持仓的绝对值上限
3. **频率限制**: `max_orders_per_window` - 滑动窗口内最大订单数 (当前简单计数)

**主要接口**
```rust
impl RiskEngine {
    // 检查订单是否符合风控规则 (预占用额度)
    pub fn check(&self, venue: &Venue, symbol: &Symbol, 
                 side: OrderSide, amount: &OrderAmount) -> RiskCheckResult;
    
    // 订单成交后回写持仓
    pub fn on_filled(&self, venue: &Venue, symbol: &Symbol, 
                     side: OrderSide, filled_qty: Decimal);
    
    // 订单失败释放预占用
    pub fn release(&self, venue: &Venue, symbol: &Symbol);
    
    // 查询当前持仓
    pub fn position(&self, venue: &Venue, symbol: &Symbol) -> Decimal;
}
```

**风控配置示例**
```rust
let mut risk_limits = HashMap::new();
risk_limits.insert(
    (Venue::new("binance"), Symbol::new("BTC", "USDT")),
    RiskLimits {
        max_order_amount: Decimal::new(1, 0),      // 单笔最大 1 BTC
        max_position: Decimal::new(10, 0),         // 最大持仓 10 BTC
        max_orders_per_window: 100,                // 最多 100 单
    },
);
let risk_engine = Arc::new(RiskEngine::new(risk_limits));
```

### 3. ExecutionEngine

**职责**
- 根据 `venue` 路由订单到对应的交易所适配器
- 处理交易所返回结果
- 发布订单事件 (Accepted/Filled/PartiallyFilled/Rejected)

**主要接口**
```rust
impl ExecutionEngine {
    // 执行订单 (路由到对应交易所)
    pub async fn execute(&self, order: Order) -> Order;
}
```

### 4. ExchangeAdapter

**职责**
- 封装现有的 `OrderProvider` (binance/kraken 现货、binance 合约)
- 将内部 `Order` 转换为 `MarketOrderRequest`
- 调用交易所 API 实际下单

**构建示例**
```rust
// 注册多个交易所适配器
let mut adapters = HashMap::new();

let binance_provider = Arc::new(BinanceOrderProvider::from_env(...)?);
adapters.insert(
    Venue::new("binance"),
    Arc::new(ExchangeAdapter::new(Venue::new("binance"), binance_provider))
);

let kraken_provider = Arc::new(KrakenOrderProvider::from_env(...)?);
adapters.insert(
    Venue::new("kraken"),
    Arc::new(ExchangeAdapter::new(Venue::new("kraken"), kraken_provider))
);

let execution_engine = Arc::new(ExecutionEngine::new(adapters, event_tx));
```

## 数据类型

### OrderRequest (策略提交)
```rust
pub struct OrderRequest {
    pub strategy_name: String,      // 策略标识
    pub venue: Venue,                // 目标交易所
    pub symbol: Symbol,              // 交易对
    pub side: OrderSide,             // Buy/Sell
    pub amount: OrderAmount,         // Base(qty) 或 Quote(amount)
    pub client_order_id: Option<String>,
    pub group_id: Option<String>,   // 关联同组订单 (如套利两条腿)
    pub metadata: Option<String>,   // 策略自定义元数据
}
```

### Order (内部订单状态)
```rust
pub struct Order {
    pub order_id: OrderId,          // OrderManager 生成的唯一ID
    pub request: OrderRequest,      // 原始请求
    pub status: OrderStatus,        // New/Filled/PartiallyFilled/Rejected
    pub filled_qty: Decimal,        // 已成交数量
    pub avg_price: Option<Decimal>, // 平均成交价
    pub exchange_order_id: Option<String>,  // 交易所返回的ID
    pub created_at_ms: u64,         // 创建时间戳
    pub updated_at_ms: u64,         // 最后更新时间戳
    pub reject_reason: Option<String>,  // 拒绝原因
}
```

### OrderEvent (通知策略)
```rust
pub enum OrderEvent {
    Submitted { order_id },                          // 已提交到风控
    Accepted { order_id },                           // 通过风控，已发送交易所
    RejectedByRisk { order_id, reason },            // 风控拒绝
    RejectedByExchange { order_id, reason },        // 交易所拒绝
    PartiallyFilled { order_id, filled_qty, avg_price },  // 部分成交
    Filled { order_id, filled_qty, avg_price },     // 完全成交
}
```

## 使用示例

### 1. 初始化订单管理系统

```rust
use arb_scanner::order_manager::{OrderManager, RiskEngine, ExecutionEngine};
use arb_scanner::order_manager::execution::ExchangeAdapter;
use arb_scanner::order_manager::risk::RiskLimits;
use arb_scanner::order_manager::types::OrderEvent;
use std::sync::Arc;
use tokio::sync::mpsc;

async fn setup_order_manager() -> Arc<OrderManager> {
    // 1. 配置风控限额
    let mut risk_limits = HashMap::new();
    risk_limits.insert(
        (Venue::new("binance"), Symbol::new("BTC", "USDT")),
        RiskLimits {
            max_order_amount: Decimal::new(1, 0),
            max_position: Decimal::new(10, 0),
            max_orders_per_window: 100,
        },
    );
    let risk_engine = Arc::new(RiskEngine::new(risk_limits));

    // 2. 创建事件通道
    let (event_tx, event_rx) = mpsc::channel::<OrderEvent>(1000);

    // 3. 注册交易所适配器
    let proxy = net::proxy_from_env();
    let binance_provider = Arc::new(
        BinanceOrderProvider::from_env(Venue::new("binance"), false, proxy.as_deref())?
    );
    
    let mut adapters = HashMap::new();
    adapters.insert(
        Venue::new("binance"),
        Arc::new(ExchangeAdapter::new(Venue::new("binance"), binance_provider))
    );

    let execution_engine = Arc::new(ExecutionEngine::new(adapters, event_tx.clone()));

    // 4. 创建 OrderManager
    let order_manager = Arc::new(OrderManager::new(
        risk_engine,
        execution_engine,
        event_tx,
    ));

    // 5. 启动事件分发任务 (分发给所有策略)
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            // 分发给所有策略的 on_order_event
            for strategy in &strategies {
                strategy.on_order_event(&event);
            }
        }
    });

    order_manager
}
```

### 2. 策略提交订单

```rust
use arb_scanner::order_manager::types::OrderRequest;
use arb_scanner::order::types::{OrderAmount, OrderSide};

async fn submit_arbitrage_orders(order_manager: &OrderManager) {
    // 跨交易所套利：币安买入，Kraken 卖出
    
    // 买单
    let buy_request = OrderRequest {
        strategy_name: "cross_exchange".to_string(),
        venue: Venue::new("binance"),
        symbol: Symbol::new("BTC", "USDT"),
        side: OrderSide::Buy,
        amount: OrderAmount::Base(Decimal::new(1, 1)), // 0.1 BTC
        client_order_id: Some("arb-buy-001".to_string()),
        group_id: Some("arb-group-001".to_string()),   // 同组关联
        metadata: Some("expected_profit_bps=150".to_string()),
    };

    let buy_response = order_manager.submit_order(buy_request).await;
    println!("买单已提交: {}", buy_response.order_id);

    // 卖单
    let sell_request = OrderRequest {
        strategy_name: "cross_exchange".to_string(),
        venue: Venue::new("kraken"),
        symbol: Symbol::new("BTC", "USDT"),
        side: OrderSide::Sell,
        amount: OrderAmount::Base(Decimal::new(1, 1)),
        client_order_id: Some("arb-sell-001".to_string()),
        group_id: Some("arb-group-001".to_string()),   // 同组关联
        metadata: Some("expected_profit_bps=150".to_string()),
    };

    let sell_response = order_manager.submit_order(sell_request).await;
    println!("卖单已提交: {}", sell_response.order_id);

    // 可选：等待结果 (测试用)
    if let Ok(result) = buy_response.result_rx.await {
        match result {
            Ok(order) => println!("买单成交: {:?}", order),
            Err(reason) => println!("买单失败: {}", reason),
        }
    }
}
```

### 3. 策略接收订单事件

```rust
use arb_scanner::strategy::Strategy;
use arb_scanner::order_manager::types::OrderEvent;
use std::sync::Mutex;

pub struct MyArbitrageStrategy {
    // ... 现有字段
    pending_orders: Mutex<HashMap<OrderId, OrderRequest>>,
}

impl Strategy for MyArbitrageStrategy {
    fn name(&self) -> &str {
        "my_arbitrage"
    }

    fn on_update(&self, view: &MarketView, changed: &MarketEvent) -> Vec<Opportunity> {
        // 现有逻辑：发现套利机会
        // ...
    }

    // 新增：处理订单事件
    fn on_order_event(&self, event: &OrderEvent) {
        match event {
            OrderEvent::Submitted { order_id } => {
                log::info!("订单已提交: {}", order_id);
            }
            OrderEvent::Accepted { order_id } => {
                log::info!("订单已接受: {}", order_id);
            }
            OrderEvent::Filled { order_id, filled_qty, avg_price } => {
                log::info!("订单成交: {} qty={} price={}", order_id, filled_qty, avg_price);
                // 更新策略内部状态
                self.pending_orders.lock().unwrap().remove(order_id);
            }
            OrderEvent::RejectedByRisk { order_id, reason } => {
                log::warn!("订单被风控拒绝: {} reason={}", order_id, reason);
                self.pending_orders.lock().unwrap().remove(order_id);
            }
            OrderEvent::RejectedByExchange { order_id, reason } => {
                log::warn!("订单被交易所拒绝: {} reason={}", order_id, reason);
                self.pending_orders.lock().unwrap().remove(order_id);
            }
            OrderEvent::PartiallyFilled { order_id, filled_qty, avg_price } => {
                log::info!("订单部分成交: {} qty={} price={}", order_id, filled_qty, avg_price);
            }
        }
    }
}
```

### 4. 查询订单状态

```rust
// 查询单个订单
if let Some(order) = order_manager.get_order(&order_id) {
    println!("订单状态: {:?}", order.status);
    println!("成交数量: {}", order.filled_qty);
    println!("成交均价: {:?}", order.avg_price);
}

// 查询所有订单 (监控/调试)
let all_orders = order_manager.all_orders();
for order in all_orders {
    println!("{} {} {} {:?}", 
        order.order_id, 
        order.request.venue, 
        order.request.symbol, 
        order.status
    );
}

// 查询风控持仓
let position = risk_engine.position(
    &Venue::new("binance"), 
    &Symbol::new("BTC", "USDT")
);
println!("当前持仓: {} BTC", position);
```

## 测试

已包含完整的单元测试:

```bash
# 运行订单管理模块测试
cargo test --lib order_manager

# 测试覆盖:
# - RiskEngine: 单笔限额/持仓限额/频率限制/释放额度
# - OrderManager: 提交订单/生成唯一ID/事件顺序
```

## 后期扩展计划

### 1. Redis 持仓存储
当前持仓状态存储在内存中，重启丢失。后期可接入 Redis:

```rust
// 扩展 RiskEngine，增加 Redis 后端
pub trait PositionStore: Send + Sync {
    fn get_position(&self, venue: &Venue, symbol: &Symbol) -> Decimal;
    fn update_position(&self, venue: &Venue, symbol: &Symbol, delta: Decimal);
}

pub struct RedisPositionStore {
    client: redis::Client,
}

impl PositionStore for RedisPositionStore {
    // 实现 Redis 读写
}
```

### 2. 真实滑动时间窗口
当前 `max_orders_per_window` 是简单计数，后期可改为基于时间戳的滑动窗口:

```rust
struct OrderTimestamps {
    window_ms: u64,
    timestamps: VecDeque<u64>,
}

impl OrderTimestamps {
    fn add(&mut self, ts_ms: u64) {
        let cutoff = ts_ms.saturating_sub(self.window_ms);
        while self.timestamps.front().map(|&t| t < cutoff).unwrap_or(false) {
            self.timestamps.pop_front();
        }
        self.timestamps.push_back(ts_ms);
    }
    
    fn count(&self) -> usize {
        self.timestamps.len()
    }
}
```

### 3. group_id 订单组管理
当前 `group_id` 字段仅存储，未实现按组查询/等待。后期可添加:

```rust
impl OrderManager {
    // 查询同组所有订单
    pub fn orders_by_group(&self, group_id: &str) -> Vec<Order>;
    
    // 等待整组订单完成
    pub async fn wait_for_group(&self, group_id: &str) -> Vec<Order>;
}
```

## 总结

订单管理系统提供了完整的订单生命周期管理能力:
- ✅ 风控检查 (单笔限额/持仓限额/频率限制)
- ✅ 执行路由 (多交易所适配器)
- ✅ 状态跟踪 (内存订单表)
- ✅ 事件通知 (Strategy.on_order_event 回调)
- ✅ 单元测试覆盖
- 🔄 预留 Redis 扩展接口
- 🔄 group_id 关联字段 (暂未实现组查询)
