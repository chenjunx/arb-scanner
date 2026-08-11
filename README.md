# arb-scanner

一个通用、可扩展的加密货币交易套利机会监控框架。当前只做**发现与记录套利机会**，不涉及真实下单。

## 架构

```
MarketDataSource (行情源，可扩展)
        │  MarketEvent (venue, symbol, quote)
        ▼
  ArbitrageEngine (维护 (venue,symbol)->Quote 快照缓存)
        │  MarketView (只读快照视图)
        ▼
     Strategy (可扩展)  ──emit──▶  Opportunity
        │
        ▼
  OpportunitySink (可扩展，如日志)
```

- `market_data::MarketDataSource`：行情数据源扩展点。当前只有 `market_data::mock::MockSource`，用随机游走模拟行情，便于跑通全链路和写测试。**接入真实交易所时，实现该 trait 并在 `main.rs` 中注册即可**，无需改动引擎或策略代码。
- `strategy::Strategy`：套利策略扩展点。已提供两个示例实现：
  - `strategy::cross_exchange::CrossExchangeStrategy`：跨交易所同交易对价差套利。
  - `strategy::triangular::TriangularStrategy`：单交易所内的三角套利。
- `sink::OpportunitySink`：套利机会输出扩展点。已提供 `sink::log_sink::LogSink`，把机会打到日志里。

## 运行

```bash
cargo run
# 或指定配置文件
cargo run -- path/to/config.toml
```

日志级别通过 `RUST_LOG` 环境变量控制（默认 `info`）。

## 配置

见 `config.toml` 示例。核心字段：
- `venues`：监控的交易所/场所列表及各自的 taker 手续费（bps）。
- `symbols`：监控的交易对、（用于 mock 数据源的）初始价格与波动/价差参数。
- `triangular_paths`：三角套利路径，每条路径包含同一 venue 上首尾相接的三腿。
- `min_profit_bps`：低于该收益（基点）的机会不会被上报。

## 测试

```bash
cargo test
```

- 单元测试：`src/strategy/cross_exchange.rs`、`src/strategy/triangular.rs` 中验证价差/三角收益计算逻辑。
- 集成测试：`tests/engine_tests.rs` 通过 mock `MarketEvent` 驱动完整的 engine -> strategy -> sink 链路。

## 后续扩展方向（未在当前版本实现）

- 接入真实交易所 WS/REST 行情（实现 `MarketDataSource`）。
- 支持深度行情（而非仅最优一档），用于估算可成交数量与滑点。
- 模拟下单/回测执行层，以及对接真实交易所 API 的执行层。
