# arb-scanner

一个通用、可扩展的加密货币交易套利机会监控框架。核心链路（`market_data` -> `engine` -> `strategy` -> `sink`）只做**发现与记录套利机会**，不会自动下单；`order` 模块提供了按需调用的真实下单能力（见下文"下单（执行层）"），但目前不接入这条自动化链路，需要调用方显式触发。

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

## 钱包（转账层）

`wallet` 模块是独立于上面"发现套利机会"主链路的**按需调用的库接口**，不接入 `main.rs` 的 engine 主循环，也没有常驻轮询任务。它提供的是转账相关能力，不做账户余额查询/追踪：

- 读取某个币种支持哪些链/网络，以及每条链的充值/提币开关、最小提币量、手续费（`WalletProvider::asset_info`）。
- 读取充值地址（`WalletProvider::deposit_address`）。
- 发起提币（`WalletProvider::withdraw`）。

统一接口定义在 `src/wallet/mod.rs` 的 `WalletProvider` trait（`async-trait`，可用 `Box<dyn WalletProvider>`），每个交易所在各自文件里实现：

- `wallet::binance::BinanceWalletProvider`：签名用 **Ed25519**（非对称公私钥），不是 HMAC。凭证通过环境变量传入：
  - `BINANCE_API_KEY`
  - `BINANCE_API_SECRET`（完整 PEM 文本，直接存内容，不是文件路径）
- `wallet::kraken::KrakenWalletProvider`：签名沿用 Kraken 标准的 HMAC-SHA512。凭证：
  - `KRAKEN_SPOT_API_KEY`
  - `KRAKEN_SPOT_API_SECRET`

  **注意语义差异**：Kraken 的提币接口不接受任意链上地址，`WithdrawRequest.address` 对 Kraken 而言是**预先在 Kraken 账户网页上登记好的地址别名**（`key`），不是原始地址；而 Binance 是直接传原始链上地址。两边接口签名一致，但这个字段的实际含义不同，使用时不能混用。

`WalletProvider::withdraw()` 是 trait 的默认方法，内置两道护栏，各交易所实现无法绕过：
1. 调用前自动查询链信息，校验目标网络是否开放提币、金额是否达到最小提币量。
2. 支持 `WithdrawRequest.dry_run = true`：校验通过后直接返回（`id = "dry-run"`），不会真正发起提币请求。

各交易所只需要实现 `asset_info`/`deposit_address`/`withdraw_raw`（真正发起提币请求的原语），不需要重复写校验逻辑。

## 下单（执行层）

`order` 模块和 `wallet` 一样是独立于"发现套利机会"主链路的**按需调用的库接口**，不接入 `main.rs` 的 engine 主循环。它提供的是真实下单能力，目前只实现**市价单**：

- 查询某个交易对的下单精度限制：数量步进、最小下单量（`OrderProvider::market_info`）。
- 提交市价单（`OrderProvider::place_market_order`）。

统一接口定义在 `src/order/mod.rs` 的 `OrderProvider` trait（`async-trait`，可用 `Box<dyn OrderProvider>`），每个交易所在各自文件里实现：

- `order::binance::BinanceOrderProvider`（现货）：签名方式和 `wallet::binance::BinanceWalletProvider` 一致（Ed25519），复用同一套环境变量（`BINANCE_API_KEY` / `BINANCE_API_SECRET`）。
- `order::binance_futures::BinanceFuturesOrderProvider`（USDT-M 永续合约）：host 是 `fapi.binance.com`（测试网 `testnet.binancefuture.com`），签名方式和现货一致（Ed25519），复用同一套环境变量（`BINANCE_API_KEY` / `BINANCE_API_SECRET`，同一个 Key 上勾选现货+合约交易权限即可）。**注意限制**：只支持币安账户默认的单向持仓模式（One-way Mode），不传 `positionSide`；如果账户被手动切换成双向持仓模式（Hedge Mode），下单会报错，需要用户自行确保账户模式一致。杠杆、保证金模式、限价单、平仓/`reduceOnly` 暂不支持。
- `order::kraken::KrakenOrderProvider`：签名方式和 `wallet::kraken::KrakenWalletProvider` 一致（HMAC-SHA512），复用同一套环境变量（`KRAKEN_SPOT_API_KEY` / `KRAKEN_SPOT_API_SECRET`）。

  **注意限制**：Kraken 的 `AddOrder` 接口对市价单只同步返回 `txid`，不保证同时告知成交结果，因此该实现的下单结果固定是 `OrderStatus::New`、`filled_qty=0`、`avg_price=None`；Binance 的下单接口会同步返回成交数量和均价。要拿到 Kraken 市价单的真实成交结果，需要额外查询订单状态（本模块暂未实现，见下方"后续扩展方向"）。

`OrderProvider::place_market_order()` 是 trait 的默认方法，内置护栏，各交易所实现无法绕过：
1. 调用前校验下单数量为正、达到最小下单量、是数量步进的整数倍。
2. 支持 `MarketOrderRequest.dry_run = true`：校验通过后直接返回（`order_id = "dry-run"`），不会真正提交订单。

各交易所只需要实现 `market_info`/`place_market_order_raw`（真正提交订单的原语），不需要重复写校验逻辑。

除了按基础币数量下单，币安现货还支持按计价币金额下单（`quoteOrderQty`）：`OrderProvider::place_market_order_by_quote()`，花指定数量的计价币（如 100 USDT），买/卖多少基础币由交易所按下单那一刻的价格决定，下单前不知道精确的基础币数量，因此不做 `qty_step`/`min_qty` 校验（只校验金额为正），`dry_run` 语义和 `place_market_order` 一致。目前只有 `order::binance::BinanceOrderProvider`（现货）实现了这个能力，`order::binance_futures::BinanceFuturesOrderProvider` 和 `order::kraken::KrakenOrderProvider` 都没有重写，调用会走 trait 默认实现直接报错"不支持"。

## 开仓（跨所对冲，`open` 子命令）

`src/execution/mod.rs` 把 `order`/`wallet` 两个模块串成一个手动触发的完整流程：**币安现货按 USDT 金额买入 -> 币安 U 本位合约等量做空对冲 -> 买入量的一半划转到 Kraken 现货**。这个流程不接入 `engine`/`strategy`/`sink` 主链路，只能通过 CLI 子命令手动触发：

```bash
cargo run -- open --symbol BTC/USDT --amount 100
# 加 --live 才会真正下单/提币，默认是 dry-run
cargo run -- open --symbol BTC/USDT --amount 100 --live
```

参数：
- `--symbol BASE/QUOTE`（必填）：如 `BTC/USDT`。
- `--amount <数量>`（必填）：花费的计价币（USDT）金额。
- `--asset <coin>`（可选）：划转到 Kraken 的资产代码，默认等于 `symbol` 的 base（如 `BTC`）。
- `--testnet`（可选）：同时作用于币安现货/合约 provider。
- `--dry-run` / `--live`：**默认是 `--dry-run`**（涉及真实资金操作的安全默认值），必须显式传 `--live` 才会真正提交订单和提币。
- `--client-order-id-prefix <prefix>`（可选）：追加到现货/合约订单的客户端订单号上。

凭证仍然是环境变量：`BINANCE_API_KEY`/`BINANCE_API_SECRET`（同时用于现货和合约）、`KRAKEN_SPOT_API_KEY`/`KRAKEN_SPOT_API_SECRET`。

**已知限制**：
- 假设合约账户已经预先充值好保证金，不做现货 -> 合约的内部划转。
- `--dry-run`（默认）下只会校验并模拟现货买入这一步；合约对冲和划转数量都依赖真实成交量，dry-run 下不做模拟，`open` 命令打印结果里的 `note` 字段会说明这一点。
- 划转网络完全自动匹配（同时查币安 `asset_info` 可提币网络和 Kraken `asset_info` 可存款方式，用内置的关键词表做子串匹配），**不提供任何网络相关的 CLI 参数**，匹配不到唯一网络会直接报错列出两边候选。这份关键词表是内置的静态猜测，只覆盖了几个常见资产（BTC/ETH/TRX/SOL/BNB Smart Chain/Polygon），且没有逐一核对过 Kraken 方式名的真实拼写——**首次对某个新资产启用 `--live` 之前，强烈建议先看日志里 `resolve_transfer_network` 打印出的匹配结果人工确认一遍**，避免因为表里的关键词猜错而把币转去错误的网络。
- 任何一步失败都直接报错退出，不做自动回滚/重试，半吊子仓位需要人工介入。

## 测试

```bash
cargo test
```

- 单元测试：`src/strategy/cross_exchange.rs`、`src/strategy/triangular.rs` 中验证价差/三角收益计算逻辑；`src/wallet/mod.rs` 中验证 `withdraw()` 护栏逻辑（用测试替身，不发真实请求）；`src/wallet/binance.rs`、`src/wallet/kraken.rs` 中验证签名算法和响应 JSON 解析；`src/order/mod.rs` 中验证 `place_market_order()`/`place_market_order_by_quote()` 护栏逻辑；`src/order/binance.rs`、`src/order/binance_futures.rs`、`src/order/kraken.rs` 中验证签名算法和响应 JSON 解析；`src/execution/mod.rs` 中用测试替身验证开仓流程编排（dry-run 短路、下单失败提前中止、合约数量取整、划转数量减半截断、划转网络自动匹配）。
- 集成测试：`tests/engine_tests.rs` 通过 mock `MarketEvent` 驱动完整的 engine -> strategy -> sink 链路。

## 后续扩展方向（未在当前版本实现）

- 接入真实交易所 WS/REST 行情（实现 `MarketDataSource`）。
- 支持深度行情（而非仅最优一档），用于估算可成交数量与滑点。
- 订单状态查询/撤单，以及限价单等其它订单类型。
