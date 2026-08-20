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

## 选币扫描（`scan` 子命令）

`scan` 是 `monitor`/`open`/`rotate` 之前的只读选币工具：找出币安（USDT 现货 + 可用于对冲的 USDT 永续合约）和 Kraken（USDT 现货）之间"有交集"的币种，打印详情供人工参考，不产生任何交易或划转。不接入 `engine` 主循环，也不读取除黑名单外的 `config.toml` 配置。

```bash
cargo run -- scan
cargo run -- scan --testnet
```

参数：
- `--testnet`（可选）：作用于币安 exchange-info/wallet provider（Kraken 无测试网）。

运行流程和判定标准与 `monitor` 第 1 步完全一致（复用同一个 `scan::find_overlap`）：币安有 USDT 现货且配对着 USDT 永续、Kraken 有 USDT 现货、且两边钱包信息里至少共享一条可转账链；命中 `config.toml` 里 `[scan] blacklist` 的币种在发起任何网络请求前就被剔除。区别在于 `scan` 只打印结果就退出，不会像 `monitor` 那样接着起行情源持续监控价差，也不查询交易手续费。

输出内容（依次打印）：
- 币安 USDT 现货里"配好永续对冲"的完整 symbol 列表。
- Kraken USDT 现货完整 symbol 列表。
- 两边真正的交集表（币种 + 币安永续 symbol + Kraken 现货 symbol + 共同可转账链）。
- 求交集过程中被跳过的候选（两边都挂牌、但钱包信息查询失败或没有共同链），附带原因。
- 命中黑名单被提前剔除的币种列表。

## 实时价差监控（`monitor` 子命令）

`monitor` 不走 `config.toml` 驱动的 mock 数据源，而是直接对接真实交易所接口，自动发现币安现货（且该币种在币安还有 USDT 永续合约可用于对冲）与 Kraken USDT 现货之间的重叠币种，并持续监控两边价差。同时把仓位/组合盈亏/资金费追踪/定期报告这几个"基础服务"当作 `monitor` 常驻进程的一部分一起跑起来（除非传了 `--no-portfolio`），不再需要分开手动起 `accounting`/`report` 进程。不会自动下单，下单走 `open` 子命令，`open --live` 写进 Redis 的仓位会被这里自动读到并持续跟踪。

```bash
cargo run -- monitor
```

参数：
- `--testnet`（可选）：作用于币安 exchange-info/wallet/期货行情/资金费 provider（Kraken 无测试网）。
- `--min-profit-bps <bps>`（可选，默认 0）：低于该收益（基点）的机会不打日志。
- `--no-portfolio`（可选）：关闭仓位/组合盈亏/资金费/报告追踪，退回纯价差扫描，不连接 Redis。
- `--funding-interval-secs <secs>`（可选，默认 1800）：资金费轮询间隔，同 `accounting` 子命令的 `--interval-secs`。
- `--funding-initial-lookback-hours <hours>`（可选，默认 168）：资金费首次轮询时的历史回溯窗口，同 `accounting` 子命令的 `--initial-lookback-hours`。
- `--report-interval-secs <secs>`（可选，默认 300）：定期报告打印间隔，同 `report` 子命令的 `--interval-secs`。

运行流程：
1. 调用 `scan::find_overlap`（`BinanceExchangeInfoProvider` + `KrakenExchangeInfoProvider` + 两边 `WalletProvider`）算出候选币种交集：币安有 USDT 现货且配对着 USDT 永续（用于对冲）、Kraken 有 USDT 现货、且两边钱包信息里至少共享一条可转账链。命中 `config.toml` 里 `[scan] blacklist` 的币种会在发起任何网络请求前被剔除，打印在 `blacklisted` 列表里。
2. 对剩下的候选币种并发查询真实 taker 手续费（`ExchangeInfoProvider::spot_trading_fee`），币安一侧再乘以 `config.toml` 里 `[venues.binance] fee_discount`（如 BNB 抵扣，默认 1 不打折）算出实际有效费率。查询失败的币种直接跳过、不会进入监控列表，汇总打印在"Skipped"里。
3. 用查完手续费的 symbol 列表起两个真实行情源——`BinanceSpotSource`（venue=`binance`）和 `KrakenSpotSource`（venue=`kraken`）——推流进 `ArbitrageEngine`。
4. 给每个 symbol 建一个 `strategy::cross_exchange::CrossExchangeStrategy`，配 `sink::log_sink::LogSink`，标准 `engine -> strategy -> sink` 链路跑起来，只有双边价差（扣除两边手续费后）≥ `--min-profit-bps` 才会打日志。
5. 除非传了 `--no-portfolio`：连接 `REDIS_URL`（默认 `redis://127.0.0.1:6379/`，连不上直接报错退出）建出 `PositionManager`/`PortfolioManager`；额外起一个 `BinanceFuturesSource`（venue=`binance_futures`，同一批 symbol）把期货行情也推进 `ArbitrageEngine::shared_cache()`，供 `PortfolioManager` 做现货+期货两条腿的 mark-to-market（`unrealized_pnl`/`market_value` 不再是 `N/A`）；起一个 `FundingFeeTracker` 按 `--funding-interval-secs` 轮询资金费流水；起一个 `ReportTracker` 按 `--report-interval-secs` 打印投资组合/仓位/订单报告。

已知限制：
- 只支持"币安现货+永续对冲 vs Kraken 现货"这一对交易所组合，写死在代码里。
- 黑名单和手续费折扣固定从当前目录下的 `config.toml` 读取（`ScanConfig::load_blacklist` / `VenueConfig::load_fee_discount`），没有 `--config` 参数可以指定其它路径。
- 只发现机会、打日志，不做任何下单动作；要真正开仓走 `open` 子命令。

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
- `--symbol BASE/QUOTE`（必填，`--from-transfer` 模式下可省略，见下）：如 `BTC/USDT`。
- `--amount <数量>`（完整流程必填）：花费的计价币（USDT）金额。
- `--asset <coin>`（可选）：划转到 Kraken 的资产代码，默认等于 `symbol` 的 base（如 `BTC`）。
- `--testnet`（可选）：同时作用于币安现货/合约 provider。
- `--dry-run` / `--live`：**默认是 `--dry-run`**（涉及真实资金操作的安全默认值），必须显式传 `--live` 才会真正提交订单和提币。
- `--client-order-id-prefix <prefix>`（可选）：追加到现货/合约订单的客户端订单号上。
- `--transfer-to-kraken`（可选，默认不划转）：完整流程执行到最后是否把买入量的一半划到 Kraken；不传则只做"现货买入 + 合约对冲"两步，不触发链上转账。
- `--fill-timeout-secs <secs>`（可选，默认 60）：`--live` 模式下等待现货/合约成交回报的超时时间。
- `--from-transfer`（可选，独立模式，见下）。
- `--filled-qty <数量>`（`--from-transfer` 模式必填）：原先现货买入腿的实际成交数量。

**`--from-transfer` 模式**：跳过下单，只执行"把已有仓位的一半划转到 Kraken"这一步（用于现货买入、合约对冲都已经手动/分开完成，只需要补上划转动作的场景）。此时忽略 `--amount`，`--asset`（或 `--symbol` 的 base）+ `--filled-qty` 决定划多少：

```bash
cargo run -- open --from-transfer --asset BTC --filled-qty 0.01 --live
```

完整流程示例：
```bash
cargo run -- open --symbol BTC/USDT --amount 100
# 加 --live 才会真正下单/提币，默认是 dry-run；加 --transfer-to-kraken 才会触发划转
cargo run -- open --symbol BTC/USDT --amount 100 --live --transfer-to-kraken
```

凭证仍然是环境变量：`BINANCE_API_KEY`/`BINANCE_API_SECRET`（同时用于现货和合约）、`KRAKEN_SPOT_API_KEY`/`KRAKEN_SPOT_API_SECRET`。

**已知限制**：
- 假设合约账户已经预先充值好保证金，不做现货 -> 合约的内部划转。
- `--dry-run`（默认）下只会校验并模拟现货买入这一步；合约对冲和划转数量都依赖真实成交量，dry-run 下不做模拟，`open` 命令打印结果里的 `note` 字段会说明这一点。
- `--live` 模式下两条腿（现货买入、合约做空）都要走完整的 `OrderManager` 流水线（风控 -> 执行引擎 -> 交易所私有 WS 成交确认），需要能连上 `REDIS_URL`（默认 `redis://127.0.0.1:6379/`），连不上直接快速失败。
- 划转网络完全自动匹配（同时查币安 `asset_info` 可提币网络和 Kraken `asset_info` 可存款方式，用内置的关键词表做子串匹配），**不提供任何网络相关的 CLI 参数**，匹配不到唯一网络会直接报错列出两边候选。这份关键词表是内置的静态猜测，只覆盖了几个常见资产（BTC/ETH/TRX/SOL/BNB Smart Chain/Polygon），且没有逐一核对过 Kraken 方式名的真实拼写——**首次对某个新资产启用 `--live` 之前，强烈建议先看日志里 `resolve_transfer_network` 打印出的匹配结果人工确认一遍**，避免因为表里的关键词猜错而把币转去错误的网络。
- 任何一步失败都直接报错退出，不做自动回滚/重试，半吊子仓位需要人工介入。

## 库存轮转（`rotate` 子命令）

在两个交易所之间调整现货库存：一个交易所卖出、另一个交易所买入等量同一资产，两条腿真实市价单**并发**发起，不涉及任何链上划转（比 `open` 的划转步骤更快）。和 `open` 一样是独立的手动触发操作，不接入 `engine` 主循环，也不读取 `config.toml`。

```bash
cargo run -- rotate --symbol BTC/USDT --qty 0.5 --sell binance --buy kraken
# 加 --live 才会真正下单，默认是 dry-run
cargo run -- rotate --symbol BTC/USDT --qty 0.5 --sell binance --buy kraken --live
```

参数：
- `--symbol BASE/QUOTE`（必填）。
- `--qty <数量>`（必填）：按基础币数量指定（而非计价币金额），因为 Kraken 市价单只支持按基础币下单，两条腿要用同一套校验路径。
- `--sell <venue>` / `--buy <venue>`（均必填，必须不同）：取值只能是 `binance`（现货）或 `kraken`（现货）。
- `--testnet`（可选）：只影响 binance 一侧，Kraken 下单客户端不支持 testnet。
- `--dry-run` / `--live`：默认 `--dry-run`。
- `--client-order-id-prefix <prefix>`（可选）。

**已知限制**：两条腿互相独立、不做自动回滚——如果一条腿失败、另一条已经成交，会留下单边仓位，返回的错误信息里会带上已成交那条腿的完整订单详情，需要人工介入对账。

## 平仓（`close` 子命令）

平掉币安现货、Kraken 现货、币安合约三条腿，**每条腿相互独立、可以只传其中一部分**（例如只平掉某个从没转过 Kraken 的币种的两条腿）。三条腿的数量都要在命令行里显式指定——代码库里没有余额/持仓查询接口，没法自动算出"全部"是多少，需要调用方自己核对仓位后传入。同样不接入 `engine` 主循环，也不读取 `config.toml`。

```bash
cargo run -- close --symbol BTC/USDT --binance-spot-qty 0.005 --futures-qty 0.01
cargo run -- close --symbol BTC/USDT --kraken-spot-qty 0.005 --live
```

参数：
- `--symbol BASE/QUOTE`（必填）。
- `--binance-spot-qty <数量>` / `--kraken-spot-qty <数量>` / `--futures-qty <数量>`（三者至少传一个）：只有对应的 `--xxx-qty` 被传入，才会构造那个交易所的 provider——例如只传 `--binance-spot-qty` 时不需要配置 Kraken 的 API key。现货两条腿是卖出，合约腿是买回（对应 `open` 里合约腿卖出开空，平仓自然是买回平空）。
- `--testnet`（可选）：只影响币安现货/合约。
- `--dry-run` / `--live`：默认 `--dry-run`。
- `--client-order-id-prefix <prefix>`（可选）。

**已知限制**：三条腿并发发起、互相独立，不做自动回滚——任意一条腿失败，其它已经成交的腿不会被撤销，返回的错误信息里会带上三条腿各自的成交/跳过/失败情况，需要人工介入对账。

## 资金费追踪（`accounting` 子命令）

独立常驻进程：定期轮询交易所资金费流水，通过 `PositionManager::apply_adjustment`（`AdjustmentReason::Funding`）累加进对应仓位的 `realized_pnl`。跟踪对象是 `PositionManager`（Redis 支撑）里每次轮询时读到的**当前非零仓位**，而不是启动时固定的一份列表，所以 `open`/`close` 开平的期货仓位不需要重启这个进程就能被自动跟踪/停止跟踪。如果 `monitor` 已经在跑且没加 `--no-portfolio`，通常不需要单独起本命令；只需要资金费追踪、不想启动价差扫描和行情连接时单独使用。

```bash
cargo run -- accounting
cargo run -- accounting --testnet --interval-secs 900 --initial-lookback-hours 72
```

参数：
- `--testnet`（可选）：作用于币安合约 provider。
- `--interval-secs <secs>`（可选，默认 1800）：轮询间隔。
- `--initial-lookback-hours <hours>`（可选，默认 168）：某个 `(venue, symbol)` 第一次被轮询、还没有游标时往回补多久的历史资金费记录。

流程：每次 tick 从 `PositionManager::all_positions()` 重新读非零仓位，按 venue 找对应的 `FundingFeeProvider`（目前只注册了 `binance_futures` 一家）；用 Redis 里的游标（`arb_scanner:funding_cursor` Hash，field=`"{venue}|{symbol}"`，同时存 `last_time_ms`/`last_tran_id`，因为 Binance 只能按时间范围查、没有增量参数）算出本次查询起点，拉回流水后按 `tran_id` 过滤掉已入账的记录，累加进仓位已实现盈亏（`PositionManager::apply_adjustment`），游标推进到最新位置。

依赖 `REDIS_URL`（默认 `redis://127.0.0.1:6379/`）连接 `RedisPositionStore`/`RedisPnlStore`/`RedisFundingCursorStore`，凭证走 `BINANCE_API_KEY`/`BINANCE_API_SECRET`。跑起来后一直轮询直到 ctrl-c。

## 定期报告（`report` 子命令）

独立常驻进程：定期把投资组合盈亏/仓位明细/订单概览汇总成一份报告，分发给已注册的输出渠道（目前只有把内容打到日志的 `LogChannel`）。单独跑本命令时只连接 Redis 读数据，不接入实时行情，所以报告里的 `market_value`/`unrealized_pnl` 会显示为 `N/A`；如果通过 `monitor`（未加 `--no-portfolio`）驱动，接了实时行情，会有真实数字。如果 `monitor` 已经在跑且没加 `--no-portfolio`，通常不需要单独起本命令；只需要报告、不想启动价差扫描和行情连接时单独使用。

```bash
cargo run -- report
cargo run -- report --interval-secs 60
```

参数：
- `--interval-secs <secs>`（可选，默认 300）：报告生成/分发间隔。

三个内置 section（各自实现 `ReportSection` trait，`render()` 是同步方法）：
- **投资组合盈亏**：按 base 资产聚合，列出 `net_qty`/`market_value`(N/A)/`realized_pnl`/`fees_paid`/`unrealized_pnl`(N/A)/`net_pnl`。资产列表从当前持仓里出现过的 symbol.base 去重得到。
- **仓位明细**：按 venue+symbol 列出当前非零仓位的净数量和均价，已平仓的记录不列出。
- **订单概览**：订单状态计数（New/PartiallyFilled/Filled/Rejected/Expired）+ 最近挂单明细（最多列 20 条），直接读 `OrderStore`（跨进程可见的实时数据），而不是 `OrderManager` 进程内存态。

单个 channel 发送失败只记录警告并继续尝试其它 channel，不影响下一轮报告生成。依赖 `REDIS_URL`（默认 `redis://127.0.0.1:6379/`）连接 `RedisPositionStore`/`RedisPnlStore`/`RedisOrderStore`。跑起来后一直定时生成直到 ctrl-c。

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
