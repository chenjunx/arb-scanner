# 代码规范

## 策略提交订单必须走 `Strategy` trait 的方法

`src/strategy/mod.rs` 里 `Strategy` trait 提供了 `submit_order`（市价）、
`submit_limit_ioc_order`（限价 IOC）、`submit_transfer`（划转）三个默认方法，
统一负责拼 `strategy_id`/`client_order_id`、通过 `self.bus()` 发布到
`Topic::order_submit()`。

任何 `Strategy` 实现在下单时都必须调用这些方法，禁止绕过它们手搓
`OrderRequest`/`TransferRequest` 再自己 `bus.publish(...)`——包括策略自带的
配置结构体（如 `CrossExecutionConfig`）里另外存一份 `bus`/`strategy_name`
字段来发布订单。原因：

- `self.bus()`/`self.name()` 是唯一可信来源，另存一份等价字段会造成两边可能
  不一致（例如配置里的 `strategy_name` 打错、和 `Strategy::name()` 对不上，
  会导致订单事件订阅/路由错位却不会立刻报错）。
- 手搓的 `OrderRequest` 容易漏字段或跟 trait 默认实现的字段语义产生偏差。

需要在下单前拿到 `client_order_id`（比如要先登记进本地的 pending 表再发布）
时，自己生成好 id 传给 `client_order_id` 参数即可，不需要因此放弃这几个方法。
