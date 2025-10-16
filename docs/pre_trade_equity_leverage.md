# 预交易总权益与杠杆率计算说明

本文记录 `pre_trade` 模块在推送 `pre_trade_risk` 过程中，总权益与杠杆率的计算方式，便于排查与对齐后台/前台的数值。

## 1. 数据来源概览

1. `pre_trade::runner` 根据最新的账户快照（现货余额、UM 持仓、价格表）驱动 `ExposureManager`。
2. `ExposureManager` 维护一份资产维度的敞口表，并在收到最新标记价后执行 `revalue_with_prices`。
3. `revalue_with_prices` 会更新以下三个聚合指标（单位均为 USDT）：
   - `total_equity`
   - `abs_total_exposure`
   - `total_position`
4. 在 `PreTradeRiskResampleEntry` 中，杠杆率通过 `total_position / total_equity` 计算（权益为零时直接置 0）。

## 2. 总权益 `total_equity`

计算逻辑位于 `ExposureManager::revalue_with_prices`（`src/pre_trade/exposure_manager.rs`）：

- 对每个资产 `asset`：
  1. 推导其对应的 `mark` 价格：
     - 若资产为 `USDT`，固定使用 1。
     - 否则尝试从价格表中查找 `<asset>USDT` 的标记价。
  2. 计算现货估值 `spot_value = spot_total_wallet * mark`。
  3. 计算借币与利息的估值：
     - `borrowed_value = spot_cross_borrowed * mark`
     - `interest_value = spot_cross_interest * mark`
- `ExposureManager` 在构建状态时会累计所有 UM 持仓的 `unrealized_profit`，记作 `total_um_unrealized`。
- `total_equity = Σ (spot_value - borrowed_value - interest_value) + total_um_unrealized`。

注意事项：
- 当某个资产缺少标记价（`mark == 0`）时，该资产会被跳过；日志中会输出 `缺少 ... 的标记价格` 提醒。
- `USDT` 行只作为对账辅助，不计入敞口，但其余额会完整进入 `total_equity`。
- 若账户存在借贷/负余额，`spot_total_wallet` 为负时会降低总权益。
- `spot_cross_borrowed` 与 `spot_cross_interest` 都来自统一账户的交叉保证金数据；若资产为非 USDT，它们会在计算中按当天标记价折算为 USDT。
- UM 未实现盈亏直接来自每个 `BinanceUmPosition::unrealized_profit` 的求和，确保权益中体现合约浮盈/亏。

## 3. 总头寸 `total_position`

同样在 `revalue_with_prices` 中维护，用于衡量整体名义规模：

- 对于非 `USDT` 资产，计算：
  - `spot_value = spot_total_wallet * mark`
  - `um_value = um_net_position * mark`
- 将二者的绝对值累加：`total_position += |spot_value| + |um_value|`

这意味着：
- 现货和合约的多/空都会以绝对值计入规模；
- 仅在有对应标记价时才会计入（缺价则视为 0）。

## 4. 杠杆率 `leverage`

在生成 `PreTradeRiskResampleEntry` 时执行：

```rust
let leverage = if total_equity.abs() <= f64::EPSILON {
    0.0
} else {
    total_position / total_equity
};
```

说明：
- 总权益为 0 或接近 0 时，杠杆直接置 0，避免浮点发散。
- 若总权益为负，则杠杆结果也可能为负值（需要结合实际业务判断是否合理）。

## 5. 常见偏差原因

- **价格缺失**：如果行情表里没有 `<asset>USDT` 标记价，该资产的估值会变为 0，导致权益偏低、杠杆偏小。
- **旧快照叠加**：`ExposureManager` 会沿用上一轮的 USDT 汇总，若接口缺失最新数据可能造成滞后。
- **双向持仓**：UM 持仓通过 `signed_position_amount` 将多/空分开处理。若交易对符号无法解析（例如资产前缀不在白名单），相应持仓会被忽略。
- **负值处理**：借贷或负余额会以负数形式参与计算，需确认预期行为是否一致。

## 6. 排查建议

1. 在 `pre_trade/runner` 开启 `debug` 日志，观察 `revalue_with_prices` 输出的缺价提示。
2. 比对 ICEORYX 推送的 `pre_trade_exposure` 与 `pre_trade_risk`：敞口表中的 `spot_qty/um_net_qty` 与风险条目中的总权益/杠杆是否匹配。
3. 若需自定义估值方式，可以在 `ExposureManager::revalue_with_prices` 内调整公式，或在进入该函数前先补全价格表。

如需进一步校准，请结合生产环境的真实资产与价格快照，逐项核对上述公式。***
