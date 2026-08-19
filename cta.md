# CTA BatchExec 初步设计

## 1. 进程

新增独立进程 `exec-pre-trade`：

```bash
exec-pre-trade \
  --venue binance-futures
```

一个进程对应一个账户和一个 venue，不存在 open/hedge leg。进程直接订阅：

```text
spread_pbs/<venue>/ask_bid_spread
```

`trade_signal` 不参与 BatchExec。

当前只支持 `binance-futures` 和 `okex-futures`。每次进程启动或重启，先撤销该
futures 账户的全部挂单；撤单失败则进程不启动。

## 2. 策略标识

- `strategy_name` 是 CTA 业务名称。
- 每个 `strategy_name + symbol` 在 `exec-pre-trade` 内分配一个唯一的整数 `strategy_id`。
- 同一账户进程中，一个 symbol 只能属于一个 `strategy_name`。
- `strategy_id` 用于 batch、子订单和成交回报归属，不写入 Redis。

## 3. Redis 配置

固定索引键：

```text
<env>:<venue>:batch_exec:strategy_names
```

Value 是名称数组：

```json
["cta_alpha", "cta_beta"]
```

进程定期读取此索引，不扫描 Redis。每个 `strategy_name` 再使用一个 Redis
String KV：

```text
<env>:<venue>:batch_exec:<strategy_name>
```

Value 为 JSON，同时保存下单参数和全部目标仓位：

```json
{
  "single_order_usdt": 100.0,
  "orders_per_batch": 3,
  "maker_price_anchor": "own_best",
  "tick_spacing": 2,
  "batch_interval_ms": 500,
  "maker_timeout_ms": 1000,
  "max_maker_requotes": 2,
  "target_tolerance_usdt": 10.0,
  "targets": {
    "BTCUSDT": {"qty": 0.03, "signal": 0},
    "ETHUSDT": {"qty": -0.5, "signal": -1}
  }
}
```

每个 symbol 的目标是对象：`qty` 是目标仓位，`signal` 是整数，只允许
`-2`、`-1`、`0`、`1`、`2`。省略 `signal` 或继续写旧的数字仓位，都按
`signal = 0` 处理。

`signal == ±1` 时，该 symbol 当前这次执行全部走 taker，不再先挂 maker
再超时转 taker。`0` 保持原来的 maker 后 taker；`±2` 先解析并保存，暂无
额外执行语义。

`exec-pre-trade` 以 Redis 为运行时真源。Manager 在 Redis 写确认后通过
`<IPC_NAMESPACE>/batch_exec_pubs/reload_notify` 主动唤醒一次 reload；notify
丢失时仍按 `--config-reload-ms` 兜底（代码默认 30s，不是 1s）。reload 读取整个 KV：

- 下单参数变化：从下一个 batch 或下一次重报开始生效，不撤销当前挂单。
- 某个 symbol 的 `qty` 或 `signal` 变化：撤销该 `strategy_id` 的所有挂单，等待撤单确认后读取最新仓位并重新建仓。
- symbol 从 `targets` 删除：按目标仓位 `0`、`signal = 0` 处理。
- strategy_name 从索引删除：该名称下的旧目标全部按 `0` 处理。
- Redis 读取或 JSON 校验失败：保留上一次有效配置。

这里的目标仓位不同于账户实际仓位：实际仓位由
`account_pubs` 实时更新，并每 60 秒通过交易所快照纠偏。
Binance 标准账户的 Exec 快照只查询 UM 余额和 UM 仓位，不查询现货账户。

## 4. 拆单算法

先计算尚未分配的仓位：

```text
remaining_qty = target_qty - current_qty - committed_batch_qty
remaining_usdt = abs(remaining_qty) * reference_price
```

当 `remaining_usdt <= target_tolerance_usdt` 时，不再创建新 batch。

每个 batch 的最大金额：

```text
batch_usdt = single_order_usdt * orders_per_batch
```

一个 batch 最多拆成 `orders_per_batch` 手，每手金额不超过
`single_order_usdt`，最后一手可以更小。数量按该手价格换算，并按交易所
`qty_step`、`min_qty` 和合约乘数对齐。

`maker_price_anchor` 支持两个起点：

```text
own_best:
  Sell = ask0
  Buy  = bid0

opposite_best_plus_one_tick:
  Sell = bid0 + 1 tick
  Buy  = ask0 - 1 tick
```

maker 价格阶梯：

```text
Sell[i] = start_price + i * tick_spacing * price_tick
Buy[i]  = start_price - i * tick_spacing * price_tick
```

例如本批卖出约 300 USDT，单手 100 USDT、一次 3 手、
`maker_price_anchor = own_best`、`tick_spacing = 2`：

```text
ask0
ask0 + 2 tick
ask0 + 4 tick
```

## 5. Batch 生命周期

- 每个 batch 分配内部 `batch_seq`，所有子订单记录所属 `batch_seq`。
- batch 保存每一手的 `level_index` 和剩余量；子订单也记录该档位归属。
- 相邻 batch 至少间隔 `batch_interval_ms`，batch 内子订单同时发出。
- maker 超过 `maker_timeout_ms` 后，撤销该 batch 未成交订单。
- post-only 拒单后等待更新的 BBO，再按原 `level_index` 重算价格；不从第 0 档重新拆分，`batch_seq` 不变。
- `signal == ±1` 时，该 symbol 当前这次执行从第一个 batch 起就使用 taker。
- 其他 signal 下，maker 重报超过 `max_maker_requotes` 后，剩余量使用 taker 成交。
- 新目标生效前必须完成旧挂单撤销，随后重新读取账户仓位。
- 启动后必须先完成首次账户仓位快照，之后才允许创建第一个 batch。
- BBO 缺失或过期时暂停创建订单。

## 6. 风控与观测

下单前复用 `pre_trade` 的杠杆率、最大仓位金额、截面失衡、挂单数量和报单频率检查。
`exec-pre-trade` 不发布旧的 open/hedge resample，改为每秒发布：

```text
viz_pubs/exec_pre_trade_state
viz_pubs/exec_pre_trade_risk
```

策略状态观测：

```text
strategy_name
symbol
position_ready
current_qty
target_qty
delta_qty
live_order_qty
pending_qty
active_batches
```

账户风险观测净值、long/short notional、net/gross notional 和杠杆率。

`viz_server` 必须显式配置独立的 Exec namespace，不继承普通 pre-trade 的
`servers.namespaces`：

```toml
[[servers]]

[servers.http]
bind = "0.0.0.0"
port = 10041
ws_path = "/ws"

[servers.pre_trade]
enabled = false

[servers.exec_pre_trade]
enabled = true
namespace = "cta_exec_trade"
```

Exec dashboard 由上述 `viz_server` 的根路径直接提供，展示账户风险以及按
`strategy_name + symbol` 分组的目标仓位、当前仓位、差额、挂单和 batch 状态。

配置页面使用独立进程：

```bash
ENV_NAME=cta_exec_trade \
VENUE=binance-futures \
PORT=18161 \
./scripts/start_exec_config_server.sh
```

页面只从固定索引键读取已有的 `strategy_name`。选择一个名称后，页面读取：

```text
<env>:<venue>:batch_exec:<strategy_name>
```

Config 浏览器页面保持只读。下单参数由 CTA Manager 的账户级配置页面修改；Manager
调用服务端受限接口时，以 `updated_at_us` 做乐观并发校验，并通过 Redis 事务只合并
下单参数，策略索引和 `targets` 均保持不变。

策略发布方仍通过完整的 `POST /api/strategy` 写入目标仓位和创建新名称：新名称首次
发布时接收初始下单参数，先写配置 KV，再将名称加入索引；已有名称后续发布只更新
`targets`，服务端会保留 Redis 中的下单参数。这个完整发布接口不由浏览器页面调用。
dashboard 默认链接到同域 `/config/`；未配置反向代理时，可以使用查询参数指定：

```text
http://<viz-host>:10041/?config=http://<config-host>:18161/
```

仓位推送不再走 Exec Config 客户端。`exec_config_client.py` 已删除；
`/config/exec_config_client.py` 不再提供下载。更新仓位模板并发布到 Redis
使用 Manager：

```text
http://172.16.30.42:10041/manager/api/manager_publish_client.py
```

```bash
wget http://172.16.30.42:10041/manager/api/manager_publish_client.py
export MANAGER_API_URL=http://172.16.30.42:10041/manager/api/
python3 manager_publish_client.py put-position @cta.json
python3 manager_publish_client.py publish binance_exec_trade01 cta_alpha
```

Exec `/config/` 页面保持只读。

## 7. 部署与启停

远程编排入口与 JP Meta 相同：`publish-exec.sh` / `start-exec.sh` /
`stop-exec.sh`。默认 SSH 主机是 `cta_exec`，目标目录是远程 `$HOME/<env-name>`。

```bash
./scripts/publish-exec.sh --env-name binance_exec_trade01
./scripts/start-exec.sh --env-name binance_exec_trade01
./scripts/stop-exec.sh --env-name binance_exec_trade01
```

`publish-exec.sh` 只上传 Exec 二进制和启停脚本，不改 `env.sh`，也不启停进程。
目标进程仍在跑时会在替换文件前失败。`start-exec.sh` 不会启动 `trade_signal`。
三个脚本都支持 `--check-only`；publish 还支持 `--skip-build`。

首次落地或补齐 operator 配置仍用 `deploy_exec.sh`，它同样不会自动启动进程：

```bash
./scripts/deploy_exec.sh --env-name binance_exec_trade01 --venue binance-futures
cd ~/binance_exec_trade01
python scripts/sync_exec_risk_params.py
./scripts/start_exec_persist_manager.sh
./scripts/start_exec_trade_engine.sh
./scripts/start_account_monitor.sh
./scripts/start_exec_pre_trade.sh --venue binance-futures
./scripts/start_exec_viz_server.sh
./scripts/start_exec_config_server.sh
```

`deploy_exec.sh` 会同时部署 `trade_engine`、对应交易所的 `account_monitor`、
`persist_manager`，以及 `~/spread_pbs/<venue>` 下的行情发布进程。对应停止脚本为
`stop_exec_persist_manager.sh`、`stop_exec_trade_engine.sh`、
`stop_account_monitor.sh`、`stop_exec_pre_trade.sh`、`stop_exec_viz_server.sh` 和
`stop_exec_config_server.sh`。部署脚本不会自动启动进程；启动 `exec-pre-trade`
会先撤销该账户在目标 futures venue 上的全部未完成订单。

Exec 环境名必须以实例号结尾。默认端口按实例号自动错开：`trade01` 使用
viz/config `10041/18161`，`trade02` 使用 `10042/18162`，依此类推；可用
`--viz-port` 和 `--config-port` 显式覆盖。`IPC_NAMESPACE` 和 Redis key prefix
始终使用完整环境名，例如 `binance_exec_trade01`。
