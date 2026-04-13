# Public Bridge Deploy

## Goal

把固定的跨机公共流量从实例业务桥里拆出来，单独部署一个 `public bridge`：

- HK: 本地 OKEX 行情 fan-out 到 `bridge/okex-*/*`
- JP: 本地 Binance futures 行情 fan-out 到 `bridge/binance-futures/*`
- JP -> HK:
  - `bridge/binance-futures/incremental`
  - `bridge/binance-futures/ask_bid_spread`
  - `bridge/binance-futures/derivatives`
  - `factor_pub/binance-futures/rl_return_volatility`
  - `model_output/binance-futures-mm-xgb-test`（当前已在 config 中注释，暂不转发）

实例级业务 IPC 例如 `order_reqs/*`、`query_reqs/*`、`account_pubs/*` 不在这里，后续继续归到 private bridge。

## Configs

- JP: [config/ipc_bridge_public_jp.yaml](/home/fanghaizhou/mkt_signal/config/ipc_bridge_public_jp.yaml)
- HK: [config/ipc_bridge_public_hk.yaml](/home/fanghaizhou/mkt_signal/config/ipc_bridge_public_hk.yaml)

当前默认约定：

- 公共 ZMQ 端口: `6360`
- HK 公网地址: `47.238.128.48`
- `model_output` 预留的 public route:
  - `model_output/binance-futures-mm-xgb-test`（当前注释）
- `fusion factor vol` 目前桥接的是兼容单因子流:
  - `factor_pub/binance-futures/rl_return_volatility`

## Route Contract

跨机 ZMQ route 的 `id` 必须在 JP/HK 两端完全一致。`ipc_bridge` 发送时把 `route.id` 作为 ZMQ header，接收端也是按这个 id 找目标 publisher。

这也是 public bridge 单独维护配置的原因之一：公共跨机流量固定，route id 不应该跟实例业务桥混在一起各自演化。

## Deploy

JP:

```bash
cd ~/mkt_signal
bash scripts/deploy_public_bridge.sh --env jp
cd ~/bridge_jp_public
./scripts/start_ipc_bridge.sh
```

HK:

```bash
cd ~/mkt_signal
bash scripts/deploy_public_bridge.sh --env hk
cd ~/bridge_hk_public
./scripts/start_ipc_bridge.sh
```

## Scope

这套 public bridge 当前覆盖三类 JP -> HK 公共流量：

1. `binance-futures` 行情
2. `binance-futures` 的 `rl_return_volatility`
3. `binance-futures-mm-xgb-test` model output（当前注释）

private bridge 后续再单独拆：

- `order_reqs/binance`
- `order_resps/binance`
- `query_reqs/binance`
- `query_resps/binance`
- `account_pubs/binance_pm`
