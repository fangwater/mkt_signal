# 撤单 / 平敞口常用命令

同所期现 (intra) 与做市 (mm) 环境的「全撤单」和「平敞口」入口都遵循同一套约定：

- 两类入口都默认 **dry-run**，只打印计划；加 `--execute` 才真实下单。
- env-name 必须匹配各自的环境目录名，脚本会自动 `source $HOME/<env-name>/env.sh`。
- 所有脚本都接受位置参数形式（`<入口> <env-name> --execute`）或 `--env-name <name>` 形式。
- 多余参数（如 `--symbol BTCUSDT` / `--min-net-usdt 10` / `--timeout 15`）会原样透传给底层 python。

---

## 一、Intra（同所期现，spot+futures 套利）

支持 `binance-intra-<suffix>`、`okex-intra-<suffix>`、`bybit-intra-<suffix>`。
suffix 通常是 `trade`、`arb01`、`arb02`、`arb03`。

### 1) 撤销全部期货 + 现货挂单

```bash
# Dry-run（只打印计划）
bash scripts/close_intra_all_orders.sh --env-name binance-intra-trade
bash scripts/close_intra_all_orders.sh --env-name okex-intra-trade
bash scripts/close_intra_all_orders.sh --env-name bybit-intra-trade

# 真实撤单
bash scripts/close_intra_all_orders.sh --env-name binance-intra-trade --execute
bash scripts/close_intra_all_orders.sh --env-name okex-intra-trade    --execute
bash scripts/close_intra_all_orders.sh --env-name bybit-intra-trade   --execute

# 限定单个 symbol
bash scripts/close_intra_all_orders.sh --env-name okex-intra-trade --execute --symbol BTCUSDT
```

底层调用：

| 交易所 | 现货撤单 | 期货撤单 |
| --- | --- | --- |
| Binance STANDARD | `binance_cancel_all_std_spot_orders.py` | `binance_cancel_all_std_um_ws_orders.py` |
| OKEx | `okx_cancel_all_margin_orders.py` | `okx_swap_open_orders.py --cancel --real` |
| Bybit | `bybit_cancel_all_spot_orders.py` (category=spot) | `bybit_cancel_all_um_orders.py` (category=linear) |

### 2) 仅期货侧平 intra 净敞口（调平）

```bash
# Dry-run（打印 net_qty + 计划下的市价单）
bash scripts/close_intra_futures_exposure.sh --env-name binance-intra-trade
bash scripts/close_intra_futures_exposure.sh --env-name okex-intra-trade
bash scripts/close_intra_futures_exposure.sh --env-name bybit-intra-trade

# 真实下单
bash scripts/close_intra_futures_exposure.sh --env-name binance-intra-trade --execute
bash scripts/close_intra_futures_exposure.sh --env-name okex-intra-trade    --execute
bash scripts/close_intra_futures_exposure.sh --env-name bybit-intra-trade   --execute

# 仅处理 |net_usdt| ≥ 10 的资产
bash scripts/close_intra_futures_exposure.sh --env-name binance-intra-trade --execute --min-net-usdt 10

# 自定义跳过的资产（Binance 默认跳过 BNB）
bash scripts/close_intra_futures_exposure.sh --env-name binance-intra-trade --execute --skip-assets BNB,ETH
```

数据来源：本机 dashboard `http://127.0.0.1:4191/intra/<exchange>-intra-<suffix>/snapshot` 的 `pre_trade_exposure`。
执行规则：

- `net_qty > 0` → 现货端净多 → 期货 SELL 平
- `net_qty < 0` → 现货端净空 → 期货 BUY 平
- OKX 自动用 `/api/v5/public/instruments` 的 `ctVal` 把 base_qty 换算成 contracts；Bybit 自动用 `qtyStep` 对齐数量。

> ⚠️ 顺序建议：先 `close_intra_all_orders.sh --execute` 把挂单清空，再 `close_intra_futures_exposure.sh --execute` 调平，避免存量 maker 单干扰。

---

## 二、MM（做市）

支持 `binance_mm_<suffix>`、`okex_mm_<suffix>`、`bybit_mm_<suffix>`、`bitget_mm_<suffix>`、`gate_mm_<suffix>`。

### 1) 撤销全部 UM 期货挂单

```bash
# Dry-run
bash scripts/close_mm_all_um_ws_orders.sh --env-name binance_mm_alpha
bash scripts/close_mm_all_um_ws_orders.sh --env-name okex_mm_alpha
bash scripts/close_mm_all_um_ws_orders.sh --env-name bybit_mm_alpha
bash scripts/close_mm_all_um_ws_orders.sh --env-name bitget_mm_alpha
bash scripts/close_mm_all_um_ws_orders.sh --env-name gate_mm_alpha

# 真实撤单
bash scripts/close_mm_all_um_ws_orders.sh --env-name binance_mm_alpha --execute
bash scripts/close_mm_all_um_ws_orders.sh --env-name okex_mm_alpha    --execute

# 单个 symbol
bash scripts/close_mm_all_um_ws_orders.sh --env-name binance_mm_alpha --execute --symbol BTCUSDT
```

### 2) 平 UM 期货敞口

```bash
# Dry-run
bash scripts/close_mm_all_um_exposure.sh --env-name binance_mm_alpha
bash scripts/close_mm_all_um_exposure.sh --env-name okex_mm_alpha
bash scripts/close_mm_all_um_exposure.sh --env-name bybit_mm_alpha

# 真实平仓
bash scripts/close_mm_all_um_exposure.sh --env-name binance_mm_alpha --execute

# 仅平指定 symbol（多个用 --symbols）
bash scripts/close_mm_all_um_exposure.sh --env-name binance_mm_alpha --execute --symbol BTCUSDT
bash scripts/close_mm_all_um_exposure.sh --env-name binance_mm_alpha --execute --symbols BTCUSDT,ETHUSDT
```

---

## 三、应急组合命令

按部署目录批量调用（在 cwd 自动推断 env-name）：

```bash
# 在 ~/binance-intra-trade 目录里
cd ~/binance-intra-trade
bash ~/mkt_signal/scripts/close_intra_all_orders.sh --execute
bash ~/mkt_signal/scripts/close_intra_futures_exposure.sh --execute

# 在 ~/binance_mm_alpha 目录里
cd ~/binance_mm_alpha
bash ~/mkt_signal/scripts/close_mm_all_um_ws_orders.sh --execute
bash ~/mkt_signal/scripts/close_mm_all_um_exposure.sh   --execute
```

---

## 四、常见参数速查

| 参数 | 含义 |
| --- | --- |
| `--env-name <name>` | 指定环境目录名；不传时按 cwd 自动推断 |
| `--env-dir <path>` | 直接指定环境目录路径（覆盖 env-name） |
| `--execute` | 真实下单/撤单（默认 dry-run） |
| `--symbol BTCUSDT` | 限定单个 symbol；可重复或换 `--symbols BTCUSDT,ETHUSDT` |
| `--min-net-usdt N` | flatten 时只处理 |net_usdt| ≥ N 的资产（默认 5） |
| `--skip-assets BNB,ETH` | flatten 时跳过的资产（Binance 默认 `BNB`） |
| `--python <path>` | 指定使用的 python 解释器（默认 `python3`） |

---

## 五、注意事项

- Binance intra/MM 仅支持 `BINANCE_ACCOUNT_MODE=STANDARD`；其他模式脚本会直接拒绝。
- OKX 默认使用 `tdMode=cross`、net 持仓模式，与 trade_engine 的 OKX 路径一致。若环境跑的是 isolated 或双向持仓，需手动加 `--td-mode isolated` 或在脚本里改。
- `close_intra_futures_exposure.sh` 必须在 dashboard 还在跑、`pre_trade_exposure` 通道有最新数据时调用；否则 net_qty 会偏。
- 撤单/平仓默认会 `exec` 进 python，stderr 上的 `[ERR]` 行就是真实失败，注意排查 weight / order_count_1m 限流。
