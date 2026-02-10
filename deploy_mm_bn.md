# 部署 Binance 做市（MM）流程（公共服务 + MM 独立）

本文按你确认的规则整理：
- `mkt_pub` / `depth_pub` / `kline_pub` / `factor_pub` 属于**公共服务**，用各自默认目录命名空间；
- MM 服务（`trade_engine` / `pre_trade` / `manual_mm_signal` / `viz_server`）只放在 `binance_mm_<suffix>` 目录下，独立运行。

---

## 0. 前置准备

### 0.1 MM 环境变量（仅 MM 服务使用）

```bash
export MM_SUFFIX=beta
export MM_ENV="binance_mm_${MM_SUFFIX}"
export MM_VENUE="binance-futures"
```

### 0.2 必要依赖与密钥

```bash
# trade_engine / pre_trade（binance）必需
export BINANCE_API_KEY="<your_key>"
export BINANCE_API_SECRET="<your_secret>"
export BINANCE_ACCOUNT_MODE="standard"   # 或 portfolio

# 参数同步脚本会用到
python3 -m pip install --user redis
```

> 说明：`trade_engine` 会读取 `~/mkt_pub/config/mkt_cfg.yaml`，所以必须先部署 `mkt_pub`。

---

## 1. 部署并启动 mkt_pub（公共）

> 不要给它传 `MM_ENV`，保持公共命名空间（默认 `mkt_pub`）。

```bash
unset PM2_NAMESPACE

cd ~/crypto_mkt/mkt_signal
bash scripts/deploy_mkt_pub.sh

cd ~/mkt_pub
./scripts/start_mkt_pub.sh --exchange binance
```

检查：

```bash
pm2 status --namespace mkt_pub
pm2 logs --namespace mkt_pub mkt_pub_binance-futures --lines 80
pm2 logs --namespace mkt_pub mkt_pub_binance-margin --lines 80
```

---

## 2. 部署并启动 depth_pub（公共）

> 已清理：`deploy_depth_pub.sh` 仅支持 `[--dir]`，不再有 `trade/test` 参数。

```bash
unset PM2_NAMESPACE

cd ~/crypto_mkt/mkt_signal
bash scripts/deploy_depth_pub.sh

cd ~/depth_pub
./scripts/start_depth_pub.sh --exchange binance
```

检查：

```bash
pm2 status --namespace depth_pub
pm2 logs --namespace depth_pub depth_pub_binance-futures --lines 80
pm2 logs --namespace depth_pub depth_pub_binance-margin --lines 80
```

---

## 3. 部署并启动 kline_pub（公共）

```bash
unset PM2_NAMESPACE

cd ~/crypto_mkt/mkt_signal
bash scripts/deploy_kline_pub.sh

cd ~/kline_pub
./scripts/start_kline_pub.sh --exchange binance
```

检查：

```bash
pm2 status --namespace kline_pub
pm2 logs --namespace kline_pub kline_pub_binance-futures --lines 80
pm2 logs --namespace kline_pub kline_pub_binance-margin --lines 80
```

---

## 4. 部署并启动 factor_pub 系列（公共）

这 4 个组件默认部署到同一目录 `~/factor_pub`，命名空间默认 `factor_pub`。

```bash
unset PM2_NAMESPACE

cd ~/crypto_mkt/mkt_signal
bash scripts/deploy_depth_factor_pub.sh
bash scripts/deploy_kline_factor_pub.sh
bash scripts/deploy_trade_factor_pub.sh
bash scripts/deploy_pairmm_resample.sh

cd ~/factor_pub
./scripts/start_depth_factor_pub.sh --exchange binance
./scripts/start_kline_factor_pub.sh --exchange binance
./scripts/start_trade_factor_pub.sh --exchange binance
./scripts/start_pairmm_resample.sh --exchange binance
```

检查：

```bash
pm2 status --namespace factor_pub
pm2 logs --namespace factor_pub depth_factor_pub_binance-futures --lines 80
pm2 logs --namespace factor_pub kline_factor_pub_binance-futures --lines 80
pm2 logs --namespace factor_pub trade_factor_pub_binance-futures --lines 80
pm2 logs --namespace factor_pub pairmm_resample_binance-futures --lines 80
```

---

## 5. 部署 MM 专属服务（落到 `~/$MM_ENV`）

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_mm_trade_engine.sh \
  --exchange binance \
  --env-suffix "$MM_SUFFIX"

bash scripts/deploy_mm_pre_trade.sh \
  --exchange binance \
  --env-suffix "$MM_SUFFIX"

bash scripts/deploy_mm_manual_signal.sh \
  --exchange binance \
  --env-suffix "$MM_SUFFIX" \
  --port 6366

bash scripts/deploy_mm_viz_server.sh \
  --exchange binance \
  --env-suffix "$MM_SUFFIX" \
  --port 10231
```

如需自动更新并应用 Nginx 映射，可在最后一个命令追加 `--apply-nginx`。

---

## 6. 初始化 Redis 参数（必须）

`manual_mm_signal` 和 `pre_trade` 启动前，必须先写入 Redis 参数。

```bash
cd ~/$MM_ENV

# 6.1 MM 策略参数（Hash: mm_strategy_params_<venue>）
python3 ./scripts/sync_mm_strategy_params.py --venue "$MM_VENUE"

# 6.2 MM 交易对列表（String: mm_trade_symbols:<venue>）
python3 ./scripts/sync_mm_symbol_list.py --venue "$MM_VENUE"

# 6.3 pre_trade 风控参数（Hash: <dir>:<open>:<hedge>:pre_trade_risk_params）
python3 ~/crypto_mkt/mkt_signal/scripts/sync_fr_risk_params.py \
  --open-venue "$MM_VENUE" \
  --hedge-venue "$MM_VENUE"
```

可选核验：

```bash
cd ~/$MM_ENV
python3 ./scripts/print_mm_strategy_params.py --venue "$MM_VENUE"
python3 ./scripts/print_mm_symbol_list.py --venue "$MM_VENUE"
python3 ~/crypto_mkt/mkt_signal/scripts/print_fr_risk_params.py \
  --open-venue "$MM_VENUE" \
  --hedge-venue "$MM_VENUE"
```

---

## 7. 启动 MM 专属服务

> 这里需要 `IPC_NAMESPACE`，用于 MM 服务内部 IceOryx 通道隔离。

```bash
cd ~/$MM_ENV

export IPC_NAMESPACE="$MM_ENV"
unset PM2_NAMESPACE

./mm_scripts/start_mm_trade_engine.sh binance
./mm_scripts/start_mm_pre_trade.sh --venue "$MM_VENUE"
./mm_scripts/start_manual_mm_signal.sh --config config/manual_mm_signal.yaml
./mm_scripts/start_mm_viz_server.sh --exchange binance
```

检查：

```bash
pm2 status --namespace "$MM_ENV"
pm2 logs --namespace "$MM_ENV" "mm_trade_engine_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "mm_pre_trade_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "manual_mm_signal_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "viz_server_${MM_ENV}" --lines 80
```

---

## 8. 快速验收（按命名空间分组）

```bash
pm2 status --namespace mkt_pub
pm2 status --namespace depth_pub
pm2 status --namespace kline_pub
pm2 status --namespace factor_pub
pm2 status --namespace "$MM_ENV"
```

如果以上 5 个 namespace 都正常，即为“公共服务 + MM 服务”分层部署成功。

---

## 9. 停止 / 重启（常用）

### 9.1 停止 MM 专属

```bash
cd ~/$MM_ENV
./mm_scripts/stop_mm_viz_server.sh --exchange binance
./mm_scripts/stop_manual_mm_signal.sh
./mm_scripts/stop_mm_pre_trade.sh
./mm_scripts/stop_mm_trade_engine.sh
```

### 9.2 停止公共层

```bash
cd ~/factor_pub
./scripts/stop_pairmm_resample.sh --exchange binance
./scripts/stop_trade_factor_pub.sh --exchange binance
./scripts/stop_kline_factor_pub.sh --exchange binance
./scripts/stop_depth_factor_pub.sh --exchange binance

cd ~/kline_pub
./scripts/stop_kline_pub.sh --exchange binance

cd ~/depth_pub
./scripts/stop_depth_pub.sh --exchange binance

cd ~/mkt_pub
./scripts/stop_mkt_pub.sh --exchange binance
```
