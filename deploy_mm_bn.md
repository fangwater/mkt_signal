# 部署 Binance 做市（MM）流程（公共服务 + MM 独立）

本文按你确认的规则整理：
- `dat_pbs` / `depth_pub` / `kline_pub` / `factor_pub` 属于**公共服务**，用各自默认目录命名空间；
- MM 服务（`trade_engine` / `pre_trade` / `persist_manager` / `manual_mm_signal` / `viz_server`）只放在 `binance_mm_<suffix>` 目录下，独立运行。

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

> 说明：`trade_engine` 会读取 `~/dat_pbs/config/mkt_cfg.yaml`，所以必须先部署 `dat_pbs`。

---

## 1. 部署并启动 dat_pbs（公共）

> 不要给它传 `MM_ENV`，保持公共命名空间（默认 `dat_pbs`）。

```bash
unset PM2_NAMESPACE

cd ~/crypto_mkt/mkt_signal
bash scripts/deploy_dat_pbs.sh

cd ~/dat_pbs
./scripts/start_dat_pbs.sh --exchange binance
```

检查：

```bash
pm2 status --namespace dat_pbs
pm2 logs --namespace dat_pbs dat_pbs_binance-futures --lines 80
pm2 logs --namespace dat_pbs dat_pbs_binance-margin --lines 80
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

bash scripts/deploy_mm_account_monitor.sh \
  --exchange binance \
  --env-suffix "$MM_SUFFIX"

bash scripts/deploy_mm_trade_engine.sh \
  --exchange binance \
  --env-suffix "$MM_SUFFIX"

bash scripts/deploy_mm_pre_trade.sh \
  --exchange binance \
  --env-suffix "$MM_SUFFIX"

bash scripts/deploy_mm_persist_manager.sh \
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

### 5.1 仅更新 trade_engine 后重启 pre_trade（你当前场景）

```bash
cd ~/$MM_ENV

export IPC_NAMESPACE="$MM_ENV"
unset PM2_NAMESPACE

# 1) 重启 trade_engine
./mm_scripts/start_mm_trade_engine.sh binance

# 2) trade_engine 就绪后，重启 pre_trade
./mm_scripts/stop_mm_pre_trade.sh
./mm_scripts/start_mm_pre_trade.sh

# 3) 可选：重启 persist_manager（保证订阅链路干净）
./mm_scripts/stop_mm_persist_manager.sh
./mm_scripts/start_mm_persist_manager.sh

# 4) 查看日志
pm2 logs --namespace "$MM_ENV" "mm_trade_engine_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "mm_pre_trade_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "mm_persist_manager_${MM_ENV}" --lines 80
```

---

## 6. 初始化 Redis 参数（必须）

`manual_mm_signal` 和 `pre_trade` 启动前，必须先写入 Redis 参数。

```bash
cd ~/$MM_ENV

# 6.1 MM 策略参数（Hash: mm_strategy_params_<venue>）
python3 ./scripts/sync_mm_strategy_params.py

# 6.2 MM 交易对列表（String: mm_trade_symbols:<venue>）
python3 ./scripts/sync_mm_symbol_list.py

# 6.3 pre_trade 风控参数（Hash: <dir>:<open>:<hedge>:pre_trade_risk_params）
python3 ./scripts/sync_mm_risk_params.py
```

可选核验：

```bash
cd ~/$MM_ENV
python3 ./scripts/print_mm_strategy_params.py
python3 ./scripts/print_mm_symbol_list.py
python3 ./scripts/print_mm_risk_params.py
```

---

## 7. 启动 MM 专属服务

> 这里需要 `IPC_NAMESPACE`，用于 MM 服务内部 IceOryx 通道隔离。

```bash
cd ~/$MM_ENV

export IPC_NAMESPACE="$MM_ENV"
unset PM2_NAMESPACE

./scripts/start_account_monitor.sh
./mm_scripts/start_mm_trade_engine.sh binance
./mm_scripts/start_mm_pre_trade.sh
./mm_scripts/start_mm_persist_manager.sh
# manual_mm_signal 用于 mock 测试，建议手动前台启动（不走 start/stop 脚本）
./manual_mm_signal --config config/manual_mm_signal.yaml
./mm_scripts/start_mm_viz_server.sh --exchange binance
```

检查：

```bash
pm2 status --namespace "$MM_ENV"
pm2 logs --namespace "$MM_ENV" "account_monitor_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "mm_trade_engine_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "mm_pre_trade_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "mm_persist_manager_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "manual_mm_signal_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "viz_server_${MM_ENV}" --lines 80
```

---

## 8. 快速验收（按命名空间分组）

```bash
pm2 status --namespace dat_pbs
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
# manual_mm_signal 为手动进程，按需 Ctrl+C 或 pkill 停止
./mm_scripts/stop_mm_persist_manager.sh
./mm_scripts/stop_mm_pre_trade.sh
./mm_scripts/stop_mm_trade_engine.sh
./scripts/stop_account_monitor.sh
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

cd ~/dat_pbs
./scripts/stop_dat_pbs.sh --exchange binance
```

---

## 10. 公共 Persist Auto Exporter（推荐）

> 这是公共服务，不是 MM 专属。用于统一管理所有导出 target（含 MM/xarb）。

```bash
cd ~/crypto_mkt/mkt_signal

# 1) 部署公共自动导出服务
bash scripts/deploy_persist_auto_exporter_public.sh

# 2) 部署配置管理/手动导出 Web 服务
bash scripts/deploy_persist_admin_server.sh

# 3) 启动
cd ~/persist_exporter_public
./start_persist_auto_exporter_public.sh
./start_persist_admin_server.sh
```

检查：

```bash
pm2 status --namespace persist_exporter
pm2 logs --namespace persist_exporter persist_auto_exporter_public --lines 80
pm2 logs --namespace persist_exporter persist_admin_server --lines 80
```

访问：

```text
http://<your-host>:10331/
```

停止：

```bash
cd ~/persist_exporter_public
./stop_persist_admin_server.sh
./stop_persist_auto_exporter_public.sh
```
