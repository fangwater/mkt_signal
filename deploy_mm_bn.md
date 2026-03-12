# 部署 Binance 做市（MM）流程（公共服务 + MM 独立）

本文按你确认的规则整理：
- `dat_pbs` / `depth_pub` / `kline_pub` / `fusion_factor_pub` / `pairmm_resample` 属于**公共服务**，用各自默认目录命名空间；
- MM 服务（`trade_signal` / `trade_engine` / `pre_trade` / `persist_manager` / `manual_mm_signal` / `viz_server`）只放在 `binance_mm_<suffix>` 目录下，独立运行。

---

## 0. 一键 deploy-only（推荐）

如果你只是想像 `deploy_fr_binance.sh` 一样，先把 Binance MM 环境整体部署到 `~/binance_mm_<suffix>`，现在可以直接用：

```bash
cd ~/crypto_mkt/mkt_signal

# 完整部署（不启动）
bash scripts/deploy_mm_binance.sh beta

# 仅替换二进制（不改脚本/配置/nginx）
bash scripts/deploy_mm_binance.sh beta --bin
```

当前固定支持：
- `beta` / `trade`：`config=18131`，`viz=10231`，`manual_mm_signal=6366`
- `alpha`：`config=18132`，`viz=10232`，`manual_mm_signal=6367`

这一步会串行调用：
- `deploy_mm_config_server.sh`
- `deploy_mm_account_monitor.sh`
- `deploy_mm_trade_engine.sh`
- `deploy_mm_signal.sh`
- `deploy_mm_viz_server.sh`
- `deploy_mm_persist_manager.sh`
- `deploy_mm_pre_trade.sh`
- `deploy_mm_manual_signal.sh`

---

## 1. 前置准备

### 1.1 MM 环境变量（仅 MM 服务使用）

```bash
export MM_SUFFIX=beta
export MM_ENV="binance_mm_${MM_SUFFIX}"
export MM_VENUE="binance-futures"
```

### 1.2 必要依赖与密钥

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

## 2. 部署并启动 dat_pbs（公共）

> 不要给它传 `MM_ENV`，保持公共命名空间（默认 `dat_pbs`）。

```bash
unset PM2_NAMESPACE

cd ~/crypto_mkt/mkt_signal
bash scripts/dat_pbs/deploy_dat_pbs.sh

cd ~/dat_pbs
./scripts/dat_pbs/start_dat_pbs.sh --exchange binance
```

检查：

```bash
pm2 status --namespace dat_pbs
pm2 logs --namespace dat_pbs dat_pbs_binance-futures --lines 80
pm2 logs --namespace dat_pbs dat_pbs_binance-margin --lines 80
```

---

## 3. 部署并启动 depth_pub（公共）

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

## 4. 部署并启动 kline_pub（公共）

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

## 5. 部署并启动 fusion_factor_pub + pairmm_resample（公共）

```bash
unset PM2_NAMESPACE

cd ~/crypto_mkt/mkt_signal
bash scripts/deploy_fusion_factor_pub.sh --exchange binance
bash scripts/deploy_pairmm_resample.sh

cd ~/fusion_factor/binance-futures
./scripts/start_fusion_factor_pub.sh

cd ~/fusion_factor/binance-margin
./scripts/start_fusion_factor_pub.sh

cd ~/factor_pub
./scripts/start_pairmm_resample.sh --exchange binance
```

检查：

```bash
pmdaemon list | rg fusion_factor_pub
pmdaemon logs fusion_factor_pub_binance-futures --lines 80
pmdaemon logs fusion_factor_pub_binance-margin --lines 80

pm2 status --namespace factor_pub
pm2 logs --namespace factor_pub pairmm_resample_binance-futures --lines 80
```

---

## 6. 部署 MM 专属服务（落到 `~/$MM_ENV`）

新增后，MM config server 也建议一起部署：

```bash
bash scripts/deploy_mm_config_server.sh \
  --env-name "$MM_ENV" \
  --exchange binance \
  --port 18131 \
  --apply-nginx
```

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_mm_account_monitor.sh \
  --exchange binance \
  --env-suffix "$MM_SUFFIX"

bash scripts/deploy_mm_trade_engine.sh \
  --exchange binance \
  --env-suffix "$MM_SUFFIX"

bash scripts/deploy_mm_signal.sh \
  --exchange binance \
  --env-name "$MM_ENV"

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

### 6.1 仅更新 trade_engine 后重启 pre_trade（你当前场景）

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

## 7. 初始化 Redis 参数（必须）

`trade_signal` / `manual_mm_signal` / `pre_trade` 启动前，必须先写入 Redis 参数。

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

## 8. 启动 MM 专属服务

> 这里需要 `IPC_NAMESPACE`，用于 MM 服务内部 IceOryx 通道隔离。

```bash
cd ~/$MM_ENV

export IPC_NAMESPACE="$MM_ENV"
unset PM2_NAMESPACE

./scripts/start_account_monitor.sh
./scripts/start_trade_signal.sh
./mm_scripts/start_mm_trade_engine.sh binance
./mm_scripts/start_mm_pre_trade.sh
./mm_scripts/start_mm_persist_manager.sh
# manual_mm_signal 用于 mock / 手工注入测试，按需启动
./manual_mm_signal --config config/manual_mm_signal.yaml
./mm_scripts/start_mm_viz_server.sh --exchange binance
```

检查：

```bash
pm2 status --namespace "$MM_ENV"
pm2 logs --namespace "$MM_ENV" "account_monitor_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "trade_signal_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "mm_trade_engine_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "mm_pre_trade_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "mm_persist_manager_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "manual_mm_signal_${MM_ENV}" --lines 80
pm2 logs --namespace "$MM_ENV" "viz_server_${MM_ENV}" --lines 80
```

---

## 9. 快速验收（按命名空间分组）

```bash
pm2 status --namespace dat_pbs
pm2 status --namespace depth_pub
pm2 status --namespace kline_pub
pmdaemon list | rg fusion_factor_pub
pm2 status --namespace factor_pub
pm2 status --namespace "$MM_ENV"
```

如果以上检查都正常，即为“公共服务 + MM 服务”分层部署成功。

---

## 10. 停止 / 重启（常用）

### 10.1 停止 MM 专属

```bash
cd ~/$MM_ENV
./mm_scripts/stop_mm_viz_server.sh --exchange binance
# manual_mm_signal 为手动/mock 进程，按需 Ctrl+C 或 pkill 停止
./mm_scripts/stop_mm_persist_manager.sh
./mm_scripts/stop_mm_pre_trade.sh
./mm_scripts/stop_mm_trade_engine.sh
./scripts/stop_trade_signal.sh
./scripts/stop_account_monitor.sh
```

### 10.2 停止公共层

```bash
cd ~/factor_pub
./scripts/stop_pairmm_resample.sh --exchange binance

cd ~/fusion_factor/binance-futures
./scripts/stop_fusion_factor_pub.sh

cd ~/fusion_factor/binance-margin
./scripts/stop_fusion_factor_pub.sh

cd ~/kline_pub
./scripts/stop_kline_pub.sh --exchange binance

cd ~/depth_pub
./scripts/stop_depth_pub.sh --exchange binance

cd ~/dat_pbs
./scripts/dat_pbs/stop_dat_pbs.sh --exchange binance
```

---

## 11. 公共 Persist Auto Exporter（推荐）

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
