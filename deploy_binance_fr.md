# 部署 Binance FR 环境（老组 + 新组）

本文包含两组环境：
- 老组：`binance_fr_trade` / `binance_fr_hf01` / `binance_fr_hf02`
- 新组：`binance_fr_trade01` / `binance_fr_trade02` / `binance_fr_trade03`

端口映射：
- 老组（viz）：`10031` / `10041` / `10042`
- 老组（config）：`18031` / `18041` / `18042`
- 新组（viz）：`10051` / `10052` / `10053`
- 新组（config）：`18051` / `18052` / `18053`

说明：下文步骤以老组举例；新组流程完全一致，只需将 `env-name` 与端口替换为 `trade01/02/03` 对应值。

---

## 步骤 1：部署 config server 并写入 risk 参数

> 说明：risk 参数按目录前缀隔离（`<dir>:<open>:<hedge>:pre_trade_risk_params`）。
> 现在如果 Redis 没有对应 key，点击“读取”会直接报错，所以必须先保存。

### 1.1 binance_fr_trade

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_fr_config_server.sh \
  --env-name binance_fr_trade \
  --exchange binance \
  --port 18031 \
  --apply-nginx

cd ~/binance_fr_trade
./scripts/start_fr_config_server.sh
```

打开配置页：
```
http://<host>:4191/fr/binance_fr_trade/config/
```

在页面中填写并保存 **Risk Params**（open/hedge = `binance-margin` / `binance-futures`）。

可选确认：
```bash
cd ~/binance_fr_trade
./scripts/print_fr_risk_params.py --open-venue binance-margin --hedge-venue binance-futures
```

---

### 1.2 binance_fr_hf01

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_fr_config_server.sh \
  --env-name binance_fr_hf01 \
  --exchange binance \
  --port 18041 \
  --apply-nginx

cd ~/binance_fr_hf01
./scripts/start_fr_config_server.sh
```

打开配置页：
```
http://<host>:4191/fr/binance_fr_hf01/config/
```

在页面中填写并保存 **Risk Params**（open/hedge = `binance-margin` / `binance-futures`）。

可选确认：
```bash
cd ~/binance_fr_hf01
./scripts/print_fr_risk_params.py --open-venue binance-margin --hedge-venue binance-futures
```

---

### 1.3 binance_fr_hf02

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_fr_config_server.sh \
  --env-name binance_fr_hf02 \
  --exchange binance \
  --port 18042 \
  --apply-nginx

cd ~/binance_fr_hf02
./scripts/start_fr_config_server.sh
```

打开配置页：
```
http://<host>:4191/fr/binance_fr_hf02/config/
```

在页面中填写并保存 **Risk Params**（open/hedge = `binance-margin` / `binance-futures`）。

可选确认：
```bash
cd ~/binance_fr_hf02
./scripts/print_fr_risk_params.py --open-venue binance-margin --hedge-venue binance-futures
```

---

## 步骤 2：部署 account_monitor 和 trade_engine

### 2.1 account_monitor

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_account_monitor.sh \
  --exchange binance \
  --env-name binance_fr_trade

bash scripts/deploy_account_monitor.sh \
  --exchange binance \
  --env-name binance_fr_hf01

bash scripts/deploy_account_monitor.sh \
  --exchange binance \
  --env-name binance_fr_hf02
```

启动：
```bash
cd ~/binance_fr_trade
./scripts/start_account_monitor.sh

cd ~/binance_fr_hf01
./scripts/start_account_monitor.sh

cd ~/binance_fr_hf02
./scripts/start_account_monitor.sh
```

---

### 2.2 trade_engine

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_fr_trade_engine.sh \
  --exchange binance \
  --env-name binance_fr_trade

bash scripts/deploy_fr_trade_engine.sh \
  --exchange binance \
  --env-name binance_fr_hf01

bash scripts/deploy_fr_trade_engine.sh \
  --exchange binance \
  --env-name binance_fr_hf02
```

启动：
```bash
cd ~/binance_fr_trade
./scripts/start_trade_engine.sh

cd ~/binance_fr_hf01
./scripts/start_trade_engine.sh

cd ~/binance_fr_hf02
./scripts/start_trade_engine.sh
```

---

## 步骤 3：部署 viz server（面板）

### 3.1 部署

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_trade \
  --exchange binance \
  --port 10031 \
  --apply-nginx

bash scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_hf01 \
  --exchange binance \
  --port 10041 \
  --apply-nginx

bash scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_hf02 \
  --exchange binance \
  --port 10042 \
  --apply-nginx
```

### 3.2 启动

```bash
cd ~/binance_fr_trade
./scripts/start_fr_viz_server.sh

cd ~/binance_fr_hf01
./scripts/start_fr_viz_server.sh

cd ~/binance_fr_hf02
./scripts/start_fr_viz_server.sh
```

---

## 步骤 4：部署 persist_manager

### 4.1 部署二进制与脚本

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_fr_persist_manager.sh \
  --exchange binance \
  --env-name binance_fr_trade

bash scripts/deploy_fr_persist_manager.sh \
  --exchange binance \
  --env-name binance_fr_hf01

bash scripts/deploy_fr_persist_manager.sh \
  --exchange binance \
  --env-name binance_fr_hf02
```

### 4.2 启动

```bash
cd ~/binance_fr_trade
./scripts/start_fr_persist_manager.sh

cd ~/binance_fr_hf01
./scripts/start_fr_persist_manager.sh

cd ~/binance_fr_hf02
./scripts/start_fr_persist_manager.sh
```

---

## 步骤 5：部署 pre_trade

> 注意：pre_trade 启动时会读取 Redis 中带目录前缀的 risk params；若缺失会 panic。

### 5.1 部署二进制与脚本

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_fr_pre_trade.sh \
  --exchange binance \
  --env-name binance_fr_trade

bash scripts/deploy_fr_pre_trade.sh \
  --exchange binance \
  --env-name binance_fr_hf01

bash scripts/deploy_fr_pre_trade.sh \
  --exchange binance \
  --env-name binance_fr_hf02
```

### 5.2 启动（如需）

```bash
cd ~/binance_fr_trade
./scripts/start_fr_pre_trade.sh

cd ~/binance_fr_hf01
./scripts/start_fr_pre_trade.sh

cd ~/binance_fr_hf02
./scripts/start_fr_pre_trade.sh
```

---

## 步骤 6：部署 trade_signal（只部署，不启动）

```bash
cd ~/crypto_mkt/mkt_signal

bash scripts/deploy_fr_signal.sh \
  --exchange binance \
  --env-name binance_fr_trade

bash scripts/deploy_fr_signal.sh \
  --exchange binance \
  --env-name binance_fr_hf01

bash scripts/deploy_fr_signal.sh \
  --exchange binance \
  --env-name binance_fr_hf02
```

> 如需后续启动：
> `cd ~/binance_fr_trade && ./scripts/start_trade_signal.sh`
> `cd ~/binance_fr_hf01 && ./scripts/start_trade_signal.sh`
> `cd ~/binance_fr_hf02 && ./scripts/start_trade_signal.sh`
