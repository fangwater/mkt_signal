 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --execute
 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --buy --execute
 python scripts/okx_balance_and_positions_ws.py --inst-type MARGIN --duration 5
 python scripts/okx_post_only_reject_test.py --inst-id SOL-USDT-SWAP --px 200 --notional-usdt 1000 --execute

//mock 壳子，用于测试交易链路是否正常
# 目录名需要包含 `<suffix>-<namespace>-trade` 或 `<suffix>_<namespace>_trade`，用于从 CWD 推断 namespace/suffix
# - 例：`okex_fr_trade` -> namespace=fr, suffix=okex -> 读 `fr_*:okex`
# - 例：`okex-binance-xarb-trade` -> namespace=xarb, suffix=okex-binance -> 读 `xarb_*:okex-binance`
cargo run --bin manual_signal -- --open okex-futures --hedge binance-futures --port 8911

//
bash scripts/deploy_setup_env_xarb.sh --open-venue okex-futures --hedge-venue binance-futures
scripts/deploy_xarb_trade_engine.sh --open-venue okex-futures --hedge-venue binance-futures

# deploy manual_signal
scripts/deploy_fr_manual_signal.sh trade --exchange okex
scripts/deploy_xarb_manual_signal.sh trade --open-venue okex-futures --hedge-venue binance-futures

# xarb symbol lists (Redis)
python scripts/sync_xarb_symbol_lists.py --open-venue okex-futures --hedge-venue binance-futures
python scripts/print_xarb_symbol_lists.py --env-name okex-binance-xarb-trade

# xarb configs (Redis)
python scripts/sync_xarb_strategy_params.py --env-name okex-binance-xarb-trade
python scripts/sync_xarb_funding_rate_thresholds.py --env-name okex-binance-xarb-trade
python scripts/sync_xarb_spread_thresholds.py --env-name okex-binance-xarb-trade


python /home/ubuntu/crypto_mkt/mkt_signal/scripts/okx_swap_buy.py --inst-id APT-USDT-SWAP --side buy --sz 40 --reduce-only --execute
bash scripts/deploy_xarb_viz_server.sh trade --open-venue okex-futures --hedge-venue binance-futures --port 10111
http://54.64.147.6:4191/xarb/okex-binance/


• 本机部署（mkt_signal 仓库内）

  - 启动（PM2）：./scripts/start_order_query.sh
      - 可选环境变量：PORT=18080 API_BASE=http://127.0.0.1:8089 DATA_DIR=./data/order_query PM2_NAMESPACE=$(basename "$PWD")
  - 停止：./scripts/stop_order_query.sh
  - 访问：http://<host>:18080/
  - 页面操作：
      - Export All：从 API_BASE 拉全量 parquet 到 DATA_DIR/export_data/ 并生成 DATA_DIR/meta.json
      - 选择 symbol + dataset 点 Latest 100：预览该数据集最新 100 行
      - Download Zip：下载该 symbol 的 parquet 子集 zip

  xarb 部署（部署到 ~/okex-binance-xarb-trade 这类目录）

  - 部署：scripts/deploy_xarb_order_query.sh trade --open-venue okex-futures --hedge-venue binance-futures --port 18089
  - 启动（在目标目录）：PYTHON_BIN=/home/ubuntu/jupyter_env/bin/python ./xarb_scripts/start_xarb_order_query.sh --port 18080 --api-base http://127.0.0.1:8089
  - 停止（在目标目录）：./xarb_scripts/stop_xarb_order_query.sh

  nginx 反代（可选）

  - 部署脚本会在目标目录生成：config/nginx_locations_order_query.txt（两行映射）
  - 直接启用一个独立映射文件（需要 sudo）：
    cd ~/okex-binance-xarb-trade && PORT=4191 MAPPING_FILE=config/nginx_locations_order_query.txt scripts/setup_nginx_4191.sh
  - 访问路径默认是：/xarb/<open>-<hedge>/order_query/（例如 /xarb/okex-binance/order_query/）

  前置条件

  - persist_manager HTTP 已在 API_BASE 提供导出接口（同 scripts/export_all.sh 所需）
  - 运行环境有 python3、pm2（通过 npx pm2）、以及 pandas + parquet 依赖（你当前环境已满足）
