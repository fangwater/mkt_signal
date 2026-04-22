scripts/close_binance_fr_all_orders.sh trade01 trade02 trade03 --execute 
source /home/ubuntu/binance_fr_trade01/env.sh
python3 scripts/flatten_fr_futures_exposure.py --exchange binance --suffix trade01 --execute
bash scripts/close_mm_all_um_ws_orders.sh --env-name binance_mm_alpha --execute
bash scripts/close_mm_all_um_exposure.sh --env-name binance_mm_alpha --execute

bash scripts/close_mm_all_um_ws_orders.sh --env-name okex_mm_alpha --execute
bash scripts/close_mm_all_um_exposure.sh --env-name okex_mm_alpha --execute 

bash scripts/deploy_mm_pre_trade.sh --exchange okex --env-suffix alpha --bin-only
bash scripts/deploy_mm_signal.sh --exchange okex --env-name okex_mm_alpha --bin-only
bash scripts/deploy_mm_signal.sh --exchange binance --env-name binance_mm_alpha --bin-only
bash scripts/deploy_mm_pre_trade.sh --exchange binance --env-suffix alpha --bin-only
bash scripts/deploy_mm_trade_engine.sh --exchange binance --env-suffix alpha --bin-only
bash scripts/deploy_mm_trade_engine.sh --exchange okex --env-suffix alpha --bin-only

bash scripts/deploy_mm_account_monitor.sh --exchange binance --env-suffix alpha --bin-only

bash scripts/deploy_mm_signal.sh --exchange bybit --env-name bybit_mm_alpha --bin-only
bash scripts/deploy_mm_pre_trade.sh --exchange bybit --env-suffix alpha --bin-only
bash scripts/deploy_mm_trade_engine.sh --exchange bybit --env-suffix alpha --bin-only

bash scripts/close_mm_all_um_ws_orders.sh --env-name bybit_mm_alpha --execute
bash scripts/close_mm_all_um_exposure.sh --env-name bybit_mm_alpha --execute 

bash scripts/deploy_mm_signal.sh --exchange bitget --env-name bitget_mm_alpha --bin-only
bash scripts/deploy_mm_pre_trade.sh --exchange bitget --env-suffix alpha --bin-only
bash scripts/deploy_mm_trade_engine.sh --exchange bitget --env-suffix alpha --bin-only