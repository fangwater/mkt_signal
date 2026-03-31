scripts/close_binance_fr_all_orders.sh trade01 trade02 trade03 --execute 
source /home/ubuntu/binance_fr_trade01/env.sh
python3 scripts/flatten_fr_futures_exposure.py --exchange binance --suffix trade01 --execute
  bash scripts/close_binance_mm_all_um_ws_orders.sh --env-name binance_mm_alpha --execute
  bash scripts/close_binance_mm_all_um_exposure.sh --env-name binance_mm_alpha --execute