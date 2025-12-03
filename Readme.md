pm2 start ./fr_signal --name fr_signal_binance -- --exchange binance
pm2 start ./pre_trade --name pre_trade_binance -- --exchange binance
pm2 start ./trade_engine --name trade_engine_binance -- --exchange binance