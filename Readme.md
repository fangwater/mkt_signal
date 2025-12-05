pm2 start ./fr_signal --name fr_signal_binance -- --exchange binance
pm2 start ./pre_trade --name pre_trade_binance -- --exchange binance
pm2 start ./trade_engine --name trade_engine_binance -- --exchange binance

1、解决目前的敞口计算问题，计算了现货不计算借币
2、修改account，配置自动化，不依赖配置文件。以及修改key的重启方式。保证错开重启。
3、