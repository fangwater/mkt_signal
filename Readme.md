pm2 start ./fr_signal --name fr_signal_binance -- --exchange binance
pm2 start ./pre_trade --name pre_trade_binance -- --exchange binance
pm2 start ./trade_engine --name trade_engine_binance -- --exchange binance

1、解决目前的敞口计算问题，计算了现货不计算借币
2、修改account，配置自动化，不依赖配置文件，修改fix account channel

Binance 账户监控不再读取配置文件，WS/REST 地址、主备 IP、2 小时时长全部硬编码
OKEx 账户监控同样硬编码 WS 地址与主备 IP，并不再读取会话时长配置
移除账户配置解析模块与配置文件：删去 pm_cfg 模块

3、以及修改key的重启方式。保证错开重启。

monitor分主备，一个新的转发进程。
