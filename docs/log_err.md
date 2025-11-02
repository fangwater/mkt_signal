> 注：以下日志截取于拆分前版本，现已替换为 `mkt_signal::signal::binance_forward_arb_mt` / `_mm` 模块，并将信号类型标记为 `BinSingleForwardArbOpenMT` / `BinSingleForwardArbOpenMM` 等新枚举值。

[2025-10-22T11:24:59Z DEBUG mkt_signal::pre_trade::runner] max_pending_limit_orders updated to 5
[2025-10-22T11:24:59Z DEBUG mkt_signal::pre_trade::runner] pre_trade params updated: max_pos_u=100.00 sym_ratio=0.5000 total_ratio=0.5000 max_leverage=2.00 refresh=30s
[2025-10-22T11:24:59Z INFO  mkt_signal::pre_trade::runner] account stream subscribed: service=account_pubs/binance/pm label=account_binance
[2025-10-22T11:24:59Z INFO  mkt_signal::pre_trade::runner] trade response subscribed: service=order_resps/binance label=trade_resp_binance
[2025-10-22T11:24:59Z INFO  mkt_signal::pre_trade::runner] signal subscribed: node=signalsmt_arbitrage service=signal_pubs/mt_arbitrage channel=mt_arbitrage
[2025-10-22T11:24:59Z INFO  mkt_signal::pre_trade::runner] derivatives metrics subscribed: service=data_pubs/binance-futures/derivatives
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] trade signal received: type=BinSingleForwardArbOpen generation_time=1761132303062704 ctx_len=62
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] decoded open ctx: spot_symbol=A2ZUSDT amount=3864 price=0.00388200 exp_time_us=30000000
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] strategy init for open signal: strategy_id=22255470 symbol=A2ZUSDT qty=3864.000000 price=0.00388200 tick=0.00000100 type=Limit
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 构造 margin 开仓参数 ["symbol=A2ZUSDT", "side=BUY", "type=LIMIT", "quantity=3864", "newClientOrderId=95586515807109120", "timeInForce=GTC", "price=0.003882"]
[2025-10-22T11:25:03Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 提交 margin 开仓请求 symbol=A2ZUSDT qty=3864.000000 type=LIMIT price=0.00388200 order_id=95586515807109120
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 成功创建开仓订单
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 margin 开仓单状态=COMMIT，等待成交
[2025-10-22T11:25:03Z INFO  mkt_signal::signal::strategy] 策略管理器: 新增策略 id=22255470，当前活跃策略数=1
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] strategy inserted: strategy_id=22255470 symbol=A2ZUSDT active_total=1
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] trade signal received: type=BinSingleForwardArbOpen generation_time=1761132303062817 ctx_len=62
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] decoded open ctx: spot_symbol=A2ZUSDT amount=3904 price=0.00384300 exp_time_us=30000000
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] strategy init for open signal: strategy_id=22255716 symbol=A2ZUSDT qty=3904.000000 price=0.00384300 tick=0.00000100 type=Limit
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 数量/名义金额对齐 raw_qty=3904.00000000 step=1.00000000 min_qty=1.00000000 min_notional=5.00000000 price=0.00384300 aligned_qty=3904.00000000
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: symbol=A2ZUSDT 现货持仓限制通过 current=0.0000USDT add=15.3124USDT projected=15.3124USDT limit=100.0000USDT
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 margin 开仓价格对齐 raw=0.00384300 tick=0.00000100 aligned=0.00384300
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 构造 margin 开仓参数 ["symbol=A2ZUSDT", "side=BUY", "type=LIMIT", "quantity=3904", "newClientOrderId=95587572369063936", "timeInForce=GTC", "price=0.003843"]
[2025-10-22T11:25:03Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 提交 margin 开仓请求 symbol=A2ZUSDT qty=3904.000000 type=LIMIT price=0.00384300 order_id=95587572369063936
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 成功创建开仓订单
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 margin 开仓单状态=COMMIT，等待成交
[2025-10-22T11:25:03Z INFO  mkt_signal::signal::strategy] 策略管理器: 新增策略 id=22255716，当前活跃策略数=2
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] strategy inserted: strategy_id=22255716 symbol=A2ZUSDT active_total=2
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] trade signal received: type=BinSingleForwardArbOpen generation_time=1761132303062832 ctx_len=62
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] decoded open ctx: spot_symbol=A2ZUSDT amount=3944 price=0.00380400 exp_time_us=30000000
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] strategy init for open signal: strategy_id=22255840 symbol=A2ZUSDT qty=3944.000000 price=0.00380400 tick=0.00000100 type=Limit
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 数量/名义金额对齐 raw_qty=3944.00000000 step=1.00000000 min_qty=1.00000000 min_notional=5.00000000 price=0.00380400 aligned_qty=3944.00000000
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: symbol=A2ZUSDT 现货持仓限制通过 current=0.0000USDT add=15.4693USDT projected=15.4693USDT limit=100.0000USDT
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 margin 开仓价格对齐 raw=0.00380400 tick=0.00000100 aligned=0.00380400
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 构造 margin 开仓参数 ["symbol=A2ZUSDT", "side=BUY", "type=LIMIT", "quantity=3944", "newClientOrderId=95588104945008640", "timeInForce=GTC", "price=0.003804"]
[2025-10-22T11:25:03Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 提交 margin 开仓请求 symbol=A2ZUSDT qty=3944.000000 type=LIMIT price=0.00380400 order_id=95588104945008640
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 成功创建开仓订单
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 margin 开仓单状态=COMMIT，等待成交
[2025-10-22T11:25:03Z INFO  mkt_signal::signal::strategy] 策略管理器: 新增策略 id=22255840，当前活跃策略数=3
第一个信号

[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] strategy inserted: strategy_id=22255840 symbol=A2ZUSDT active_total=3
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] pre_trade period tick: inspected=3 active_before=3 active_after=3 params_refreshed=false resample_entries=0 elapsed_ms=0.00 strategy_activity=true ladder_cancel_activity=false
定时器

[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 trade_response req_type=BinanceNewMarginOrder client_order_id=95586515807109120 status=200 body_len=329
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 margin 开仓单状态=CREATE，等待成交
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=95586515807109120 ord_id=124499276 status=NEW
[2025-10-22T11:25:03Z WARN  mkt_signal::pre_trade::order_manager] unexpected OrderExecutionStatus
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 margin execution report status=NEW execution_type=NEW
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132303170 hash=0xd4ec2c17a531a028
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 trade_response req_type=BinanceNewMarginOrder client_order_id=95587572369063936 status=200 body_len=329
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 margin 开仓单状态=CREATE，等待成交
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=95587572369063936 ord_id=124499277 status=NEW
[2025-10-22T11:25:03Z WARN  mkt_signal::pre_trade::order_manager] unexpected OrderExecutionStatus
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 margin execution report status=NEW execution_type=NEW
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132303212 hash=0xf3aa43e6f91cd5f2
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 trade_response req_type=BinanceNewMarginOrder client_order_id=95588104945008640 status=200 body_len=329
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 margin 开仓单状态=CREATE，等待成交
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=95588104945008640 ord_id=124499278 status=NEW
[2025-10-22T11:25:03Z WARN  mkt_signal::pre_trade::order_manager] unexpected OrderExecutionStatus
[2025-10-22T11:25:03Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 margin execution report status=NEW execution_type=NEW
[2025-10-22T11:25:03Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132303247 hash=0xf7ebd75f333584db


第二批信号
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] trade signal received: type=BinSingleForwardArbOpen generation_time=1761132304363402 ctx_len=62
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] decoded open ctx: spot_symbol=A2ZUSDT amount=3864 price=0.00388200 exp_time_us=30000000
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] strategy init for open signal: strategy_id=23556105 symbol=A2ZUSDT qty=3864.000000 price=0.00388200 tick=0.00000100 type=Limit
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 数量/名义金额对齐 raw_qty=3864.00000000 step=1.00000000 min_qty=1.00000000 min_notional=5.00000000 price=0.00388200 aligned_qty=3864.00000000
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: symbol=A2ZUSDT 现货持仓限制通过 current=0.0000USDT add=15.1523USDT projected=15.1523USDT limit=100.0000USDT
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 margin 开仓价格对齐 raw=0.00388200 tick=0.00000100 aligned=0.00388200
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 构造 margin 开仓参数 ["symbol=A2ZUSDT", "side=BUY", "type=LIMIT", "quantity=3864", "newClientOrderId=101172700596142080", "timeInForce=GTC", "price=0.003882"]
[2025-10-22T11:25:04Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 提交 margin 开仓请求 symbol=A2ZUSDT qty=3864.000000 type=LIMIT price=0.00388200 order_id=101172700596142080
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 成功创建开仓订单
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 margin 开仓单状态=COMMIT，等待成交
[2025-10-22T11:25:04Z INFO  mkt_signal::signal::strategy] 策略管理器: 新增策略 id=23556105，当前活跃策略数=4
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] strategy inserted: strategy_id=23556105 symbol=A2ZUSDT active_total=4
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] trade signal received: type=BinSingleForwardArbOpen generation_time=1761132304363447 ctx_len=62
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] decoded open ctx: spot_symbol=A2ZUSDT amount=3904 price=0.00384300 exp_time_us=30000000


[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] strategy init for open signal: strategy_id=23556339 symbol=A2ZUSDT qty=3904.000000 price=0.00384300 tick=0.00000100 type=Limit
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 数量/名义金额对齐 raw_qty=3904.00000000 step=1.00000000 min_qty=1.00000000 min_notional=5.00000000 price=0.00384300 aligned_qty=3904.00000000
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: symbol=A2ZUSDT 现货持仓限制通过 current=0.0000USDT add=15.3092USDT projected=15.3092USDT limit=100.0000USDT
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 margin 开仓价格对齐 raw=0.00384300 tick=0.00000100 aligned=0.00384300
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 构造 margin 开仓参数 ["symbol=A2ZUSDT", "side=BUY", "type=LIMIT", "quantity=3904", "newClientOrderId=101173705618489344", "timeInForce=GTC", "price=0.003843"]
[2025-10-22T11:25:04Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 提交 margin 开仓请求 symbol=A2ZUSDT qty=3904.000000 type=LIMIT price=0.00384300 order_id=101173705618489344
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 成功创建开仓订单
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 margin 开仓单状态=COMMIT，等待成交
[2025-10-22T11:25:04Z INFO  mkt_signal::signal::strategy] 策略管理器: 新增策略 id=23556339，当前活跃策略数=5
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] strategy inserted: strategy_id=23556339 symbol=A2ZUSDT active_total=5
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] trade signal received: type=BinSingleForwardArbOpen generation_time=1761132304363460 ctx_len=62
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] decoded open ctx: spot_symbol=A2ZUSDT amount=3944 price=0.00380400 exp_time_us=30000000
[2025-10-22T11:25:04Z WARN  mkt_signal::pre_trade::runner] BinSingleForwardArbStrategy: symbol=A2ZUSDT 当前限价挂单数=5 已达到上限 5，忽略开仓信号
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] pre_trade period tick: inspected=5 active_before=5 active_after=5 params_refreshed=false resample_entries=0 elapsed_ms=0.00 strategy_activity=true ladder_cancel_activity=false
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 trade_response req_type=BinanceNewMarginOrder client_order_id=101172700596142080 status=200 body_len=330
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 margin 开仓单状态=CREATE，等待成交
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=101172700596142080 ord_id=124499279 status=NEW
[2025-10-22T11:25:04Z WARN  mkt_signal::pre_trade::order_manager] unexpected OrderExecutionStatus
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 margin execution report status=NEW execution_type=NEW
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132304389 hash=0x5960d05900940611
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 trade_response req_type=BinanceNewMarginOrder client_order_id=101173705618489344 status=200 body_len=330
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 margin 开仓单状态=CREATE，等待成交
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=101173705618489344 ord_id=124499280 status=NEW
[2025-10-22T11:25:04Z WARN  mkt_signal::pre_trade::order_manager] unexpected OrderExecutionStatus
[2025-10-22T11:25:04Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 margin execution report status=NEW execution_type=NEW
[2025-10-22T11:25:04Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132304413 hash=0x8e9ea51d3026c090
[2025-10-22T11:25:05Z DEBUG mkt_signal::pre_trade::runner] trade signal received: type=BinSingleForwardArbOpen generation_time=1761132305523199 ctx_len=62
[2025-10-22T11:25:05Z DEBUG mkt_signal::pre_trade::runner] decoded open ctx: spot_symbol=A2ZUSDT amount=3864 price=0.00388200 exp_time_us=30000000

[2025-10-22T11:25:05Z WARN  mkt_signal::pre_trade::runner] BinSingleForwardArbStrategy: symbol=A2ZUSDT 当前限价挂单数=5 已达到上限 5，忽略开仓信号
[2025-10-22T11:25:05Z DEBUG mkt_signal::pre_trade::runner] trade signal received: type=BinSingleForwardArbOpen generation_time=1761132305523246 ctx_len=62
[2025-10-22T11:25:05Z DEBUG mkt_signal::pre_trade::runner] decoded open ctx: spot_symbol=A2ZUSDT amount=3904 price=0.00384300 exp_time_us=30000000
[2025-10-22T11:25:05Z WARN  mkt_signal::pre_trade::runner] BinSingleForwardArbStrategy: symbol=A2ZUSDT 当前限价挂单数=5 已达到上限 5，忽略开仓信号
[2025-10-22T11:25:05Z DEBUG mkt_signal::pre_trade::runner] trade signal received: type=BinSingleForwardArbOpen generation_time=1761132305523259 ctx_len=62
[2025-10-22T11:25:05Z DEBUG mkt_signal::pre_trade::runner] decoded open ctx: spot_symbol=A2ZUSDT amount=3944 price=0.00380400 exp_time_us=30000000
[2025-10-22T11:25:05Z WARN  mkt_signal::pre_trade::runner] BinSingleForwardArbStrategy: symbol=A2ZUSDT 当前限价挂单数=5 已达到上限 5，忽略开仓信号
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] dispatch ladder cancel signal: symbol=A2ZUSDT strategies=5 bidask_sr=0.000255 threshold=0.000500 ctx_len=46 generation_time=1761132307917588
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] forward signal to strategy_id=22255470 raw_len=70
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 阶梯撤单触发 bidask_sr=0.000255 threshold=0.000500
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 阶梯撤单提交 margin 限价单
    +-------------------+---------+------+------+--------+--------+----------+--------+
    | OrderId           | Symbol  | Side | Qty  | Filled | Remain | Price    | Status |
    +===================+=========+======+======+========+========+==========+========+
    | 95586515807109120 | A2ZUSDT | BUY  | 3864 | 0      | 3864   | 0.003882 | CREATE |
    +-------------------+---------+------+------+--------+--------+----------+--------+
[2025-10-22T11:25:07Z WARN  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 缺少 margin 开仓单，返回非活跃
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 生命周期结束 订单生命周期汇总
    +-------------------+------------+------------+------+------+----------+--------+---------------+----------+
    | OrderId           | ExchangeId | Kind       | Side | Qty  | Price    | Status | CreateTs      | UpdateTs |
    +===================+============+============+======+======+==========+========+===============+==========+
    | 95586515807109120 | 124499276  | MarginOpen | BUY  | 3864 | 0.003882 | COMMIT | 1761132303170 | -        |
    +-------------------+------------+------------+------+------+----------+--------+---------------+----------+
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255470 生命周期结束，相关订单已回收
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] forward signal to strategy_id=22255716 raw_len=70
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 阶梯撤单触发 bidask_sr=0.000255 threshold=0.000500
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 阶梯撤单提交 margin 限价单
    +-------------------+---------+------+------+--------+--------+----------+--------+
    | OrderId           | Symbol  | Side | Qty  | Filled | Remain | Price    | Status |
    +===================+=========+======+======+========+========+==========+========+
    | 95587572369063936 | A2ZUSDT | BUY  | 3904 | 0      | 3904   | 0.003843 | CREATE |
    +-------------------+---------+------+------+--------+--------+----------+--------+
[2025-10-22T11:25:07Z WARN  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 缺少 margin 开仓单，返回非活跃
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 生命周期结束 订单生命周期汇总
    +-------------------+------------+------------+------+------+----------+--------+---------------+----------+
    | OrderId           | ExchangeId | Kind       | Side | Qty  | Price    | Status | CreateTs      | UpdateTs |
    +===================+============+============+======+======+==========+========+===============+==========+
    | 95587572369063936 | 124499277  | MarginOpen | BUY  | 3904 | 0.003843 | COMMIT | 1761132303212 | -        |
    +-------------------+------------+------------+------+------+----------+--------+---------------+----------+
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255716 生命周期结束，相关订单已回收
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] forward signal to strategy_id=22255840 raw_len=70
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 阶梯撤单触发 bidask_sr=0.000255 threshold=0.000500
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 阶梯撤单提交 margin 限价单
    +-------------------+---------+------+------+--------+--------+----------+--------+
    | OrderId           | Symbol  | Side | Qty  | Filled | Remain | Price    | Status |
    +===================+=========+======+======+========+========+==========+========+
    | 95588104945008640 | A2ZUSDT | BUY  | 3944 | 0      | 3944   | 0.003804 | CREATE |
    +-------------------+---------+------+------+--------+--------+----------+--------+
[2025-10-22T11:25:07Z WARN  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 缺少 margin 开仓单，返回非活跃
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 生命周期结束 订单生命周期汇总
    +-------------------+------------+------------+------+------+----------+--------+---------------+----------+
    | OrderId           | ExchangeId | Kind       | Side | Qty  | Price    | Status | CreateTs      | UpdateTs |
    +===================+============+============+======+======+==========+========+===============+==========+
    | 95588104945008640 | 124499278  | MarginOpen | BUY  | 3944 | 0.003804 | COMMIT | 1761132303247 | -        |
    +-------------------+------------+------------+------+------+----------+--------+---------------+----------+
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=22255840 生命周期结束，相关订单已回收
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] forward signal to strategy_id=23556105 raw_len=70
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 阶梯撤单触发 bidask_sr=0.000255 threshold=0.000500
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 阶梯撤单提交 margin 限价单
    +--------------------+---------+------+------+--------+--------+----------+--------+
    | OrderId            | Symbol  | Side | Qty  | Filled | Remain | Price    | Status |
    +====================+=========+======+======+========+========+==========+========+
    | 101172700596142080 | A2ZUSDT | BUY  | 3864 | 0      | 3864   | 0.003882 | CREATE |
    +--------------------+---------+------+------+--------+--------+----------+--------+
[2025-10-22T11:25:07Z WARN  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 缺少 margin 开仓单，返回非活跃
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 生命周期结束 订单生命周期汇总
    +--------------------+------------+------------+------+------+----------+--------+---------------+----------+
    | OrderId            | ExchangeId | Kind       | Side | Qty  | Price    | Status | CreateTs      | UpdateTs |
    +====================+============+============+======+======+==========+========+===============+==========+
    | 101172700596142080 | 124499279  | MarginOpen | BUY  | 3864 | 0.003882 | COMMIT | 1761132304389 | -        |
    +--------------------+------------+------------+------+------+----------+--------+---------------+----------+
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556105 生命周期结束，相关订单已回收
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] forward signal to strategy_id=23556339 raw_len=70
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 阶梯撤单触发 bidask_sr=0.000255 threshold=0.000500
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 阶梯撤单提交 margin 限价单
    +--------------------+---------+------+------+--------+--------+----------+--------+
    | OrderId            | Symbol  | Side | Qty  | Filled | Remain | Price    | Status |
    +====================+=========+======+======+========+========+==========+========+
    | 101173705618489344 | A2ZUSDT | BUY  | 3904 | 0      | 3904   | 0.003843 | CREATE |
    +--------------------+---------+------+------+--------+--------+----------+--------+
[2025-10-22T11:25:07Z WARN  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 缺少 margin 开仓单，返回非活跃
[2025-10-22T11:25:07Z INFO  mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 生命周期结束 订单生命周期汇总
    +--------------------+------------+------------+------+------+----------+--------+---------------+----------+
    | OrderId            | ExchangeId | Kind       | Side | Qty  | Price    | Status | CreateTs      | UpdateTs |
    +====================+============+============+======+======+==========+========+===============+==========+
    | 101173705618489344 | 124499280  | MarginOpen | BUY  | 3904 | 0.003843 | COMMIT | 1761132304413 | -        |
    +--------------------+------------+------------+------+------+----------+--------+---------------+----------+
[2025-10-22T11:25:07Z DEBUG mkt_signal::signal::binance_forward_arb] BinSingleForwardArbStrategy: strategy_id=23556339 生命周期结束，相关订单已回收
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=95586515807109120 ord_id=124499276 status=CANCELED
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] executionReport unmatched: sym=A2ZUSDT cli_id=95586515807109120 ord_id=124499276 status=CANCELED expect_strategy=22255470
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132307926 hash=0xb2efff9e5d706c9d
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=95587572369063936 ord_id=124499277 status=CANCELED
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] executionReport unmatched: sym=A2ZUSDT cli_id=95587572369063936 ord_id=124499277 status=CANCELED expect_strategy=22255716
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132307942 hash=0x4518ee39ca6358d3
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] pre_trade period tick: inspected=0 active_before=0 active_after=0 params_refreshed=false resample_entries=0 elapsed_ms=0.00 strategy_activity=false ladder_cancel_activity=true
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=95588104945008640 ord_id=124499278 status=CANCELED
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] executionReport unmatched: sym=A2ZUSDT cli_id=95588104945008640 ord_id=124499278 status=CANCELED expect_strategy=22255840
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132307960 hash=0xac7096d44d5be85f
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=101172700596142080 ord_id=124499279 status=CANCELED
[2025-10-22T11:25:07Z DEBUG mkt_signal::pre_trade::runner] executionReport unmatched: sym=A2ZUSDT cli_id=101172700596142080 ord_id=124499279 status=CANCELED expect_strategy=23556105
[2025-10-22T11:25:08Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132307988 hash=0x48d13129479ed57
[2025-10-22T11:25:08Z DEBUG mkt_signal::pre_trade::runner] executionReport: sym=A2ZUSDT cli_id=101173705618489344 ord_id=124499280 status=CANCELED
[2025-10-22T11:25:08Z DEBUG mkt_signal::pre_trade::runner] executionReport unmatched: sym=A2ZUSDT cli_id=101173705618489344 ord_id=124499280 status=CANCELED expect_strategy=23556339
[2025-10-22T11:25:08Z DEBUG mkt_signal::pre_trade::runner] accountUpdateFlush: scope=ACCOUNT_POSITION event_time=1761132307998 hash=0x138660959b416677
^C[2025-10-22T11:25:25Z INFO  mkt_signal::pre_trade::runner] pre_trade exiting


先修复定时器问题
1、先发送open，订单进入commit
2、价差反转导致立刻发送cancel，但此时cancel还没有产生回报
3、检查到strategy没有有效订单，strategy被移除

我想知道strategy判断active的条件，commit待确认的不算么？代码的位置给到我
