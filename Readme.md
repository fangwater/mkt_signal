pm2 start ./fr_signal --name fr_signal_binance -- --exchange binance
pm2 start ./pre_trade --name pre_trade_binance -- --open-venue binance-margin --hedge-venue binance-futures
pm2 start ./trade_engine --name trade_engine_binance -- --exchange binance

1、解决目前的敞口计算问题，计算了现货不计算借币
2、修改account，配置自动化，不依赖配置文件，修改fix account channel

Binance 账户监控不再读取配置文件，WS/REST 地址、主备 IP、2 小时时长全部硬编码
OKEx 账户监控同样硬编码 WS 地址与主备 IP，并不再读取会话时长配置
移除账户配置解析模块与配置文件：删去 pm_cfg 模块

3、以及修改key的重启方式。保证错开重启。

monitor分主备，一个新的转发进程。


• 已经把 Binance account monitor 的下发改成 basic 模式了，包含你风控需要的余额/持仓/负债 + 订单回报。

  改动内容

  1. 新增 Binance basic 解析器

  - 新文件：src/parser/binance_basic_account_event_parser.rs
  - 只处理 4 类事件并输出 BasicAccountEventMsg：
      - executionReport → BasicAccountEventType::OrderUpdate，payload 仍是旧的 ExecutionReportMsg（方便复用既有 trait 实现）
      - ORDER_TRADE_UPDATE → BasicAccountEventType::OrderUpdate，payload 仍是旧的 OrderTradeUpdateMsg
      - ACCOUNT_UPDATE →
          - balances: 发 BasicBalanceMsg（timestamp=E，symbol=a，balance=cw crossWalletBalance）
          - positions: 发 BasicPositionMsg（timestamp=E，inst_id=s，ps: LONG->L, SHORT->S, 其他->N，amount=pa）
      - liabilityChange → BasicBorrowInterestMsg（timestamp=E，symbol=a，borrowed=p，interest=i）
  - 模块已注册：src/parser/mod.rs:2

  2. Binance account monitor 使用 basic 解析器并按 basic 去重/日志

  - src/bin/binance_account_monitor.rs
      - parser 换成 BinanceBasicAccountEventParser（src/bin/binance_account_monitor.rs:204）
      - log_parsed_event 和 AccountEventDeduper 全部改为 basic 头解析：
          - Balance/Position/BorrowInterest 直接用 Basic*Msg::from_bytes
          - OrderUpdate 根据 payload 前 4 字节判断是旧的 ExecutionReport 或 OrderTradeUpdate，再解析/去重/打印。

  3. pre-trade 侧 OrderUpdate 兼容 Binance basic payload

  - src/pre_trade/monitor_channel.rs:966
      - OrderUpdate 分支现在按 exchange 解码：
          - OKX → OkexOrderMsg
          - Binance → 看 body 内部类型（ExecutionReport / OrderTradeUpdate），解析后 dispatch_order_update_generic。

  现状效果

  - account_pubs/binance_pm 现在只会收到 BasicAccountEventMsg。
  - 风控用到的 spot/margin 余额、futures 持仓、borrow/interest 都能被 pre-trade 正常吃到。
  - Binance 的订单回报也能继续驱动策略（因为 payload 还是旧的两种 order msg，trait 已实现）。

  如果你想把 Binance balance 的口径从 cw 换成别的（比如 wb），告诉我字段语义，我再改 parser 的那一行即可。
