pm2 start ./trade_signal --name trade_signal_binance -- --exchange binance
pm2 start ./pre_trade --name pre_trade_binance -- --open-venue binance-margin --hedge-venue binance-futures
pm2 start ./trade_engine --name trade_engine_binance -- --exchange binance


• 已经把 Binance account monitor 的下发改成 basic 模式了，包含你风控需要的余额/持仓/负债 + 订单回报。

  改动内容

  - Binance basic 的 OrderUpdate payload 已统一为一个纯 basic 结构 `BinanceBasicOrderMsg`
    src/common/basic_account_msg.rs 新增：
      - struct BinanceBasicOrderMsg（单一 schema，带 venue 字段区分 margin/um）
  - Binance basic parser 统一生成该结构并下发
    src/parser/binance_basic_account_event_parser.rs
  - pre‑trade / account monitor 侧直接解析 `BinanceBasicOrderMsg`
    src/pre_trade/monitor_channel.rs
    src/bin/binance_account_monitor.rs
    OrderUpdate 不再需要二次细分/额外判断，直接按统一 schema 解码即可。
● 完成！已经移除了所有 FR (Funding Rate) 相关的可视化代码。修改内容如下：

  src/bin/viz_server.rs:5
  - 移除了 spawn_fr_resample_listener 的 import
  - 移除了对 spawn_fr_resample_listener(hub.clone()) 的调用（src/bin/viz_server.rs:27-29）

  src/common/viz/subscribers.rs:1
  - 移除了 FundingRateArbResampleEntry 的 import
  - 移除了 FR_RESAMPLE_MSG_CHANNEL 常量
  - 移除了 spawn_fr_resample_listener 整个函数（原 line 33-49）

  现在 viz_server 只专注于转发 pre_trade 两路重采样数据：
  - pre_trade_exposure
  - pre_trade_risk

  FR 相关的可视化由独立的 fr_visualization.rs 进程处理。编译通过 ✓


1、旧程序启动
