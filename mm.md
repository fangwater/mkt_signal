
• - 现象：07:46:30 三个 HedgeArb 策略的对冲单都打印了“🛡️ 对冲成交 … qty=230”，说明实际已经完全成交；但 07:46:32 新的 ArbOpen 被判定 “限价挂单数量=6 达到上限3”，而 07:46:34 又触发 “对冲订
    单超时，直接撤单” 并收到 Binance -2011 Unknown order sent 错误——典型是本地仍认为挂单未终结，因此又去撤早已成交的订单。
  - 代码原因：
      - handle_hedge_leg_timeout (src/strategy/hedge_arb_strategy.rs:598-645) 只有在 self.hedge_expire_ts 仍为 Some 时才会跑；而 self.hedge_expire_ts 只会在 process_hedge_leg_trade 里，
        当 has_pending_hedge_order() 返回 false 时才置空 (src/strategy/hedge_arb_strategy.rs:1078-1110)。
      - has_pending_hedge_order() (src/strategy/hedge_arb_strategy.rs:648-662) 判断最后一个对冲单在 OrderManager 里是否终态 (order.status.is_terminal())。
      - 但我们从未把订单状态更新为 Filled：apply_order_update (src/strategy/hedge_arb_strategy.rs:1129-1186) 只处理 New / Canceled / Expired / ExpiredInMatch，收到 OrderStatus::Filled
        时会走到 panic 分支，因此 trade engine 不会发这种消息；而 apply_trade_update (src/strategy/hedge_arb_strategy.rs:1057-1130) 也只处理策略自身的数据，并没有调用
        order_manager.update(... OrderExecutionStatus::Filled ...)。于是 OrderManager 中的对冲单永远停留在 Commit/Create 状态。
      - OrderManager::get_symbol_pending_limit_order_count (src/pre_trade/order_manager.rs:286-304) 会把所有 !status.is_terminal() 的限价单都算进 pending 数，所以才会在 log 里看到 “当前
        限价挂单数=6”，虽然真实已经成交。
      - 由于订单状态未终结，has_pending_hedge_order() 始终返回 true，self.hedge_expire_ts 不会被清理，几秒后周期任务 handle_hedge_leg_timeout() 继续把这些“幻影挂单”当作超时撤单，向交易
        所再发一次撤单请求，自然收到 -2011 Unknown order sent。
  - 处理思路：
      1. 在 apply_order_update 里补上 OrderStatus::Filled 分支，把 OrderExecutionStatus 置为 Filled、更新 cumulative_filled_quantity / timestamp.filled_t/end_t，必要时把 hedge_order_ids
         中对应订单移除。
      2. 或者在 apply_trade_update 监听到完全成交时同步更新 OrderManager（调用 order_manager.borrow_mut().update(...) 并在 hedge_order_ids 中剔除），再让 self.hedge_expire_ts = None。
      3. 确认 pending_limit_order_count 会在订单终态时递减，这样 ArbOpen 就不会被“虚假的 pending 单数”挡住，也不会再对已完成的订单重复撤单。


MT 成交--->报单，对冲侧不存在cancel逻辑。MT 模式下，如果无法对冲且没有待处理的对冲订单，关闭策略
定时器处理不处理MT模式的对冲侧。remove = !strategy.is_active();直接移除

MM 模式下，完全成交后需要关闭策略，防止定时器触发对冲逻辑