
1、strategy保活问题

每个strategy对应一次开仓，且都会产生hedge，区别是hedge的方式
目前支持市价单taker，和市价单marker两种平仓方式

is_active()是一个trait，检测strategy是否有有效订单，如果无效就清理掉。

```rust
fn with_strategy_mut<F>(&mut self, strategy_id: i32, mut f: F)
where
    F: FnMut(&mut dyn Strategy),
{
    if let Some(mut strategy) = self.strategy_mgr.take(strategy_id) {
        f(strategy.as_mut());
        if strategy.is_active() {
            self.strategy_mgr.insert(strategy);
        } else {
            drop(strategy);
            self.remove_strategy(strategy_id);
        }
    }
}
```



2、