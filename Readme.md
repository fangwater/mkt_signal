1. 去掉每次触发的全量序列重建
   build_symbol_series 每次 trade 都把几十个 VecDeque 全拷贝成 Vec：src/factor_pub/fusion_factor_pub/app.rs:742、src/factor_pub/fusion_factor_pub/
   app.rs:781。
   这是当前最大 CPU/分配热点之一。建议改成：

- 直接基于 state 计算（不建 SymbolSeries），或
- 只为“本 symbol 实际需要”的字段做按需 materialize。





