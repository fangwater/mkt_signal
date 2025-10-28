用中文，我现在想增加一个二进制进程，直接加入到cargo的bin中，进rolling_metrics中实现。

这个进程需要和funding rate signal的一部分功能类似，目前，之需要订阅币安的ask bid期货现货，计算两个sr值.
但我需加上前缀，即谁和谁 为之后的跨所做准备。

我需要在 redis 中增加可配置的选项：
MAX_LENGTH = 150000
refresh_sec = 60 //1min重新计算一次
reload_param_sec = 3 //3s重新读取一次参数
factors = {
  "bidask": {
    "resample_interval_ms": 1000,
    "rolling_window": 100000,
    "min_periods": 90000,
    "quantiles": [0.05, 0.70]
  },
  "askbid": {
    "resample_interval_ms": 1000,
    "rolling_window": 100000,
    "min_periods": 90000,
    "quantiles": [0.30, 0.95]
  }
}

然后有一个类似sync_binance_forward_arb_params.py的脚本，可以修改参数redis中的参数

计算的逻辑参考

df["binancespotbid_binanceswapask_sr"] = (df["binancespot_bid"] - df["binanceswap_ask"]) / df["binancespot_bid"]
df["binancespotask_binanceswapbid_sr"] = (df["binancespot_ask"] - df["binanceswap_bid"]) / df["binancespot_ask"]

threshold_dict = { 
    'bidask_sr': df["binancespotbid_binanceswapask_sr"].iloc[-1], 
    'askbid_sr': df["binancespotask_binanceswapbid_sr"].iloc[-1], 
    'bidask_lower': df["binancespotbid_binanceswapask_sr"].rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).quantile(0.05).iloc[-1],
    'bidask_upper': df["binancespotask_binanceswapbid_sr"].rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).quantile(0.70).iloc[-1],
    'askbid_upper': df["binancespotask_binanceswapbid_sr"].rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).quantile(0.95).iloc[-1],
    'askbid_lower': df["binancespotbid_binanceswapask_sr"].rolling(ROLLING_WINDOW, min_periods=MIN_PERIODS).quantile(0.30).iloc[-1],
} 

Series.rolling() 不会因为窗口或 min_periods 而减少输出长度，它总会生成 与原始数据同样长度 的结果 Series。
只是前面的部分（不足 min_periods 的）结果是 NaN。iloc[-1]的本质相当于只计算最后一个窗口。

请用 Rust 实现一个小型服务，用于对所有 U 本位币种维护滑动窗口的价差因子百分位阈值。服务应满足以下要求：


阈值（分位数）更新频率较低，但分位点集合是可配置/可变的（例如 [0.05, 0.30, 0.70, 0.95]，未来可改动）。
存储足够历史数据且内存友好：严格禁止使用链表/跳表等指针密集结构保存历史；只用**定长环形缓冲（ring buffer）**保存最近 MAX_LENGTH 条 f32。
计算尽量不阻塞数据读取，不丢消息。

数据流与并发模型
读入线程（Tokio）
从 icexy2（进程间通讯）持续接收各 symbol 的 tick，计算价差因子（sr），将 sr: f32 push 到该 symbol 对应的 ring buffer。
写入操作为 O(1)，超出max length就丢弃。

计算线程（独立 std::thread）
定时触发，参考redis

对每个 symbol：
若 ring 中数据量 < MIN_PERIODS，则阈值全部为nan
否则从 ring 快照+拷贝最近 ROLLING_WINDOW 条到本地 Vec<f32>（scratch），在其上计算分位。
顺序计算完所有 symbol 后，一次性写入 Redis 的一个key，格式参考binance_arb_price_spread_threshold.py。

打印本轮计算总耗时、处理的 symbol 数、被跳过数量等指标。

读写解耦
计算线程绝不在 ring 上做重操作；只做快照+内存复制。

存储结构
每个 symbol：一个 Ring<f32>（固定容量 MAX_LENGTH），内部使用连续内存（如 Vec<f32>），用 head/tail 或单调计数 + 模运算定位。
一个全局 DashMap<Symbol, Arc<Ring>> 管理所有币种。不得使用链表/跳表等指针密集结构。

scratch Vec<f32> 上使用 slice::select_nth_unstable 实现线性插值分位（与 pandas 默认 method="linear" 对齐）。
对任意分位集合 qs: Vec<f32>：
计算目标秩 k = q*(n-1)；
通过 select_nth_unstable 取 lo/hi 并线性插值；
为降低常数开销，可按从小到大顺序处理并收缩区间


MAX_LENGTH（环形缓冲容量）、MIN_PERIODS、ROLLING_WINDOW、QUANTILES（分位数组）、触发周期/阈值，均从redis加载。
都可以在运行的时候更新，MAX_LENGTH更新会触发扩容，此时可以接受丢数据

Redis 写入
参考binance_arb_price_spread_threshold.py的写入格式

指标与日志
按需增加日志

Tokio 读线程持续满速写入时不丢包

内存占用与 symbols * MAX_LENGTH * 4B 同量级；额外开销只含管理结构与 scratch（按 ROLLING_WINDOW 预分配一份）。


ring/: 无锁 SPSC 环形缓冲实现，提供 push, len, copy_latest(n, dst)，原子快照。
quantile/:
quantiles_linear_select_unstable(values: &mut [f32], qs: &[f32]) -> Vec<f32>
main.rs: 组装 Tokio 读线程 + 独立计算线程。

最近出现问题的原因是这2个
1、两个网络还是归属一个服务商，出问题以后还是都出问题。
2、专线断开的时候，kafka无法同步


