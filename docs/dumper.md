接下来我还需要一个binance_funding_rate_arb_dumper进程，这个进程用于订阅这个ws(当然ws也会被别的东西订阅)

显然这个进程需要3个表，需要解析这个消息。然后分开落盘。

因此我需要的是一共是3个table，分别记录资费套利策略的开仓单信息、对冲单信息、以及成交单信息
先设计三种结构体，代表每个table的col，以及表头。

然后，这个binance_funding_rate_arb_dumper不是记录表，而是记录rocksdb。直接把收到的json，写入rocksdb。
但是binance_funding_rate_arb_dumper需要支持一个http请求，这个请求会给出参数:
注意加锁，防止http的并发访问。
symbol 交易的币种，date 表示起始点、结束的时间。以及table。开仓、平仓、还是撤单
