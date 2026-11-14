# cn-futures-pbs

国内期货来源适配 → 通用 `MarketDataMsg` → Iceoryx2 IPC。

```text
cn-futures-pbs --zmq|--ctp|--colo --config PATH
```

当前 `--zmq` 是公司内网通联转发，裸解析六家交易所五档快照；CTP 只编译验证，colo／公网待实际接口。易达自带单档行情只作审计备份参考，本轮不接入。

配置、消息布局、接收语义和验证见 [统一行情说明](../../docs/cn_futures_pbs.md)。
