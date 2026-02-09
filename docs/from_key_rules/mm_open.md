# MMOpen `from_key` 规则

当前仓库内，`MMOpen` 的 `from_key` 未定义统一业务拼装格式，属于**上游信号透传字段**。

规则：
- 类型：UTF-8 字符串字节（`Vec<u8>`）。
- 约束：仓库内未对分隔符、字段个数、数值精度做固定校验。
- 使用：消费侧仅做反序列化与透传，不做结构化解析。

建议：
- 由上游统一制定格式（例如 `time:scene:tag`），并在上游文档中维护版本。

代码依据：
- 字段定义/序列化：`src/signal/open_signal.rs:57`
- 消费入口（仅解码，不解析 from_key 结构）：`src/pre_trade/signal_channel.rs:569`
