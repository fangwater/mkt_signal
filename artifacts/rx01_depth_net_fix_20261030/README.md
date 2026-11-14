# RX01 净流量分母修正

按用户确认，将 `depth_flow_net`、`depth_touch_net` 统一改为
`(B - A) / (abs(B) + abs(A))`，B/A 分别为买/卖侧新增名义额减撤减名义额。
双边净流量均为零或任一净流量非有限时为 NaN，其他符号组合均有效。
Rust 与 Python 参考实现同步采用等价缩放计算，避免有限大数的分子/分母溢出。
其余21个因子在既有基准行上的输出保持一致。

源提交 `dc8d82f6`。更新 `jp-meta-elvpn:~/binance-cta-perp-rx01` 的
`cta_perp_factor_model_15m_pub` 和 `config/README.md`。二进制在本地 arbmm release
构建，上传到独立临时目录，校验 SHA-256 后备份并原子替换。
`manifest.json`、`receipt.json` 保存产物哈希、保护配置哈希和部署结果。

本次7项针对性 Rust 测试、3项 Python 测试通过。基准为17行×23个因子，
覆盖净撤单、正负抵消、近似抵消、单边为零、双边为零、非有限输入及极端有限值。
按真实15min预热门槛进行195根合成回放：96/67标准化第67根开始产出，
192/96分位第162根开始两项均就绪，无需降低门槛或延长窗口。
未重复完整 Kafka 回放，不能将此前09:05旧公式的22/23结果当作本次公式的实测结果。

正式币种、因子与NQ清单继续为空，`enabled=false`，无Manager绑定。
不启动常驻服务、不开IPC发布通道、不发交易信号，不修改env、账户、订单、CPU或IP分配。
验收确认空universe仍在创建历史目录或连接Kafka前阻止启动，10195/19185继续关闭。
受保护进程的PID、启动时间和cwd保持；具体数量见回执。

远端备份、清单及回执保存在
`~/binance-cta-perp-rx01/run/depth_net_fix_20261030/`，临时上传目录验收后移除。
