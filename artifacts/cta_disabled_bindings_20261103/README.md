# CTA 账户绑定，保持停止交易

按用户要求配置：RX01（Exec06）绑定 Prep01 1min/5min/15min；RX03（Exec07）绑定
Prep02 1min/5min/15min；现有 OKX–Binance 组合账户绑定 Spread01 1min。
Spread01 在两条物理执行源各保存一条同名绑定，前台仍是一个账户、一份策略。
因此共有七份仓位策略、八条物理绑定，所有 shares=0。

RX03 的 Manager source 曾被移除。本次从原私有备份恢复原 source 块及 LGQ 查看权限，
保持原 source ID、账户目录、网关端口和历史库归属，保留当前其他账户及组合账户配置。
不恢复旧因子绑定、virtual04 或 Funding 出口例外，Funding 采集仍关闭。
Prep 沿用 lgq_grid_prep，Spread 沿用 lgq_grid_spread，模板参数不改。

Manager 的正常停止语义是在保存零份数时发布一次同名零目标，并排除后续自动推送。
执行前已核验四套 Exec 下单进程停止、七份仓位推送开关关闭、策略目标全零。
配置候选通过已部署二进制的配置及不可变归属校验，只重启 Manager 加载 RX03 入口。
执行和校验回执见 verified.json；运行配置、执行模板、PRC 绑定及受保护进程身份均核验。

完整配置及目录备份只留在远端私有维护目录：
`~/crypto_cta_manager/maintenance-backups/cta_disabled_bindings_20261103T061615Z/`。
prepare 只备份并校验；apply 需要 --execute；verify 只检查结果。
本操作不调用交易所下单接口，不启动下单进程，不更改因子额度、源 IP 或 CPU 分配。
