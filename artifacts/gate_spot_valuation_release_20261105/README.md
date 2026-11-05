# Gate FR 现货估值修复发布

`gate_fr_arb02` 已于 2026-11-05 02:53 UTC 在 `jp-meta-elvpn` 替换 `pre_trade`
并恢复信号。`gate_fr_arb01` 等待策略在途双腿平仓完成，尚未完成发布。

构建源：本地同步 `origin/arbmm` 的 `6a5d6416`，在维护工作区构建 release。
仅上传该构建产物，经显式临时路径、远端 SHA-256 校验后原子安装，生产机未编译。

```
pre_trade SHA-256:
acdf14a029ff5c09568f809fe4c13ab52a78fac2f302e1c6e80b0ec308a53c0e
```

发版前连续三次检查现货/合约挂单0，暂停信号后再次核验，才停止 pre_trade。
小额尾量按现有10 U对冲触发门槛及Gate最小合约张数判断，不强制taker、不人工平仓。
启停全部经环境内脚本；env.sh、已采样配置、风控参数/名单、IP及CPU设置保持。

| 环境 | pre_trade PID | trade_signal PID | CPU mask | 状态 |
| --- | ---: | ---: | --- | --- |
| gate_fr_arb02 | 925127 | 925266 | 0-5,48-95 | 已发布、信号运行 |
| gate_fr_arb01 | — | — | 0-5,48-95 | 等待平仓收尾 |

arb02 实际运行文件及安装文件均匹配上述哈希，136个其他受保护进程身份保持。
集中度已连续按分钟刷新，SLVON/TSLAX/CRCLX不再列为未知价格，现货补价无请求
错误。冷启动BAND/PUNDIX等合约mark暂缺时仍检查其他资产；02:58起已恢复至
`priced_assets=50 unknown_assets=0`，连续四轮均`sync=ok`。
未新增pos_dump，全部风控参数及名单哈希/内容核对保持。通用Viz/合约mark表不写
本次现货补价，因此对应现货在通用敞口表仍可显示无估值。

arb01 首次验收脚本只检查stdout，遗漏stderr中的启动完成日志，误报超时后自动
恢复旧二进制和原信号。脚本已改为检查两路日志；随后策略产生新的双腿平仓订单，
继续等待其正常收尾，不在途中再次停机。

arb02 备份：`~/gate_fr_arb02/run/gate_spot_valuation_20261105_verified/`。
原始核验及步骤在Git忽略目录 `data/ops/gate_spot_valuation_release_20261105/`。
发布前验证：主库1489项通过、3项忽略；`cargo check --bin pre_trade`、格式检查及
`cargo build --release --bin pre_trade`通过。未触发实盘ADL测试。
