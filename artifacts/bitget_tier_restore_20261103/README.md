# Bitget FR 仓位档位缓存已恢复

恢复范围：JP Bitget FR01/02/03，2026-11-03 15:18–15:36 UTC。

原PM2路径 `~/mkt_signal/scripts/bitget_position_tier_sidecar.py` 已不存在。
已独立部署至 `~/bitget_position_tier_sidecar`，PID544774 / CPU `0-5,48-95`，
通过环境本地脚本启动并保存PM2新路径。公开请求沿用 `http://127.0.0.1:28902`。

先用原工具dry-run、核验公开端点，再重建三个环境的注册池，一次性补全当前22个在线
币种后启动常驻刷新。active=22、cached=22、missing=0、errors为空。10月18日启动
时的34币是历史名单，不能作为当前币种数。一次性补全后才启动常驻服务，避免部分预热
缓存替换完整旧内存状态。三个pre_trade均确认热加载：

| 环境 | 原PID | 成功时间 UTC | 限额/在线 | missing |
| --- | ---: | --- | --- | ---: |
| bitget_fr_arb01 | 142609 | 15:20:06 | 22/22 | 0 |
| bitget_fr_arb02 | 142765 | 15:19:50 | 22/22 | 0 |
| bitget_fr_arb03 | 129512 | 15:19:43 | 22/22 | 0 |

交易进程未重启，没有下单、撤单或修改杠杆/风控参数。原解析仍报告1个leverage fallback，
本轮未改选档算法。启动包装新增env.sh加载、明确cwd及配套停止脚本。

原上传包与最终脚本校验见 `manifest.initial.json` / `manifest.final.json`；
最后启动包装SHA-256为 `98f9933c56cc7d12abf2310f47e98c9d69db41fef38b7fc7cc5ff8fbe50aaff2`。
脚本bash语法检查通过。证据：`pool_dry_run.log`、`pool_restore_and_seed_plan.log`、
`seed.log`、`after_restore.json`。持续状态另见本轮Delist `runtime_after_install.json`。
PM2旧dump只保存在服务器私有 `run/restore_20261103/dump.before.pm2`，未复制到本地。
