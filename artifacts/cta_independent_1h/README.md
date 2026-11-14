# RX01 / RX03 小时策略合并上线

用户最终确认将1h并入原Prep进程，并接受分钟因子重新预热。2026-11-09 UTC已完成：

- 唯一Prep运行环境：`~/cta-prep-binance-futures`，PID2168358，仍使用原CPU17。
- Prep01/Prep02各频段共用原config server（PID2169023，loopback19220）和组合目录。
- 原9张分钟表加4张小时表，共596项计算；临时独立1h进程已停止并归档，没有新增独占核。
- 14:25 UTC四张小时表全部启用：RX01每规则50U，RX03三个券池每规则100U；grid、跟随1份。
- RX01现4条绑定，RX03现9条绑定；原分钟配置哈希和跟随绑定均保持。
- 265项小时计算中264项就绪；DASH `depth_imb_bps1` 在中间价±1bp内没有挂单，保留空值。
- 分钟1min为61/61、5min为56/69、15min为143/201项就绪；未就绪计算继续接收实时输入。
- 启用没有再次回放；历史只预热窗口，小时从15:00 UTC新封bar开始决策。
- RX03整个1min已下架，无活动委托/批次及非零目标；核验时16条低于交易所最小下单量的
  真实尾量约32.35U保留，未抹掉分仓或虚构平仓。

历史输入使用已完成的34标的×744根原生小时包：
`~/cta-hourly-history/current/data`。导入后从保留Kafka接续。包截止于2026-11-08 UTC；
后续启动时若Kafka留存已越过该截止点，需要先刷新历史包，现有程序会检查衔接缺口。
合成和接续证据见 `../cta_history_continuation_audit/verification.json`。

私有完整备份/操作回执：`~/cta-hourly-history/merged-deployment/`；临时小时环境归档：
`~/retired_data/cta-prep-binance-futures-1h_20261109T140207Z/runtime/`。
本目录 `verified.json` 为脱敏最终核验，`verify_merged.py` 仅观察服务并保存回执。
`merge.py`、`activate_merged.py` 默认只读，执行需要`--execute`，禁止盲目重复执行。

首次创建请求在任何策略/绑定写入前被拒绝：旧运维会话辅助函数直接写入数据库，
而新Manager发布接口读取内存会话。确认目标绑定不存在的请求返回404且不写配置，
触发目录刷新后，同一会话创建成功；保留原失败记录，仅允许经核验的首次创建前恢复。

验证：CTA库69项、Config服务4项、生命周期脚本1项通过，`cargo fmt --check`通过。
`deploy.py`、`shared_cpu.py`及早期jsonl仅记录最终合并之前的临时独立实例阶段。
