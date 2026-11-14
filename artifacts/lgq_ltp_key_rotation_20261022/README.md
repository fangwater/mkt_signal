# lgq_ltp_81857 Key 更新

2026-10-22 13:23 UTC，更新 JP `binance_exec_trade08` 的远端私有env.sh。
凭据通过无回显输入与SSH stdin传入；env及备份权限0600，文件内容未复制到本地。
新Key通过FAPI/Spot只读鉴权，PAPI返回-2015；运行读取配置改为STANDARD。
环境本地脚本重启账户监控PID157817与Config PID162464。账户快照、UM WS轮询及
网关入口正常，USD-M持仓/挂单均0。持久化与Viz不变，交易进程继续停止，Manager无绑定。
`rotation.json`及`verification.json`仅保存非敏感验收信息。
未提交资金划转、订单或交易所账户模式变更。
