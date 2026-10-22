# 旧 RX02 taker 清仓

2026-10-22，用户明确要求旧RX02全部清仓，并指定直接taker。
JP binance_exec_trade05：24个非零永续仓位、无非USDT现金资产、无普通/条件挂单。
复用仓库RapidX签名、身份校验和订单查询；source IP沿用172.31.35.228。
先GET-only检查，随后逐笔MARKET、reduceOnly=TRUE、positionSide=NONE平仓。
不发送timeInForce或limitPrice。每笔先持久化clientOrderId，再提交一次；
逐笔核对成交数量及交易所剩余仓位，不以POST ACK当作成交证据。
24笔均FILLED，最终零持仓、零挂单、零条件单、无未决订单；旧执行进程未启动。
SG只读快照确认旧仓位已清理，保留新RX02实际仓位。
证据：before.json、execute_plan.json、close_journal.json、completion.json、sg_old_positions_check.json。
API依据：https://apidocliquidity.readme.io/reference/place-order
