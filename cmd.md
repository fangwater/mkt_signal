 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --execute
 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --buy --execute
 python scripts/okx_balance_and_positions_ws.py --inst-type MARGIN --duration 5

 现在我需要一个基于新版本的敞口计算。即一个新的basic exposur_manager.你先告诉我，你可以简化什么。

1、需要测试合约头寸是否准确
2、下单对冲等所有涉及到um的地方需要判断合约张数

• - OKX 跨/逐仓默认会自动借还（有借币额度且开了自动借还时），你看到的 SOL 负债变化就是自动借入补足、成交后再自动还。
  - tgtCcy 只影响市价买单的含义：
      - tgtCcy=quote_ccy（默认）：sz 表示花多少报价货币（USDT）；成交数量由盘口换算。
      - tgtCcy=base_ccy：sz 表示买多少基础币（SOL），用多少 USDT 由系统算。
  - 对卖单/限价单：tgtCcy 不起作用，卖单 sz 始终是卖出的基础币数量；限价单需要你同时给 px 和 sz（基础币数量）。
