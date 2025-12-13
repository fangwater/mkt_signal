 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --execute
 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --buy --execute
 python scripts/okx_balance_and_positions_ws.py --inst-type MARGIN --duration 5

 现在我需要一个基于新版本的敞口计算。即一个新的basic exposur_manager.你先告诉我，你可以简化什么。

1、需要测试合约头寸是否准确
2、下单对冲等所有涉及到um的地方需要判断合约张数


【字段说明】
  - ctVal:       合约面值（Contract Value）
  - ctMult:      合约乘数（Contract Multiplier）
  - ContractSz:  实际合约大小 = ctVal × ctMult
  - MinQty:      最小下单量（张数）
  - StepSize:    下单步进（张数）
  - PriceTick:   价格最小变动单位

  extract_base_asset(s 这些函数都有了，避免反复定义

