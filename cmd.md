 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --execute
 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --buy --execute
 python scripts/okx_balance_and_positions_ws.py --inst-type MARGIN --duration 5

 现在我需要一个基于新版本的敞口计算。即一个新的basic exposur_manager.你先告诉我，你可以简化什么。我先告诉你，我需要一个处理mapping的trait。目前实
  现okex的即可，处理balance到um的symbol。然后两个basic的manager都提供获取净头寸的trait

1、需要测试合约头寸是否准确
