# lgq_ltp_81857 全仓杠杆钱包转 USD-M

用户明确要求把新账户资金划到合约钱包。当前全部可用资金为20000 USDT，
无借款、利息或锁定；maxTransferable=20000，API具有universal transfer权限。
从JP binance_exec_trade08沿用source IP172.31.35.228，提交一次
POST /sapi/v1/asset/transfer，type=MARGIN_UMFUTURE，amount=20000。
tranId=423596580418；划转历史CONFIRMED。初次ACK后钱包余额尚未同步，
仅执行--verify-only读取，没有重发POST。最终合约wallet/equity/available均20000，
Margin和Spot均0；合约持仓0，交易进程未启动。
凭据仅从远端私有env.sh读取，未复制到证据目录。
证据：dry_run.json、execute_plan.json、intent.json、response.json、completion.json。
官方接口依据：https://binance.github.io/binance-connector-js/interfaces/_binance_wallet.WalletRestAPI.AssetApiInterface.html
