 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --execute
 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --buy --execute
 python scripts/okx_balance_and_positions_ws.py --inst-type MARGIN --duration 5
 python scripts/okx_post_only_reject_test.py --inst-id SOL-USDT-SWAP --px 200 --notional-usdt 1000 --execute

//mock 壳子，用于测试交易链路是否正常
# 目录名需要包含 `<suffix>-<namespace>-trade` 或 `<suffix>_<namespace>_trade`，用于从 CWD 推断 namespace/suffix
# - 例：`okex_fr_trade` -> namespace=fr, suffix=okex -> 读 `fr_*:okex`
# - 例：`okex-binance-xarb-trade` -> namespace=xarb, suffix=okex-binance -> 读 `xarb_*:okex-binance`
cargo run --bin manual_signal -- --open okex-futures --hedge binance-futures --port 8911

//
bash scripts/deploy_setup_env_xarb.sh --open-venue okex-futures --hedge-venue binance-futures
scripts/deploy_xarb_trade_engine.sh --open-venue okex-futures --hedge-venue binance-futures

# deploy manual_signal
scripts/deploy_fr_manual_signal.sh trade --exchange okex
scripts/deploy_xarb_manual_signal.sh trade --open-venue okex-futures --hedge-venue binance-futures
