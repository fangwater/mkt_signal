 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --execute
 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --buy --execute
 python scripts/okx_balance_and_positions_ws.py --inst-type MARGIN --duration 5
 python scripts/okx_post_only_reject_test.py --inst-id SOL-USDT-SWAP --px 200 --notional-usdt 1000 --execute

//mock 壳子，用于测试交易链路是否正常
cargo run --bin fr_manual_signal -- --exchange okex --open okex-margin --hedge okex-futures --port 8911

//


