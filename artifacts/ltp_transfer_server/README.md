# LTP transfer server deployment

JP Meta deployment completed on 2026-11-12 UTC.

- Runtime: `jp-meta-elvpn:~/ltp_transfer_server`.
- Entry: `http://13.115.227.29:4191/delist/transfers/`, existing Delist login.
- Listener `127.0.0.1:8788`; CPU `0-5`; source `172.31.35.228`.
- PID: `2677702`. Binary SHA-256 is recorded in `deployment.json`.
- Read-only real API checks: two DMA accounts, six wallets, Binance→OKX USDT
  networks AVAXC, ERC20 and TRC20.
- Eight Rust integration/unit tests and mocked desktop/mobile browser flows pass.
- Published artifacts match their manifest hashes. Nginx master and unrelated
  monitored application processes remained in place. Only this service's reboot
  entry was added; the other 34 entries are unchanged.
- No credentials in browser assets, receipts, logs or supervisor registry.
- Actual fund transfers: **zero**; durable audit count: **zero**.
- Authenticated production browser acceptance was not run because no existing
  Delist password/session was available. Actual anonymous/CSRF gateway checks
  and the matching mocked authenticated browser flows passed.

`manifest.json` records exact source/artifact fingerprints. `deployment.json`
records the remote backup location. `verification.json` contains final checks.
Runtime/deployment instructions: `docs/ltp_transfer_server.md`.
