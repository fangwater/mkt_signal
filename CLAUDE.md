# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
cargo build --release          # build all binaries → target/release/
cargo run --bin <name> -- ...  # run a specific binary
cargo test                     # run all tests
cargo fmt --all                # format (required before pushing)
cargo clippy -- -D warnings    # lint (required before pushing)
```

Run a single test:
```bash
cargo test <test_name>
cargo test --lib <module>::tests::<test_name>
```

Build and deploy via PM2:
```bash
ln -sf target/release/mkt_signal ./crypto_proxy
./start_proxy.sh binance
./stop_proxy.sh binance
pm2 logs crypto_proxy_binance
```

## Architecture

This is a **multi-binary Rust trading system** for cryptocurrency arbitrage and market making across Binance, OKEx, Gate, Bybit, Bitget, and Hyperliquid.

### Data Pipeline (left to right)

```
Exchange WS
  → dat_pbs          (market data broker, dual-connection with auto-restart)
  → depth_pub        (order book)
  → kline_pub        (candlesticks)
  → trade_flow_feature_pub  (trade flow features)
  → kline_factor_pub / fusion_factor_pub / model_pub  (ML factors + XGBoost inference)
  → trade_signal     (signal generation: FR arb or XARB or MM)
  → pre_trade        (risk/exposure validation)
  → trade_engine     (order execution)
  → persist_manager  (RocksDB persistence)
```

Supporting processes: `*_account_monitor`, `rolling_metrics`, `viz_server`, `pairmm_resample`.

### IPC

Processes communicate via **iceoryx2** (shared memory IPC) and **ZMQ**. Redis is used for shared state (symbol lists, thresholds). RocksDB backs `persist_manager`.

### Library structure (`src/lib.rs`)

The crate is both a library and a collection of binaries. Key modules:

- `mkt_pub` — `MktSignalApp` orchestrator, config loader (`cfg.rs`), WS proxy
- `connection/` — per-exchange WS clients (`binance_conn`, `okex_conn`, etc.)
- `parser/` — per-exchange message parsers
- `factor_pub/` — factor computation pipelines (fusion, kline, pairmm_resample)
- `funding_rate/` — FR/XARB signal logic, decision routing, config/threshold loaders
- `signal/` — `TradingVenue` enum, trade signal types
- `trade_engine/` — order dispatch, query parsers, per-exchange REST clients
- `pre_trade/` — exposure manager, price table, net position, auto-repay
- `strategy/` — order update handling, strategy manager
- `persist_manager/` — order/trade update persistence
- `account/` — order update records, execution records
- `portfolio_margin/` — PM account streams (Binance, OKEx, Gate)
- `bridge/` — iceoryx2 IPC bridge
- `viz/` — WebSocket visualization server
- `rolling_metrics/` — rolling quantile/ring buffer metrics
- `common/` — `Exchange` enum, `TradingVenue`, BBO, Redis client, time/symbol utils

### Config files

Each binary has a corresponding YAML in `config/`. Runtime config for `dat_pbs` is loaded from `$HOME/dat_pbs/config/mkt_cfg.yaml`. Key configs:

- `config/mkt_cfg.yaml` — dual-connection settings, data type switches, IP bindings
- `config/fusion_factor_pub.yaml` — fusion factor pipeline
- `config/depth_cfg.yaml` — depth publisher
- `config/trade_flow_feature_pub.yaml` — trade flow features
- `config/pairmm_resample.yaml` — pair MM resampling

### Environment variables

- `MM_SUFFIX` / `MM_ENV` / `MM_VENUE` — market maker environment isolation
- `BINANCE_API_KEY`, `BINANCE_API_SECRET`, `BINANCE_ACCOUNT_MODE` — exchange credentials (never in YAML)
- `RUST_LOG` — log level (e.g., `RUST_LOG=info`)

### Namespace isolation

Strategy-specific processes use environment-suffixed namespaces (e.g., `binance_mm_beta`). The `trade_signal` binary infers its namespace from the current working directory name (e.g., a CWD of `binance_mm_trade` → namespace `mm`, key suffix `binance`).

### Conventions

- `TradingVenue` (e.g., `binance-futures`, `binance-margin`, `okex-futures`) is the primary venue selector passed via `--venue`
- Timestamps are in microseconds throughout
- Errors use `anyhow` with `.context()`; no panics on network errors
- Commits: Conventional Commits (`feat:`, `fix:`, `refactor:`, `chore:`, `docs:`)
- Tests: inline `#[cfg(test)] mod tests` or under `tests/`
