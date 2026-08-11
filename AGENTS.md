# Repository Guidelines

## Project Shape

`mkt_signal` is a multi-binary Rust trading system for market data, funding-rate arbitrage, intra-exchange arbitrage, cross-exchange arbitrage, market making, account monitoring, order execution, persistence, and visualization.

The crate is both a library and a binary collection. Primary code lives under `src/`; scripts and deployment wrappers live under `scripts/`, `xarb_scripts/`, `intra_scripts/`, `cross_scripts/`, and `mm_scripts/`.

Important modules:

- `src/connection/`: websocket connection helpers and exchange-specific market-data/user streams.
- `src/parser/`: exchange message parsers.
- `src/funding_rate/`: FR/XARB signal logic and decision routing.
- `src/pre_trade/`: exposure, risk checks, auto repay, and query/orphan handling before execution.
- `src/trade_engine/`: order dispatch, query routing, REST helpers, websocket execution clients, and query parsers.
- `src/portfolio_margin/`: portfolio/unified account streams and account-monitor plumbing.
- `src/strategy/`: order-update handling and strategy state.
- `src/persist_manager/`: order/trade persistence.
- `src/factor_pub/`, `src/depth_pub/`, `src/kline_pub/`, `src/spread_pbs/`: market-data and factor pipelines.
- `src/viz/`, `src/fr_signal_dashboard/`, `src/rolling_metrics/`: dashboards and monitoring helpers.

Key binaries are listed explicitly in `Cargo.toml` because `autobins = false`. Common ones include `trade_engine`, `pre_trade`, `trade_signal`, `gate_account_monitor`, `binance_account_monitor`, `okex_account_monitor`, `bitget_account_monitor`, `bybit_account_monitor`, `dat_pbs`, `spread_pbs`, `depth_pub`, `kline_pub`, `trade_flow_feature_pub`, `fusion_factor_pub`, `model_pub`, `persist_manager`, `viz_server`, and `fr_signal_dashboard`.

## Build And Test

Use focused checks while iterating:

```bash
cargo fmt --check
cargo check --bin trade_engine
cargo check --bin gate_account_monitor
cargo test <test_name>
cargo test --lib <module>::tests::<test_name>
```

Use release builds before deployment or live smoke tests:

```bash
cargo build --release --bin trade_engine
cargo build --release --bin pre_trade
cargo build --release --bin gate_account_monitor
cargo build --release
```

Run `cargo fmt` before committing Rust changes. Run targeted tests for the touched behavior; broaden to `cargo test` when changing shared routing, parsers, IPC message formats, or strategy state.

## Runtime Layout

Deployed environments normally live in `$HOME/<env-name>` and include:

- `env.sh` with credentials, `IPC_NAMESPACE`, venues, account mode, and optional core bindings.
- `config/` copied or generated from this repo.
- binary symlinks or copied release binaries.
- start/stop scripts copied from this repo.

Most runtime wrappers source `env.sh` automatically from the environment directory. Do not hard-code credentials or account-specific values in repo files.

Common FR scripts:

```bash
scripts/start_trade_engine.sh
scripts/stop_trade_engine.sh
scripts/start_account_monitor.sh
scripts/stop_account_monitor.sh
scripts/start_fr_pre_trade.sh
scripts/stop_fr_pre_trade.sh
scripts/start_fr_signal.sh
scripts/stop_fr_signal.sh
```

Mode-specific wrappers:

- `xarb_scripts/`: cross-exchange funding arb support.
- `intra_scripts/`: same-exchange spot/futures arb support.
- `cross_scripts/`: cross-exchange deployment/runtime wrappers.
- `mm_scripts/`: manual/multi-exchange market-maker wrappers.

Use the environment-local scripts when operating a live env, for example:

```bash
cd ~/gate_fr_arb01
./scripts/stop_trade_engine.sh
./scripts/stop_account_monitor.sh
```

## Known Exec Deployment: el01

The following is a historical snapshot verified on 2026-08-10 UTC. It proves that the environment existed at that time, but it is not a statement that the same processes, binaries, ports, or configuration are still live. Re-check the remote host before any operation. The original full deployment invocation was not found; the available evidence starts with a post-deployment audit.

- Connect with `ssh cta_exec`. The local SSH config resolves this alias to the `el01` target through `el01-jump`; keep raw endpoints and all credentials out of this repository.
- Remote environment: `/home/el01/binance_exec_trade01`.
- Venue and instance: Binance Futures, instance `01`.
- Deployment entry point: `scripts/deploy_exec.sh --env-name binance_exec_trade01 --venue binance-futures`.
- Independent collector/read configuration: `config/exec.toml`; do not add this source to `config/persist.toml`.
- The standard Exec runtime consists of `exec-pre-trade`, `trade_signal`, `trade_engine`, `account_monitor`, `persist_manager`, and `viz_server`, with the matching `spread_pbs` venue deployment.
- Instance `01` defaults to viz port `10041` and config port `18161`. `deploy_exec.sh` copies/builds the runtime but deliberately does not start processes.
- At the 2026-08-10 audit, `persist_manager` had neither `PERSIST_SYNC_SOURCE_ID` nor `PERSIST_SYNC_BIND`, opened no TCP listener, and therefore did not expose its gRPC sync service. Do not assume that remains true without checking `env.sh`, the process environment, and listeners.
- `order_export` was not permanently deployed. A release binary was temporarily uploaded, used to read `/home/el01/binance_exec_trade01/data/persist_manager`, and removed after the files were copied back and SHA-256 checked.
- The one-time export remains under `data/order_exports/binance_exec_trade01/20260810T031235Z/` (normally Git-ignored): `uniform_orders.parquet`, `order_updates_unmatched.parquet`, `trade_updates_unmatched.parquet`, and `order_queue_positions.parquet`.
- Export snapshot: `uniform_orders` had 1,141 rows covering 568 unique orders; unmatched order updates had 761 rows, unmatched trade updates had 49 rows, and queue positions had 0 rows. All unmatched order IDs were represented in `uniform_orders`; the unmatched tables are append-only audit history from restart/late/duplicate dispatch paths, not evidence of order-level loss.
- Commit `72a4aff9` (`Trim hedge fields from exec order exports`) added Exec environment-name support and removes the six `signal_hedge_*` columns from Exec exports; the resulting `uniform_orders` file has 28 columns while retaining the same 1,141 rows.

Never copy passwords, API keys, or values from the remote `env.sh` into this file, chat, commits, or command output.

## IPC And Config

Processes communicate primarily through iceoryx2 shared-memory IPC, with some Redis-backed configuration/state and RocksDB persistence.

`config/iceoryx2.toml` is often required in the runtime current working directory. Start scripts may copy it into deployed envs if missing.

Key config files:

- `config/mkt_cfg.yaml`: market-data and connection config.
- `config/depth_cfg.yaml`, `config/kline_cfg.yaml`, `config/trade_flow_feature_pub.yaml`: data publishers.
- `config/fusion_factor_pub.toml`, `config/model_pub.toml`, `config/pairmm_resample.yaml`: factor/model pipelines.
- `config/viz.toml`: visualization server.

Prefer existing config loaders and Redis sync/print scripts over ad hoc parsing.

## Exchange-Specific Notes

Gate futures decimal contract sizes require `X-Gate-Size-Decimal: 1` on affected REST and WS paths. Without it, decimal positions/orders can be reported or interpreted as integer-compatible sizes. Keep this in mind for Gate futures order placement, position snapshots, user streams, account monitors, and flatten scripts.

Gate order queries over WS have a transport `req_id` that is not the same thing as the business `client_query_id`. Query responses must map through the in-flight query table and publish outcomes under the original `client_query_id`; otherwise `pre_trade` orphan/query handling will time out.

For other exchanges, check the local query path before assuming the same behavior. REST query paths usually publish using the original request context; websocket query paths need explicit id correlation.

## Live Trading Safety

This repo controls real orders. Treat these operations as live-risk unless the user explicitly says it is a sandbox:

- starting or stopping PM2 processes,
- deploying binaries into `$HOME/<env-name>`,
- canceling orders,
- flattening futures/spot exposure,
- changing leverage/account mode,
- running scripts with `--execute`.

Before live-impact commands, state the target env, exchange, symbol scope, and whether the command only observes or mutates state. Prefer dry-runs first when scripts support them.

Never revert user or operator changes in environment directories unless explicitly asked. If repo and env copies differ, inspect both and explain which one you are modifying.

## Coding Conventions

- Keep changes scoped to the touched exchange/path/strategy.
- Reuse existing parsers, signing helpers, connection wrappers, query mappings, and IPC message types.
- Add concise context to errors with `anyhow::Context`.
- Do not panic on network, exchange, or malformed-message errors in long-running services.
- Preserve microsecond timestamp conventions.
- Keep exchange-specific quirks local to the relevant exchange module when possible.
- For Python operational scripts, keep dry-run behavior and explicit `--execute` semantics intact.

## Git Hygiene

Check worktree state before editing:

```bash
git status --short
git diff --stat
```

There may be unrelated local changes. Do not revert or include them in commits unless the user asks. Stage exact files for the task, then verify with:

```bash
git diff --cached --stat
git status --short
```

Use clear commit messages that describe behavior, for example `Fix Gate query correlation and decimal sizing`.
