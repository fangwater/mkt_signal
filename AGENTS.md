# Repository Guidelines

## Project Structure & Module Organization
- This is a multi-binary Rust workspace-in-a-single-crate. `Cargo.toml` sets `autobins = false`; the real process entrypoints live in `src/bin/*.rs`.
- Core ingestion and market-data publishing live under `src/mkt_pub/`, `src/connection/`, `src/parser/`, and `src/portfolio_margin/`.
- Decisioning and execution logic live under `src/signal/`, `src/funding_rate/`, `src/strategy/`, `src/pre_trade/`, `src/trade_engine/`, and `src/market_maker/`.
- Downstream publishers and infra live under `src/depth_pub/`, `src/kline_pub/`, `src/factor_pub/`, `src/rolling_metrics/`, `src/viz/`, `src/persist_manager/`, and `src/bridge/`.
- Shared types and utilities live under `src/common/`, `src/account/`, and `src/symbol_match.rs`.
- Repo-local configs live in `config/` and are a mix of YAML and TOML: `depth_cfg.yaml`, `kline_cfg.yaml`, `trade_flow_feature_pub.yaml`, `fusion_factor_pub.toml`, `model_pub.toml`, `persist_auto_exporter.toml`, `viz.toml`, etc.
- Important runtime detail: several binaries do **not** read `config/mkt_cfg.yaml` directly. `dat_pbs`, `trade_engine`, and the account monitors resolve market config from `$HOME/dat_pbs/config/mkt_cfg.yaml`.
- Operational scripts are split by domain: generic under `scripts/`, market-maker helpers under `mm_scripts/`, and cross-exchange arb helpers under `xarb_scripts/`.
- `docs/` contains design/ops notes; `third_party/onnxruntime/` vendors the ONNX runtime used by model-related binaries.
- `src/main.rs` still exists as a legacy bootstrap, but the maintained runtime entry for market data publishing is `src/bin/dat_pbs.rs`.

## Build, Test, and Development Commands
- Build all declared binaries: `cargo build --release`
- Build one process: `cargo build --release --bin trade_engine`
- Format: `cargo fmt --all`
- Lint: `cargo clippy --all-targets -- -D warnings`
- Run tests: `cargo test`
- Common local runs:
  - `cargo run --bin dat_pbs -- --venue binance-futures`
  - `cargo run --bin trade_engine -- --exchange gate`
  - `cargo run --bin pre_trade -- --open-venue binance-margin --hedge-venue binance-futures`
  - `cargo run --bin trade_signal -- --exchange binance`
  - `cargo run --bin depth_pub -- --venue binance-futures`
  - `cargo run --bin kline_pub -- --venue binance-futures --period-ms 60000`
  - `cargo run --bin trade_flow_feature_pub -- --venue binance-futures`
  - `cargo run --bin fusion_factor_pub -- --venue binance-futures --config config/fusion_factor_pub.toml`
  - `cargo run --bin model_pub -- btcusdt`
  - `cargo run --bin rolling_metrics -- --open_venue binance-spot --hedge_venue binance-futures`
  - `cargo run --bin ipc_bridge -- --cfg config/bridge_test_node_a.yaml`
  - `cargo run --bin persist_admin_server -- --config config/persist_auto_exporter.toml`
- PM2/deploy workflows are script-driven now, not `start_proxy.sh`/`stop_proxy.sh`. Prefer the existing `scripts/start_*.sh`, `scripts/stop_*.sh`, `scripts/deploy_*.sh`, `mm_scripts/*.sh`, and `xarb_scripts/*.sh` wrappers instead of inventing new commands.

## Coding Style & Naming Conventions
- Rust: 4-space indent, `snake_case` for functions/files/modules, `PascalCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep using `anyhow` with explicit context on fallible IO/network/config paths.
- Prefer structured logs with enough identifiers to debug venue, symbol, config path, Redis key prefix, or process role.
- Follow the existing module style: most behavior lives in library modules, while `src/bin/*.rs` stays thin and orchestration-focused.
- Many binaries infer venue/namespace from the current working directory. Preserve that behavior unless you are deliberately simplifying it and have checked the affected scripts.
- Python ops scripts in `scripts/` are part of the workflow. Keep them single-purpose, `snake_case`, and compatible with the repo’s existing invocation patterns.
- JS demo scripts at repo root are lightweight utilities; keep them small and focused.
- Avoid editing generated or disposable artifacts such as `scripts/__pycache__/`, `.pyc`, `nohup.out`, or vendored third-party binaries unless the task is explicitly about them.

## Testing Guidelines
- There is currently no top-level `tests/` directory; most coverage is inline via `#[cfg(test)]`.
- When changing parser, query parser, threshold loader, orderbook, signal throttle, or strategy normalization logic, add or update nearby unit tests in the touched module.
- Prefer focused test runs while iterating, then finish with `cargo test`.
- Good current test targets include parsing, config loading, rolling/window primitives, min-qty tables, funding-rate threshold loaders, and pre-trade/account-monitor edge cases.
- If a change mainly affects shell/Python deployment tooling or live exchange behavior, document the manual verification command you used.

## Commit & Pull Request Guidelines
- Prefer Conventional Commits: `feat:`, `fix:`, `refactor:`, `chore:`, `docs:`.
- PRs should call out which binaries, configs, scripts, and runtime env vars are affected.
- Include concrete verification steps, for example the exact `cargo run --bin ...` command, `cargo test`, or the start/stop script used.
- If behavior depends on working-directory inference or Redis key prefixes, mention that explicitly in the PR summary.

## Security & Runtime Notes
- Do not commit real API keys, secrets, borrowed credentials, Redis passwords, or production state files.
- Be careful with tracked sensitive artifacts already present in the repo root, especially `.pem` files; do not modify or redistribute them unless the task explicitly requires it.
- Exchange-facing processes rely on environment variables such as `BINANCE_API_KEY`, `BINANCE_API_SECRET`, `BINANCE_ACCOUNT_MODE`, `OKX_API_KEY`, `OKX_API_SECRET`, `OKX_PASSPHRASE`, `GATE_API_KEY`, and `GATE_API_SECRET`.
- Several services default Redis to `127.0.0.1:6379`; some also derive Redis prefixes from the current directory name. Changing cwd-sensitive behavior can break deployment layouts.
- `persist_exporter` and `persist_auto_exporter` expect absolute input/output directories in config. Preserve that validation.
- `model_pub` depends on the vendored ONNX runtime and sets runtime env policy internally; avoid replacing that with ad hoc system-level assumptions.
- `src/bin/market_maker.rs` is still a minimal stub with TODOs. Treat it as non-production unless the task is specifically to flesh it out.
