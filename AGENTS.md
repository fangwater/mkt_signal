# Repository Guidelines

## Project Structure & Module Organization
- `src/`: Rust crate (binary) with modules like `app.rs` (orchestration), `cfg.rs` (YAML config), `connection/` (WS clients), `parser/` (exchange parsers), `forwarder.rs`, `receiver.rs`, `restart_checker.rs`, `sub_msg.rs`.
- Config: `mkt_cfg.yaml` (default at runtime), `mkt_cfg_dev.yaml` (dev variant to copy/merge as needed).
- Demos: `demo_*.js` Node scripts for quick WS checks; `package.json` is minimal.
- Ops: `start_proxy.sh` / `stop_proxy.sh` (PM2 wrappers), `deploy_*.sh`.
- Build outputs: `target/` (Rust), optional `./crypto_proxy` symlink/binary for scripts.

## Build, Test, and Development Commands
- Build: `cargo build --release` → `target/release/crypto_proxy`.
- Run (local): `cargo run -- --exchange binance` (also: `binance-futures`, `okex`, `okex-swap`, `bybit`, `bybit-spot`). Uses `mkt_cfg.yaml`.
- PM2 (after building): `ln -sf target/release/crypto_proxy ./crypto_proxy` then `./start_proxy.sh binance`; stop via `./stop_proxy.sh binance`; logs: `pm2 logs crypto_proxy_binance`.
- Demos: `node demo_okex.js`, `node demo_bybit.js`, etc.

## Coding Style & Naming Conventions
- Rust: run `cargo fmt --all` and `cargo clippy -- -D warnings` before pushing. 4‑space indent; snake_case files/functions; PascalCase types; SCREAMING_SNAKE_CASE consts. Prefer explicit errors with `anyhow` context and structured logs.
- JS demos: 2‑space indent, camelCase, single‑purpose scripts under repo root.

## Testing Guidelines
- Framework: Rust built‑in tests. Run `cargo test`.
- Placement: inline `#[cfg(test)] mod tests` near the code, or integration tests under `tests/` (e.g., `tests/parser_okex.rs`).
- Targets: message parsers, reconnection/heartbeat logic, and config parsing. Name tests clearly (e.g., `fn parser_handles_pong()`).

## Commit & Pull Request Guidelines
- Commits in history are terse; prefer Conventional Commits: `feat:`, `fix:`, `refactor:`, `chore:`, `docs:` with imperative mood.
- PRs: include summary, reasoning, and run/verify steps (e.g., commands used, sample `pm2 logs` output). Link issues; attach screenshots/log snippets for behavior changes.

## Security & Configuration Tips
- Do not commit secrets/keys. Keep tokens out of YAML; the binary reads `mkt_cfg.yaml` by default.
- Validate exchange symbols and handle network errors without panics; favor retries and clear logs.
