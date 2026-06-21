# Binance Futures BBO Parser Benchmark

Date: 2026-06-21

## Scope

This note records the validation run for the Binance futures `bookTicker` raw JSON parser used by
`spread_pbs`.

Two kinds of checks were run:

- Live IPC correctness and relative arrival comparison between production `spread_pbs` and a test
  `spread_pbs --test` instance.
- Local parser-only CPU benchmark using the same in-process input bytes for the old JSON tree path
  and the new raw parser path.

## Live Setup

Production:

- Binary: `/home/ubuntu/spread_pbs/binance-futures/spread_pbs`
- Args: `--venue binance-futures --core 9`
- IPC roots: `spread_pbs`, `dat_pbs`

Test:

- Binary: `/home/ubuntu/crypto_mkt/mkt_signal/target/release/spread_pbs`
- Args: `--venue binance-futures --core 17 --test`
- IPC roots: `spread_pbs_test`, `dat_pbs_test`

The test process was started from `/home/ubuntu/spread_pbs/binance-futures` with the same venue env
file and pinned to CPU 17. Binance futures BBO source race was enabled: primary whitelist
`fstream-mm.binance.com` plus secondary normal `fstream.binance.com`.

## Live Correctness

Tool: `spread_pbs_compare`

Result:

- Full BBO/bookTicker payload comparison: `mismatched=0`, `decode_errors=0`
- DOGEUSDT `trade`, `incremental`, and `derivatives`: `mismatched=0`, `decode_errors=0`,
  `pending_evicted=0`

BBO comparison uses exact payload matching within the same event key, rather than FIFO matching by
timestamp only. Binance can emit multiple `bookTicker` updates with the same symbol and exchange
timestamp, so timestamp-only FIFO pairing can produce false mismatches.

## Live Arrival Comparison

For full BBO/bookTicker live IPC output:

- Approximately 1.7 million matched messages sampled.
- Test side arrived first in about 74.1% overall.
- After the initial warmup/high-win windows, steady-state windows were about 67.1% test-first.
- All sampled windows remained `mismatched=0` and `decode_errors=0`.

This live comparison includes independent WebSocket connections, source race behavior, reconnects,
restart phase offsets, publisher/IPC behavior, and subscriber scheduling. It is useful for end-to-end
visibility, but it is not a pure parser CPU benchmark.

## Parser-Only Benchmark

Tool: `binance_bbo_parser_bench`

Compared paths:

- Old path: `serde_json::from_slice::<serde_json::Value>` + `parse_bbo_json`
- New path: `parse_book_ticker_bbo_raw_borrowed`

Timing is done outside each measured loop, not per message.

Run 1:

```text
taskset -c 0 target/release/binance_bbo_parser_bench --iters 5000000 --warmup-iters 500000 --rounds 8
summary old_ns=1141.79 new_ns=186.58 speedup=6.12x old_total_ms=45671.541 new_total_ms=7463.383
```

Run 2:

```text
taskset -c 16 target/release/binance_bbo_parser_bench --iters 5000000 --warmup-iters 500000 --rounds 4
summary old_ns=1115.16 new_ns=182.53 speedup=6.11x old_total_ms=22303.234 new_total_ms=3650.523
```

The checksum matched between old and new paths in all rounds.

## Conclusion

The new Binance futures `bookTicker` raw parser is correct for the sampled live data and is about
6.1x faster than the old JSON tree path in the local parser-only benchmark.

The live IPC arrival win rate is lower than the parser-only speedup because that test includes
network, WebSocket, source-race, restart, IPC, and subscriber scheduling effects.
