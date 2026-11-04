# Direct OKX Kafka verification

Verified 2026-11-03 03:31–03:34 UTC from jp-meta-elvpn. These are actual Kafka reads, independent of the earlier IPC check.

- Broker: `156.231.137.189:1501`; topic: `okex-swap`; partition: `0`; internal venue: `okex-futures`.
- Metadata advertises `10.140.140.55:1501`. Both clients use the existing host rewrite to the reachable bootstrap address.
- Independent Python consumer read 22 records; the locally built, SHA-256-verified repository Rust consumer independently read offsets and 8 records on JP.
- Payloads decode as zlib-compressed `PeriodMessage` protobuf. Each record contains 220 symbol entries with trades and book increments.
- Public OKX capture returned 64 trades over eight symbols. After Kafka duplicate-payload removal and exclusion of ambiguous candidates, 54 trades matched by symbol/side, price and timestamp (within 2 ms). Every matched Kafka `amount` equals official raw `sz`.
- Example DOGE: official `sz=28.99`, Kafka `amount=28.99`, ctVal=1000; no multiplier was applied.
- Eight ambiguous native matches and two unmatched trades are excluded; this is a quantity-unit check, not a completeness audit.
- The same Rust artifact/config works on JP while local reads time out. Local TCP and Metadata v0 can succeed, but API negotiation / offset reads can time out. The exact local network fault was not isolated; Kafka on JP is readable.
- Ten of the 22 records contain duplicate payloads, leaving 12 unique payloads. This observation is retained without changing producer or factor behavior.
- Diagnostic reads used temporary files and separate/no consumer groups with auto commits disabled. No production service was started/stopped/restarted and no thresholds, strategy settings, or networking settings changed.

Current summary: [verification.json](verification.json). Exact matched values: [kafka_native_comparison.json](kafka_native_comparison.json).
Raw Kafka payloads and client logs: `jp_live/`; official trades: `native_trades.json`; Rust consumer output: `jp_helper/`.
