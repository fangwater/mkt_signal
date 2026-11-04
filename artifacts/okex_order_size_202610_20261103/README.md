# OKX monthly order-size thresholds

Current verified publication: 2026-11-03 03:09:57 UTC.

- Sample month: 2026-10; application month: 2026-11.
- Current unit: `price_x_contracts`, matching the receiving/classification pipeline's `price * native amount`. OKX quantities remain contracts; ctVal is audited but not applied.
- Eight symbols, 744 nonempty UTC hours each, 65,332,809 valid trade samples; no invalid/outside-month/late rows.
- Live evidence: nine DOGE trades matched between JP IPC and official OKX public trades. An official `sz=7.8` arrives as `amount=7.8`, despite ctVal=1000. Tardis's official mapper also retains `sz` unchanged. The initial local Kafka probe timed out. A subsequent direct JP Kafka probe matched 54 trades across all eight symbols and confirmed the same raw-contract units; see [kafka_reprobe/verification.json](kafka_reprobe/verification.json).
- Target: jp-meta-elvpn, `/home/ubuntu/tlen_config_shared`, Redis DB 0, `okex-futures:amount-thresholds`.
- Current receipt: [native_alignment/verification.json](native_alignment/verification.json). Redis and TLen HTTP readback match the exact new CSV.
- Tool tests: 8 passed; publication tests: 10 passed, zero skips, isolated temporary Redis; Period tests: 17 passed.

## Correction of the initial publication

The 02:37:30 UTC publication incorrectly multiplied only historical statistics by ctVal, while the live consumer kept raw contracts. Its root-level `calculation/`, `verification.json`, `source_contract.json`, and before/after receipts are retained as historical evidence and are superseded by `native_alignment/`.

At 03:09:39 UTC, guarded correction verified every managed hash still matched our own initial publication, then atomically updated five affected symbols (ADA, DOGE, ETC, NEAR, TRB). DOT, ATOM and CRV did not change. No additions/deletions or factor-plan changes occurred. The publisher script was checksum verified and atomically installed. No trading/strategy processes were restarted.

- Current CSV SHA-256: `e81dfd6e934d5e8a21d01b214a394a0fafdf061b4cfb9fa29430544ab91a3832`.
- Initial superseded CSV SHA-256: `75368ac8ff7a8944aa7bd6d96c957f4e6dabb693accb57be87d0321490064841`.
- Current exact backup: `native_alignment/redis_before.json`.
- Initial exact backup: `redis_before.json`.
- Instrument snapshot: 2026-11-03 02:20:55 UTC; raw API response and mapper evidence retained.

## Current thresholds

Values below are price times contracts, not true USDT notionals.

| Symbol | P50 | P90 |
|---|---:|---:|
| TRBUSDT | 166.88 | 2138 |
| DOTUSDT | 4.6668 | 170.952 |
| ADAUSDT | 0.14622 | 7.53786 |
| NEARUSDT | 1.0232 | 91.609 |
| ATOMUSDT | 11.544 | 262.786 |
| DOGEUSDT | 0.0052578 | 0.8590416 |
| ETCUSDT | 0.5298 | 14.5863 |
| CRVUSDT | 2.3954 | 200.3904 |

Remote current audit files: `/home/ubuntu/tlen_config_shared/run/okex_order_size_202610_20261103/native_alignment/`.
Do not restore over later operator changes without reconciling current hashes.
