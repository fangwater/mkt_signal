# RX01 Spread factor warm-up and state reset

User chose factor warm-up, backup/reset of the old internal state, and starting
flat from the current point. Trading publication remains disabled and Exec06
remains stopped. No historical positions were reconstructed or Manager/Exec
targets submitted during this warm-up.

The initial reset was 57 strategies / 873 combinations. The user then requested
removal of ICXUSDT and STORJUSDT, removing 79 combinations. Current configuration
is 48 symbols / 57 signed strategies / 56 underlying factors / 794 combinations.
The source DS research file is unchanged; the deployment copy, publisher config,
file table and Redis table reflect the removal. All 794 internal positions remain
zero. The publisher retained its initial in-memory universe to preserve warm-up;
the signal uses only the current table, and the saved 48-symbol publisher universe
will apply at its next restart.

`final.json` verifies that Kafka replay caught up, all 48 configured symbols have
history, and 762 factor combinations are fresh and ready. There are 21 intentional
blanks due to missing size thresholds and 11 additional unready factor_114
combinations. At the recorded snapshot, 693 combinations also had both entry
filters ready; this changes with incoming data. No decode, evaluation or publish
errors occurred. Missing thresholds were not generated or written to shared TLen.

`final_config.json` records the operator's live exit settings, long=0.7 and
short=0.3, observed after the removal. At 07:26–07:30 UTC the user requested a
Spread-only default correction. Rust and config-server defaults, the local
template, deployment copy and UI now use 0.7/0.3. The original DS research file
retains 0.3/0.7. RX01 signal and config server were reloaded; the publisher and
Kafka generation were retained. Runtime config bytes and 794 zero positions
were preserved, with publication disabled and Exec06 stopped. Perp defaults,
runtime configs and processes were not changed by this correction.

Validation: 43 targeted Spread tests passed before release publication; the full
library regression passed 1298 tests, with 0 failures and 3 ignored. Config
derivation, API preview, file/Redis equality, shell syntax and diff checks passed.
The public configuration URL returned HTTP 200:
http://13.115.227.29:4191/cta/binance-cta-spread-rx01/config/

Evidence:

- `receipt.json`: checksum-verified initial publication and reset.
- `remove_symbols/receipt.json`: configuration removal and unchanged generation.
- `remove_symbols/validation.json`: matching file, Redis and import preview.
- `remove_symbols/stop_scripts/manifest.json`: published 45-second cleanup fix.
- `before/audit.json`, `after_audit.json`: unchanged protected processes/account files.
- `final.json`, `final_warmup.json`, `final_config.json`: observed runtime state.
- `library_tests.log`: library regression result.
- `spread_exit_defaults/validation.json`: default correction, 43 Rust/18 Python
  tests, API preview, unchanged account files and 94 protected processes.
- `spread_exit_defaults/receipt.json`: checksum-verified default publication.
- `spread_exit_defaults/ui_copy/receipt.json`: corrected dual-filter UI text,
  verified over HTTP without a further process restart.

Remote backups and receipts:

- `~/binance-cta-spread-rx01/run/corr50_warmup_20261019T064500Z/`
- `~/binance-cta-spread-rx01/run/corr50_remove_symbols_20261019T065917Z/`
- `~/binance-cta-spread-rx01/run/spread_exit_defaults_20261019T072100Z/`
