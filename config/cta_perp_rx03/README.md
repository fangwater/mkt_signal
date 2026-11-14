# RX03 CTA Perp

JP signal environment: `~/binance-cta-perp-rx03`.
Install these files under the environment's `config/` using their existing names.
The profile contains 16 symbols, 26 factors and 45 signed symbol/factor pairs.
The provisioning default is 100 USDT per pair, with publication disabled.
The live amount was 300 USDT on 2026-10-28 and was preserved during the update.

Entry and exit thresholds match the RX02 live configuration observed on
2026-10-14: factor entry 0.9/0.1, NQ filter 0.5/0.5, factor exit 0.7/0.3.
The amount is explicitly 100 USDT, independently of subsequent RX02 changes.

Set both publisher and signal to
`CTA_PERP_MODEL_SCOPE=binance_cta_perp_rx03`, and set
`CTA_PERP_FACTOR_PROCESS_NAME=cta_perp_factor_binance_cta_perp_rx03`.
The scope separates model IPC services while retaining shared market BBO.
Use the environment's start/stop scripts; the publisher owns CPU19.
Redis uses `binance-cta-perp-rx03:cta_symbol_factors`.

Dashboard and config bind to loopback ports 10197 and 19187 and are exposed
through the existing 4191 gateway at `/cta/binance-cta-perp-rx03/`,
`signals/` and `config/`. No Manager/Exec binding is created by this profile.

Live activation verified 2026-10-14 08:05 UTC: the deployed environment publishes to
Manager virtual04 (`cta_perp_rx03`), followed by `binance_exec_trade07` at
multiplier 1 with five bindings at one share each. That activation enabled
publication; the 2026-10-28 pause described below supersedes it. Exec Viz/Config use
loopback ports 10047/18167 through `/exec_trade07/` on the 4191 gateway.
The independent `binance_cta_perp_rx03_ctaker` template and account order limits
match RX02 at activation. Maker/taker/theoretical rates are
-0.00002 / 0.000175 / 0.0004. The amount remains 100 USDT per pair.
See `artifacts/cta_perp_rx03_activation/` for checks and release hashes.

## Symbol × factor NQ rules

`factor_rules.json` is the operator's 45-row paste example (16 USDT symbols).
Paste the whole array into Config Server's 币种与因子 JSON editor and save.
The server accepts `标的`, `nq`, `符号`, `因子`, appends USDT to bare assets,
and checks duplicate pairs, factor availability, signs and finite [0,1] ranks.
The local publisher universe and bootstrap factor table now cover that list.

The canonical runtime rules live in `cta_perp.json.factor_rules`. In this mode
that one atomic file save owns both factor direction and NQ; signal subscriptions,
recovery and replay derive the combination from it instead of Redis weights.
The example and bootstrap JSON files are not additional runtime rule stores.
Saving requires the current config revision; invalid/stale arrays change nothing.

`0.50` means long NQ > 0.50 and short NQ < 0.50. `0.2/0.8` means
long NQ > 0.2 and short NQ < 0.8. `关闭` skips NQ readiness and filtering
for that pair only. Apply factor sign before selecting the final direction's gate.
NQ never forces an exit or resizes an existing position. Unlisted pairs are absent
from the configured combination. Global position entry/exit controls remain forms;
existing symbol threshold overrides still apply before the pair's NQ override.
Rules require 1m v0 independent factors; reject 5m and v1 instead of merging
incompatible pair gates. An empty/missing `factor_rules` retains the existing
Redis symbol-factor configuration and its unchanged inclusive global NQ rules.

On 2026-10-28 the operator requested deployment while keeping RX03 trading paused.
Stopped the RX03 signal and Exec07 `exec-pre-trade`, set live `enabled=false`,
and cancelled the ten remaining RapidX Binance USDT perpetual orders through the
account's explicit trading binding `172.31.35.228`. Verified zero ordinary and
algorithmic orders; no flatten orders were submitted. Existing positions remain.
Config Server was restarted with the JSON editor and all 45 rules. Preserved the
live global amount 300, factor entry 0.9/0.1 and exits 0.3/0.7. Installed the new
signal binary while leaving it stopped. The factor publisher was restarted on
CPU19 with the 16-symbol/26-factor universe and three-day Kafka warm-up.
Restarted the read-only dashboard to load the updated parser/replay; while the
signal is stopped, its symbol collection and strategy choices use the canonical
rules instead of stale signal status. The gateway panel verifies 16 active
symbols, 45 pairs and `trading_enabled=false`.
At 10:15 UTC Kafka was caught up and every symbol had fresh output and ready NQ.
43/45 configured factor pairs were ready. ALGOUSDT and ZRXUSDT `buy_avg_price`
still require further valid warm-up samples; readiness checks remain active.

The reviewed OHLCV/depth-only factor set does not require trade-size bucket
metadata. Added the seven newly selected formulas to the existing reviewed set;
the regression compares every RX03 formula with and without changed size buckets.
No shared metadata, Redis weights, Manager strategy bindings or network settings
were changed. Backups and verification are under the live environment's
`run/nq_backup_20261028T094509Z`; local evidence is
`artifacts/rx03_nq_20261028/`.

On 2026-10-29 at 02:21 UTC the operator requested resuming trading. The live
45-rule configuration was fully ready. Preserved the operator's current
200 USDT per pair and `lgq_grid_prep` execution template, shares 1 and
virtual04 follower multiplier 1. Added the 23 missing factor strategies to
Manager, retaining the two retired factor strategies under their original
names for explicit clears (28 bindings total). Confirmed all 28 retired
symbol/factor pairs reached Exec runtime as `qty:"clear"` before starting
Exec07 pre-trade. BTC/ETH/SOL/ADA/UNI/XLM positions outside the new universe
received zero targets; execution still obeys venue minimums. Signal and
pre-trade were started through environment-local scripts. Publisher, existing
Exec services and network settings were preserved. Audit and backups live in
`run/resume_20261029/` and local `artifacts/rx03_resume_20261029/`.
The 02:25 UTC observation had ADA flat, BTC/SOL/UNI still working Grid close
orders, and ETH/XLM residuals blocked by venue minimums. Explicit clear delivery
is not proof that every factual position has already reached zero.
