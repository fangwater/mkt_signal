# RX03 CTA Perp

JP signal environment: `~/binance-cta-perp-rx03`.
Install these files under the environment's `config/` using their existing names.
The profile contains 14 symbols, 5 factors and 34 signed symbol/factor pairs.
Each pair uses 100 USDT; position publication starts disabled.

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
multiplier 1 with five bindings at one share each. Runtime publication is enabled;
this provisioning profile retains its disabled default. Exec Viz/Config use
loopback ports 10047/18167 through `/exec_trade07/` on the 4191 gateway.
The independent `binance_cta_perp_rx03_ctaker` template and account order limits
match RX02 at activation. Maker/taker/theoretical rates are
-0.00002 / 0.000175 / 0.0004. The amount remains 100 USDT per pair.
See `artifacts/cta_perp_rx03_activation/` for checks and release hashes.
