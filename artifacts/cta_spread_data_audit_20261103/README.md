# Public OKX data and estimated Funding

This cross01 aggregate uses estimated funding for both markets. Both factual
collectors remain disabled with zero records. Its actual-income controls were
removed; other accounts were preserved. No execution process was started.

`official_okx_samples.json` records official public HTTPS payload validation
using the production field rules: ten closed 1min trade candles, three settled
funding rates, and one pre-settlement closed mark minute. No private API was
called. `deployed_source_check.json` verifies these parsers match deployed code.
This is a payload/contract check, not a claim that all historical prices were
loaded into the running Manager cache.

`browser-verification.json` confirms public estimates and UI policy; `manifest.json`,
`publish.json` and `final-state.json` identify the final frontend-only replacement.
