# FR DingTalk and exposure monitoring

Verified 2026-11-04 10:04 UTC. Production source `b77e5da1a0701e85ab8cce1e001753d1657325f0` on synchronized `arbmm`.
Feature commits: `83387815` (DingTalk), `160be27b` (construction and FR source scope),
`ece09be0` / `5d6a944d` (bounded explicit cash-tail exclusions).

- Independent FR robot credentials remain private in JP
  `~/delist_risk_server/config/fr_dingtalk.env`, owner-only mode 0600.
  HTTP success and DingTalk `errcode=0` were verified with real deliveries.
- Each actual Luna processing attempt sends its outcome, including no-action and
  failures; unchanged-article cache reuse is quiet. The existing 24-hour scan sends
  the summary heartbeat. No additional daily job was created.
- UniMMR, uncovered exposure and data/provider issues send the first alarm and
  confirmed recovery only. Durable outbox/state survives ordinary restarts;
  unknown data cannot recover an issue. CTA/retired FR source history is excluded.
- Normal construction is shown as raw position difference with separate plan
  coverage; only uncovered differences alert. A plan is not proof of order progress.
- Explicit exchange/asset rules ignore the approved Gate and Bybit spot leftovers
  only within quantity caps, with a confirmed cash open leg, no futures quantity,
  and at most 1 USDT when priced. Missing prices remain null and balances visible.
  Larger or unlisted balances and any futures leg remain monitored.

At verification: 11 fresh FR accounts, all within monitoring thresholds; zero
incomplete accounts; 13 ignored tail rows (Bybit01 2, Gate01 2, Gate02 5, Gate04 4).
Four obsolete tail-price issues were retired as a policy change, with no recovery
messages. Notification state: 28 acknowledged, 0 queued, no error, 0 active issues;
actual `luna_ok` and `heartbeat` journal entries were present. No heartbeat reset
or extra setup test was sent during the tail-policy release. These are observations
at verification time; positions and balances continue to change.

The release was built locally in the active workspace from 1,784 verified committed
source files, copied immediately to a dedicated publish directory, uploaded to
explicit temporary paths, SHA-256 checked and atomically installed. The deployed
wrapper intentionally retains its existing argument defaults rather than replacing
operator/runtime differences with the repository copy. Only environment-local stop
and start scripts were used. No positions, orders, trading configuration or source
IP assignments were changed. Four private configuration hashes and existing
arguments were preserved. All 92 protected processes retained PID/affinity.
Delist PID `2922632` and SG tunnel PID `2922607` use CPU `0-5`.
Loopback/gateway exposures and notifications endpoints reject unauthenticated reads
with 401. Existing ports and tunnels remain unchanged.

Validation: `cargo fmt --check`, 56 Delist tests, local release build, authenticated
synthetic browser checks for construction/alerts/recovery/failed or stale data,
automatic refresh and desktop/mobile rendering, plus deployed HTML/live-report
replay for tail counts and retained details. Private evidence/screenshots are under
`data/ops/fr_dingtalk_20261104/`. Runtime backups:
`~/delist_risk_server/run/fr_dingtalk_20261104_83387815/`,
`~/delist_risk_server/run/fr_dingtalk_20261104_160be27b/`, and
`~/delist_risk_server/run/fr_spot_tails_20261104/`.

Final binary SHA-256: `a2a490e1f810706b6af7b08f5a05609f4607f24d3b8ec20c097bdb082684df06`.
HTML: `284b31d89105333d7a24d09d2c92c05cd9bd0844d29bf9c1e18708a0c7132338`.
Preserved wrapper: `5ab355814814da5014ee8b0c7cd4be43b0373c27ca5a128250909de27e5a0325`.
Tail policy: `2c63c5878930f39995f7a29847000e8cdebcaf620a36235e621a0ac3119afa5e`.
