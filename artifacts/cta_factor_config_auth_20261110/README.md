# CTA account configuration gateway authentication

Published `cta_factor_config_auth_20261110T043440Z` on JP from the synchronized
local `arbmm` worktree. Only `/etc/nginx/snippets/cta_factor_config.conf` changed;
the existing Nginx was gracefully reloaded. Its master PID remains `1862269`.

The account workspace and shared Prep/Spread editor now use the same Manager
session. The additional Delist login check caused a Manager-authenticated
account button to open HTTP 401. The gateway now calls Manager's existing
verifier directly, still requiring configure permission for every source in
`binance_exec_rx01,binance_exec_rx03,okex_exec_trade01,binance_exec_spread01`.
The shared catalog can affect all four sources, so its permission scope remains
complete. Anonymous requests remain 401 and unauthorized requests remain 403.

Non-GET/HEAD requests require an explicit matching Origin. The configuration
server's own cross-origin checks remain. The gateway overwrites client actor
and source-IP headers. The existing Manager verifier returns status only, so
the journal uses a truthful `manager_session` auth marker with its existing
session fingerprint; the marker is not a resolved username or human identity.

Validation:

- Candidate checksum and original checksum checked before the atomic install.
- Isolated and installed Nginx configurations both passed `nginx -t`.
- Renderer rejected malformed/duplicate account scopes.
- Real Chromium used only a temporary Manager session, with no Delist cookie.
  Account card and exact strategy links loaded all four tabs with enabled
  28/56/18/107 rules. All configuration API reads returned 200.
- Both execution-market selections, mobile account button, existing Prep
  navigation and the Prep catalog API passed.
- Anonymous editor access returned 401, unprivileged access returned 403,
  missing/cross-origin POST and cross-origin DELETE returned 403. An incomplete
  same-origin preview returned 422 before edit handling. No save was attempted.
- The exact public gateway returned authenticated HTML/API 200 and the same
  four rule counts; see `public-gateway.json`.
- 97 protected process identities and CPU affinities and 35 configuration/
  binary hashes remained unchanged. Manager, factor and trading processes
  were not restarted. No strategy parameters or order/position state changed.

`manifest.json`, `verification.json` and `live-browser.json` contain selected
nonsecret receipts. Temporary sessions were revoked after verification and
tokens existed only in process memory. No screenshots or browser profiles were
retained. The exact upload staging directory was removed after verification.

Private rollback metadata, original/candidate snippets and before-state hashes
remain in
`~/crypto_cta_manager/maintenance-backups/cta_factor_config_auth_20261110T043440Z/`.
Rollback restores only `cta_factor_config.conf.before`, runs `nginx -t` and
gracefully reloads Nginx. It requires no trading or Manager restart.
