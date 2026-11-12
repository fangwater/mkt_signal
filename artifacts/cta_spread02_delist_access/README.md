# Spread02 Delist configuration access

Delist already discovered all four Spread02 combinations, but their configuration
links used the Manager gateway and required a separate Manager login. The Delist
page now uses `/delist/cta/config/` and adds a direct “Spread02 配置” button.
The unified, per-combination and per-strategy links retain worker/frequency selection.

The new gateway uses Delist's current login through `/auth/verify-login` for both
HTML and API requests. Non-GET/HEAD writes require an explicit same-origin header;
the audit actor comes from the verifier and overwrites browser input. The internal
verification location is inaccessible externally. The existing Manager route and
its complete account permission scope are byte-for-byte preserved.

Only the live Delist HTML and Nginx snippet were atomically installed. Nginx syntax
checks passed before/after installation and Nginx reloaded gracefully. Its master
PID, all 178 sampled application processes and 29 calculation configuration files
were preserved. Delist was not restarted, preserving current sessions; Spread02
remains disabled in all four frequencies.

Validation separates the authentication and UI checks:

- Real isolated Nginx with disposable auth/editor upstreams verifies Delist-only
  access, separation from Manager login, missing/cross-origin write rejection,
  verified actor propagation, and hidden internal verifier.
- Chromium opens the updated Delist page using actual catalog data and reads the
  real configuration backend through a private SSH tunnel. The direct button and
  all four frequency tabs load 48/79/64/128 rules, 100 USDT and `enabled=false`,
  with no JavaScript errors or write requests. This does not claim a production
  Delist session login test; no user's password or session was read or changed.
- Installed production HTML/API reject anonymous requests with 401; the internal
  verifier returns 404. Production gateway/page hashes and runtime preservation
  are recorded in `verified.json`.

Private rollback copies are under
`~/delist_risk_server/run/spread02_config_access_20261112T064604Z/before/`.
