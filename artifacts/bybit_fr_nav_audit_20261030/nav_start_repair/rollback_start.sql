\set ON_ERROR_STOP on
BEGIN;
DELETE FROM strategy_initial_snapshots WHERE strategy_slug='bybit_fr_arb01' AND snapshot_ts_ms=1792752319622;
DELETE FROM strategy_snapshots WHERE strategy_slug='bybit_fr_arb01' AND snapshot_ts_ms=1792752319622;
UPDATE strategy_envs SET st_ms=1792749809591,updated_at=CURRENT_TIMESTAMP WHERE slug='bybit_fr_arb01' AND st_ms=1792752319622;
COMMIT;
