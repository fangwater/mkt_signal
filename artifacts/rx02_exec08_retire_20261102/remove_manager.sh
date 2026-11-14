#!/usr/bin/env bash
set -euo pipefail
umask 077
audit="$HOME/crypto_cta_manager/maintenance-backups/rx02_exec08_retire_20261102"
config="$HOME/crypto_cta_manager/config/cta-manager.toml"
service=crypto-cta-manager-web.service
cmp -s "$config" "$audit/cta-manager.toml.before"
systemctl --user is-active --quiet "$service"
committed=0
recover() {
  rc=$?
  if [[ $committed -eq 0 ]]; then
    cp -p "$audit/cta-manager.toml.before" "$config.recovery"
    mv -f "$config.recovery" "$config"
  fi
  systemctl --user start "$service" || true
  exit "$rc"
}
trap recover EXIT
systemctl --user stop "$service"
cmp -s "$config" "$audit/cta-manager.toml.before"
cp -p "$audit/cta-manager.toml.after" "$config.retire-next"
chmod --reference="$config" "$config.retire-next"
mv -f "$config.retire-next" "$config"
set -a
source "$HOME/.config/crypto-cta-manager/database.env"
set +a
psql "$CRYPTO_CTA_LOCAL_DATABASE_URL" -X -q -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
DO $$
DECLARE n integer;
BEGIN
  IF (SELECT count(*) FROM cta_account_follows WHERE source_id='binance_exec_trade08') <> 1
     OR NOT EXISTS (SELECT 1 FROM cta_account_follows WHERE source_id='binance_exec_trade08' AND virtual_id='virtual02' AND multiplier=1)
     OR (SELECT count(*) FROM cta_account_strategy_bindings WHERE source_id='binance_exec_trade08') <> 1
     OR NOT EXISTS (SELECT 1 FROM cta_account_strategy_bindings WHERE source_id='binance_exec_trade08' AND binding_name='lgq_cta_symbol_mean' AND position_strategy_name='lgq_cta_symbol_mean' AND order_strategy_name='lgq_grid_prep' AND shares=1)
     OR EXISTS (SELECT 1 FROM cta_follow_publish_queue WHERE source_id='binance_exec_trade08')
     OR NOT EXISTS (SELECT 1 FROM cta_position_strategies WHERE strategy_name='lgq_cta_symbol_mean' AND (SELECT count(*) FROM jsonb_each(targets))=23 AND NOT EXISTS (SELECT 1 FROM jsonb_each(targets) AS t WHERE t.value <> '{"qty":"clear","signal":0}'::jsonb))
  THEN RAISE EXCEPTION 'Exec08 mapping or target state changed; aborting'; END IF;
  DELETE FROM cta_account_follows WHERE source_id='binance_exec_trade08' AND virtual_id='virtual02';
  GET DIAGNOSTICS n = ROW_COUNT;
  IF n <> 1 THEN RAISE EXCEPTION 'unexpected follow deletion count'; END IF;
  DELETE FROM cta_account_strategy_bindings WHERE source_id='binance_exec_trade08' AND binding_name='lgq_cta_symbol_mean';
  GET DIAGNOSTICS n = ROW_COUNT;
  IF n <> 1 THEN RAISE EXCEPTION 'unexpected binding deletion count'; END IF;
END $$;
COMMIT;
SQL
committed=1
systemctl --user start "$service"
trap - EXIT
echo 'Manager restarted with Exec08 disabled; its follow and binding were removed.'
