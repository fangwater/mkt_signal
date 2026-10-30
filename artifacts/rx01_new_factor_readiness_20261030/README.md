# RX01 15min preparation and activation

- Operator input: `operator_list.json` has the original 154 rows. `excluded_rules.json`
  preserves the eight ICX/STORJ rows removed with explicit operator approval.
- Active universe: 38 symbols, 89 calculated factor names, 145 physical factor
  pairs and 146 independent position rules. SKL/factor_170 keeps both NQ rules.
  These create 91 position-strategy names in Manager.
- `depth_validation.json`: completed BTC/ETH Kafka verification of all 23 depth
  factors, including both absolute-denominator net formulas, without IPC output.
- `signal_manifest.json` / `signal_install_receipt.json`: initial observation
  installation from source `31a7e358`; no execution processes started.
- `order_size_*`: six missing September thresholds backfilled for October;
  existing threshold values and public factor plans were unchanged.
- `live_amount_thresholds.json`: the public threshold snapshot used to build
  native event-time baseline warm-up from October 22–28 Tardis source files.
- `formal_readiness.py` checks every requested rule against one fresh closed
  timestamp. `reset_exec_state.py` is dry-run by default, verifies the exchange
  is flat including all debts and loans, then can archive/reset only retired
  RX01 Exec Redis targets and allocation ledgers. Private backups remain on host.
- `manager_restore.py` uses a temporary lgq session and guarded source/catalog/
  connection stages. New position targets start clear; only the new Perp scope
  is attached. Credentials and publication tokens never enter these artifacts.
