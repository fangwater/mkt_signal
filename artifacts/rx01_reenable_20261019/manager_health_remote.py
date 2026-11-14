"""Observe Manager health and the effective post-liquidation source anchor."""
import importlib.util
import json
from pathlib import Path

s = Path.home() / 'binance-cta-spread-rx01/run/reenable_20261019'
sp = importlib.util.spec_from_file_location('ops', s / 'ops_helpers.py')
ops = importlib.util.module_from_spec(sp)
sp.loader.exec_module(ops)
with ops.session() as api:
    health = api('/manager/api/health')
    dashboard = api('/manager/api/dashboard')
    (s / 'manager_dashboard_after.json').write_text(json.dumps(dashboard))
    print(json.dumps(dict(health=health, dashboard_keys=list(dashboard))))
