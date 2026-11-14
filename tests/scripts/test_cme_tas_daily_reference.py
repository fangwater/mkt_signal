import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("daily", ROOT / "scripts/cme_tas_daily_reference.py")
daily = importlib.util.module_from_spec(spec)
spec.loader.exec_module(daily)


def test_exchange_midnight_and_half_day_normalization():
    assert daily.event_time(daily.DAY + 10, daily.DAY - 10) == daily.DAY - 10
    assert daily.event_time(daily.DAY - 10, 10) == daily.DAY + 10
    assert daily.event_time(123, None) == 123


def test_fixed_daily_fixture():
    import json
    path = ROOT / "crates/cme_tas_replay/tests/fixtures/tas_daily_inventory.json"
    fixture = json.loads(path.read_text())
    assert daily.aggregate(fixture["events"]) == fixture["expected"]
