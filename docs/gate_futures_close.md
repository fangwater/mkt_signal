# Gate Futures Close Script

Script: `scripts/gate_futures_close.py`

Purpose: place a Gate.io USDT futures order to close (reduce) a position via REST.
It converts base-qty into contracts using `quanto_multiplier`.

## Requirements

- Env vars: `GATE_API_KEY`, `GATE_API_SECRET`
- Python deps: `requests`

## Default behavior

- Dry-run (no order sent) unless `--execute` is passed.
- Market order by default (`price=0`, `tif=ioc`).
- `reduce_only=true` by default.
- Uses `account=unified`.

## Usage

Close full position (auto-detect side from current position):
```bash
python scripts/gate_futures_close.py --contract ALCH_USDT --close-all --execute
```

Close by base quantity (base asset size, e.g. ALCH amount):
```bash
python scripts/gate_futures_close.py --contract ALCH_USDT --base-qty 123.45 --side buy --execute
```

Close by contracts:
```bash
python scripts/gate_futures_close.py --contract ALCH_USDT --contracts 1000 --side buy --execute
```

Dry-run only (prints payload and conversion):
```bash
python scripts/gate_futures_close.py --contract ALCH_USDT --close-all
```

## Notes

- Gate futures `size` is in contracts, not base units. The script uses
  `quanto_multiplier` to convert base-qty into contracts.
- If you pass `--base-qty`, `--side` is required (buy/sell).
- If `--close-all` is used, the script queries current position to infer side.
- You can override `--tif` and `--price` for limit orders.
