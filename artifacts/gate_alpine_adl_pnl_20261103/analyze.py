#!/usr/bin/env python3
"""Reproduce Gate ALPINE attribution from saved, read-only exchange evidence."""
import json
from decimal import Decimal as D
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def read(name):
    return [json.loads(line) for line in (ROOT / name).read_text().splitlines()]


def find(rows, path):
    matches = [r for r in rows if r['path'] == path]
    assert len(matches) == 1 and matches[0]['status'] == 200
    return matches[0]


initial = read('exchange_initial.jsonl')
followup = read('exchange_followup.jsonl')
valuation = read('exchange_valuation.jsonl')
prices = read('exchange_event_prices.jsonl')
trades = find(initial, '/futures/usdt/my_trades')['data']
adl = sorted([t for t in trades if t['text'] == 'auto_deleveraging'], key=lambda t: t['create_time'])
assert len(adl) == 4 and len(trades) < 1000
start = D(str(adl[0]['create_time']))
after = [t for t in trades if D(str(t['create_time'])) >= start]
repairs = [t for t in after if t not in adl]
assert len(repairs) == 77 and all(t['text'].startswith('t-neut-') and D(t['size']) < 0 for t in repairs)
timerange = find(followup, '/futures/usdt/my_trades_timerange')['data']
assert {str(t['id']) for t in after} == {str(t['trade_id']) for t in timerange}
adl_qty = sum(D(t['size']) for t in adl)
repair_qty = -sum(D(t['size']) for t in repairs)
assert adl_qty == 8506 and repair_qty == 8500
adl_notional = sum(D(t['size']) * D(t['price']) for t in adl)
repair_notional = -sum(D(t['size']) * D(t['price']) for t in repairs)
repair_fees = sum(D(t['fee']) for t in repairs)
book = find(followup, '/futures/usdt/account_book')['data']
assert len(book) < 1000
fund = sum(D(t['change']) for t in book if t['type'] == 'fund')
assert repair_fees == -sum(D(t['change']) for t in book if t['type'] == 'fee')
assert {str(t['id']) for t in adl} == {t['trade_id'] for t in book if t['type'] == 'pnl'}
spot_trades = find(initial, '/spot/my_trades')['data']
assert len(spot_trades) < 1000 and all(D(t['create_time_ms']) / 1000 < start for t in spot_trades)
spot_prices = find(prices, '/spot/trades')['data']
assert len(spot_prices) < 1000
before_adl = max((t for t in spot_prices if D(t['create_time_ms']) / 1000 <= start), key=lambda t: D(t['create_time_ms']))
event_spot = D(before_adl['price'])
latest_spot = find(valuation, '/spot/tickers')
spot_now = D(latest_spot['data'][0]['last'])
position = find(valuation, '/futures/usdt/positions/ALPINE_USDT')['data']
mark_now = D(position['mark_price'])
assert D(position['size']) == -11076
retained_short = -D(find(initial, '/futures/usdt/auto_deleverages')['data'][0]['position_size'])
assert retained_short == 2576
core_adl = [t for t in adl if D(t['size']) != 6]
core_adl_notional = sum(D(t['size']) * D(t['price']) for t in core_adl)
core_gap = core_adl_notional - repair_notional
core_funding_difference = fund * repair_qty / retained_short
core_loss = core_gap + repair_fees + core_funding_difference
all_loss = adl_notional - repair_notional - (adl_qty - repair_qty) * mark_now + repair_fees + fund * adl_qty / retained_short
result = {
    'environment': 'gate_fr_arb01', 'contract': 'ALPINE_USDT',
    'valuation_utc': latest_spot['utc'], 'adl_trades': adl,
    'adl_qty': adl_qty, 'repaired_qty': repair_qty,
    'core_adl_vwap': core_adl_notional / repair_qty,
    'repair_vwap': repair_notional / repair_qty,
    'repair_fees_usdt': repair_fees,
    'core_futures_price_gap_usdt': core_gap,
    'actual_retained_2576_funding_usdt': fund,
    'counterfactual_extra_8500_funding_usdt': core_funding_difference,
    'incremental_adl_loss_8500_usdt': core_loss,
    'incremental_adl_loss_all_8506_usdt': all_loss,
    'actual_adl_realized_pnl_usdt': sum(D(t['change']) for t in book if t['type'] == 'pnl'),
    'spot_trade_immediately_before_adl': before_adl,
    'spot_price_at_adl': event_spot, 'spot_price_at_valuation': spot_now,
    'spot_mark_to_market_drawdown_8500_usdt': repair_qty * (event_spot - spot_now),
    'spot_trade_price_difference_is_account_equity_drawdown': False,
    'caveat': 'The spot calculation uses two small-trade prices; it does not establish the valuation of the whole position '
              'or an observed account equity drawdown, and must not be reported as actual account loss. '
              'It includes spot/perpetual basis changes and is not entirely caused by ADL. '
              'Incremental ADL loss compares observed fills/fees/funding with retaining all original shorts, '
              'holding other trades fixed. It already captures the ADL settlement; never add ADL realized PnL again. '
              'Spot prices are small-trade marks, not guaranteed execution prices for the full inventory.'
}
equity_path = ROOT / 'account_equity_history.json'
equity_correction = ''
if equity_path.is_file():
    facts = json.loads(equity_path.read_text())
    points = facts['targets']
    pre = points['immediately_before_adl']
    earlier = points['one_minute_before_adl']
    latest = points['latest']
    assert pre['ts_ms'] <= int(start * 1000)
    assert latest['ts_ms'] - pre['ts_ms'] > 86400000
    result['actual_account_equity_reconciliation'] = {
        'source': 'Gate AccountRisk actual_eq_usd in deployed account-monitor log',
        'one_minute_before_adl': earlier, 'adl_time': pre, 'latest': latest,
        'equity_change_from_adl_time_usd': D(str(latest['equity_usd'])) - D(str(pre['equity_usd'])),
        'equity_change_from_one_minute_before_usd': D(str(latest['equity_usd'])) - D(str(earlier['equity_usd'])),
        'scope': 'Whole account, all assets and income; not ALPINE-only attribution',
        'timestamp_resolution_ms': 1000,
    }
    equity_correction = f'''
## Correction: observed account equity is authoritative

The earlier 6,294.25 USDT spot-only small-trade-price calculation is **not an
observed account NAV loss**. It must not be used to claim the account fell by
that amount or to answer the incremental ADL loss question.

The deployed account monitor records Gate's `actual_eq_usd` directly from
`unified.assets.e`; NAV's FR overview reads the same `actual_equity_usd` fact.
Source code: mkt_parsers/src/account_event/gate_account_event_parser.rs
(`parse_unified_assets`) and crypto_nav_manager/src/server/fr_overview.rs
(`snapshot_risk`).

| UTC time | Whole-account exchange equity, USD |
| --- | ---: |
| 2026-10-30 10:15:37 | {earlier['equity_usd']} |
| 2026-10-30 10:16:39, ADL time at second resolution | {pre['equity_usd']} |
| 2026-11-03 14:21:47 | {latest['equity_usd']} |

Observed change from the ADL-time reading is
{result['actual_account_equity_reconciliation']['equity_change_from_adl_time_usd']} USD.
From the reading one minute earlier, the change is
{result['actual_account_equity_reconciliation']['equity_change_from_one_minute_before_usd']} USD.
Equity moved rapidly around the event. Neither comparison isolates ALPINE;
both include every other asset, funding, fees and account changes. An exact
whole-account loss bridge requires reconciling those additional flows.

The PostgreSQL strategy_snapshots lookup did not supply contemporaneous data:
its latest stored snapshot for this strategy was 2026-10-11. It was excluded
from the current/ADL equity comparison. account_equity_history.json contains
the actual account-monitor history used above.

'''
(ROOT / 'analysis.json').write_text(json.dumps(result, indent=2, default=str) + '\n')
report = f'''# Gate ALPINE ADL loss attribution

Environment: gate_fr_arb01. Read-only exchange queries. Valuation: {latest_spot['utc']}.
{equity_correction}

ADL occurred 2026-10-30 10:16:39.7052–10:16:40.7977 UTC (18:16:39–40 Beijing).
Four fills bought back 8,506 shorts; 2,576 shorts remained. Spot inventory did
not trade after ADL. On 2026-11-03 13:45:12–13:45:43 UTC, 77 fills restored
8,500 shorts. The extra six ADL contracts mainly removed an existing small
overhedge; current net inventory is +0.718616 ALPINE.

## Incremental loss caused by ADL, relative to retaining the original shorts

For the main 8,500 units:

| Component | USDT loss |
| --- | ---: |
| ADL buyback VWAP 0.4348 minus replacement short VWAP {repair_notional / repair_qty} | {core_gap} |
| Replacement short fees | {repair_fees} |
| Funding difference (ADL avoided a net funding payment) | {core_funding_difference} |
| Total incremental loss | {core_loss} |

Funding uses the 12 factual account-book settlements on the unchanged remaining
2,576 shorts, scaled by 8,500 / 2,576. Actual retained-short funding summed to
{fund} USDT, so the missing shorts avoided a net payment of {-core_funding_difference} USDT.
Including all 8,506 ADL units and marking the six-unit position difference at
{mark_now} gives {all_loss} USDT incremental loss.

ADL itself booked {result['actual_adl_realized_pnl_usdt']} USDT realized futures
PnL versus the historical entry price. This is already represented in the
incremental comparison; adding it again double counts the same ADL settlement.

## Illustrative spot-trade-price calculation, not an account equity loss

The final public spot trade preceding ADL was at {event_spot}, trade
{before_adl['id']}, timestamp {before_adl['create_time_ms']} milliseconds.
At valuation the spot ticker last price was {spot_now}. Therefore the
two-price arithmetic for 8,500 units is:

8,500 × ({event_spot} − {spot_now}) = {result['spot_mark_to_market_drawdown_8500_usdt']} USDT.

This is not evidence of a 6,294.25 USDT account equity loss. It includes the
change of a large spot premium to perpetuals, and applies small-trade prices
to the whole position. It is not all incremental ADL damage and is not additive to the
futures counterfactual loss. Small public trade prices do not prove that the
entire 8,500-unit spot inventory could have been sold at those prices.

Evidence: exchange_initial.jsonl, exchange_followup.jsonl,
exchange_valuation.jsonl, exchange_event_prices.jsonl. analyze.py verifies
fill identities across both trade endpoints, fees against account-book entries,
all four ADL realized-PnL identities, and absence of subsequent spot trades.
'''
(ROOT / 'report.md').write_text(report)
print(json.dumps({k: v for k, v in result.items() if k != 'adl_trades'}, indent=2, default=str))
