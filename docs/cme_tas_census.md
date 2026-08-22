# CME TAS census

`cme_tas_census` streams CME Group futures Time and Sales gzip parts and
projects every nonempty cell onto a structured event. It is a parser / census
skeleton, not a baseline-bar writer. The source files are LSEG Tick History
extracts of those CME contracts.

Python remains the correctness baseline in
[`preprocess/lseg/tas_replay.py`](../../preprocess/lseg/tas_replay.py). The
Rust binary must keep the same column names, empty-cell rule, `Type`
subclasses, and panic conditions.

## Source

Local 2026 H1 TAS (UTC `2026-01-01_2026-06-01`):

```text
/mnt/hdd-raid5-72t/liang_torch/lseg_data/future/normalised/
  shanghai_evolution_futures_time_and_sales_ric_list_0_tas_2026-01-01_2026-06-01/
    merged-Data-part-NNNNNN.csv.gz
```

Parts are `#RIC` lexicographic shards, not calendar files. Part 0 starts at
index RICs (`.FTFTCRTWNT`, `.FTXIN9`, …) before futures. A RIC may be split
across adjacent parts. The 294-column template is shared with bonds, options
and macro series; most of those columns are empty on CME Group futures.

## Structured row

Default projection keeps **every nonempty source cell**. Empty cells are
omitted, not filled with `0` or a neighbor. The event shape is:

```json
{
  "class": "trade_printable",
  "ric": "ALIH26",
  "date_time": "2026-01-02T15:39:23.298829985Z",
  "type": "Trade",
  "index_ric": false,
  "fields": {"#RIC": "ALIH26", "Price": "2999.75", "Volume": "1"},
  "groups": {"identity": {"#RIC": "ALIH26"}, "trade": {"Price": "2999.75"}}
}
```

`fields` is the flat filled map. `groups` is the same cells keyed by the rule
table group. `class` is the classified `Type` (Trade subclasses below).

## Rule table

Rules live in
[`preprocess/lseg/tas_column_rules.json`](../../preprocess/lseg/tas_column_rules.json).
That file is the only catalog: 294 named columns in 21 groups, plus the four
known `Type` values.

| Group | Columns | Meaning |
| --- | ---: | --- |
| `identity` | 5 | `#RIC`, `Domain`, `Date-Time`, `GMT Offset`, `Type` |
| `venue` | 3 | `Ex/Cntrb.ID`, `LOC`, `Dealing Code` |
| `trade` | 7 | `Price`, `Volume`, `Market VWAP`, odd-lot, currency |
| `counterparty` | 2 | `Buyer ID`, `Seller ID` |
| `l1` | 8 | Bid/Ask price/size, buyer/seller counts, spreads |
| `event` | 8 | `Qualifiers`, `Seq. No.`, `Exch Time`, trade flags |
| `equity_value` | 1 | `PE Ratio` |
| `yield` | 88 | Bond / swap / basis yields |
| `theoretical` | 23 | Theo / fair value / limits |
| `iv` | 24 | Implied vol and greeks |
| `energy_analytic` | 18 | Crack, freight, EFP, quotas |
| `macro` | 14 | Actual / prior / forecast |
| `session` | 13 | Open/High/Low/OI/Acc. Volume/Turnover and related |
| `index_breadth` | 22 | Advancing issues, `%` change |
| `cds` | 6 | CDS spread / basis / recovery |
| `auction` | 12 | Imbalance / LULD / indicative auction |
| `book_accum` | 9 | Accumulated bid/ask, total buy/sell |
| `correction` | 19 | Original * / change type |
| `status` | 2 | `Halt Reason`, `Trading Status` |
| `identity_extra` | 6 | ISIN, display name, unique ids |
| `dealer` | 4 | Dealer / market-maker ids |

`core` is a legacy alias for the original washable futures subset (identity +
trade + L1 + event + session + optional ids + status). It is not a fourth
event type.

Column meanings for the wide groups are in
[`preprocess/data_format/lseg/tas_wide_fields.md`](../../preprocess/data_format/lseg/tas_wide_fields.md).
This extract is **not cash equities**. The 16,336-RIC list is mostly CME Group
futures plus a few index RICs. `include_index_rics = true` keeps those index
rows; it does not add stocks. Do not treat TAS L1 as Normalized LL2.

## Panic rules

Unhandled input is a hard failure so the rule table can be fixed. Do not
classify it as `other` and continue.

| Condition | Failure |
| --- | --- |
| Header or row column name missing from the 294-name catalog | `unhandled TAS column` |
| `Type` not in `Trade` / `Quote` / `Mkt. Condition` / `Correction`, or empty | `unhandled TAS Type` |
| `#RIC`, `Date-Time`, or `Type` empty | `unhandled empty required TAS field` |
| `field_groups` names an unknown group | `unknown TAS field group` |
| `field_groups` is nonempty and a leftover nonempty cell is outside it | `unhandled nonempty TAS columns` |
| Rule file does not catalogue exactly 294 names | load-time panic |

Empty `field_groups` means every catalogued column. Restricting groups is
opt-in and must not silently drop filled cells.

## `Type` subclasses

`Type` is the LSEG event class. Trade subclasses live in `Qualifiers` plus
whether `Price` / `Volume` are filled.

| Class | How it is recognized | Replay use |
| --- | --- | --- |
| `trade_printable` | `Type=Trade` and both `Price` and `Volume` nonempty | Printable print; can enter 1-minute OHLC. |
| `trade_special_user` | `Qualifiers` is exactly `Special Trades[USER]` | Volume without price; counts in Summary volume, not OHLC. |
| `trade_volume_only` | Trade with volume, no price, not the `[USER]` tag | Keep as unclassified volume; do not invent a price. |
| `trade_price_only` | Trade with price, no volume | Keep; do not invent size. |
| `trade_empty` | Trade with both price and volume empty | Drop from OHLC and volume. |
| `quote` | `Type=Quote` | L1 update. |
| `mkt_condition` | `Type=Mkt. Condition` | Halt / session flags. |
| `correction` | `Type=Correction` | Correction, not a new print. |

`[USER]` is an LSEG-normalized qualifier, not a CME raw type.

Observed occupancy on already extracted 2026 futures windows (ADF26 open +
commodity part0 trades, 5,144 rows) — not a full-part census:

| Class | Rows |
| --- | ---: |
| `trade_printable` | 3,990 |
| `quote` | 646 |
| `trade_special_user` | 496 |
| `mkt_condition` | 8 |
| `trade_empty` | 4 |

Printable trades in that extract always carried `Price`, `Volume`, same-row L1,
`Qualifiers`, `Seq. No.`, `Exch Time`, and `Acc. Volume`. `Special Trades[USER]`
had `Volume` and `Acc. Volume` only. Session `Open`/`High`/`Low`/`Turnover` were
empty on those futures trade extracts (they do appear on some index Trade
rows). Wide columns such as `PE Ratio` were empty there; if a later row fills
them, they stay in `fields`.

## Running

Default config is a 20,000-row dry run against the local 2026 H1 tree:

```bash
cargo run --bin cme_tas_census -- --config config/cme_tas_census.toml
```

Write structured JSONL (still no ClickHouse):

```bash
# set output_jsonl in the toml, or copy the config
cargo run --bin cme_tas_census -- --config config/cme_tas_census.toml
```

`dry_run=false` is allowed only when `output_jsonl` is set. The binary still
refuses a write path that is not JSONL.
