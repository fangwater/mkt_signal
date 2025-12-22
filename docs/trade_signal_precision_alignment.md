# Trade Signal Qty Alignment (OKX vs Binance)

## Problem
OKX and Binance have different contract face values (multiplier) and quantity step sizes.
If the signal layer only rounds the "open" qty, the "hedge" qty may land on a different
grid and become non-executable or mis-hedged. The fix should happen in the trade signal
layer: compute both legs together and emit only aligned quantities.

## Goal
- Emit a pair of executable quantities (open + hedge) that are aligned to both exchanges.
- Minimize notional mismatch between the two legs.
- Keep exposure close to the target amount `U` (quote currency).

## Inputs (per exchange)
- `qty_step`: minimum quantity increment (contract or base-asset unit).
- `price_tick`: minimum price increment (used for price snap).
- `contract_type`: `linear` (USDT-margined) or `inverse` (USD face value).
- `contract_multiplier`: base-asset per contract (linear) or `face_value` (inverse).
- `min_qty`, `min_notional` if enforced.

## Definitions
Let `P_i` be the price used for sizing on exchange `i`, snapped to `price_tick`.

Define the notional per one quantity step:

1) Linear (USDT-margined, qty is contract count):
```
notional_per_step = qty_step * contract_multiplier * P_i
```

2) Inverse (USD face value, qty is contract count):
```
notional_per_step = qty_step * face_value
```

3) Spot-like (qty is base-asset amount):
```
notional_per_step = qty_step * P_i
```

All qty outputs must be integer multiples of `qty_step`.

## Alignment Algorithm (Signal Layer)
Given target notional `U` (quote currency), open exchange `A`, hedge exchange `B`:

1) **Snap prices**
   - `P_A = round_to_tick(raw_price_A, tick_A)`
   - `P_B = round_to_tick(raw_price_B, tick_B)`

2) **Compute notional-per-step**
   - `N_A = notional_per_step(A)`
   - `N_B = notional_per_step(B)`

3) **Compute base unit counts**
   - `k_A = U / N_A` (ideal count on A)
   - `k_B = U / N_B` (ideal count on B)

4) **Search candidate open counts**
   - Let `c0 = round(k_A)`
   - Search `c_A` in `[c0 - W, c0 + W]`, `c_A >= min_count_A`
   - `W` can be small (e.g. 5-20), or scale with notional size.

5) **Compute matching hedge count**
   - `notional_A = c_A * N_A`
   - `c_B = round(notional_A / N_B)` (or floor/ceil based on risk preference)
   - Enforce `c_B >= min_count_B`
   - `notional_B = c_B * N_B`

6) **Score and choose best pair**
   - `mismatch = abs(notional_A - notional_B)`
   - `deviation = abs(notional_A - U) + abs(notional_B - U)`
   - Choose `(c_A, c_B)` with minimal `mismatch`, then minimal `deviation`.
   - Optional tie-breaker: prefer smaller exposure, or prioritize open-side fidelity.

7) **Emit aligned quantities**
   - `qty_A = c_A * qty_step_A`
   - `qty_B = c_B * qty_step_B`
   - Include `notional_A`, `notional_B`, and `mismatch` in the signal for logging.

## Risk Preference (Floor vs Round)
If you want "never exceed U" on either leg, use `floor` when mapping to counts.
If you want "always hedge at least as much as open", use `ceil` for the hedge leg.
This can be encoded per side:
- Open leg: `round` or `floor` (exposure control)
- Hedge leg: `round` or `ceil` (coverage control)

## Example (OKX qty 0.0022 vs Binance step 0.001)
Assume the open leg yields `qty_A = 0.0022` on OKX, but Binance step is `0.001`.
The alignment process searches nearby `c_A` so that the implied hedge is aligned.
Possible outcomes:
- Adjust open to `0.0020` to match hedge `0.0020` (minimal exposure).
- Or adjust open to `0.0030` if you require coverage >= target and can accept larger size.
The chosen pair should minimize mismatch and stay within the risk preference.

## Why this belongs in `trade_signal`
The signal layer has all the context: target `U`, prices, exchange metadata, and
hedge pairing. Emitting already-aligned quantities prevents downstream failures
and makes the trading layer deterministic and auditable.
