# Bitget Intra Margin-Lock Deleverage

## Scope

This path is active only when intra pre-trade has the account-wide
`bitget_unified_insufficient_margin` lock. It does not change normal ArbOpen,
other exchanges, or the independent UniMMR lock.

## Eligibility

An ArbOpen may use this path only when all conditions hold:

- Open venue is Bitget margin and hedge venue is Bitget USDT futures.
- The open side is `Sell`; the signal is therefore spot sell plus futures buy.
- The local spot net base balance is positive.
- The futures net base position is negative.
- The requested base quantity does not exceed both the positive spot balance and
  the absolute futures short position after venue quantity conversion.

The local balance is a pre-check. Bitget remains the authority for frozen and
available spot inventory; a rejected spot order must not create a futures order.

## Order Semantics

- The opening order routes to Bitget API category `SPOT`, never `MARGIN`.
- The futures hedge is `BUY` with `reduceOnly=YES` only when its amount can
  completely reduce the current futures short position.
- Futures hedge quantity follows actual spot fills. No futures order is sent for
  unfilled spot quantity.
- Normal Bitget margin order routing remains category `MARGIN`.

## Order Lifecycle

The order model records a Bitget spot-route flag. Both new and cancel requests
use the Bitget `SPOT` category for this order, while all ordinary Bitget margin
orders continue to use the `MARGIN` category.

## Lock Behavior

For `bitget_unified_insufficient_margin`, this is the only ArbOpen direction
that is permitted. The generic two-leg reducing exception is not used for this
lock, so a spot buy is never admitted while Bitget margin borrowing is blocked.
