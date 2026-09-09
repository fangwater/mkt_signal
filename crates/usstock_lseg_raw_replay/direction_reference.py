"""Causal direction baseline. Call reset at every audited session boundary.

Input times are integer nanoseconds; prices are integer e9. Histories contain
only source-visible events, and equal event times never count as prior evidence.
"""
from event_reference import classify_trade_direction


class DirectionState:
    def __init__(self):
        self.reset()

    def reset(self):
        self.bid = []
        self.ask = []
        self.trades = []

    def quote(self, side, event, source, order, price, size, venue):
        getattr(self, side).append((event, source, order, price, size, venue))

    def classify(self, event, source, order, price, venue, order_side=65535):
        _, _, venue_class = classify_trade_direction(venue, order_side)
        if venue_class == 2:
            return "N", 9, False, False
        if order_side in (1, 2):
            result = ("S" if order_side == 1 else "B", 1, True, True)
        else:
            def prior(rows):
                eligible = [r for r in rows if r[0] < event and r[1] <= source and r[2] < order]
                return max(eligible, key=lambda r: (r[0], r[2]), default=None)

            bid, ask = prior(self.bid), prior(self.ask)
            bid = bid if bid and bid[3] > 0 and 0 < bid[4] < 2**64 - 1 else None
            ask = ask if ask and ask[3] > 0 and 0 < ask[4] < 2**64 - 1 else None
            crossed = bid and ask and bid[3] >= ask[3]
            buy = bool(not crossed and ask and venue and ask[5] == venue and price >= ask[3])
            sell = bool(not crossed and bid and venue and bid[5] == venue and price <= bid[3])
            normal = bid and ask and not crossed
            if buy != sell:
                result = ("B" if buy else "S", 2, True, False)
            elif normal and (price >= ask[3] or price <= bid[3]):
                result = ("B" if price >= ask[3] else "S", 3, True, False)
            elif normal and 2 * price != bid[3] + ask[3]:
                result = ("B" if 2 * price > bid[3] + ask[3] else "S", 4, True, False)
            else:
                tick = prior([r for r in self.trades if r[3] != price])
                previous = prior([r for r in self.trades if r[5]])
                if tick:
                    result = ("B" if price > tick[3] else "S", 5 if normal else 6, True, not bool(normal))
                elif previous:
                    result = (previous[4], 7, True, True)
                else:
                    result = ("B", 8, True, True)
        self.trades.append((event, source, order, price, result[0], result[1] <= 6))
        return result
