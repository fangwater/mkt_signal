"""Time units shared by CTA Perp history, replay and configuration."""

BAR_MINUTES = {"1m": 1, "5m": 5, "15m": 15}


def bar_millis(interval: str) -> int:
    if interval not in BAR_MINUTES:
        raise ValueError("bar_interval must be 1m, 5m or 15m")
    return BAR_MINUTES[interval] * 60_000


def uses_linear_nq(interval: str) -> bool:
    bar_millis(interval)
    return interval != "1m"
