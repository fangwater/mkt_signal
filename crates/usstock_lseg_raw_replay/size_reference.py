"""Python correctness baseline for previous-month linear size thresholds."""

import json
import math
import sys


def linear_percentile(values, percentile):
    ordered = sorted(values)
    if not ordered or not 0.0 <= percentile <= 1.0:
        raise ValueError("invalid percentile input")
    position = percentile * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


if __name__ == "__main__":
    sample = json.load(sys.stdin)
    print(
        json.dumps(
            {
                "p50": linear_percentile(sample, 0.5),
                "p90": linear_percentile(sample, 0.9),
            }
        )
    )
