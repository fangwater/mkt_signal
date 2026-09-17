#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Sync CTA pre-trade risk defaults without changing ordinary intra defaults."""

from __future__ import annotations

import sync_intra_risk_params as intra_risk


RISK_PARAMS = dict(intra_risk.RISK_PARAMS)
RISK_PARAMS["max_pos_u"] = "1000.0"

PARAM_COMMENTS = dict(intra_risk.PARAM_COMMENTS)
PARAM_COMMENTS["max_pos_u"] = (
    "CTA live 最大单币种已成交持仓 1000U；不累计未成交 maker，接受有限超调"
)
PARAM_PRINT_ORDER = list(RISK_PARAMS.keys())


def main() -> int:
    intra_risk.RISK_PARAMS = dict(RISK_PARAMS)
    intra_risk.PARAM_COMMENTS = dict(PARAM_COMMENTS)
    return intra_risk.main()


if __name__ == "__main__":
    raise SystemExit(main())
