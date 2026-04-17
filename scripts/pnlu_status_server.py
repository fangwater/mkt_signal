#!/usr/bin/env python3
"""
Simple web server for monitoring PNLU status from Redis.

Features:
1. Choose a fixed venue pair from a dropdown.
2. Scan Redis by suffix to discover available symbols.
3. Poll PNLU Redis keys every 30 seconds and show missing / ready / non-ready.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]
EXCHANGE_DEFAULTS = {
    "binance": ("binance-margin", "binance-futures"),
    "okex": ("okex-margin", "okex-futures"),
    "bybit": ("bybit-margin", "bybit-futures"),
    "bitget": ("bitget-margin", "bitget-futures"),
    "gate": ("gate-margin", "gate-futures"),
}

PNLU_KEY_SUFFIX = "_pnlu_factor_thresholds"
DEFAULT_POLL_SECS = 30
DEFAULT_SCAN_COUNT = 1000
DEFAULT_SCAN_LIMIT = 500
PNLU_MAX_AGE_SECS = 30 * 60
PNLU_RUNTIME_THRESHOLD_INDEX = 1

INDEX_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>PNLU Status Monitor</title>
  <style>
    :root {
      --bg: #f3efe6;
      --ink: #1d1e1a;
      --muted: #6e6657;
      --panel: rgba(255, 250, 240, 0.88);
      --panel-strong: rgba(248, 241, 225, 0.94);
      --line: rgba(75, 62, 44, 0.16);
      --accent: #215732;
      --accent-2: #cf5c36;
      --missing: #b8b1a5;
      --missing-bg: #efebe4;
      --ready: #2d7a48;
      --ready-bg: #e7f5ea;
      --not-ready: #a35a15;
      --not-ready-bg: #fff1df;
      --error: #b42318;
      --error-bg: #feeceb;
      --shadow: 0 18px 40px rgba(57, 41, 23, 0.10);
      --radius: 18px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(255, 207, 120, 0.25), transparent 28%),
        radial-gradient(circle at right 20%, rgba(33, 87, 50, 0.16), transparent 24%),
        linear-gradient(180deg, #fbf7ef 0%, #f2ebdf 100%);
      min-height: 100vh;
    }
    .wrap {
      max-width: 1280px;
      margin: 0 auto;
      padding: 28px 18px 48px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 18px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    h1 {
      margin: 0;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      font-size: 18px;
      line-height: 1.05;
      letter-spacing: -0.03em;
    }
    h2 {
      margin: 0;
      font-size: 15px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    p {
      margin: 0;
      color: var(--muted);
      line-height: 1.55;
    }
    .toolbar,
    .form-grid {
      display: grid;
      gap: 12px;
    }
    .key-line code,
    .card-key code,
    textarea,
    select,
    input,
    button {
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
    }
    .section-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
    }
    .toolbar { align-items: end; }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 0;
    }
    label {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }
    input,
    select,
    textarea {
      width: 100%;
      border-radius: 12px;
      border: 1px solid rgba(75, 62, 44, 0.20);
      background: rgba(255,255,255,0.78);
      color: var(--ink);
      padding: 10px 12px;
      font-size: 14px;
      outline: none;
    }
    textarea {
      min-height: 220px;
      resize: vertical;
      line-height: 1.5;
    }
    input:focus,
    select:focus,
    textarea:focus {
      border-color: rgba(33, 87, 50, 0.45);
      box-shadow: 0 0 0 3px rgba(33, 87, 50, 0.10);
    }
    button {
      border: 0;
      border-radius: 999px;
      padding: 10px 16px;
      cursor: pointer;
      font-size: 13px;
      transition: transform 140ms ease, opacity 140ms ease;
      color: white;
      background: linear-gradient(90deg, var(--accent), #3d8a52);
    }
    button.alt {
      background: linear-gradient(90deg, #a94a2d, var(--accent-2));
    }
    button.ghost {
      background: rgba(34, 34, 34, 0.06);
      color: var(--ink);
      border: 1px solid rgba(75, 62, 44, 0.20);
    }
    button:hover { transform: translateY(-1px); }
    button:disabled { opacity: 0.55; cursor: default; transform: none; }
    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .hint,
    .status,
    .small {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
    }
    .status.ok { color: var(--ready); }
    .status.err { color: var(--error); }
    .status.warn { color: var(--not-ready); }
    .form-grid {
      grid-template-columns: 1.15fr 0.85fr;
    }
    .key-line {
      margin-top: 10px;
      padding: 10px 12px;
      border-radius: 12px;
      background: rgba(255,255,255,0.54);
      border: 1px solid rgba(75, 62, 44, 0.14);
      overflow-wrap: anywhere;
      font-size: 12px;
      line-height: 1.5;
    }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 14px;
    }
    .legend-chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid rgba(75, 62, 44, 0.14);
      background: rgba(255,255,255,0.44);
      font-size: 12px;
    }
    .dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      display: inline-block;
    }
    .dot.missing { background: var(--missing); }
    .dot.ready { background: var(--ready); }
    .dot.not-ready { background: var(--not-ready); }
    .dot.error { background: var(--error); }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 12px;
    }
    .status-card {
      border-radius: 16px;
      padding: 16px;
      border: 1px solid rgba(75, 62, 44, 0.12);
      background: rgba(255,255,255,0.56);
      min-height: 194px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .status-card.missing {
      background: linear-gradient(180deg, var(--missing-bg), #f7f2eb);
      border-color: rgba(111, 103, 92, 0.18);
    }
    .status-card.ready {
      background: linear-gradient(180deg, var(--ready-bg), #f1fbf2);
      border-color: rgba(45, 122, 72, 0.18);
    }
    .status-card.not_ready {
      background: linear-gradient(180deg, var(--not-ready-bg), #fff8ee);
      border-color: rgba(163, 90, 21, 0.18);
    }
    .status-card.error {
      background: linear-gradient(180deg, var(--error-bg), #fff7f6);
      border-color: rgba(180, 35, 24, 0.18);
    }
    .card-head {
      display: flex;
      align-items: start;
      justify-content: space-between;
      gap: 10px;
    }
    .card-title {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .card-title strong {
      font-size: 20px;
      line-height: 1;
      letter-spacing: -0.03em;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 12px;
      background: rgba(255,255,255,0.56);
      border: 1px solid rgba(75, 62, 44, 0.10);
      white-space: nowrap;
    }
    .card-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .mini {
      border-radius: 12px;
      padding: 10px;
      background: rgba(255,255,255,0.40);
      border: 1px solid rgba(75, 62, 44, 0.10);
    }
    .mini span {
      display: block;
      color: var(--muted);
      font-size: 11px;
      margin-bottom: 4px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .mini strong {
      font-size: 14px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }
    .card-key {
      margin-top: auto;
      font-size: 11px;
      color: var(--muted);
      overflow-wrap: anywhere;
    }
    .pair-list {
      display: grid;
      gap: 4px;
    }
    .pair-row {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      font-size: 13px;
      line-height: 1.35;
    }
    .pair-row code {
      font-size: 12px;
    }
    .pair-row .arrow {
      color: var(--muted);
      flex: 0 0 auto;
    }
    .pair-empty {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.4;
    }
    .pair-block {
      margin: 0;
    }
    @media (max-width: 980px) {
      .form-grid,
      .toolbar {
        grid-template-columns: 1fr;
      }
      .card-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="panel">
      <div class="section-head">
        <h2>Venue Pair</h2>
        <div class="small">Choose open venue and hedge venue, then query one symbol or all scanned symbols.</div>
      </div>
      <div class="toolbar" style="grid-template-columns: 1fr 1fr 1fr;">
        <div class="field">
          <label for="open-venue">open venue</label>
          <select id="open-venue"></select>
        </div>
        <div class="field">
          <label for="hedge-venue">hedge venue</label>
          <select id="hedge-venue"></select>
        </div>
        <div class="field">
          <label for="selected-symbol">symbol</label>
          <select id="selected-symbol"></select>
        </div>
      </div>
      <div class="actions" style="margin-top: 12px;">
        <button id="refresh-status" class="alt">refresh now</button>
      </div>
      <div id="toolbar-status" class="status"></div>
    </section>

    <div class="form-grid">
      <section class="panel">
        <div class="section-head">
          <h2>Monitor Control</h2>
          <div class="actions">
            <button id="start-polling">start polling</button>
            <button id="stop-polling" class="ghost">stop</button>
          </div>
        </div>
        <p>
          Pick a symbol from the scanned list or keep <code>ALL_SCANNED_SYMBOLS</code> selected
          to query all discovered symbols for the chosen venue pair.
        </p>
        <div class="key-line">Discovered symbols are scanned directly from Redis by suffix.</div>
        <div class="key-line">PNLU key pattern: <code id="pnlu-key-line">-</code></div>
        <div class="key-line">Last refresh: <code id="last-refresh">never</code></div>
        <div id="monitor-status" class="status"></div>
      </section>
    </div>

    <section class="panel">
      <div class="section-head">
        <h2>Live Status</h2>
        <div class="small">State follows the Rust <code>check_pnlu_factor</code> gating rule, not only the raw <code>ready</code> field. Missing keys are shown separately.</div>
      </div>
      <div class="legend">
        <div class="legend-chip"><span class="dot missing"></span>missing</div>
        <div class="legend-chip"><span class="dot ready"></span>ready</div>
        <div class="legend-chip"><span class="dot not-ready"></span>non-ready</div>
        <div class="legend-chip"><span class="dot error"></span>error</div>
      </div>
      <div id="cards" class="cards"></div>
    </section>
  </div>

  <script>
    const BOOTSTRAP = __BOOTSTRAP__;
    const ALL_SYMBOLS = "ALL_SCANNED_SYMBOLS";

    const openVenueEl = document.getElementById("open-venue");
    const hedgeVenueEl = document.getElementById("hedge-venue");
    const selectedSymbolEl = document.getElementById("selected-symbol");
    const cardsEl = document.getElementById("cards");
    const lastRefreshEl = document.getElementById("last-refresh");

    let pollTimer = null;
    let lastScannedSymbols = [];

    function normalizeVenue(value) {
      return String(value || "").trim().toLowerCase();
    }

    function normalizeSymbol(value) {
      return String(value || "").trim().toUpperCase();
    }

    function setStatus(id, message, kind = "ok") {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = message || "";
      el.className = "status " + kind;
    }

    function selectedPair() {
      return {
        openVenue: normalizeVenue(openVenueEl.value),
        hedgeVenue: normalizeVenue(hedgeVenueEl.value),
      };
    }

    function makePnluProfile() {
      const pair = selectedPair();
      if (!pair.openVenue || !pair.hedgeVenue) return "";
      return `${pair.openVenue}-${pair.hedgeVenue}`;
    }

    function effectiveSelectedSymbol() {
      const symbol = normalizeSymbol(selectedSymbolEl.value);
      return symbol && symbol !== ALL_SYMBOLS ? symbol : "";
    }

    function makeExamplePnluKey() {
      const symbol = effectiveSelectedSymbol() || (lastScannedSymbols[0] || "ETHUSDT");
      const profile = makePnluProfile();
      if (!profile) return "";
      return `${symbol}_pnlu_factor_thresholds_${profile}`;
    }

    function refreshDerivedInfo() {
      document.getElementById("pnlu-key-line").textContent = makeExamplePnluKey() || "-";
    }

    function fillVenueOptions(selectEl, venues, defaultValue) {
      selectEl.innerHTML = "";
      for (const venue of venues || []) {
        const option = document.createElement("option");
        option.value = venue;
        option.textContent = venue;
        if (venue === defaultValue) option.selected = true;
        selectEl.appendChild(option);
      }
    }

    function renderSymbolOptions(symbols) {
      const normalized = Array.isArray(symbols) ? symbols : [];
      lastScannedSymbols = normalized;
      const previous = selectedSymbolEl.value;
      selectedSymbolEl.innerHTML = "";

      const allOption = document.createElement("option");
      allOption.value = ALL_SYMBOLS;
      allOption.textContent = ALL_SYMBOLS;
      selectedSymbolEl.appendChild(allOption);

      for (const symbol of normalized) {
        const option = document.createElement("option");
        option.value = symbol;
        option.textContent = symbol;
        selectedSymbolEl.appendChild(option);
      }

      if (previous && [...selectedSymbolEl.options].some(opt => opt.value === previous)) {
        selectedSymbolEl.value = previous;
      } else {
        selectedSymbolEl.value = ALL_SYMBOLS;
      }
      refreshDerivedInfo();
    }

    function toQuery(params) {
      const query = new URLSearchParams();
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== null && String(value) !== "") {
          query.set(key, String(value));
        }
      }
      return query.toString();
    }

    async function fetchJson(path, options = {}) {
      const response = await fetch(path, options);
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `HTTP ${response.status}`);
      }
      return await response.json();
    }

    async function loadSymbols() {
      setStatus("toolbar-status", "scanning symbols ...", "warn");
      try {
        const query = toQuery({
          open_venue: selectedPair().openVenue,
          hedge_venue: selectedPair().hedgeVenue,
        });
        const data = await fetchJson(`/api/symbols?${query}`);
        const symbols = data.symbols || [];
        renderSymbolOptions(symbols);
        refreshDerivedInfo();
        setStatus(
          "toolbar-status",
          `loaded ${symbols.length} symbols using ${data.source || "unknown"}`,
          "ok"
        );
      } catch (error) {
        setStatus("toolbar-status", `scan failed: ${error}`, "err");
      }
    }

    function statusLabel(status) {
      if (status === "ready") return "ready";
      if (status === "missing") return "missing";
      if (status === "not_ready") return "non-ready";
      return "error";
    }

    function valueOrDash(value) {
      if (value === null || value === undefined || value === "") return "-";
      return String(value);
    }

    function formatThresholdPairs(item) {
      const pairs = Array.isArray(item.threshold_pairs) ? item.threshold_pairs : [];
      if (pairs.length) {
        return `
          <div class="pair-list">
            ${pairs.map(pair => `
              <div class="pair-row">
                <code>${valueOrDash(pair.quantile)}</code>
                <span class="arrow">-&gt;</span>
                <code>${valueOrDash(pair.threshold)}</code>
              </div>
            `).join("")}
          </div>
        `;
      }

      const quantiles = Array.isArray(item.quantiles) ? item.quantiles : [];
      const thresholds = Array.isArray(item.thresholds) ? item.thresholds : [];
      if (quantiles.length && !thresholds.length) {
        return `<div class="pair-empty">quantiles=${JSON.stringify(quantiles)}<br>thresholds=[]</div>`;
      }
      if (!quantiles.length && thresholds.length) {
        return `<div class="pair-empty">quantiles=[]<br>thresholds=${JSON.stringify(thresholds)}</div>`;
      }
      if (quantiles.length || thresholds.length) {
        return `<div class="pair-empty">quantiles=${JSON.stringify(quantiles)}<br>thresholds=${JSON.stringify(thresholds)}</div>`;
      }
      return `<div class="pair-empty">-</div>`;
    }

    function renderCards(items) {
      cardsEl.innerHTML = "";
      if (!items.length) {
        const empty = document.createElement("div");
        empty.className = "status-card missing";
        empty.innerHTML = `
          <div class="card-head">
            <div class="card-title">
              <strong>No symbols</strong>
              <div class="small">No Redis keys matched the derived suffix for this venue pair.</div>
            </div>
          </div>
        `;
        cardsEl.appendChild(empty);
        return;
      }

      for (const item of items) {
        const card = document.createElement("div");
        card.className = `status-card ${item.status}`;
        card.innerHTML = `
          <div class="card-head">
            <div class="card-title">
              <strong>${item.symbol}</strong>
            </div>
            <div class="pill"><span class="dot ${item.status === "not_ready" ? "not-ready" : item.status}"></span>${statusLabel(item.status)}</div>
          </div>
          <div class="card-grid">
            <div class="mini">
              <span>runtime ok</span>
              <strong>${valueOrDash(item.runtime_ok)}</strong>
            </div>
            <div class="mini">
              <span>runtime reason</span>
              <strong>${valueOrDash(item.runtime_reason)}</strong>
            </div>
            <div class="mini">
              <span>ready(raw)</span>
              <strong>${valueOrDash(item.ready)}</strong>
            </div>
            <div class="mini">
              <span>factor</span>
              <strong>${valueOrDash(item.factor)}</strong>
            </div>
            <div class="mini">
              <span>threshold[idx=1]</span>
              <strong>${valueOrDash(item.threshold)}</strong>
            </div>
            <div class="mini">
              <span>threshold pairs</span>
              <div class="pair-block">${formatThresholdPairs(item)}</div>
            </div>
            <div class="mini">
              <span>ts</span>
              <strong>${valueOrDash(item.ts)}</strong>
            </div>
            <div class="mini">
              <span>age_sec</span>
              <strong>${valueOrDash(item.age_sec)}</strong>
            </div>
            <div class="mini">
              <span>target_ts</span>
              <strong>${valueOrDash(item.target_ts)}</strong>
            </div>
            <div class="mini">
              <span>target_age_sec</span>
              <strong>${valueOrDash(item.target_age_sec)}</strong>
            </div>
          </div>
          <div class="card-key">key: <code>${item.key}</code></div>
        `;
        cardsEl.appendChild(card);
      }
    }

    async function refreshStatus() {
      setStatus("monitor-status", "refreshing ...");
      try {
        const query = {
          open_venue: selectedPair().openVenue,
          hedge_venue: selectedPair().hedgeVenue,
        };
        const symbol = effectiveSelectedSymbol();
        if (symbol) query.symbol = symbol;
        const data = await fetchJson(`/api/pnlu-status?${toQuery(query)}`);
        renderCards(data.items || []);
        lastRefreshEl.textContent = new Date().toISOString();
        setStatus(
          "monitor-status",
          `loaded ${data.items.length} item(s) using ${data.symbol_source || "unknown"}`,
          "ok"
        );
      } catch (error) {
        setStatus("monitor-status", `refresh failed: ${error}`, "err");
      }
    }

    function startPolling() {
      if (pollTimer !== null) window.clearInterval(pollTimer);
      refreshStatus();
      pollTimer = window.setInterval(refreshStatus, BOOTSTRAP.poll_secs * 1000);
      setStatus("toolbar-status", `polling every ${BOOTSTRAP.poll_secs}s`, "warn");
    }

    function stopPolling() {
      if (pollTimer !== null) {
        window.clearInterval(pollTimer);
        pollTimer = null;
      }
      setStatus("toolbar-status", "polling stopped", "warn");
    }

    fillVenueOptions(openVenueEl, BOOTSTRAP.venues, BOOTSTRAP.default_open_venue);
    fillVenueOptions(hedgeVenueEl, BOOTSTRAP.venues, BOOTSTRAP.default_hedge_venue);
    renderSymbolOptions([]);
    refreshDerivedInfo();

    function handleVenueChange() {
      refreshDerivedInfo();
      loadSymbols();
    }

    openVenueEl.addEventListener("change", handleVenueChange);
    hedgeVenueEl.addEventListener("change", handleVenueChange);
    selectedSymbolEl.addEventListener("change", refreshDerivedInfo);

    document.getElementById("refresh-status").addEventListener("click", refreshStatus);
    document.getElementById("start-polling").addEventListener("click", startPolling);
    document.getElementById("stop-polling").addEventListener("click", stopPolling);

    loadSymbols().then(() => refreshStatus());
  </script>
</body>
</html>
"""


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(exchange: str) -> str:
    value = (exchange or "").strip().lower()
    if value == "okx":
        return "okex"
    return value


def sanitize_profile_token(raw: str) -> str:
    token = raw.strip().strip("/")
    token = token.split("/")[-1]
    if token.endswith(".toml"):
        token = token[:-5]
    token = token.lower().replace("_", "-").replace("/", "-").strip("-")
    if "-" in token:
        left, right = token.split("-", 1)
        if left.isdigit() and right:
            token = right
    return token or "default"


def suffix_from_profile(profile: str) -> str:
    return f"{PNLU_KEY_SUFFIX}_{sanitize_profile_token(profile)}"


def infer_exchange_from_cwd() -> Optional[str]:
    name = os.path.basename(os.getcwd()).strip().lower()
    if not name:
        return None
    candidates = [name]
    if "_" in name:
        candidates.append(name.split("_", 1)[0])
    if "-" in name:
        candidates.append(name.split("-", 1)[0])
    for candidate in candidates:
        normalized = normalize_exchange(candidate)
        for exchange in SUPPORTED_EXCHANGES:
            if normalized.startswith(exchange):
                return exchange
    return None


def default_exchange_name(cli_exchange: str) -> str:
    normalized = normalize_exchange(cli_exchange)
    if normalized in SUPPORTED_EXCHANGES:
        return normalized
    inferred = infer_exchange_from_cwd()
    if inferred:
        return inferred
    return "binance"


def build_all_venues() -> List[str]:
    venues = set()
    for open_venue, hedge_venue in EXCHANGE_DEFAULTS.values():
        venues.add(open_venue)
        venues.add(hedge_venue)
    return sorted(venues)


def normalize_venue(value: str) -> str:
    return (value or "").strip().lower()


def make_store_suffix(open_venue: str, hedge_venue: str) -> str:
    open_norm = normalize_venue(open_venue)
    hedge_norm = normalize_venue(hedge_venue)
    if not open_norm or not hedge_norm:
        raise ValueError("open_venue and hedge_venue are required")
    return f"{open_norm}_{hedge_norm}"


def make_pnlu_profile(open_venue: str, hedge_venue: str) -> str:
    open_norm = normalize_venue(open_venue)
    hedge_norm = normalize_venue(hedge_venue)
    if not open_norm or not hedge_norm:
        raise ValueError("open_venue and hedge_venue are required")
    return f"{open_norm}-{hedge_norm}"


def resolve_profile(open_venue: str, hedge_venue: str) -> str:
    return sanitize_profile_token(make_pnlu_profile(open_venue, hedge_venue))


def resolve_suffix(profile: str) -> str:
    return suffix_from_profile(profile)


def normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def scan_symbols_by_suffix(
    rds: Any,
    suffix: str,
    scan_count: int = DEFAULT_SCAN_COUNT,
    limit: int = DEFAULT_SCAN_LIMIT,
) -> List[str]:
    if not suffix:
        return []

    pattern = f"*{suffix}"
    cursor = 0
    symbols: set[str] = set()

    while True:
        cursor, keys = rds.scan(cursor=cursor, match=pattern, count=max(scan_count, 1))
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode("utf-8", "ignore")
            else:
                key = str(key)
            if key.endswith(suffix):
                symbol = normalize_symbol(key[: -len(suffix)])
                if symbol:
                    symbols.add(symbol)
                    if len(symbols) >= limit:
                        return sorted(symbols)[:limit]
        if cursor == 0:
            break

    return sorted(symbols)


def make_pnlu_key(
    symbol: str,
    open_venue: str,
    hedge_venue: str,
) -> str:
    normalized_symbol = normalize_symbol(symbol)
    if not normalized_symbol:
        raise ValueError("symbol is required")
    suffix = resolve_suffix(resolve_profile(open_venue, hedge_venue))
    if not suffix:
        raise ValueError("suffix is required")
    return f"{normalized_symbol}{suffix}"


def normalize_pnlu_ts_us(ts: Any) -> Optional[int]:
    if not isinstance(ts, int):
        return None
    if ts <= 0:
        return None
    abs_ts = abs(ts)
    if abs_ts > 1_000_000_000_000_000:
        return ts
    if abs_ts > 1_000_000_000_000:
        return ts * 1_000
    return ts * 1_000_000


def coerce_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def select_runtime_threshold(payload: Dict[str, Any]) -> Optional[float]:
    thresholds = payload.get("thresholds")
    if not isinstance(thresholds, list):
        return None
    if len(thresholds) <= PNLU_RUNTIME_THRESHOLD_INDEX:
        return None
    return coerce_number(thresholds[PNLU_RUNTIME_THRESHOLD_INDEX])


def evaluate_runtime_pnlu_check(payload: Dict[str, Any]) -> Dict[str, Any]:
    now_us = int(time.time() * 1_000_000)
    ts = payload.get("ts")
    target_ts = payload.get("target_ts")
    ready = payload.get("ready")
    factor = coerce_number(payload.get("factor"))
    threshold = select_runtime_threshold(payload)

    ts_us = normalize_pnlu_ts_us(ts)
    if ts_us is not None and ts_us > now_us:
        return {
            "ok": False,
            "reason": "ts_in_future",
            "factor": factor,
            "threshold": threshold,
            "ts": ts,
            "target_ts": target_ts,
            "age_sec": None,
            "target_age_sec": None,
        }

    target_ts_us = normalize_pnlu_ts_us(target_ts)
    if target_ts_us is not None and target_ts_us > now_us:
        return {
            "ok": False,
            "reason": "target_ts_in_future",
            "factor": factor,
            "threshold": threshold,
            "ts": ts,
            "target_ts": target_ts,
            "age_sec": None,
            "target_age_sec": None,
        }

    age_sec = None
    if ts_us is not None:
        age_sec = (now_us - ts_us) // 1_000_000

    target_age_sec = None
    if target_ts_us is not None:
        target_age_sec = (now_us - target_ts_us) // 1_000_000

    ts_fresh = age_sec is not None and age_sec <= PNLU_MAX_AGE_SECS
    target_ts_fresh = target_age_sec is not None and target_age_sec <= PNLU_MAX_AGE_SECS
    factor_ok = factor is not None and threshold is not None and factor > threshold
    ok = (
        ts_fresh
        and target_ts_fresh
        and ready is True
        and factor is not None
        and threshold is not None
        and factor_ok
    )

    if ok:
        reason = "ok"
    elif ts_us is None:
        reason = "missing_ts"
    elif not ts_fresh:
        reason = "ts_timeout"
    elif target_ts_us is None:
        reason = "missing_target_ts"
    elif not target_ts_fresh:
        reason = "target_ts_timeout"
    elif ready is not True:
        reason = "ready_false" if ready is False else "missing_ready"
    elif factor is None:
        reason = "missing_factor"
    elif threshold is None:
        reason = "missing_threshold"
    else:
        reason = "factor_not_gt_threshold"

    return {
        "ok": ok,
        "reason": reason,
        "factor": factor,
        "threshold": threshold,
        "ts": ts,
        "target_ts": target_ts,
        "age_sec": age_sec,
        "target_age_sec": target_age_sec,
    }


def build_threshold_pairs(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    quantiles = payload.get("quantiles")
    thresholds = payload.get("thresholds")
    if not isinstance(quantiles, list) or not isinstance(thresholds, list):
        return []

    pairs: List[Dict[str, Any]] = []
    for quantile, threshold in zip(quantiles, thresholds):
        pairs.append({"quantile": quantile, "threshold": threshold})
    return pairs


@dataclass
class AppConfig:
    host: str
    port: int
    redis_url: str
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]
    default_open_venue: str
    default_hedge_venue: str


class PnluStatusServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler_cls, config: AppConfig, redis_client):
        super().__init__(server_address, handler_cls)
        self.config = config
        self.redis = redis_client


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "PnluStatusServer/0.1"

    @property
    def app(self) -> PnluStatusServer:
        return self.server  # type: ignore[return-value]

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), fmt % args)
        )

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/" or parsed.path == "":
                self._handle_index()
                return
            if parsed.path == "/healthz":
                self._send_json({"ok": True})
                return
            if parsed.path == "/api/bootstrap":
                self._send_json(self._bootstrap())
                return
            if parsed.path == "/api/symbols":
                self._handle_get_symbols(parsed)
                return
            if parsed.path == "/api/pnlu-status":
                self._handle_get_pnlu_status(parsed)
                return
            self.send_error(404, "not found")
        except ValueError as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=400)
        except Exception as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=500)

    def _bootstrap(self) -> Dict[str, Any]:
        return {
            "venues": build_all_venues(),
            "default_open_venue": self.app.config.default_open_venue,
            "default_hedge_venue": self.app.config.default_hedge_venue,
            "poll_secs": DEFAULT_POLL_SECS,
        }

    def _handle_index(self) -> None:
        bootstrap = json.dumps(self._bootstrap(), separators=(",", ":"))
        html = INDEX_HTML_TEMPLATE.replace("__BOOTSTRAP__", bootstrap)
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_get_symbols(self, parsed) -> None:
        params = parse_qs(parsed.query)
        open_venue, hedge_venue = self._resolve_venues(params)
        profile = resolve_profile(open_venue, hedge_venue)
        suffix = resolve_suffix(profile)
        symbols = scan_symbols_by_suffix(self.app.redis, suffix)
        self._send_json(
            {
                "open_venue": open_venue,
                "hedge_venue": hedge_venue,
                "suffix": suffix,
                "source": "redis_scan_by_suffix" if symbols else "empty",
                "symbols": symbols,
            }
        )

    def _handle_get_pnlu_status(self, parsed) -> None:
        params = parse_qs(parsed.query)
        open_venue, hedge_venue = self._resolve_venues(params)
        profile = resolve_profile(open_venue, hedge_venue)
        suffix = resolve_suffix(profile)
        symbol = normalize_symbol(self._first_query_value(params, "symbol"))
        if symbol:
            symbols = [symbol]
            symbol_source = "explicit_symbol"
        else:
            symbols = scan_symbols_by_suffix(self.app.redis, suffix)
            symbol_source = "redis_scan_by_suffix" if symbols else "empty"

        items = [
            self._read_pnlu_status(symbol_item, open_venue, hedge_venue, profile, suffix)
            for symbol_item in symbols
        ]
        self._send_json(
            {
                "open_venue": open_venue,
                "hedge_venue": hedge_venue,
                "suffix": suffix,
                "symbol_source": symbol_source,
                "items": items,
            }
        )

    def _read_pnlu_status(
        self,
        symbol: str,
        open_venue: str,
        hedge_venue: str,
        profile: str,
        suffix: str,
    ) -> Dict[str, Any]:
        key = make_pnlu_key(symbol, open_venue, hedge_venue)
        raw = self.app.redis.get(key)
        if raw is None:
            return {
                "symbol": symbol,
                "suffix": suffix,
                "key": key,
                "status": "missing",
                "ready": None,
                "note": "missing_key",
                "factor": None,
                "threshold": None,
                "ts": None,
                "age_sec": None,
            }

        if isinstance(raw, bytes):
            text = raw.decode("utf-8", "ignore")
        else:
            text = str(raw)

        try:
            payload = json.loads(text)
        except Exception as exc:
            return {
                "symbol": symbol,
                "suffix": suffix,
                "key": key,
                "status": "error",
                "ready": None,
                "note": f"invalid_json:{exc}",
                "factor": None,
                "threshold": None,
                "ts": None,
                "age_sec": None,
            }

        if not isinstance(payload, dict):
            return {
                "symbol": symbol,
                "suffix": suffix,
                "key": key,
                "status": "error",
                "ready": None,
                "note": "invalid_payload_type",
                "factor": None,
                "threshold": None,
                "ts": None,
                "age_sec": None,
            }

        quantiles = payload.get("quantiles")
        thresholds = payload.get("thresholds")
        threshold_pairs = build_threshold_pairs(payload)
        runtime = evaluate_runtime_pnlu_check(payload)
        ready = payload.get("ready")
        status = "ready" if runtime["ok"] else "not_ready"
        note = runtime["reason"]

        return {
            "symbol": symbol,
            "suffix": suffix,
            "key": key,
            "status": status,
            "ready": ready,
            "note": note,
            "runtime_ok": runtime["ok"],
            "runtime_reason": runtime["reason"],
            "factor": runtime["factor"],
            "threshold": runtime["threshold"],
            "threshold_pairs": threshold_pairs,
            "quantiles": quantiles if isinstance(quantiles, list) else [],
            "thresholds": thresholds if isinstance(thresholds, list) else [],
            "ts": runtime["ts"],
            "target_ts": runtime["target_ts"],
            "age_sec": runtime["age_sec"],
            "target_age_sec": runtime["target_age_sec"],
        }

    def _resolve_venues(self, params: Dict[str, List[str]]) -> tuple[str, str]:
        open_venue = normalize_venue(self._first_query_value(params, "open_venue"))
        hedge_venue = normalize_venue(self._first_query_value(params, "hedge_venue"))
        if open_venue and hedge_venue:
            return open_venue, hedge_venue
        return self.app.config.default_open_venue, self.app.config.default_hedge_venue

    def _first_query_value(self, params: Dict[str, List[str]], key: str) -> str:
        values = params.get(key)
        if not values:
            return ""
        return values[0]

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_error(self, code: int, message: Optional[str] = None, explain: Optional[str] = None) -> None:
        payload = {"ok": False, "error": message or "error", "status": code}
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PNLU status web server")
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", ""))
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=6327)
    parser.add_argument("--redis-host", default="127.0.0.1")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-db", type=int, default=0)
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--exchange", default="")
    parser.add_argument("--open-venue", default="")
    parser.add_argument("--hedge-venue", default="")
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> AppConfig:
    exchange = default_exchange_name(args.exchange)
    default_open_venue, default_hedge_venue = EXCHANGE_DEFAULTS[exchange]
    open_venue = normalize_venue(args.open_venue) or default_open_venue
    hedge_venue = normalize_venue(args.hedge_venue) or default_hedge_venue
    redis_url = (args.redis_url or "").strip()
    return AppConfig(
        host=args.host,
        port=args.port,
        redis_url=redis_url,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_password=args.redis_password,
        default_open_venue=open_venue,
        default_hedge_venue=hedge_venue,
    )


def main() -> int:
    args = parse_args()
    config = build_config(args)

    redis = try_import_redis()
    if redis is None:
        print("redis package is required. Install with: pip install redis", file=sys.stderr)
        return 2

    if config.redis_url:
        redis_client = redis.from_url(config.redis_url, decode_responses=False)
    else:
        redis_client = redis.Redis(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            password=config.redis_password,
            decode_responses=False,
        )
    try:
        redis_client.ping()
    except Exception as exc:
        target = config.redis_url or f"{config.redis_host}:{config.redis_port}/{config.redis_db}"
        print(f"failed to connect redis {target}: {exc}", file=sys.stderr)
        return 2

    server = PnluStatusServer((config.host, config.port), RequestHandler, config, redis_client)
    print(
        f"PNLU status server listening on http://{config.host}:{config.port} "
        f"(redis={config.redis_url or f'{config.redis_host}:{config.redis_port}/{config.redis_db}'}, "
        f"default_pair={config.default_open_venue}->{config.default_hedge_venue})"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
