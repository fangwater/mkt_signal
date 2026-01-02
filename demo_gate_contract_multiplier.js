#!/usr/bin/env node
"use strict";

const https = require("https");

function hasFlag(flag) {
  return process.argv.includes(`--${flag}`);
}

function getArg(flag, fallback) {
  const idx = process.argv.indexOf(`--${flag}`);
  if (idx === -1 || idx + 1 >= process.argv.length) {
    return fallback;
  }
  return process.argv[idx + 1];
}

function printHelp() {
  console.log(`Gate futures contract multiplier demo

Usage:
  node demo_gate_contract_multiplier.js --contract APT_USDT --size 1 --settle usdt

Options:
  --contract  Futures contract (default: APT_USDT)
  --size      Contract size (default: 1)
  --settle    Settle currency (default: usdt)
  --price     Price override for notional calc (optional)
  --raw       Print raw JSON response
  --help      Show this help
`);
}

function httpGetJson(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        let body = "";
        res.on("data", (chunk) => {
          body += chunk;
        });
        res.on("end", () => {
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            try {
              resolve(JSON.parse(body));
            } catch (err) {
              reject(new Error(`failed to parse JSON: ${err.message}`));
            }
            return;
          }
          reject(new Error(`HTTP ${res.statusCode}: ${body}`));
        });
      })
      .on("error", reject);
  });
}

async function main() {
  if (hasFlag("help")) {
    printHelp();
    return;
  }

  const contract = String(getArg("contract", "APT_USDT")).toUpperCase();
  const settle = String(getArg("settle", "usdt")).toLowerCase();
  const sizeStr = String(getArg("size", "1"));
  const priceStr = String(getArg("price", ""));
  const size = Number(sizeStr);
  const priceOverride = priceStr ? Number(priceStr) : NaN;

  const url = `https://api.gateio.ws/api/v4/futures/${settle}/contracts/${contract}`;
  const data = await httpGetJson(url);

  if (hasFlag("raw")) {
    console.log(JSON.stringify(data, null, 2));
  }

  const quanto = Number(data.quanto_multiplier);
  const minContracts = Number(data.order_size_min);
  const stepContracts = Number(data.order_size_step);
  const markPrice = Number(data.mark_price);
  const lastPrice = Number(data.last_price);
  const priceRef = Number.isFinite(priceOverride) && priceOverride > 0
    ? priceOverride
    : Number.isFinite(markPrice) && markPrice > 0
      ? markPrice
      : lastPrice;

  const baseQty = Number.isFinite(quanto) && Number.isFinite(size) ? size * quanto : NaN;
  const minBaseQty = Number.isFinite(quanto) && Number.isFinite(minContracts)
    ? minContracts * quanto
    : NaN;
  const stepBaseQty = Number.isFinite(quanto) && Number.isFinite(stepContracts)
    ? stepContracts * quanto
    : NaN;
  const notional = Number.isFinite(baseQty) && Number.isFinite(priceRef)
    ? baseQty * priceRef
    : NaN;

  console.log(`contract=${contract} settle=${settle}`);
  console.log(`quanto_multiplier=${data.quanto_multiplier}`);
  console.log(`order_size_min=${data.order_size_min} contracts`);
  console.log(`order_size_step=${data.order_size_step} contracts`);
  if (Number.isFinite(baseQty)) {
    console.log(`size_contracts=${size} -> base_qty=${baseQty}`);
  } else {
    console.log(`size_contracts=${sizeStr} -> base_qty=N/A`);
  }
  if (Number.isFinite(minBaseQty)) {
    console.log(`min_base_qty=${minBaseQty}`);
  }
  if (Number.isFinite(stepBaseQty)) {
    console.log(`step_base_qty=${stepBaseQty}`);
  }
  if (Number.isFinite(notional)) {
    console.log(`notional≈${notional} @ price=${priceRef}`);
  }
}

main().catch((err) => {
  console.error(`error: ${err.message}`);
  process.exit(1);
});
