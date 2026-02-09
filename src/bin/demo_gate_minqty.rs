/// Gate futures min-qty demo using VenueMinQtyTable and raw contract info.
use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use mkt_signal::signal::{common::TradingVenue, venue_min_qty_table::VenueMinQtyTable};
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(
    name = "demo_gate_minqty",
    about = "Gate futures min-qty demo (contracts -> base)"
)]
struct Args {
    /// Gate contract, e.g. SOL_USDT
    #[arg(long, default_value = "SOL_USDT")]
    contract: String,

    /// Settle currency, e.g. usdt
    #[arg(long, default_value = "usdt")]
    settle: String,

    /// Contract size in contracts (not base qty)
    #[arg(long, default_value_t = 1.0)]
    size: f64,

    /// Price override for notional calc (<=0 uses mark/last price)
    #[arg(long, default_value_t = 0.0)]
    price: f64,

    /// Print raw API JSON
    #[arg(long, default_value_t = false)]
    show_raw: bool,
}

async fn fetch_gate_contract(settle: &str, contract: &str) -> Result<GateContractResponse> {
    let url = format!(
        "https://api.gateio.ws/api/v4/futures/{}/contracts/{}",
        settle, contract
    );
    let client = reqwest::Client::new();
    let resp = client.get(&url).send().await?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        bail!("Gate API error: status={} body={}", status, body);
    }
    let json: Value = serde_json::from_str(&body)
        .with_context(|| format!("parse Gate contract: {}", contract))?;
    Ok(GateContractResponse { json, raw: body })
}

struct GateContractResponse {
    json: Value,
    raw: String,
}

fn value_to_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|v| v.parse::<f64>().ok()))
}

fn raw_field(value: Option<&Value>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "<missing>".to_string(),
    }
}

fn require_field<'a>(value: &'a Value, key: &str) -> Result<&'a Value> {
    value
        .get(key)
        .ok_or_else(|| anyhow!("missing field '{}'", key))
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let contract = args.contract.to_uppercase();
    let settle = args.settle.to_lowercase();
    let symbol_key = contract.replace('_', "");

    println!("\n=== Gate futures min-qty demo ===\n");
    println!(
        "contract={} settle={} size_contracts={:.8}\n",
        contract, settle, args.size
    );

    let gate = fetch_gate_contract(&settle, &contract).await?;
    if args.show_raw {
        println!("--- raw contract json ---\n{}\n--- end ---\n", gate.raw);
    }

    let name = require_field(&gate.json, "name")?;
    let quanto_value = require_field(&gate.json, "quanto_multiplier")?;
    let min_contracts_value = require_field(&gate.json, "order_size_min")?;

    let quanto = value_to_f64(quanto_value)
        .ok_or_else(|| anyhow!("invalid quanto_multiplier: {}", quanto_value))?;
    let min_contracts = value_to_f64(min_contracts_value)
        .ok_or_else(|| anyhow!("invalid order_size_min: {}", min_contracts_value))?;
    let step_contracts = gate.json.get("order_size_step").and_then(value_to_f64);
    let price_tick = gate.json.get("order_price_round").and_then(value_to_f64);
    let mark_price = gate.json.get("mark_price").and_then(value_to_f64);
    let last_price = gate.json.get("last_price").and_then(value_to_f64);
    let price_ref = if args.price > 0.0 {
        Some(args.price)
    } else {
        mark_price.or(last_price)
    };

    let base_qty = args.size * quanto;
    let min_base_qty = min_contracts * quanto;

    println!("raw name              = {}", raw_field(Some(name)));
    println!(
        "raw quanto_multiplier = {}",
        raw_field(gate.json.get("quanto_multiplier"))
    );
    println!(
        "raw order_size_min    = {}",
        raw_field(gate.json.get("order_size_min"))
    );
    println!(
        "raw order_size_step   = {}",
        raw_field(gate.json.get("order_size_step"))
    );
    println!(
        "raw order_price_round = {}",
        raw_field(gate.json.get("order_price_round"))
    );
    println!(
        "raw mark_price        = {}",
        raw_field(gate.json.get("mark_price"))
    );
    println!(
        "raw last_price        = {}",
        raw_field(gate.json.get("last_price"))
    );
    match price_tick {
        Some(price_tick) => println!("price_tick            = {:.12}", price_tick),
        None => println!("price_tick            = N/A"),
    }
    println!();
    println!(
        "contracts -> base_qty = {:.8} * {:.8} = {:.8}",
        args.size, quanto, base_qty
    );
    println!("min_base_qty          = {:.8}", min_base_qty);
    match step_contracts {
        Some(step_contracts) => println!("step_base_qty         = {:.8}", step_contracts * quanto),
        None => println!("step_base_qty         = N/A (order_size_step missing)"),
    }
    if let Some(price) = price_ref {
        let notional = base_qty * price;
        println!("notional~{:.8} @ price={:.8}", notional, price);
    } else {
        println!("notional: N/A (no price override/mark/last)");
    }
    println!(
        "over_hedge_factor~1/quanto = {:.6} (if contracts misread as base qty)",
        1.0 / quanto
    );

    println!("\n--- VenueMinQtyTable (GateFutures) ---");
    let mut table = VenueMinQtyTable::new(TradingVenue::GateFutures);
    table.refresh().await?;
    let min_qty = table.min_qty(&symbol_key);
    let step_size = table.step_size(&symbol_key);
    let price_tick_table = table.price_tick(&symbol_key);
    let contract_sz = table.contract_multiplier_opt(&symbol_key);

    println!("symbol_key   = {}", symbol_key);
    match min_qty {
        Some(min_qty) => println!("min_qty      = {:.12}", min_qty),
        None => println!("min_qty      = N/A"),
    }
    match step_size {
        Some(step_size) => println!("step_size    = {:.12}", step_size),
        None => println!("step_size    = N/A"),
    }
    match price_tick_table {
        Some(price_tick_table) => println!("price_tick   = {:.12}", price_tick_table),
        None => println!("price_tick   = N/A"),
    }
    match contract_sz {
        Some(contract_sz) => println!("contract_sz  = {:.12}", contract_sz),
        None => println!("contract_sz  = N/A"),
    }

    match min_qty {
        Some(min_qty) => {
            let min_qty_pass = base_qty + 1e-12 >= min_qty;
            println!(
                "min_qty check (base_qty vs min_qty): {}",
                if min_qty_pass { "PASS" } else { "FAIL" }
            );
        }
        None => println!("min_qty check (base_qty vs min_qty): SKIP (missing)"),
    }

    Ok(())
}
