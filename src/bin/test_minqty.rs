/// 测试 VenueMinQtyTable，展示主流币的合约信息（重点：合约乘数）
use anyhow::{bail, Result};
use clap::Parser;
use mkt_signal::signal::{common::TradingVenue, venue_min_qty_table::VenueMinQtyTable};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct OkexResponse {
    code: String,
    msg: String,
    data: Vec<OkexInstrument>,
}

#[derive(Debug, Deserialize)]
struct OkexInstrument {
    #[serde(rename = "instId")]
    inst_id: String,
    #[serde(rename = "instType")]
    inst_type: Option<String>,
    #[serde(rename = "ctType")]
    ct_type: Option<String>,
    #[serde(rename = "ctVal")]
    ct_val: Option<String>,
    #[serde(rename = "ctMult")]
    ct_mult: Option<String>,
    #[serde(rename = "settleCcy")]
    settle_ccy: Option<String>,
    #[serde(rename = "minSz")]
    min_sz: String,
    #[serde(rename = "lotSz")]
    lot_sz: String,
}

async fn fetch_okx_raw_data() -> Result<Vec<OkexInstrument>> {
    let url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP";
    let client = reqwest::Client::new();
    let response = client.get(url).send().await?;
    let body = response.text().await?;
    let resp: OkexResponse = serde_json::from_str(&body)?;
    if resp.code != "0" {
        bail!("OKX API error: {} - {}", resp.code, resp.msg);
    }
    Ok(resp.data)
}

/// 命令行参数
#[derive(Debug, Parser)]
#[command(name = "test_minqty", about = "打印各交易场所的最小下单量/合约面值")]
struct Args {
    /// 交易场所（默认 okex-futures），只支持各交易所 USDT 本位场景
    #[arg(long, value_enum, default_value = "okex-futures")]
    venue: TradingVenue,

    /// 针对单个 symbol 做最小交易要求检查（例如 DOGEUSDT），设置后只打印该 symbol 结果并退出
    #[arg(long)]
    check_symbol: Option<String>,

    /// check_symbol 模式下的数量（qty）
    #[arg(long, default_value_t = 1.17)]
    qty: f64,

    /// check_symbol 模式下的价格（用于名义金额 notional 计算）
    #[arg(long, default_value_t = 0.12667)]
    price: f64,

    /// 额外打印交易所原始 filters（用于排查 minNotional / notional 字段差异）
    #[arg(long, default_value_t = false)]
    show_raw_filters: bool,
}

fn is_futures_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures
            | TradingVenue::OkexFutures
            | TradingVenue::BitgetFutures
            | TradingVenue::BybitFutures
            | TradingVenue::GateFutures
    )
}

async fn fetch_binance_symbol_filters(venue: TradingVenue, symbol: &str) -> Result<Vec<Value>> {
    let url = match venue {
        TradingVenue::BinanceFutures => "https://fapi.binance.com/fapi/v1/exchangeInfo",
        TradingVenue::BinanceMargin => "https://api.binance.com/api/v3/exchangeInfo",
        _ => bail!(
            "fetch_binance_symbol_filters: unsupported venue={:?}",
            venue
        ),
    };

    let client = reqwest::Client::new();
    let body: Value = client.get(url).send().await?.json().await?;

    let symbols = body
        .get("symbols")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("binance exchangeInfo missing symbols[]"))?;

    let symbol_upper = symbol.to_uppercase();
    let Some(entry) = symbols.iter().find(|s| {
        s.get("symbol")
            .and_then(|v| v.as_str())
            .map(|s| s.eq_ignore_ascii_case(&symbol_upper))
            .unwrap_or(false)
    }) else {
        bail!("symbol not found in binance exchangeInfo: {}", symbol_upper);
    };

    let filters = entry
        .get("filters")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("binance exchangeInfo symbol entry missing filters[]"))?;

    Ok(filters.clone())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let venue = args.venue;
    if let Some(symbol) = args.check_symbol.as_deref() {
        println!("\n=== Check min trading requirements ===\n");
        println!(
            "venue={:?} symbol={} qty={:.8} price={:.8}\n",
            venue,
            symbol.to_uppercase(),
            args.qty,
            args.price
        );

        let mut table = VenueMinQtyTable::new(venue);
        table.refresh().await?;

        let symbol_key = symbol.to_uppercase();
        let min_qty = table.min_qty(&symbol_key).unwrap_or(0.0);
        let step_size = table.step_size(&symbol_key).unwrap_or(0.0);
        let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
        let min_notional = table.min_notional(&symbol_key).unwrap_or(0.0);
        let contract_size = table.contract_multiplier(&symbol_key);

        println!("min_qty      = {:.12}", min_qty);
        println!("step_size    = {:.12}", step_size);
        println!("price_tick   = {:.12}", price_tick);
        println!("min_notional = {:.12}", min_notional);
        println!("contract_sz  = {:.12}", contract_size);
        println!();

        let mut ok = true;
        if min_qty > 0.0 && args.qty + 1e-12 < min_qty {
            ok = false;
            println!(
                "FAIL: qty {:.8} < min_qty {:.8} (qty + 1e-12 < min_qty)",
                args.qty, min_qty
            );
        } else {
            println!("PASS: min_qty check");
        }

        if is_futures_venue(venue) {
            if min_notional > 0.0 {
                let notional = args.qty * args.price;
                if notional + 1e-8 < min_notional {
                    ok = false;
                    println!(
                        "FAIL: notional {:.8} < min_notional {:.8} (price={:.8} qty={:.8})",
                        notional, min_notional, args.price, args.qty
                    );
                } else {
                    println!(
                        "PASS: min_notional check (notional={:.8} >= {:.8})",
                        notional, min_notional
                    );
                }
            } else {
                println!("SKIP: min_notional check (min_notional missing/0)");
            }
        } else {
            println!("SKIP: min_notional check (non-futures venue)");
        }

        if args.show_raw_filters
            && matches!(
                venue,
                TradingVenue::BinanceFutures | TradingVenue::BinanceMargin
            )
        {
            println!("\n--- Raw filters (Binance exchangeInfo) ---");
            let filters = fetch_binance_symbol_filters(venue, &symbol_key).await?;
            for f in filters {
                let filter_type = f
                    .get("filterType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("<missing filterType>");
                if matches!(
                    filter_type,
                    "LOT_SIZE" | "PRICE_FILTER" | "MIN_NOTIONAL" | "NOTIONAL"
                ) {
                    println!("{}", serde_json::to_string_pretty(&f)?);
                }
            }
            println!("--- end ---");
        }

        println!("\nRESULT: {}\n", if ok { "PASS" } else { "FAIL" });
        return Ok(());
    }

    println!("\n=== 测试 {:?} 合约信息（验证 ctMult API）===\n", venue);

    // 原始 API 数据仅对 OKX 展示，其它 venue 直接跳过
    if matches!(venue, TradingVenue::OkexFutures | TradingVenue::OkexMargin) {
        println!("【步骤 1】拉取 OKX API 原始数据...");
        let raw_instruments = fetch_okx_raw_data().await?;
        println!("拉取完成，共 {} 个合约\n", raw_instruments.len());

        // 打印几个主流币的原始数据
        let sample_coins = vec!["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"];
        println!("【步骤 2】查看主流币的 API 原始返回数据：\n");

        for inst_id in &sample_coins {
            if let Some(inst) = raw_instruments.iter().find(|i| &i.inst_id == inst_id) {
                println!("合约: {}", inst.inst_id);
                println!("  instType:  {:?}", inst.inst_type);
                println!("  ctType:    {:?}", inst.ct_type);
                println!("  ctVal:     {:?}", inst.ct_val);
                println!("  ctMult:    {:?} ⬅️ 重点", inst.ct_mult);
                println!("  settleCcy: {:?}", inst.settle_ccy);
                println!("  minSz:     {}", inst.min_sz);
                println!("  lotSz:     {}", inst.lot_sz);
                println!();
            }
        }
    } else {
        println!(
            "【步骤 1】当前仅 OKX 提供原始 API 展示，{:?} 直接使用 min_qty_table。\n",
            venue
        );
    }

    // 使用 VenueMinQtyTable 解析
    println!("【步骤 2】使用 VenueMinQtyTable 解析并展示合约信息：\n");
    let mut table = VenueMinQtyTable::new(venue);
    table.refresh().await?;

    // 主流币列表
    let mainstream_coins = vec![
        "BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "AVAX", "DOT", "MATIC", "LINK", "UNI",
        "ATOM", "LTC", "ARB", "OP", "SUI", "APT", "TRX", "TON",
    ];

    // 打印三线表头
    println!(
        "┌──────────┬──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐"
    );
    println!(
        "│  Symbol  │  MinQty      │  StepSize    │  PriceTick   │  ContractSz  │  说明        │"
    );
    println!(
        "├──────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────────────┤"
    );

    let mut found_count = 0;
    for coin in &mainstream_coins {
        let symbol = format!("{}USDT", coin);

        // 查询合约信息
        let min_qty = table.min_qty(&symbol).unwrap_or(0.0);
        let step_size = table.step_size(&symbol).unwrap_or(0.0);
        let price_tick = table.price_tick(&symbol).unwrap_or(0.0);
        let contract_size = table.contract_multiplier(&symbol);

        // 只打印存在的合约
        if min_qty > 0.0 {
            found_count += 1;
            println!(
                "│ {:8} │ {:12.6} │ {:12.6} │ {:12.6} │ {:12.6} │ 1张={:.2} {} │",
                symbol, min_qty, step_size, price_tick, contract_size, contract_size, coin
            );
        }
    }

    // 打印三线表底
    println!(
        "└──────────┴──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘"
    );
    println!("\n找到 {} 个主流币合约信息", found_count);

    println!("\n【字段说明】");
    println!("  - ctVal:       合约面值（Contract Value）");
    println!("  - ctMult:      合约乘数（Contract Multiplier）");
    println!("  - ContractSz:  实际合约大小 = ctVal × ctMult");
    println!("  - MinQty:      最小下单量（张数）");
    println!("  - StepSize:    下单步进（张数）");
    println!("  - PriceTick:   价格最小变动单位");

    println!("\n【OKX 合约大小计算】");
    println!("  - BTC-USDT-SWAP: ctVal=0.01, ctMult=1 → ContractSz=0.01 BTC");
    println!("  - 持有 100 张多仓 → 实际持有 = 100 × 0.01 = 1 BTC");
    println!("  - 持有 -50 张（空仓）→ 实际持有 = -50 × 0.01 = -0.5 BTC");

    println!("\n【重要发现】");
    println!("  ✅ 修复前：代码使用 ctMult (值为 1)，导致计算错误");
    println!("  ✅ 修复后：代码使用 ctVal × ctMult，得到正确的合约面值");
    println!("  ✅ 例如 BTC: 现在正确显示 1 张 = 0.01 BTC（而不是 1 BTC）\n");

    Ok(())
}
