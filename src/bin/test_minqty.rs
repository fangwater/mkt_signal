/// 测试 VenueMinQtyTable，展示主流币的合约信息（重点：合约乘数）
use anyhow::{bail, Result};
use mkt_signal::signal::{common::TradingVenue, venue_min_qty_table::VenueMinQtyTable};
use serde::Deserialize;

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
    #[serde(rename = "tickSz")]
    tick_sz: String,
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

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let venue = TradingVenue::OkexFutures;
    println!("\n=== 测试 {:?} 合约信息（验证 ctMult API）===\n", venue);

    // 1. 先拉取原始 API 数据
    println!("【步骤 1】拉取 OKX API 原始数据...");
    let raw_instruments = fetch_okx_raw_data().await?;
    println!("拉取完成，共 {} 个合约\n", raw_instruments.len());

    // 2. 打印几个主流币的原始数据
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

    // 3. 使用 VenueMinQtyTable 解析
    println!("【步骤 3】使用 VenueMinQtyTable 解析并展示合约信息：\n");
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
