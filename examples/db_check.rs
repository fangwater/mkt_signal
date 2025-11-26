// 直接在 examples 目录创建一个检查脚本
use rocksdb::{DB, Options, IteratorMode, ColumnFamilyDescriptor, Direction};

fn main() {
    let path = "/home/ubuntu/fr_trade/data/persist_manager";
    
    let mut opts = Options::default();
    opts.create_if_missing(false);
    
    let cfs = vec![
        ColumnFamilyDescriptor::new("default", Options::default()),
        ColumnFamilyDescriptor::new("signals_arb_open", Options::default()),
        ColumnFamilyDescriptor::new("signals_arb_hedge", Options::default()),
        ColumnFamilyDescriptor::new("signals_arb_cancel", Options::default()),
        ColumnFamilyDescriptor::new("signals_arb_close", Options::default()),
        ColumnFamilyDescriptor::new("trade_updates", Options::default()),
        ColumnFamilyDescriptor::new("order_updates", Options::default()),
    ];
    
    let db = DB::open_cf_descriptors(&opts, path, cfs).expect("Failed to open DB");
    
    println!("=== order_updates (last 3 keys) ===");
    let cf = db.cf_handle("order_updates").unwrap();
    for (i, item) in db.iterator_cf(cf, IteratorMode::End).enumerate() {
        if i >= 3 { break; }
        let (key, value) = item.unwrap();
        let key_str = String::from_utf8_lossy(&key);
        println!("key={} value_len={}", key_str, value.len());
        let hex: String = value.iter().take(80).map(|b| format!("{:02x}", b)).collect();
        println!("  hex={}", hex);
    }
    
    println!("\n=== trade_updates (last 3 keys) ===");
    let cf = db.cf_handle("trade_updates").unwrap();
    for (i, item) in db.iterator_cf(cf, IteratorMode::End).enumerate() {
        if i >= 3 { break; }
        let (key, value) = item.unwrap();
        let key_str = String::from_utf8_lossy(&key);
        println!("key={} value_len={}", key_str, value.len());
        let hex: String = value.iter().take(80).map(|b| format!("{:02x}", b)).collect();
        println!("  hex={}", hex);
    }
}
