use anyhow::Result;
use crypto_cta_manager::{config::AppConfig,model::{decode_uniform_order,decode_order_update,decode_trade_update},rocks_source::read_latest_column_families};
use std::{collections::{BTreeMap,BTreeSet},path::Path};
fn main()->Result<()> {
 let args:Vec<String>=std::env::args().collect();
 let cfg=AppConfig::load(Path::new(&args[1]))?;let since:i64=args[2].parse()?;
 for s in cfg.sources.iter().filter(|s|s.enabled) {
  let records=read_latest_column_families(&s.rocksdb_path,&["uniform_orders","order_updates","order_updates_unmatched","trade_updates","trade_updates_unmatched"],10000)?;
  let mut new_orders=BTreeSet::new();let mut accepted=BTreeSet::new();let mut filled=BTreeSet::new();let mut exchange_filled=BTreeSet::new();let mut statuses=BTreeMap::<String,u64>::new();let mut latest=0;let mut decode_errors=0;
  for (cf,rs) in records {for r in rs {
   match cf.as_str() {
    "uniform_orders"=>match decode_uniform_order(&r.key,&r.value) {
      Ok(e)=>{if e.create_ts_us>=since {new_orders.insert(e.client_order_id);} if e.update_ts_us>=since && e.amount_update>0.0 {filled.insert(e.client_order_id);} latest=latest.max(e.event_ts_us);},
      Err(_)=>decode_errors+=1},
    "order_updates"|"order_updates_unmatched"=>match decode_order_update(&r.key,&r.value) {
      Ok(e)=>{if e.event_ts_us>=since {latest=latest.max(e.event_ts_us);*statuses.entry(e.raw_status.clone()).or_default()+=1;if e.order_id>0 {accepted.insert(e.client_order_id);}}},
      Err(_)=>decode_errors+=1},
    _=>match decode_trade_update(&r.key,&r.value) {
      Ok(e)=>{if e.trade_ts_us>=since {filled.insert(e.client_order_id);exchange_filled.insert(e.client_order_id);latest=latest.max(e.trade_ts_us);}},
      Err(_)=>decode_errors+=1}
   }
  }}
  println!("{}",serde_json::json!({"env":s.id,"since_us":since,"new_orders":new_orders.len(),"exchange_orders_with_updates":accepted.len(),"new_orders_with_exchange_updates":new_orders.intersection(&accepted).count(),"filled_orders":filled.len(),"exchange_filled_orders":exchange_filled.len(),"order_update_statuses":statuses,"latest_event_us":latest,"decode_errors":decode_errors}));
 }
 Ok(())
}
