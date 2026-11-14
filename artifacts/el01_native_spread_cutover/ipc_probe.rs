use anyhow::Result;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use std::collections::{BTreeMap,BTreeSet};
use std::time::{Duration,Instant};
fn main() -> Result<()> {
 let node=NodeBuilder::new().name(&NodeName::new(&format!("el01_readonly_probe_{}",std::process::id()))?).create::<ipc::Service>()?;
 let names=["spread_pbs/binance-futures/ask_bid_spread","dat_pbs/binance-futures/derivatives"];
 let mut subs=Vec::new();
 for name in names { let s=node.service_builder(&ServiceName::new(name)?).publish_subscribe::<[u8;128]>().open()?; subs.push(s.subscriber_builder().buffer_size(8192).create()?); }
 let mut counts=[0u64;2]; let mut symbols=[BTreeSet::new(),BTreeSet::new()]; let mut types=BTreeMap::<u32,u64>::new(); let mut last_ts=[0i64;2];
 let until=Instant::now()+Duration::from_secs(30);
 while Instant::now()<until { let mut any=false;
  for (i,sub) in subs.iter().enumerate() { for _ in 0..512 { let Some(s)=sub.receive()? else {break}; any=true; let b=s.payload(); let t=u32::from_le_bytes(b[0..4].try_into()?); let n=u32::from_le_bytes(b[4..8].try_into()?) as usize; anyhow::ensure!(n>0 && 16+n<=b.len(),"invalid symbol length"); let symbol=std::str::from_utf8(&b[8..8+n])?.to_string(); counts[i]+=1; symbols[i].insert(symbol); *types.entry(t).or_default()+=1; last_ts[i]=last_ts[i].max(i64::from_le_bytes(b[8+n..16+n].try_into()?)); } }
  if !any { std::thread::sleep(Duration::from_micros(100)); }
 }
 println!("{}",serde_json::json!({"duration_secs":30,"channels":names,"counts":counts,"symbols":symbols,"message_types":types,"last_event_ts_us":last_ts}));
 anyhow::ensure!(counts.iter().all(|n| *n>0),"empty channel");
 anyhow::ensure!(symbols[0].iter().any(|s|s.ends_with("USDC")),"no USDC BBO");
 Ok(())
}
