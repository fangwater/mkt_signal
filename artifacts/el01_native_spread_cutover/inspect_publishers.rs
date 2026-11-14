use anyhow::Result;
use iceoryx2::prelude::*;
fn main()->Result<()> {
 let n=NodeBuilder::new().name(&NodeName::new("el01_readonly_publisher_inspect")?).create::<ipc::Service>()?;
 for name in ["spread_pbs/binance-futures/ask_bid_spread","dat_pbs/binance-futures/derivatives"] {
 let s=n.service_builder(&ServiceName::new(name)?).publish_subscribe::<[u8;128]>().open()?;
 s.dynamic_config().list_publishers(|p| {println!("service={} node={} pid={} publisher={:?}",name,p.node_id.value(),p.node_id.pid().value(),p.publisher_id);CallbackProgression::Continue});
 }
 Ok(())
}
