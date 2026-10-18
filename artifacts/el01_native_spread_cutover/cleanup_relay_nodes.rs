use anyhow::Result;
use iceoryx2::{prelude::*,config::Config,node::{NodeState,NodeView}};
fn main()->Result<()> {
 let execute=std::env::args().any(|s|s=="--execute");
 Node::<ipc::Service>::list(Config::global_config(),|state| {
  if let NodeState::<ipc::Service>::Dead(dead)=state {
   let pid=dead.id().pid().value();
   if pid==405428 || pid==1063353 || pid==1063374 {
    println!("dead relay/native node pid={} id={} execute={}",pid,dead.id().value(),execute);
    if execute { println!("cleanup={:?}",dead.remove_stale_resources()); }
   }
  }
  CallbackProgression::Continue
 })?;
 Ok(())
}
