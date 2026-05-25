use anyhow::{bail, Context, Result};
use clap::Parser;
use iceoryx2::config::Config;
use iceoryx2::node::NodeId;
use iceoryx2::prelude::*;
use iceoryx2::service::internal::ServiceInternal;
use iceoryx2::service::{ipc, service_id::ServiceId};

#[derive(Debug, Parser)]
#[command(
    name = "iceoryx_remove_node_from_service",
    about = "Remove one stale iceoryx node from one service dynamic config"
)]
struct Args {
    /// Fully qualified service path, for example model_output/binance_futures_direction_model.
    service: String,

    /// Stale iceoryx node id reported by inspect_iceoryx_service.
    #[arg(long)]
    node_id: u128,

    /// Print the resolved service id and exit without modifying iceoryx state.
    #[arg(long)]
    dry_run: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let service_id = find_service_id(&args.service)?;
    let node_id = node_id_from_value(args.node_id);

    println!(
        "service={} service_id={} node_id={} dry_run={}",
        args.service,
        service_id.as_str(),
        node_id.value(),
        args.dry_run
    );

    if args.dry_run {
        return Ok(());
    }

    <ipc::Service as ServiceInternal<ipc::Service>>::__internal_remove_node_from_service(
        &node_id,
        &service_id,
        Config::global_config(),
    )
    .map_err(|err| anyhow::anyhow!("{err:?}"))
    .context("failed to remove node from iceoryx service")?;

    println!(
        "removed node_id={} from service={}",
        node_id.value(),
        args.service
    );
    Ok(())
}

fn find_service_id(service_name: &str) -> Result<ServiceId> {
    let mut found = None;

    ipc::Service::list(Config::global_config(), |details| {
        if details.static_details.name().as_str() == service_name {
            found = Some(*details.static_details.service_id());
            return CallbackProgression::Stop;
        }

        CallbackProgression::Continue
    })
    .context("failed to list iceoryx services")?;

    match found {
        Some(service_id) => Ok(service_id),
        None => bail!("service not found in discovery: {service_name}"),
    }
}

fn node_id_from_value(value: u128) -> NodeId {
    // iceoryx2 exposes NodeId::value() but not the inverse. The public type is
    // #[repr(C)] over UniqueSystemId, whose public From<u128> uses the same layout.
    unsafe { std::mem::transmute::<u128, NodeId>(value) }
}
