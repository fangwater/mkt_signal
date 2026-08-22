use anyhow::{bail, Context, Result};
use clap::Parser;
use iceoryx2::config::Config;
use iceoryx2::node::{NodeId, NodeView};
use iceoryx2::prelude::*;
use iceoryx2::service::internal::ServiceInternal;
use iceoryx2::service::static_config::messaging_pattern::MessagingPattern;
use iceoryx2::service::{ipc, ServiceDetails};

#[derive(Debug, Parser)]
#[command(
    name = "iceoryx_remove_node_from_service",
    about = "Remove one stale iceoryx node from one service dynamic config"
)]
struct Args {
    /// Fully qualified service path, for example model_output/binance_futures_direction_model.
    service: String,

    /// Stale iceoryx node id.
    #[arg(long)]
    node_id: Option<u128>,

    /// Print service configuration and registered nodes without modifying iceoryx state.
    #[arg(long)]
    dry_run: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let details = find_service_details(&args.service)?;
    print_service_details(&details);
    let service_id = *details.static_details.service_id();

    if args.dry_run {
        return Ok(());
    }
    let node_id = args
        .node_id
        .map(node_id_from_value)
        .context("--node-id is required unless --dry-run is used")?;

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

fn find_service_details(service_name: &str) -> Result<ServiceDetails<ipc::Service>> {
    let mut found = None;

    ipc::Service::list(Config::global_config(), |details| {
        if details.static_details.name().as_str() == service_name {
            found = Some(details);
            return CallbackProgression::Stop;
        }

        CallbackProgression::Continue
    })
    .context("failed to list iceoryx services")?;

    match found {
        Some(details) => Ok(details),
        None => bail!("service not found in discovery: {service_name}"),
    }
}

fn print_service_details(details: &ServiceDetails<ipc::Service>) {
    let static_details = &details.static_details;
    println!(
        "service={} service_id={} pattern={}",
        static_details.name(),
        static_details.service_id().as_str(),
        static_details.messaging_pattern()
    );
    if let MessagingPattern::PublishSubscribe(config) = static_details.messaging_pattern() {
        println!(
            "publish_subscribe max_nodes={} max_publishers={} max_subscribers={} history_size={} subscriber_max_buffer_size={}",
            config.max_nodes(),
            config.max_publishers(),
            config.max_subscribers(),
            config.history_size(),
            config.subscriber_max_buffer_size()
        );
    }

    let Some(dynamic) = &details.dynamic_details else {
        println!("dynamic_details=unavailable");
        return;
    };
    println!("registered_nodes={}", dynamic.nodes.len());
    for state in &dynamic.nodes {
        let status = match state {
            NodeState::Alive(_) => "alive",
            NodeState::Dead(_) => "dead",
            NodeState::Inaccessible(_) => "inaccessible",
            NodeState::Undefined(_) => "undefined",
        };
        let node_details = match state {
            NodeState::Alive(node) => node.details().as_ref(),
            NodeState::Dead(node) => node.details().as_ref(),
            NodeState::Inaccessible(_) | NodeState::Undefined(_) => None,
        };
        let (name, executable) = node_details
            .map(|value| {
                (
                    value.name().as_str().to_owned(),
                    value.executable().to_string(),
                )
            })
            .unwrap_or_else(|| ("<unknown>".to_owned(), "<unknown>".to_owned()));
        println!(
            "node_id={} pid={} status={} name={} exe={}",
            state.node_id().value(),
            state.node_id().pid().value(),
            status,
            name,
            executable
        );
    }
}

fn node_id_from_value(value: u128) -> NodeId {
    // iceoryx2 exposes NodeId::value() but not the inverse. The public type is
    // #[repr(C)] over UniqueSystemId, whose public From<u128> uses the same layout.
    unsafe { std::mem::transmute::<u128, NodeId>(value) }
}
