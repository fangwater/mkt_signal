use anyhow::{bail, Context, Result};
use clap::Parser;
use iceoryx2::node::{NodeState, NodeView};
use iceoryx2::prelude::*;
use iceoryx2::service::dynamic_config::publish_subscribe::{PublisherDetails, SubscriberDetails};
use iceoryx2::service::ipc;
use std::collections::BTreeMap;

#[derive(Debug, Parser)]
#[command(
    name = "inspect_iceoryx_service",
    about = "Inspect an iceoryx publish-subscribe service and print node/publisher/subscriber owners"
)]
struct Args {
    /// Fully qualified service path, for example factor_pub/binance-margin/rl_vol
    service: String,

    /// Payload size in bytes. rl_vol uses 256; latency snapshots use 512.
    #[arg(long, default_value_t = 256)]
    payload_size: usize,
}

#[derive(Debug, Clone)]
struct NodeSummary {
    status: &'static str,
    pid: Option<i32>,
    name: String,
    exe: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let local_node = NodeBuilder::new()
        .name(&NodeName::new("inspect_iceoryx_service")?)
        .create::<ipc::Service>()
        .context("failed to create local iceoryx inspection node")?;

    let service_exists = print_service_discovery(&args.service)?;
    if !service_exists {
        bail!("service not found in discovery: {}", args.service);
    }

    match args.payload_size {
        64 => inspect_typed::<64>(&local_node, &args.service),
        128 => inspect_typed::<128>(&local_node, &args.service),
        256 => inspect_typed::<256>(&local_node, &args.service),
        512 => inspect_typed::<512>(&local_node, &args.service),
        1024 => inspect_typed::<1024>(&local_node, &args.service),
        2048 => inspect_typed::<2048>(&local_node, &args.service),
        other => {
            bail!("unsupported payload_size={other}; supported: 64, 128, 256, 512, 1024, 2048")
        }
    }
}

fn print_service_discovery(service_name: &str) -> Result<bool> {
    let mut found = false;

    ipc::Service::list(Config::global_config(), |details| {
        if details.static_details.name().as_str() != service_name {
            return CallbackProgression::Continue;
        }

        found = true;
        println!("service={}", details.static_details.name().as_str());
        println!(
            "service_id={}",
            details.static_details.service_id().as_str()
        );
        println!(
            "discovery_nodes={}",
            details
                .dynamic_details
                .as_ref()
                .map(|d| d.nodes.len())
                .unwrap_or(0)
        );

        if let Some(dynamic) = &details.dynamic_details {
            for node in &dynamic.nodes {
                print_node_state("discovery_node", node);
            }
        }

        CallbackProgression::Stop
    })
    .context("failed to list iceoryx services")?;

    Ok(found)
}

fn inspect_typed<const SIZE: usize>(node: &Node<ipc::Service>, service_name: &str) -> Result<()> {
    let pubsub = node
        .service_builder(&ServiceName::new(service_name)?)
        .publish_subscribe::<[u8; SIZE]>()
        .open()
        .with_context(|| {
            format!(
                "failed to open service={} with payload_size={SIZE}",
                service_name
            )
        })?;

    print_pubsub_details(&pubsub)
}

fn print_pubsub_details<const SIZE: usize>(
    pubsub: &iceoryx2::service::port_factory::publish_subscribe::PortFactory<
        ipc::Service,
        [u8; SIZE],
        (),
    >,
) -> Result<()> {
    println!("payload_size={SIZE}");
    println!("max_publishers={}", pubsub.static_config().max_publishers());
    println!(
        "max_subscribers={}",
        pubsub.static_config().max_subscribers()
    );
    println!(
        "subscriber_max_buffer_size={}",
        pubsub.static_config().subscriber_max_buffer_size()
    );
    println!("history_size={}", pubsub.static_config().history_size());
    println!(
        "active_publishers={}",
        pubsub.dynamic_config().number_of_publishers()
    );
    println!(
        "active_subscribers={}",
        pubsub.dynamic_config().number_of_subscribers()
    );

    let mut node_summaries = BTreeMap::new();
    pubsub
        .nodes(|node_state| {
            let key = node_state.node_id().value();
            node_summaries.insert(key, summarize_node_state(&node_state));
            CallbackProgression::Continue
        })
        .context("failed to list service nodes")?;

    println!("connected_nodes={}", node_summaries.len());
    for (node_id, summary) in &node_summaries {
        print_summary("connected_node", *node_id, summary);
    }

    pubsub.dynamic_config().list_publishers(|publisher| {
        print_publisher_details(publisher, &node_summaries);
        CallbackProgression::Continue
    });

    pubsub.dynamic_config().list_subscribers(|subscriber| {
        print_subscriber_details(subscriber, &node_summaries);
        CallbackProgression::Continue
    });

    Ok(())
}

fn summarize_node_state(node_state: &NodeState<ipc::Service>) -> NodeSummary {
    match node_state {
        NodeState::Alive(node) => {
            let (name, exe) = node
                .details()
                .as_ref()
                .map(|d| (d.name().as_str().to_owned(), d.executable().to_string()))
                .unwrap_or_else(|| ("<unknown>".to_owned(), "<unknown>".to_owned()));
            NodeSummary {
                status: "alive",
                pid: Some(node.id().pid().value() as i32),
                name,
                exe,
            }
        }
        NodeState::Dead(node) => {
            let (name, exe) = node
                .details()
                .as_ref()
                .map(|d| (d.name().as_str().to_owned(), d.executable().to_string()))
                .unwrap_or_else(|| ("<unknown>".to_owned(), "<unknown>".to_owned()));
            NodeSummary {
                status: "dead",
                pid: Some(node.id().pid().value() as i32),
                name,
                exe,
            }
        }
        NodeState::Inaccessible(node_id) => NodeSummary {
            status: "inaccessible",
            pid: Some(node_id.pid().value() as i32),
            name: "<inaccessible>".to_owned(),
            exe: "<inaccessible>".to_owned(),
        },
        NodeState::Undefined(node_id) => NodeSummary {
            status: "undefined",
            pid: Some(node_id.pid().value() as i32),
            name: "<undefined>".to_owned(),
            exe: "<undefined>".to_owned(),
        },
    }
}

fn print_node_state(prefix: &str, node_state: &NodeState<ipc::Service>) {
    let summary = summarize_node_state(node_state);
    print_summary(prefix, node_state.node_id().value(), &summary);
}

fn print_summary(prefix: &str, node_id: u128, summary: &NodeSummary) {
    println!(
        "{prefix} node_id={} status={} pid={} name={} exe={}",
        node_id,
        summary.status,
        summary
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "<unknown>".to_owned()),
        summary.name,
        summary.exe,
    );
}

fn print_publisher_details(
    publisher: &PublisherDetails,
    node_summaries: &BTreeMap<u128, NodeSummary>,
) {
    let node_id = publisher.node_id.value();
    let summary = node_summaries.get(&node_id);
    println!(
        "publisher node_id={} pid={} status={} name={} exe={} publisher_id={:?} samples={} max_slice_len={} data_segment_type={:?} max_segments={}",
        node_id,
        summary
            .and_then(|s| s.pid)
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "<unknown>".to_owned()),
        summary.map(|s| s.status).unwrap_or("<unknown>"),
        summary
            .map(|s| s.name.as_str())
            .unwrap_or("<unknown>"),
        summary
            .map(|s| s.exe.as_str())
            .unwrap_or("<unknown>"),
        publisher.publisher_id,
        publisher.number_of_samples,
        publisher.max_slice_len,
        publisher.data_segment_type,
        publisher.max_number_of_segments,
    );
}

fn print_subscriber_details(
    subscriber: &SubscriberDetails,
    node_summaries: &BTreeMap<u128, NodeSummary>,
) {
    let node_id = subscriber.node_id.value();
    let summary = node_summaries.get(&node_id);
    println!(
        "subscriber node_id={} pid={} status={} name={} exe={} subscriber_id={:?} buffer_size={}",
        node_id,
        summary
            .and_then(|s| s.pid)
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "<unknown>".to_owned()),
        summary.map(|s| s.status).unwrap_or("<unknown>"),
        summary.map(|s| s.name.as_str()).unwrap_or("<unknown>"),
        summary.map(|s| s.exe.as_str()).unwrap_or("<unknown>"),
        subscriber.subscriber_id,
        subscriber.buffer_size,
    );
}
