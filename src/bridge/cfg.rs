use anyhow::{anyhow, Context, Result};
use runtime_common::redis_client::RedisSettings;
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

/// Bridge process configuration.
///
/// Each route directly describes its source and destination endpoint type:
/// - `ipc -> ipc`
/// - `ipc -> zmq`
/// - `zmq -> ipc`
/// - `redis -> zmq`
/// - `zmq -> redis`
#[derive(Debug, Clone, Deserialize)]
pub struct BridgeConfig {
    /// Forwarding routes.
    pub routes: Vec<RouteConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteConfig {
    /// Route id used as ZMQ multipart header and log tag.
    pub id: String,
    pub from: RouteEndpoint,
    pub to: RouteEndpoint,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EndpointType {
    Ipc,
    Zmq,
    Redis,
}

#[derive(Debug, Clone, Copy, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RedisKeyType {
    Hash,
    String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteEndpoint {
    /// Endpoint type.
    #[serde(rename = "type")]
    pub kind: EndpointType,
    /// For `ipc`, this is the service name.
    /// For `zmq`, this is the full ZMQ address, e.g. tcp://0.0.0.0:16666.
    /// For `redis`, this is the Redis key.
    pub endpoint: String,
    /// Payload size in bytes for IPC endpoints.
    #[serde(default)]
    pub size: usize,
    /// Redis connection used when `type: redis`.
    #[serde(default)]
    pub redis: RedisSettings,
    /// Redis value type used when `type: redis`.
    #[serde(default = "default_redis_key_type")]
    pub redis_type: RedisKeyType,
    /// Poll interval used by a `redis -> zmq` source route.
    #[serde(default = "default_redis_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Optional IceOryx service max_publishers override for open_or_create.
    #[serde(default)]
    pub max_publishers: Option<usize>,
    /// Optional IceOryx service max_subscribers override for open_or_create.
    #[serde(default)]
    pub max_subscribers: Option<usize>,
    /// Optional IceOryx service history size override for open_or_create.
    #[serde(default)]
    pub history_size: Option<usize>,
    /// Optional IceOryx subscriber_max_buffer_size override for open_or_create.
    #[serde(default)]
    pub subscriber_max_buffer_size: Option<usize>,
}

impl BridgeConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read bridge cfg {}", path.as_ref().display()))?;
        let cfg: BridgeConfig =
            serde_yaml::from_str(&content).context("failed to parse bridge cfg yaml")?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        let mut route_ids: HashSet<&str> = HashSet::new();

        for r in &self.routes {
            if r.id.trim().is_empty() {
                return Err(anyhow!("route.id cannot be empty"));
            }
            if !route_ids.insert(r.id.as_str()) {
                return Err(anyhow!("duplicate route id '{}'", r.id));
            }
            if r.from.endpoint.trim().is_empty() || r.to.endpoint.trim().is_empty() {
                return Err(anyhow!(
                    "route '{}' endpoint cannot be empty (from='{}' to='{}')",
                    r.id,
                    r.from.endpoint,
                    r.to.endpoint
                ));
            }
            validate_endpoint_options(&r.id, "from", &r.from)?;
            validate_endpoint_options(&r.id, "to", &r.to)?;
            validate_route_direction(r)?;
            if (r.from.kind == EndpointType::Ipc && r.from.size >= 32_768)
                || (r.to.kind == EndpointType::Ipc && r.to.size >= 32_768)
            {
                panic!(
                    "route '{}' uses too large payload size (from.size={} to.size={}); ipc_bridge does not support sizes >=32768",
                    r.id, r.from.size, r.to.size
                );
            }
        }

        Ok(())
    }
}

fn validate_endpoint_options(route_id: &str, side: &str, endpoint: &RouteEndpoint) -> Result<()> {
    if endpoint.kind == EndpointType::Ipc && endpoint.size == 0 {
        return Err(anyhow!(
            "route '{}' {}.size must be >0 for ipc endpoint",
            route_id,
            side
        ));
    }

    for (field, value) in [
        ("max_publishers", endpoint.max_publishers),
        ("max_subscribers", endpoint.max_subscribers),
        ("history_size", endpoint.history_size),
        (
            "subscriber_max_buffer_size",
            endpoint.subscriber_max_buffer_size,
        ),
    ] {
        if let Some(v) = value {
            if v == 0 {
                return Err(anyhow!(
                    "route '{}' {}.{} must be >0",
                    route_id,
                    side,
                    field
                ));
            }
            if endpoint.kind != EndpointType::Ipc {
                return Err(anyhow!(
                    "route '{}' {}.{} is only supported for ipc endpoints",
                    route_id,
                    side,
                    field
                ));
            }
        }
    }
    Ok(())
}

fn validate_route_direction(route: &RouteConfig) -> Result<()> {
    match (route.from.kind, route.to.kind) {
        (EndpointType::Ipc, EndpointType::Ipc)
        | (EndpointType::Ipc, EndpointType::Zmq)
        | (EndpointType::Zmq, EndpointType::Ipc)
        | (EndpointType::Redis, EndpointType::Zmq)
        | (EndpointType::Zmq, EndpointType::Redis) => {
            if route.from.kind == EndpointType::Redis && route.from.poll_interval_ms == 0 {
                return Err(anyhow!(
                    "route '{}' from.poll_interval_ms must be >0",
                    route.id
                ));
            }
            Ok(())
        }
        _ => Err(anyhow!(
            "route '{}' does not support {:?}->{:?} forwarding",
            route.id,
            route.from.kind,
            route.to.kind
        )),
    }
}

const fn default_redis_key_type() -> RedisKeyType {
    RedisKeyType::Hash
}

const fn default_redis_poll_interval_ms() -> u64 {
    5_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn parses_and_validates_local_ipc_cfg() {
        let yaml = r#"
routes:
  - id: local_askbid
    from:
      type: ipc
      endpoint: "dat_pbs/binance-futures/ask_bid_spread"
      size: 128
    to:
      type: ipc
      endpoint: "bridge/binance-futures/ask_bid_spread"
      size: 128
      max_subscribers: 64
"#;

        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.routes[0].to.kind, EndpointType::Ipc);
        assert_eq!(cfg.routes[0].to.max_subscribers, Some(64));
    }

    #[test]
    fn parses_and_validates_zmq_routes() {
        let yaml = r#"
routes:
  - id: outgoing_route
    from:
      type: ipc
      endpoint: "order_reqs/binance"
      size: 4096
    to:
      type: zmq
      endpoint: "tcp://10.0.0.2:5555"
      size: 4096
  - id: incoming_route
    from:
      type: zmq
      endpoint: "tcp://0.0.0.0:5555"
      size: 64
    to:
      type: ipc
      endpoint: "order_resps/binance"
      size: 64
"#;

        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn reject_duplicate_route_ids() {
        let yaml = r#"
routes:
  - id: duplicate
    from:
      type: ipc
      endpoint: "a"
      size: 64
    to:
      type: ipc
      endpoint: "b"
      size: 64
  - id: duplicate
    from:
      type: ipc
      endpoint: "c"
      size: 64
    to:
      type: ipc
      endpoint: "d"
      size: 64
"#;

        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn reject_zmq_endpoint_options() {
        let yaml = r#"
routes:
  - id: bad_route
    from:
      type: ipc
      endpoint: "order_reqs/binance"
      size: 4096
    to:
      type: zmq
      endpoint: "tcp://10.0.0.2:5555"
      size: 4096
      max_subscribers: 64
"#;

        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn reject_zmq_to_zmq() {
        let yaml = r#"
routes:
  - id: bad_route
    from:
      type: zmq
      endpoint: "tcp://0.0.0.0:5555"
      size: 64
    to:
      type: zmq
      endpoint: "tcp://10.0.0.2:5555"
      size: 64
"#;

        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn loads_public_bridge_configs() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let jp = BridgeConfig::load_from_file(root.join("config/ipc_bridge_public_jp.yaml"))
            .expect("load public jp bridge config");
        let hk = BridgeConfig::load_from_file(root.join("config/ipc_bridge_public_hk.yaml"))
            .expect("load public hk bridge config");
        let sg = BridgeConfig::load_from_file(root.join("config/ipc_bridge_public_sg.yaml"))
            .expect("load public sg bridge config");
        let model_sender = BridgeConfig::load_from_file(
            root.join("config/ipc_bridge_local_to_sg_binance_models.yaml"),
        )
        .expect("load local-to-sg model sender bridge config");
        let model_receiver = BridgeConfig::load_from_file(
            root.join("config/ipc_bridge_sg_public_binance_models.yaml"),
        )
        .expect("load sg model receiver bridge config");

        assert!(!jp.routes.is_empty());
        assert!(!hk.routes.is_empty());
        assert!(!sg.routes.is_empty());
        assert!(!model_sender.routes.is_empty());
        assert!(!model_receiver.routes.is_empty());
    }

    #[test]
    fn public_cross_host_route_ids_match_between_jp_and_hk() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let jp = BridgeConfig::load_from_file(root.join("config/ipc_bridge_public_jp.yaml"))
            .expect("load public jp bridge config");
        let hk = BridgeConfig::load_from_file(root.join("config/ipc_bridge_public_hk.yaml"))
            .expect("load public hk bridge config");

        let jp_outgoing: HashSet<String> = jp
            .routes
            .iter()
            .filter(|r| r.from.kind == EndpointType::Ipc && r.to.kind == EndpointType::Zmq)
            .map(|r| r.id.clone())
            .collect();
        let hk_incoming: HashSet<String> = hk
            .routes
            .iter()
            .filter(|r| r.from.kind == EndpointType::Zmq && r.to.kind == EndpointType::Ipc)
            .map(|r| r.id.clone())
            .collect();

        assert!(
            hk_incoming.is_subset(&jp_outgoing),
            "HK incoming routes must be a subset of JP outgoing routes"
        );
        assert_eq!(
            hk_incoming,
            HashSet::from(["public_binance_futures_direction_model_output".to_string()])
        );
    }

    #[test]
    fn public_cross_host_route_ids_match_between_hk_and_sg() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let hk = BridgeConfig::load_from_file(root.join("config/ipc_bridge_public_hk.yaml"))
            .expect("load public hk bridge config");
        let sg = BridgeConfig::load_from_file(root.join("config/ipc_bridge_public_sg.yaml"))
            .expect("load public sg bridge config");

        let hk_to_sg: HashSet<String> = hk
            .routes
            .iter()
            .filter(|r| r.from.kind == EndpointType::Ipc && r.to.kind == EndpointType::Zmq)
            .filter(|r| r.to.endpoint.contains("47.131.162.78"))
            .map(|r| r.id.clone())
            .collect();
        let sg_incoming: HashSet<String> = sg
            .routes
            .iter()
            .filter(|r| r.from.kind == EndpointType::Zmq && r.to.kind == EndpointType::Ipc)
            .map(|r| r.id.clone())
            .collect();

        assert_eq!(hk_to_sg, sg_incoming);
    }

    #[test]
    fn local_to_sg_model_route_ids_match() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let sender = BridgeConfig::load_from_file(
            root.join("config/ipc_bridge_local_to_sg_binance_models.yaml"),
        )
        .expect("load local-to-sg model sender bridge config");
        let receiver = BridgeConfig::load_from_file(
            root.join("config/ipc_bridge_sg_public_binance_models.yaml"),
        )
        .expect("load sg model receiver bridge config");

        let outgoing: HashSet<String> = sender
            .routes
            .iter()
            .filter(|r| r.from.kind == EndpointType::Ipc && r.to.kind == EndpointType::Zmq)
            .map(|r| r.id.clone())
            .collect();
        let incoming: HashSet<String> = receiver
            .routes
            .iter()
            .filter(|r| r.from.kind == EndpointType::Zmq && r.to.kind == EndpointType::Ipc)
            .filter(|r| r.to.endpoint.contains("binance-futures-mid-re"))
            .map(|r| r.id.clone())
            .collect();

        assert_eq!(outgoing, incoming);
        assert_eq!(outgoing.len(), 3);
    }

    #[test]
    fn parses_redis_hash_routes_without_ipc_sizes() {
        let yaml = r#"
routes:
  - id: model_thresholds
    from:
      type: redis
      endpoint: "model_score_rolling_thresholds_mid_chg_30s"
      redis_type: hash
      poll_interval_ms: 3000
      redis:
        host: 127.0.0.1
        db: 0
    to:
      type: zmq
      endpoint: "tcp://10.0.0.2:6360"
  - id: model_thresholds_sink
    from:
      type: zmq
      endpoint: "tcp://0.0.0.0:6360"
    to:
      type: redis
      endpoint: "model_score_rolling_thresholds_mid_chg_30s"
      redis_type: hash
"#;

        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.routes[0].from.kind, EndpointType::Redis);
        assert_eq!(cfg.routes[0].from.redis_type, RedisKeyType::Hash);
        assert_eq!(cfg.routes[0].from.poll_interval_ms, 3000);
    }

    #[test]
    fn rejects_zero_redis_poll_interval() {
        let yaml = r#"
routes:
  - id: bad_redis_poll
    from:
      type: redis
      endpoint: "model_thresholds"
      poll_interval_ms: 0
    to:
      type: zmq
      endpoint: "tcp://10.0.0.2:6360"
"#;

        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }
}
