use anyhow::{anyhow, Context, Result};
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteEndpoint {
    /// Endpoint type.
    #[serde(rename = "type")]
    pub kind: EndpointType,
    /// For `ipc`, this is the service name.
    /// For `zmq`, this is the full ZMQ address, e.g. tcp://0.0.0.0:16666.
    pub endpoint: String,
    /// Payload size in bytes for this route endpoint.
    pub size: usize,
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
            if r.from.size == 0 || r.to.size == 0 {
                return Err(anyhow!(
                    "route '{}' size must be >0 (from.size={} to.size={})",
                    r.id,
                    r.from.size,
                    r.to.size
                ));
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
            if r.from.size >= 32_768 || r.to.size >= 32_768 {
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
            if endpoint.kind == EndpointType::Zmq {
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
        | (EndpointType::Zmq, EndpointType::Ipc) => Ok(()),
        (EndpointType::Zmq, EndpointType::Zmq) => Err(anyhow!(
            "route '{}' does not support zmq->zmq forwarding",
            route.id
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
