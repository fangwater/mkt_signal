use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Bridge process configuration.
///
/// One bridge instance runs on each machine/namespace. `self_id` selects which
/// `nodes` entry is local, and `routes` describe how to forward messages between
/// two Iceoryx2 namespaces via ZMQ.
#[derive(Debug, Clone, Deserialize)]
pub struct BridgeConfig {
    /// The id of current bridge instance (must exist in `nodes`)
    pub self_id: String,
    /// All bridge nodes in the topology
    pub nodes: Vec<NodeConfig>,
    /// Forwarding routes (bidirectional routes should be declared twice)
    pub routes: Vec<RouteConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    /// Unique node id
    pub id: String,
    /// ZMQ address (e.g. tcp://0.0.0.0:5555 for bind, or tcp://10.0.0.2:5555 for connect)
    pub addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteConfig {
    /// Route id used as ZMQ multipart header
    pub id: String,
    pub from: RouteEndpoint,
    pub to: RouteEndpoint,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouteEndpoint {
    /// Node id of this endpoint
    pub node: String,
    /// Iceoryx base service name (will be namespaced via IPC_NAMESPACE unless starts with dat_pbs/)
    pub service: String,
    /// Iceoryx payload size in bytes for this endpoint
    pub size: usize,
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
        if self.self_id.trim().is_empty() {
            return Err(anyhow!("self_id cannot be empty"));
        }
        let mut nodes: HashMap<&str, &NodeConfig> = HashMap::new();
        for n in &self.nodes {
            if n.id.trim().is_empty() {
                return Err(anyhow!("node.id cannot be empty"));
            }
            if nodes.insert(n.id.as_str(), n).is_some() {
                return Err(anyhow!("duplicate node id '{}'", n.id));
            }
            if n.addr.trim().is_empty() {
                return Err(anyhow!("node.addr cannot be empty (id={})", n.id));
            }
        }

        if !nodes.contains_key(self.self_id.as_str()) {
            return Err(anyhow!("self_id '{}' not found in nodes", self.self_id));
        }

        for r in &self.routes {
            if r.id.trim().is_empty() {
                return Err(anyhow!("route.id cannot be empty"));
            }
            if !nodes.contains_key(r.from.node.as_str()) {
                return Err(anyhow!(
                    "route '{}' from.node '{}' not found in nodes",
                    r.id,
                    r.from.node
                ));
            }
            if !nodes.contains_key(r.to.node.as_str()) {
                return Err(anyhow!(
                    "route '{}' to.node '{}' not found in nodes",
                    r.id,
                    r.to.node
                ));
            }
            if r.from.service.trim().is_empty() || r.to.service.trim().is_empty() {
                return Err(anyhow!(
                    "route '{}' service cannot be empty (from='{}' to='{}')",
                    r.id,
                    r.from.service,
                    r.to.service
                ));
            }
            if r.from.size == 0 || r.to.size == 0 {
                return Err(anyhow!(
                    "route '{}' size must be >0 (from.size={} to.size={})",
                    r.id,
                    r.from.size,
                    r.to.size
                ));
            }
            if r.from.size >= 32_768 || r.to.size >= 32_768 {
                panic!(
                    "route '{}' uses too large payload size (from.size={} to.size={}); ipc_bridge does not support sizes >=32768",
                    r.id, r.from.size, r.to.size
                );
            }
        }

        Ok(())
    }

    pub fn self_node(&self) -> &NodeConfig {
        self.nodes
            .iter()
            .find(|n| n.id == self.self_id)
            .expect("self_id validated")
    }

    pub fn node_addr(&self, id: &str) -> Option<&str> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| n.addr.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_validates_minimal_cfg() {
        let yaml = r#"
self_id: pretrade_dc1
nodes:
  - id: pretrade_dc1
    addr: "tcp://0.0.0.0:5555"
  - id: tradeeng_dc2
    addr: "tcp://10.0.0.2:5555"
routes:
  - id: order_req
    from:
      node: pretrade_dc1
      service: "order_reqs/binance"
      size: 4096
    to:
      node: tradeeng_dc2
      service: "order_reqs/binance"
      size: 4096
"#;

        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.self_node().id, "pretrade_dc1");
        assert_eq!(cfg.node_addr("tradeeng_dc2"), Some("tcp://10.0.0.2:5555"));
    }

    #[test]
    fn reject_duplicate_node_ids() {
        let yaml = r#"
self_id: a
nodes:
  - id: a
    addr: "tcp://0.0.0.0:1"
  - id: a
    addr: "tcp://0.0.0.0:2"
routes: []
"#;
        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn reject_unknown_route_nodes() {
        let yaml = r#"
self_id: a
nodes:
  - id: a
    addr: "tcp://0.0.0.0:1"
routes:
  - id: r1
    from: { node: a, service: "x", size: 64 }
    to: { node: b, service: "x", size: 64 }
"#;
        let cfg: BridgeConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.validate().is_err());
    }
}
