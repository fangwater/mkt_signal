use serde::Deserialize;

/// API key configuration shared by trading/account components.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKey {
    pub name: String,
    pub key: String,
    pub secret: String,
}
