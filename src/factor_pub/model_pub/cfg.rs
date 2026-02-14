use anyhow::Result;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ModelPubConfig {
    pub model_manager_base_url: String,
    #[serde(default = "default_model_manager_request_timeout_ms")]
    pub model_manager_request_timeout_ms: u64,
    #[serde(default)]
    pub model_manager_password: Option<String>,
    #[serde(default)]
    pub model_manager_bearer_token: Option<String>,
    #[serde(default = "default_input_service_template")]
    pub input_service_template: String,
    #[serde(default = "default_output_service_template")]
    pub output_service_template: String,
}

impl ModelPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.model_manager_base_url.trim().is_empty() {
            anyhow::bail!("model_manager_base_url must not be empty");
        }
        if self.model_manager_request_timeout_ms == 0 {
            anyhow::bail!("model_manager_request_timeout_ms must be > 0");
        }
        if self.input_service_template.trim().is_empty() {
            anyhow::bail!("input_service_template must not be empty");
        }
        if self.output_service_template.trim().is_empty() {
            anyhow::bail!("output_service_template must not be empty");
        }
        if !self.input_service_template.contains("{model_name}") {
            anyhow::bail!("input_service_template must contain {{model_name}}");
        }
        if !self.output_service_template.contains("{model_name}") {
            anyhow::bail!("output_service_template must contain {{model_name}}");
        }
        Ok(())
    }

    pub fn render_input_service(&self, model_name: &str) -> Result<String> {
        render_service_template(
            "input_service_template",
            &self.input_service_template,
            model_name,
        )
    }

    pub fn render_output_service(&self, model_name: &str) -> Result<String> {
        render_service_template(
            "output_service_template",
            &self.output_service_template,
            model_name,
        )
    }
}

fn render_service_template(field_name: &str, template: &str, model_name: &str) -> Result<String> {
    let trimmed_model_name = model_name.trim();
    if trimmed_model_name.is_empty() {
        anyhow::bail!("model_name must not be empty");
    }

    if !template.contains("{model_name}") {
        anyhow::bail!("{field_name} must contain {{model_name}}");
    }

    let rendered = template.replace("{model_name}", trimmed_model_name);
    if rendered.trim().is_empty() {
        anyhow::bail!("{field_name} renders to empty value");
    }
    Ok(rendered)
}

fn default_model_manager_request_timeout_ms() -> u64 {
    5_000
}

fn default_input_service_template() -> String {
    "model_input/binance-futures/{model_name}".to_string()
}

fn default_output_service_template() -> String {
    "model_output/binance-futures/{model_name}".to_string()
}
