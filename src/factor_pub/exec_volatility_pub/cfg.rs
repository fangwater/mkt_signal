use anyhow::{ensure, Context, Result};
use period_pbs::kafka::KafkaConsumerConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolatilityPubConfig {
    pub kafka: KafkaInputConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaInputConfig {
    pub lookback_secs: u64,
    pub catchup_timeout_secs: u64,
    pub topic: String,
    pub cash_topic: String,
    #[serde(flatten)]
    pub consumer: KafkaConsumerConfig,
}

impl VolatilityPubConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
        let value: Self = toml::from_str(&raw).with_context(|| format!("parse {path}"))?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        let kafka = &self.kafka;
        kafka.consumer.validate()?;
        ensure!(
            kafka.lookback_secs >= 744 * 5 && kafka.catchup_timeout_secs > 0,
            "volatility replay needs at least 744 five-second bars and a positive timeout"
        );
        ensure!(
            kafka.topic == "binance-futures"
                && kafka.cash_topic == "binance-spot"
                && kafka.consumer.topics.len() == 2
                && kafka.consumer.topics.contains(&kafka.topic)
                && kafka.consumer.topics.contains(&kafka.cash_topic),
            "execution volatility requires the Binance futures and spot Kafka markets"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_requires_both_markets_and_complete_volatility_warmup() {
        let mut config: VolatilityPubConfig =
            toml::from_str(include_str!("../../../config/exec_volatility_pub.toml")).unwrap();
        config.validate().unwrap();
        config.kafka.lookback_secs = 3719;
        assert!(config.validate().is_err());
        config.kafka.lookback_secs = 7200;
        config.kafka.consumer.topics.pop();
        assert!(config.validate().is_err());
    }
}
