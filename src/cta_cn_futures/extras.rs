//! Independent minute-frequency factor states and factual target ledgers.
use super::{
    atomic,
    config::Config,
    engine::{Bars, Engine, Ledger},
    history::Dominant,
    live::Context,
};
use crate::cta_factor_calc::config::Interval;
use anyhow::{ensure, Result};
use cn_market_proto::MinuteBar;
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub const INTERVALS: [Interval; 3] = [
    Interval::Minute,
    Interval::FiveMinutes,
    Interval::FifteenMinutes,
];

struct Slot {
    config: Config,
    engine: Engine,
    bars: Bars,
    saved_decision_ms: i64,
    error: Option<String>,
}
pub struct Extras {
    slots: BTreeMap<Interval, Slot>,
}
impl Extras {
    pub fn ledgers(&self) -> BTreeMap<Interval, Ledger> {
        self.slots
            .iter()
            .map(|(interval, slot)| (*interval, slot.engine.ledger.clone()))
            .collect()
    }
    pub fn new(root: &Config) -> Result<Self> {
        let mut slots = BTreeMap::new();
        for interval in INTERVALS {
            let config = root.for_interval(interval);
            let ledger = super::ledger(&config)?;
            let saved_decision_ms = ledger.last_decision_ms;
            slots.insert(
                interval,
                Slot {
                    engine: Engine::new(&config, ledger)?,
                    bars: Bars::new(interval),
                    config,
                    saved_decision_ms,
                    error: None,
                },
            );
        }
        Ok(Self { slots })
    }
    pub fn push(
        &mut self,
        row: MinuteBar,
        multiple: f64,
        main: &Dominant,
        context: &Context,
        live_after_ms: Option<i64>,
        fresh: bool,
    ) -> Result<()> {
        for (interval, slot) in &mut self.slots {
            if slot.error.is_some() {
                continue;
            }
            let result = (|| -> Result<()> {
                let mut closed = slot.bars.push(row.clone(), multiple)?;
                if slot
                    .bars
                    .last_minute_ms()
                    .is_some_and(|last| context.ends_after(last))
                {
                    if let Some(bar) = slot.bars.finish(multiple)? {
                        closed.push(bar);
                    }
                }
                for bar in closed {
                    let now = chrono::Utc::now().timestamp_millis();
                    let live = live_after_ms.is_some_and(|cutoff| {
                        fresh
                            && bar.end_ms > cutoff
                            && bar.end_ms <= now
                            && now - bar.end_ms <= 120_000
                    });
                    slot.engine.observe(&bar, &main.contract, multiple, live)?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                let message = format!("{error:#}");
                eprintln!(
                    "{} frequency paused; factual target retained: {}",
                    interval.name(),
                    message
                );
                slot.error = Some(message);
            }
        }
        Ok(())
    }
    pub fn check_enabled_ready(&self) -> Result<()> {
        for (interval, slot) in &self.slots {
            if !slot.config.enabled {
                continue;
            }
            ensure!(
                slot.error.is_none(),
                "{} frequency input: {}",
                interval.name(),
                slot.error.as_deref().unwrap_or("")
            );
            ensure!(
                slot.engine.ready(),
                "{} warm-up incomplete: {}/{} rules",
                interval.name(),
                slot.engine.latest.iter().filter(|r| r.ready).count(),
                slot.config.rules.len()
            );
        }
        Ok(())
    }
    pub fn prepare(&self, interval: Interval, next: &Config) -> Result<Engine> {
        let slot = self
            .slots
            .get(&interval)
            .ok_or_else(|| anyhow::anyhow!("unknown minute frequency"))?;
        ensure!(
            !next.enabled || slot.error.is_none(),
            "frequency data needs repair before configuration: {}",
            slot.error.as_deref().unwrap_or("")
        );
        let replacement = slot.engine.reconfigure(next)?;
        ensure!(
            replacement.ledger == slot.engine.ledger,
            "frequency change modified factual positions"
        );
        Ok(replacement)
    }
    pub fn apply(&mut self, interval: Interval, config: Config, engine: Engine) -> Result<()> {
        let slot = self
            .slots
            .get_mut(&interval)
            .ok_or_else(|| anyhow::anyhow!("unknown minute frequency"))?;
        slot.config = config;
        slot.engine = engine;
        Ok(())
    }
    pub fn persist(&mut self) -> Result<()> {
        for (interval, slot) in &mut self.slots {
            if slot.saved_decision_ms != slot.engine.ledger.last_decision_ms {
                atomic(
                    &slot
                        .config
                        .data_directory
                        .join(format!("positions_{}.json", interval.name())),
                    &slot.engine.ledger,
                )?;
                atomic(
                    &slot
                        .config
                        .data_directory
                        .join(format!("targets_{}.json", interval.name())),
                    &slot.engine.targets(),
                )?;
                slot.saved_decision_ms = slot.engine.ledger.last_decision_ms;
            }
        }
        Ok(())
    }
    pub fn views(&self, live_ready: bool) -> Value {
        self.slots.iter().map(|(interval, slot)| (interval.name(), json!({
            "enabled":slot.config.enabled, "factor_ready":slot.error.is_none() && slot.engine.ready(),
            "live_ready":live_ready && slot.error.is_none() && slot.engine.ready(), "last_bar_end_ms":slot.engine.last_end_ms,
            "error":slot.error,
            "rules":slot.engine.latest, "positions":slot.engine.targets(),
        }))).collect()
    }
}
