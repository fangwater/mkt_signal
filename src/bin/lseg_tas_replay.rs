//! Stream LSEG Tick History TAS gzip parts into structured, filled-only events.
//!
//! Python `preprocess/lseg/tas_replay.py` is the correctness baseline. Every
//! nonempty source cell must belong to `tas_column_rules.json`. Unknown `Type`
//! values, unknown column names, and leftover nonempty fields outside the
//! selected groups fail immediately so the rule table can be fixed.
//!
//! This binary does not write ClickHouse, does not synthesize bars, and does
//! not treat TAS L1 as 10-level depth.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use csv::StringRecord;
use flate2::read::GzDecoder;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Legacy `core` group: the original washable futures columns.
const CORE_COLUMNS: &[&str] = &[
    "#RIC",
    "Domain",
    "Date-Time",
    "GMT Offset",
    "Type",
    "Price",
    "Volume",
    "Market VWAP",
    "Bid Price",
    "Bid Size",
    "No. Buyers",
    "Ask Price",
    "Ask Size",
    "No. Sellers",
    "Qualifiers",
    "Seq. No.",
    "Exch Time",
    "Open",
    "High",
    "Low",
    "Open Interest",
    "Acc. Volume",
    "Turnover",
    "Unique Trade Identification",
    "Trade Sequence Number",
    "Halt Reason",
    "Trading Status",
];

const SPECIAL_TRADES_USER: &str = "Special Trades[USER]";
const EXPECTED_COLUMN_COUNT: usize = 294;

#[derive(Parser, Debug)]
#[command(name = "lseg_tas_replay")]
#[command(about = "Replay LSEG TAS parts into structured filled-only events")]
struct Args {
    #[arg(long, default_value = "config/lseg_tas_replay.toml")]
    config: PathBuf,
    /// Override the config's diagnostic source-row limit.
    #[arg(long)]
    max_source_rows: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayConfig {
    data_root: PathBuf,
    period: String,
    #[serde(default)]
    rics: Vec<String>,
    #[serde(default)]
    include_index_rics: bool,
    /// Restrict projection to these catalog groups. Empty means every catalogued column.
    #[serde(default)]
    field_groups: Vec<String>,
    #[serde(default = "default_dry_run")]
    dry_run: bool,
    /// JSONL of structured rows. Required unless dry_run is true.
    #[serde(default)]
    output_jsonl: Option<PathBuf>,
    #[serde(default)]
    max_source_rows: Option<u64>,
    #[serde(default = "default_column_rules")]
    column_rules: PathBuf,
}

fn default_column_rules() -> PathBuf {
    PathBuf::from("../preprocess/lseg/tas_column_rules.json")
}

fn default_dry_run() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
enum EventClass {
    TradePrintable,
    TradeSpecialUser,
    TradeVolumeOnly,
    TradePriceOnly,
    TradeEmpty,
    Quote,
    MktCondition,
    Correction,
}

impl EventClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::TradePrintable => "trade_printable",
            Self::TradeSpecialUser => "trade_special_user",
            Self::TradeVolumeOnly => "trade_volume_only",
            Self::TradePriceOnly => "trade_price_only",
            Self::TradeEmpty => "trade_empty",
            Self::Quote => "quote",
            Self::MktCondition => "mkt_condition",
            Self::Correction => "correction",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ColumnRules {
    #[serde(default)]
    policy: BTreeMap<String, String>,
    types: Vec<String>,
    required_identity: Vec<String>,
    columns: BTreeMap<String, String>,
}

impl ColumnRules {
    fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)
            .with_context(|| format!("read TAS column rules {}", path.display()))?;
        let rules: Self = serde_json::from_str(&text)
            .with_context(|| format!("parse TAS column rules {}", path.display()))?;
        if rules.columns.len() != EXPECTED_COLUMN_COUNT {
            bail!(
                "TAS column rules must catalogue {EXPECTED_COLUMN_COUNT} names, got {} from {}",
                rules.columns.len(),
                path.display()
            );
        }
        if rules.required_identity != ["#RIC", "Date-Time", "Type"] {
            bail!(
                "TAS required_identity must be [#RIC, Date-Time, Type], got {:?}",
                rules.required_identity
            );
        }
        if rules.policy.get("extract").map(String::as_str) != Some("filled_only") {
            bail!(
                "TAS column rules must declare policy.extract=filled_only, got {:?}",
                rules.policy.get("extract")
            );
        }
        let known_types: BTreeSet<&str> = ["Trade", "Quote", "Mkt. Condition", "Correction"]
            .into_iter()
            .collect();
        let listed: BTreeSet<&str> = rules.types.iter().map(String::as_str).collect();
        if listed != known_types {
            bail!("TAS column rules types must be {known_types:?}, got {listed:?}");
        }
        Ok(rules)
    }

    fn group_of(&self, name: &str) -> Result<&str> {
        self.columns.get(name).map(String::as_str).ok_or_else(|| {
            anyhow!("unhandled TAS column {name:?}; add it to tas_column_rules.json and rebuild")
        })
    }

    fn allowed_columns(&self, groups: &[String]) -> Result<BTreeSet<String>> {
        if groups.is_empty() {
            return Ok(self.columns.keys().cloned().collect());
        }
        let available: BTreeSet<&str> = self.columns.values().map(String::as_str).collect();
        let mut allowed = BTreeSet::new();
        for group in groups {
            if group == "core" {
                for name in CORE_COLUMNS {
                    if !self.columns.contains_key(*name) {
                        bail!("core column {name} is missing from tas_column_rules.json");
                    }
                    allowed.insert((*name).to_string());
                }
                continue;
            }
            if !available.contains(group.as_str()) {
                let mut known: Vec<&str> = available.into_iter().collect();
                known.sort_unstable();
                bail!(
                    "unknown TAS field group {group:?}; known: core, {}",
                    known.join(", ")
                );
            }
            for (name, mapped) in &self.columns {
                if mapped == group {
                    allowed.insert(name.clone());
                }
            }
        }
        Ok(allowed)
    }
}

#[derive(Debug, Clone)]
struct HeaderMap {
    names: Vec<String>,
    by_name: BTreeMap<String, usize>,
}

impl HeaderMap {
    fn from_headers(headers: &StringRecord, rules: &ColumnRules) -> Result<Self> {
        let mut names = Vec::with_capacity(headers.len());
        let mut by_name = BTreeMap::new();
        for (index, name) in headers.iter().enumerate() {
            rules.group_of(name)?;
            names.push(name.to_string());
            by_name.insert(name.to_string(), index);
        }
        for required in &rules.required_identity {
            if !by_name.contains_key(required) {
                bail!("TAS header missing required column {required}");
            }
        }
        Ok(Self { names, by_name })
    }

    fn required_cell(&self, record: &StringRecord, name: &str) -> Result<String> {
        let idx = self
            .by_name
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("TAS header missing required column {name}"))?;
        let value = nonempty(record, idx).unwrap_or_default();
        if value.is_empty() {
            bail!("unhandled empty required TAS field {name:?}");
        }
        Ok(value)
    }
}

#[derive(Debug, Serialize)]
struct ProjectedEvent {
    class: EventClass,
    ric: String,
    date_time: String,
    #[serde(rename = "type")]
    event_type: String,
    index_ric: bool,
    fields: BTreeMap<String, String>,
    groups: BTreeMap<String, BTreeMap<String, String>>,
}

#[derive(Debug, Default, Clone)]
struct Census {
    source_rows: u64,
    selected_rows: u64,
    skipped_index: u64,
    skipped_ric_filter: u64,
    occupancies: BTreeMap<String, u64>,
    classes: BTreeMap<&'static str, u64>,
    rics: BTreeMap<String, u64>,
    first_date_time: Option<String>,
    last_date_time: Option<String>,
}

impl Census {
    fn observe(&mut self, event: &ProjectedEvent) {
        self.selected_rows += 1;
        *self.classes.entry(event.class.as_str()).or_insert(0) += 1;
        *self.rics.entry(event.ric.clone()).or_insert(0) += 1;
        if self.first_date_time.is_none() {
            self.first_date_time = Some(event.date_time.clone());
        }
        self.last_date_time = Some(event.date_time.clone());
        for name in event.fields.keys() {
            *self.occupancies.entry(name.clone()).or_insert(0) += 1;
        }
    }
}

fn nonempty(record: &StringRecord, idx: usize) -> Option<String> {
    record.get(idx).and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn classify_trade(price: Option<&str>, volume: Option<&str>, qualifiers: Option<&str>) -> EventClass {
    if qualifiers == Some(SPECIAL_TRADES_USER) {
        return EventClass::TradeSpecialUser;
    }
    match (price.is_some(), volume.is_some()) {
        (true, true) => EventClass::TradePrintable,
        (false, true) => EventClass::TradeVolumeOnly,
        (true, false) => EventClass::TradePriceOnly,
        (false, false) => EventClass::TradeEmpty,
    }
}

fn classify_event(event_type: &str, fields: &BTreeMap<String, String>) -> Result<EventClass> {
    match event_type {
        "Trade" => Ok(classify_trade(
            fields.get("Price").map(String::as_str),
            fields.get("Volume").map(String::as_str),
            fields.get("Qualifiers").map(String::as_str),
        )),
        "Quote" => Ok(EventClass::Quote),
        "Mkt. Condition" => Ok(EventClass::MktCondition),
        "Correction" => Ok(EventClass::Correction),
        "" => bail!("unhandled TAS Type '' (empty); add a rule or reject the file"),
        other => bail!("unhandled TAS Type {other:?}; add a rule or reject the file"),
    }
}

fn project_record(
    headers: &HeaderMap,
    rules: &ColumnRules,
    record: &StringRecord,
    allowed: &BTreeSet<String>,
    selected_groups: &[String],
) -> Result<ProjectedEvent> {
    let mut leftover: BTreeSet<String> = BTreeSet::new();
    let mut fields = BTreeMap::new();
    let mut groups: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();

    for name in &headers.names {
        let idx = headers.by_name[name];
        let Some(value) = nonempty(record, idx) else {
            continue;
        };
        let group = rules.group_of(name)?;
        if !allowed.contains(name) {
            leftover.insert(name.clone());
            continue;
        }
        fields.insert(name.clone(), value.clone());
        groups
            .entry(group.to_string())
            .or_default()
            .insert(name.clone(), value);
    }

    if !leftover.is_empty() {
        bail!(
            "unhandled nonempty TAS columns {:?}; widen the selected groups or add rules (field_groups={:?})",
            leftover.into_iter().collect::<Vec<_>>(),
            selected_groups
        );
    }

    let event_type = headers.required_cell(record, "Type")?;
    let class = classify_event(&event_type, &fields)?;
    Ok(ProjectedEvent {
        class,
        ric: headers.required_cell(record, "#RIC")?,
        date_time: headers.required_cell(record, "Date-Time")?,
        event_type,
        index_ric: fields.get("#RIC").is_some_and(|ric| ric.starts_with('.')),
        fields,
        groups,
    })
}

fn period_dir(config: &ReplayConfig) -> PathBuf {
    config.data_root.join(format!(
        "shanghai_evolution_futures_time_and_sales_ric_list_0_tas_{}",
        config.period
    ))
}

fn discover_parts(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut parts = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read TAS period {}", dir.display()))? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with("merged-Data-part-") && name.ends_with(".csv.gz") {
            parts.push(path);
        }
    }
    parts.sort();
    if parts.is_empty() {
        bail!("no merged-Data-part-*.csv.gz under {}", dir.display());
    }
    Ok(parts)
}

fn selected_rics(config: &ReplayConfig) -> Option<BTreeSet<String>> {
    if config.rics.is_empty() {
        None
    } else {
        Some(config.rics.iter().cloned().collect())
    }
}

fn replay_part(
    path: &Path,
    config: &ReplayConfig,
    rules: &ColumnRules,
    allowed: &BTreeSet<String>,
    ric_filter: Option<&BTreeSet<String>>,
    remaining: &mut Option<u64>,
    census: &mut Census,
    mut writer: Option<&mut BufWriter<File>>,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let decoder = GzDecoder::new(BufReader::new(file));
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(decoder);
    let headers = reader.headers().context("read TAS header")?.clone();
    let map = HeaderMap::from_headers(&headers, rules)?;
    for record in reader.records() {
        if remaining.is_some_and(|left| left == 0) {
            break;
        }
        let record = record.with_context(|| format!("read TAS row from {}", path.display()))?;
        census.source_rows += 1;
        if let Some(left) = remaining.as_mut() {
            *left = left.saturating_sub(1);
        }
        let event = project_record(&map, rules, &record, allowed, &config.field_groups)
            .with_context(|| format!("project TAS row from {}", path.display()))?;
        if !config.include_index_rics && event.ric.starts_with('.') {
            census.skipped_index += 1;
            continue;
        }
        if ric_filter.is_some_and(|wanted| !wanted.contains(&event.ric)) {
            census.skipped_ric_filter += 1;
            continue;
        }
        if let Some(out) = writer.as_mut() {
            serde_json::to_writer(&mut **out, &event)
                .with_context(|| format!("write JSONL event from {}", path.display()))?;
            out.write_all(b"\n")?;
        }
        census.observe(&event);
    }
    Ok(())
}

fn replay(config: &ReplayConfig) -> Result<()> {
    if !config.dry_run && config.output_jsonl.is_none() {
        bail!("lseg_tas_replay requires output_jsonl when dry_run=false; ClickHouse is not implemented");
    }
    let rules = ColumnRules::load(&config.column_rules)?;
    let allowed = rules.allowed_columns(&config.field_groups)?;
    let dir = period_dir(config);
    let parts = discover_parts(&dir)?;
    let ric_filter = selected_rics(config);
    let mut remaining = config.max_source_rows;
    let mut census = Census::default();
    let mut writer = match &config.output_jsonl {
        Some(path) => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)
                        .with_context(|| format!("create JSONL parent {}", parent.display()))?;
                }
            }
            Some(BufWriter::new(
                File::create(path).with_context(|| format!("create JSONL {}", path.display()))?,
            ))
        }
        None => None,
    };
    let started = Instant::now();
    info!(
        "lseg_tas_replay period={} parts={} dry_run={} output_jsonl={:?} field_groups={:?} max_source_rows={:?}",
        config.period,
        parts.len(),
        config.dry_run,
        config.output_jsonl,
        config.field_groups,
        config.max_source_rows
    );
    for path in &parts {
        info!("replay {}", path.display());
        replay_part(
            path,
            config,
            &rules,
            &allowed,
            ric_filter.as_ref(),
            &mut remaining,
            &mut census,
            writer.as_mut(),
        )?;
        if remaining.is_some_and(|left| left == 0) {
            break;
        }
    }
    if let Some(out) = writer.as_mut() {
        out.flush()?;
    }
    println!(
        "lseg_tas_replay finished period={} source_rows={} selected_rows={} skipped_index={} skipped_ric_filter={} elapsed_ms={}",
        config.period,
        census.source_rows,
        census.selected_rows,
        census.skipped_index,
        census.skipped_ric_filter,
        started.elapsed().as_millis()
    );
    println!(
        "window first={} last={}",
        census.first_date_time.as_deref().unwrap_or(""),
        census.last_date_time.as_deref().unwrap_or("")
    );
    println!("classes");
    for (name, count) in &census.classes {
        println!("  {name}={count}");
    }
    println!("occupancy selected_rows={}", census.selected_rows);
    let mut occupancy: Vec<_> = census.occupancies.iter().collect();
    occupancy.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    for (name, count) in occupancy {
        println!("  {name}={count}");
    }
    println!("rics {}", census.rics.len());
    let mut ric_counts: Vec<_> = census.rics.into_iter().collect();
    ric_counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    for (ric, count) in ric_counts.into_iter().take(20) {
        println!("  {ric}={count}");
    }
    Ok(())
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let content = fs::read_to_string(&args.config)
        .with_context(|| format!("read replay config {}", args.config.display()))?;
    let mut config: ReplayConfig = toml::from_str(&content)
        .with_context(|| format!("parse replay config {}", args.config.display()))?;
    if args.max_source_rows.is_some() {
        config.max_source_rows = args.max_source_rows;
    }
    replay(&config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../preprocess/lseg/tas_column_rules.json")
    }

    fn test_rules() -> ColumnRules {
        ColumnRules::load(&rules_path()).expect("column rules")
    }

    fn record(
        rules: &ColumnRules,
        values: &[(&str, &str)],
    ) -> (HeaderMap, StringRecord, BTreeSet<String>) {
        let names: Vec<&str> = values.iter().map(|(name, _)| *name).collect();
        let headers = StringRecord::from(names);
        let map = HeaderMap::from_headers(&headers, rules).expect("headers");
        let cells: Vec<&str> = values.iter().map(|(_, value)| *value).collect();
        (
            map,
            StringRecord::from(cells),
            rules.allowed_columns(&[]).expect("all columns"),
        )
    }

    fn project(
        rules: &ColumnRules,
        values: &[(&str, &str)],
        groups: &[String],
    ) -> Result<ProjectedEvent> {
        let names: Vec<&str> = values.iter().map(|(name, _)| *name).collect();
        let headers = StringRecord::from(names);
        let map = HeaderMap::from_headers(&headers, rules)?;
        let cells: Vec<&str> = values.iter().map(|(_, value)| *value).collect();
        let allowed = rules.allowed_columns(groups)?;
        project_record(
            &map,
            rules,
            &StringRecord::from(cells),
            &allowed,
            groups,
        )
    }

    #[test]
    fn rule_catalog_covers_the_294_column_template() {
        let rules = test_rules();
        assert_eq!(rules.columns.len(), 294);
        let groups: BTreeSet<_> = rules.columns.values().cloned().collect();
        assert_eq!(groups.len(), 21);
        for name in CORE_COLUMNS {
            assert!(rules.columns.contains_key(*name), "{name}");
        }
    }

    #[test]
    fn classifies_printable_special_and_empty_trades() {
        let rules = test_rules();
        let (map, printable, allowed) = record(
            &rules,
            &[
                ("#RIC", "ADF26"),
                ("Date-Time", "2026-01-02T00:38:28Z"),
                ("Type", "Trade"),
                ("Price", "0.66825"),
                ("Volume", "1"),
                ("Qualifiers", "BID"),
            ],
        );
        let event = project_record(&map, &rules, &printable, &allowed, &[]).unwrap();
        assert_eq!(event.class, EventClass::TradePrintable);
        assert_eq!(event.fields.get("Price").map(String::as_str), Some("0.66825"));
        assert!(!event.fields.contains_key("Halt Reason"));

        let special = project(
            &rules,
            &[
                ("#RIC", "ADF26"),
                ("Date-Time", "2026-01-02T10:22:03Z"),
                ("Type", "Trade"),
                ("Volume", "1"),
                ("Qualifiers", SPECIAL_TRADES_USER),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(special.class, EventClass::TradeSpecialUser);
        assert!(!special.fields.contains_key("Price"));

        let empty = project(
            &rules,
            &[
                ("#RIC", "ADF26"),
                ("Date-Time", "2026-01-01T02:59:34Z"),
                ("Type", "Trade"),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(empty.class, EventClass::TradeEmpty);
    }

    #[test]
    fn quote_and_condition_are_not_trades() {
        let rules = test_rules();
        let quote = project(
            &rules,
            &[
                ("#RIC", "ADF26"),
                ("Date-Time", "t"),
                ("Type", "Quote"),
                ("Bid Price", "0.66815"),
                ("Bid Size", "56"),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(quote.class, EventClass::Quote);
        assert_eq!(quote.groups["l1"]["Bid Price"], "0.66815");

        let halt = project(
            &rules,
            &[
                ("#RIC", "ADF26"),
                ("Date-Time", "t"),
                ("Type", "Mkt. Condition"),
                ("Qualifiers", "0[HALT_REASN]"),
                ("Halt Reason", "0"),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(halt.class, EventClass::MktCondition);
    }

    #[test]
    fn nonempty_wide_fields_are_kept_in_structured_row() {
        let rules = test_rules();
        let event = project(
            &rules,
            &[
                ("#RIC", ".FTXIN9"),
                ("Date-Time", "2010-01-04T01:30:05.161586000Z"),
                ("Type", "Trade"),
                ("Price", "12024.72"),
                ("PE Ratio", "18.2"),
                ("Yield", "1.1"),
                ("Imp. Vol.", "0.22"),
                ("Interpolated CDS Spread", "45"),
                ("Forecast", "1.6"),
                ("Percentage Change", "0.15"),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(event.fields["PE Ratio"], "18.2");
        assert_eq!(event.fields["Yield"], "1.1");
        assert_eq!(event.fields["Imp. Vol."], "0.22");
        assert_eq!(event.fields["Interpolated CDS Spread"], "45");
        assert_eq!(event.fields["Forecast"], "1.6");
        assert_eq!(event.fields["Percentage Change"], "0.15");
        assert_eq!(event.groups["equity_value"]["PE Ratio"], "18.2");
        assert!(event.index_ric);
    }

    #[test]
    fn restricting_groups_panics_on_leftover_nonempty_fields() {
        let rules = test_rules();
        let err = project(
            &rules,
            &[
                ("#RIC", ".FTXIN9"),
                ("Date-Time", "2010-01-04T01:30:05.161586000Z"),
                ("Type", "Trade"),
                ("Price", "12024.72"),
                ("PE Ratio", "18.2"),
            ],
            &["core".into()],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("unhandled nonempty TAS columns"),
            "{err}"
        );
    }

    #[test]
    fn unknown_type_and_column_panic() {
        let rules = test_rules();
        let err = project(
            &rules,
            &[
                ("#RIC", "X"),
                ("Date-Time", "t"),
                ("Type", "Auction"),
            ],
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains("unhandled TAS Type"), "{err}");

        let headers = StringRecord::from(vec!["#RIC", "Date-Time", "Type", "Not A Real Column"]);
        let err = HeaderMap::from_headers(&headers, &rules).unwrap_err();
        assert!(err.to_string().contains("unhandled TAS column"), "{err}");
    }

    #[test]
    fn unknown_field_group_is_rejected() {
        let rules = test_rules();
        let err = rules.allowed_columns(&["not-a-group".into()]).unwrap_err();
        assert!(err.to_string().contains("unknown TAS field group"), "{err}");
    }

    #[test]
    fn core_alias_still_resolves_legacy_washable_names() {
        let rules = test_rules();
        let allowed = rules.allowed_columns(&["core".into()]).unwrap();
        let expected: BTreeSet<_> = CORE_COLUMNS.iter().map(|name| (*name).to_string()).collect();
        assert_eq!(allowed, expected);
    }
}
