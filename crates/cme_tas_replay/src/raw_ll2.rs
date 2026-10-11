//! Source-exact Raw LL2 grouping and sparse low-frequency extraction.
//! Missing patches never initialise a field; quality masks distinguish unknown
//! from explicit source null. State and unfinished messages survive part edges.
use anyhow::{anyhow, bail, Context, Result};
use csv::StringRecord;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Read;

use crate::ll2_1min::{encode_ll2_minute, Ll2Minute};
use crate::ll2_source::{
    parse_count, parse_extended_exch_time_ns, parse_gmt_offset_minutes, parse_ll2_datetime_ns,
    parse_scaled, MISSING_COUNT,
};
use crate::{encode_ric, MISSING_PRICE};

pub const HEADER: &str = "#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs";
pub const UPDATE_TYPES: [&str; 12] = [
    "UNSPECIFIED",
    "QUOTE",
    "TRADE",
    "NEWS_ALERT",
    "VOLUME_ALERT",
    "ORDER_INDICATION",
    "CLOSING_RUN",
    "CORRECTION",
    "MARKET_DIGEST",
    "QUOTES_TRADE",
    "MULTIPLE",
    "VERIFY",
];
pub fn valid_update_type(value: &str) -> bool {
    UPDATE_TYPES.contains(&value)
        || (!value.is_empty()
            && value.bytes().all(|b| b.is_ascii_digit())
            && value.parse::<u8>().is_ok())
}
const PREFIXES: [&str; 6] = [
    "BEST_BID",
    "BEST_BSIZ",
    "NO_BIDORD",
    "BEST_ASK",
    "BEST_ASIZ",
    "NO_ASKORD",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawMessage {
    pub ric: String,
    pub ts: u64,
    pub offset: i16,
    pub order: u64,
    pub update_type: String,
    pub message_class: String,
    pub fields: Vec<(String, String)>,
    pub declared: usize,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Census {
    pub messages: u64,
    pub fids: u64,
    pub depth_messages: u64,
    pub messages_by_ric: BTreeMap<String, u64>,
    pub update_types: BTreeMap<String, u64>,
    pub message_classes: BTreeMap<String, u64>,
    pub fields: BTreeMap<String, u64>,
}

#[derive(Default)]
pub struct RawScanner {
    pub census: Census,
    current: Option<RawMessage>,
    pub leading_fids: Vec<(String, String)>,
}

impl RawScanner {
    /// Returns true only when the complete-message dry-run limit was reached.
    pub fn scan<R: Read, F: FnMut(RawMessage) -> Result<()>>(
        &mut self,
        reader: R,
        require_header: bool,
        limit: Option<u64>,
        emit: &mut F,
    ) -> Result<bool> {
        self.scan_impl(reader, require_header, false, limit, emit)
    }
    pub fn scan_independent<R: Read, F: FnMut(RawMessage) -> Result<()>>(
        &mut self,
        reader: R,
        require_header: bool,
        limit: Option<u64>,
        emit: &mut F,
    ) -> Result<bool> {
        self.scan_impl(reader, require_header, true, limit, emit)
    }
    pub fn take_pending(&mut self) -> Option<RawMessage> {
        self.current.take()
    }
    fn scan_impl<R: Read, F: FnMut(RawMessage) -> Result<()>>(
        &mut self,
        reader: R,
        require_header: bool,
        leading: bool,
        limit: Option<u64>,
        emit: &mut F,
    ) -> Result<bool> {
        let mut csv = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(reader);
        let mut row = StringRecord::new();
        let mut first = true;
        while csv.read_record(&mut row)? {
            let is_header = row.iter().eq(HEADER.split(','));
            if first && require_header && !is_header {
                bail!("Raw LL2 part 0 needs the exact CSV header");
            }
            first = false;
            if is_header {
                continue;
            }
            if row.len() < 10 || row.len() > 14 {
                bail!("invalid Raw LL2 width {}", row.len());
            }
            if !row[0].is_empty() {
                if self.current.is_some() {
                    bail!("incomplete Raw LL2 message before next header");
                }
                if row.len() != 14 || &row[1] != "Market Price" || &row[4] != "Legacy Level 2" {
                    bail!("invalid Raw LL2 message header: {row:?}");
                }
                encode_ric(&row[0])?;
                if !["UPDATE", "REFRESH", "STATUS"].contains(&&row[5]) {
                    bail!("unsupported Raw LL2 message header {row:?}");
                }
                if !row[13].bytes().all(|b| b.is_ascii_digit()) || row[13].is_empty() {
                    bail!("invalid Raw Number of FIDs");
                }
                if (&row[5] == "UPDATE" && !valid_update_type(&row[6]))
                    || (["REFRESH", "STATUS"].contains(&&row[5]) && !row[6].is_empty())
                {
                    bail!("unsupported Raw LL2 update header {row:?}");
                }
                let declared = row[13].parse::<usize>().context("Raw Number of FIDs")?;
                if &row[5] == "STATUS" && declared != 0 {
                    bail!("nonempty Raw STATUS payload requires explicit policy: {row:?}");
                }
                if declared > 65_535 {
                    bail!("Raw Number of FIDs exceeds 65535");
                }
                self.current = Some(RawMessage {
                    ric: row[0].to_owned(),
                    ts: parse_ll2_datetime_ns(row[2].as_bytes())?,
                    offset: parse_gmt_offset_minutes(row[3].as_bytes())?,
                    order: self
                        .census
                        .messages
                        .checked_add(1)
                        .context("Raw source order overflow")?,
                    update_type: row[6].to_owned(),
                    message_class: row[5].to_owned(),
                    fields: Vec::with_capacity(declared),
                    declared,
                });
            } else {
                if &row[4] != "FID"
                    || row[5].is_empty()
                    || !row[5].bytes().all(|b| b.is_ascii_digit())
                    || row[5].parse::<u32>().is_err()
                {
                    bail!("invalid Raw LL2 FID row: {row:?}");
                }
                if self.current.is_none() && leading && self.census.messages == 0 {
                    self.leading_fids
                        .push((row[7].to_owned(), row[8].to_owned()));
                    continue;
                }
                let current = self.current.as_mut().context("orphan Raw LL2 FID")?;
                current.fields.push((row[7].to_owned(), row[8].to_owned()));
            }
            if self
                .current
                .as_ref()
                .is_some_and(|m| m.fields.len() == m.declared)
            {
                let message = self.current.take().unwrap();
                self.census.observe(&message);
                emit(message)?;
                if limit.is_some_and(|n| self.census.messages >= n) {
                    return Ok(true);
                }
            }
        }
        if first && require_header {
            bail!("empty Raw LL2 part 0");
        }
        Ok(false)
    }

    pub fn finish(&self) -> Result<()> {
        if let Some(m) = &self.current {
            bail!(
                "truncated Raw LL2 {}: expected {} FIDs, got {}",
                m.ric,
                m.declared,
                m.fields.len()
            );
        }
        Ok(())
    }
}

fn field_index(name: &str) -> Result<Option<(usize, usize)>> {
    // Vendor dictionary aliases: FIDs 2421, 2426, 6553, 6563.
    match name {
        "BEST_BSZ10" => return Ok(Some((9, 1))),
        "BEST_ASZ10" => return Ok(Some((9, 4))),
        "NO_BIDRD10" => return Ok(Some((9, 2))),
        "NO_ASKRD10" => return Ok(Some((9, 5))),
        _ => {}
    }
    for (field, prefix) in PREFIXES.iter().enumerate() {
        if let Some(suffix) = name.strip_prefix(prefix) {
            if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
                let n = suffix.parse::<usize>()?;
                if !(1..=10).contains(&n) {
                    bail!("unsupported Raw depth level {name}");
                }
                return Ok(Some((n - 1, field)));
            }
        }
    }
    Ok(None)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub value: Ll2Minute,
    pub known: u64,
    pub last_ts: u64,
    /// False after a REFRESH knowledge boundary in this source part.
    pub inherit_prior: bool,
}
impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            value: Ll2Minute {
                source_ts_utc_ns: 0,
                source_order: 0,
                exch_time_ns: u64::MAX,
                gmt_offset_minutes: 0,
                bid_prices: [MISSING_PRICE; 10],
                bid_sizes: [MISSING_PRICE; 10],
                bid_counts: [MISSING_COUNT; 10],
                ask_prices: [MISSING_PRICE; 10],
                ask_sizes: [MISSING_PRICE; 10],
                ask_counts: [MISSING_COUNT; 10],
            },
            known: 0,
            last_ts: 0,
            inherit_prior: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RawSnapshot {
    pub ric: String,
    pub seconds: u32,
    pub bucket_ns: u64,
    pub value: Ll2Minute,
    pub known: u64,
    pub inherit_prior: bool,
}
impl RawSnapshot {
    pub fn key(&self) -> Result<[u8; 24]> {
        if self.bucket_ns == 0 || self.bucket_ns % (u64::from(self.seconds) * 1_000_000_000) != 0 {
            bail!("invalid Raw LL2 bucket key");
        }
        let mut key = [0; 24];
        key[..16].copy_from_slice(&encode_ric(&self.ric)?);
        key[16..].copy_from_slice(&self.bucket_ns.to_be_bytes());
        Ok(key)
    }
    pub fn json(&self) -> Result<serde_json::Value> {
        Ok(
            serde_json::json!({"ric": self.ric, "seconds": self.seconds, "key_hex": hex(&self.key()?),
            "value_hex": hex(&encode_ll2_minute(&self.value)), "known": self.known}),
        )
    }
}
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub struct Extractor {
    frequency: u32,
    states: BTreeMap<String, Checkpoint>,
    pending: BTreeMap<(u32, String), RawSnapshot>,
    pub depth_messages: u64,
}
impl Extractor {
    pub fn new(frequency: u32) -> Result<Self> {
        if ![15, 60].contains(&frequency) {
            bail!("frequency must be 15 or 60 seconds");
        }
        Ok(Self {
            frequency,
            states: BTreeMap::new(),
            pending: BTreeMap::new(),
            depth_messages: 0,
        })
    }
    pub fn checkpoints(&self) -> BTreeMap<String, Checkpoint> {
        self.states.clone()
    }
    pub fn seed(&mut self, ric: String, state: Checkpoint) -> Result<()> {
        if self.states.contains_key(&ric) {
            bail!("cannot replace active Raw state");
        }
        validate_quality(&state.value, state.known)?;
        self.states.insert(ric, state);
        Ok(())
    }
    pub fn apply<F: FnMut(RawSnapshot) -> Result<()>>(
        &mut self,
        message: &RawMessage,
        emit: &mut F,
    ) -> Result<()> {
        let state = self.states.entry(message.ric.clone()).or_default();
        if message.ts < state.last_ts {
            bail!("decreasing Raw timestamp for {}", message.ric);
        }
        state.last_ts = message.ts;
        let refresh = message.message_class == "REFRESH";
        if refresh {
            // CSV omits ClearCache/RefreshComplete. Discard unverifiable prior
            // knowledge; this does not assert source cache clearing/completion.
            *state = Checkpoint {
                last_ts: message.ts,
                inherit_prior: false,
                ..Checkpoint::default()
            };
        }
        // Even a metadata-only REFRESH invalidates old knowledge. Emit that
        // invalidation so an earlier snapshot in the same bucket cannot survive.
        let mut changed = refresh;
        for (name, raw) in &message.fields {
            if name == "TIMACT" {
                state.value.exch_time_ns = parse_extended_exch_time_ns(raw)?;
                state.known |= 1 << 60;
            }
            if let Some((level, field)) = field_index(name)? {
                if !raw.is_empty()
                    && ![2, 5].contains(&field)
                    && parse_scaled(raw.as_bytes(), name)? == MISSING_PRICE
                {
                    bail!("Raw numeric value equals missing sentinel");
                }
                match field {
                    0 => state.value.bid_prices[level] = parse_scaled(raw.as_bytes(), name)?,
                    1 => state.value.bid_sizes[level] = parse_scaled(raw.as_bytes(), name)?,
                    2 => state.value.bid_counts[level] = parse_count(raw.as_bytes())?,
                    3 => state.value.ask_prices[level] = parse_scaled(raw.as_bytes(), name)?,
                    4 => state.value.ask_sizes[level] = parse_scaled(raw.as_bytes(), name)?,
                    5 => state.value.ask_counts[level] = parse_count(raw.as_bytes())?,
                    _ => unreachable!(),
                }
                state.known |= 1 << (level * 6 + field);
                changed = true;
            }
        }
        if !changed {
            return Ok(());
        }
        self.depth_messages += 1;
        state.value.source_ts_utc_ns = message.ts;
        state.value.source_order = message.order;
        state.value.gmt_offset_minutes = message.offset;
        for seconds in [self.frequency] {
            let ns = u64::from(seconds) * 1_000_000_000;
            let snapshot = RawSnapshot {
                ric: message.ric.clone(),
                seconds,
                bucket_ns: message.ts / ns * ns,
                value: state.value.clone(),
                known: state.known,
                inherit_prior: state.inherit_prior,
            };
            let index = (seconds, message.ric.clone());
            if self
                .pending
                .get(&index)
                .is_some_and(|old| old.bucket_ns != snapshot.bucket_ns)
            {
                emit(self.pending.remove(&index).unwrap())?;
            }
            self.pending.insert(index, snapshot);
        }
        Ok(())
    }
    pub fn finish<F: FnMut(RawSnapshot) -> Result<()>>(&mut self, emit: &mut F) -> Result<()> {
        for (_, snapshot) in std::mem::take(&mut self.pending) {
            emit(snapshot)?;
        }
        Ok(())
    }
}

impl Census {
    pub fn observe(&mut self, m: &RawMessage) {
        self.messages += 1;
        *self
            .message_classes
            .entry(m.message_class.clone())
            .or_default() += 1;
        self.fids += m.fields.len() as u64;
        *self.messages_by_ric.entry(m.ric.clone()).or_default() += 1;
        *self.update_types.entry(m.update_type.clone()).or_default() += 1;
        for (name, _) in &m.fields {
            *self.fields.entry(name.clone()).or_default() += 1;
        }
    }
    pub fn append(&mut self, other: &Self) {
        self.messages += other.messages;
        self.fids += other.fids;
        self.depth_messages += other.depth_messages;
        for (a, b) in [
            (&mut self.messages_by_ric, &other.messages_by_ric),
            (&mut self.update_types, &other.update_types),
            (&mut self.message_classes, &other.message_classes),
            (&mut self.fields, &other.fields),
        ] {
            for (key, n) in b {
                *a.entry(key.clone()).or_default() += n;
            }
        }
    }
}
/// A part-local unknown field inherits only prior parts, never future data.
pub fn inherit_unknown(value: &mut Ll2Minute, known: &mut u64, prior: &Checkpoint) {
    for level in 0..10 {
        macro_rules! inherit {
            ($field:ident,$bit:expr) => {
                if *known & (1 << (level * 6 + $bit)) == 0
                    && prior.known & (1 << (level * 6 + $bit)) != 0
                {
                    value.$field[level] = prior.value.$field[level];
                    *known |= 1 << (level * 6 + $bit);
                }
            };
        }
        inherit!(bid_prices, 0);
        inherit!(bid_sizes, 1);
        inherit!(bid_counts, 2);
        inherit!(ask_prices, 3);
        inherit!(ask_sizes, 4);
        inherit!(ask_counts, 5);
    }
    if *known & (1 << 60) == 0 && prior.known & (1 << 60) != 0 {
        value.exch_time_ns = prior.value.exch_time_ns;
        *known |= 1 << 60;
    }
}

pub fn validate_quality(value: &Ll2Minute, known: u64) -> Result<()> {
    if known >> 61 != 0 {
        bail!("Raw quality contains reserved bits");
    }
    for level in 0..10 {
        let missing = [
            value.bid_prices[level] == MISSING_PRICE,
            value.bid_sizes[level] == MISSING_PRICE,
            value.bid_counts[level] == MISSING_COUNT,
            value.ask_prices[level] == MISSING_PRICE,
            value.ask_sizes[level] == MISSING_PRICE,
            value.ask_counts[level] == MISSING_COUNT,
        ];
        for (field, is_missing) in missing.into_iter().enumerate() {
            if known & (1 << (level * 6 + field)) == 0 && !is_missing {
                bail!("uninitialised Raw field contains a value");
            }
        }
    }
    if known & (1 << 60) == 0 && value.exch_time_ns != u64::MAX {
        bail!("uninitialised TIMACT contains a value");
    }
    Ok(())
}

pub fn raw_cf(year: u16, product: &str, seconds: u32) -> Result<String> {
    let p = crate::product::product_cf_name(year, product)?;
    match seconds {
        60 => Ok(p),
        15 => Ok(format!("s15:{p}")),
        _ => Err(anyhow!("unsupported Raw frequency")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn extract(parts: &[&[u8]]) -> Result<serde_json::Value> {
        let mut scanner = RawScanner::default();
        let mut extractor = Extractor::new(15)?;
        let mut snapshots = Vec::new();
        let mut output = |s: RawSnapshot| {
            snapshots.push(s.json()?);
            Ok(())
        };
        for (i, input) in parts.iter().enumerate() {
            scanner.scan(Cursor::new(input), i == 0, None, &mut |m| {
                extractor.apply(&m, &mut output)
            })?;
        }
        scanner.finish()?;
        extractor.finish(&mut output)?;
        scanner.census.depth_messages = extractor.depth_messages;
        snapshots.sort_by_key(|s| {
            (
                s["seconds"].as_u64().unwrap(),
                s["key_hex"].as_str().unwrap().to_owned(),
            )
        });
        Ok(serde_json::json!({"census": scanner.census, "snapshots": snapshots}))
    }

    #[test]
    fn python_fixture_is_byte_exact_even_across_part_and_fid_edges() {
        let input = include_bytes!("../tests/fixtures/raw_ll2_small.csv");
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_small.expected.json"
        ))
        .unwrap();
        assert_eq!(extract(&[input]).unwrap(), expected);
        let cut = input.windows(10).position(|s| s == b",,,,FID,40").unwrap();
        assert_eq!(extract(&[&input[..cut], &input[cut..]]).unwrap(), expected);
    }

    #[test]
    fn complete_refresh_and_vendor_tenth_level_names_match_python() {
        let input = include_bytes!("../tests/fixtures/raw_ll2_refresh.csv");
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_refresh.expected.json"
        ))
        .unwrap();
        assert_eq!(extract(&[input]).unwrap(), expected);
        assert_eq!(expected["census"]["message_classes"]["REFRESH"], 3);
        assert_eq!(field_index("BEST_BSZ10").unwrap(), Some((9, 1)));
        assert_eq!(field_index("BEST_ASZ10").unwrap(), Some((9, 4)));
        assert_eq!(field_index("NO_BIDRD10").unwrap(), Some((9, 2)));
        assert_eq!(field_index("NO_ASKRD10").unwrap(), Some((9, 5)));
        assert_eq!(
            expected["snapshots"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|s| s["known"].as_u64() == Some((1 << 61) - 1))
                .count(),
            3
        );
        let text = String::from_utf8(input.to_vec()).unwrap();
        // Retain syntactically complete rows but remove all TIMACT FIDs from REFRESH.
        // A missing TIMACT is unknown after REFRESH; it must not inherit the old time.
        let mut scanner = RawScanner::default();
        let mut extractor = Extractor::new(15).unwrap();
        let result = scanner.scan(text.as_bytes(), true, None, &mut |mut m| {
            if m.message_class == "REFRESH" {
                m.fields.retain(|(n, _)| n != "TIMACT");
                m.declared = m.fields.len();
            }
            extractor.apply(&m, &mut |_| Ok(()))
        });
        result.unwrap();
        for state in extractor.checkpoints().values() {
            assert_eq!(state.known & (1 << 60), 0);
            assert_eq!(state.value.exch_time_ns, u64::MAX);
        }
    }

    #[test]
    fn partial_refresh_matches_python_and_invalidates_old_upper_ranks() {
        let input = include_bytes!("../tests/fixtures/raw_ll2_partial_refresh.csv");
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_partial_refresh.expected.json"
        ))
        .unwrap();
        assert_eq!(extract(&[input]).unwrap(), expected);
    }

    #[test]
    fn refresh_invalidation_replaces_earlier_depth_in_the_same_bucket() {
        for zero_fids in [false, true] {
            let input = include_bytes!("../tests/fixtures/raw_ll2_partial_refresh.csv");
            let mut scanner = RawScanner::default();
            let mut extractor = Extractor::new(15).unwrap();
            let mut output = Vec::new();
            scanner
                .scan(input.as_slice(), true, None, &mut |mut m| {
                    m.ts = 1767312000000000000 + m.order * 1_000_000_000;
                    if zero_fids && m.order == 4 {
                        m.fields.clear();
                        m.declared = 0;
                    }
                    extractor.apply(&m, &mut |s| {
                        output.push(s);
                        Ok(())
                    })
                })
                .unwrap();
            extractor
                .finish(&mut |s| {
                    output.push(s);
                    Ok(())
                })
                .unwrap();
            assert_eq!(output.len(), 1);
            for s in output {
                assert_eq!(s.known, 0);
                assert!(!s.inherit_prior);
                validate_quality(&s.value, s.known).unwrap();
            }
        }
    }

    #[test]
    fn update_event_classification_does_not_change_explicit_field_patches() {
        let text =
            String::from_utf8(include_bytes!("../tests/fixtures/raw_ll2_small.csv").to_vec())
                .unwrap();
        let base = extract(&[text.as_bytes()]).unwrap();
        for kind in UPDATE_TYPES.into_iter().chain(["127", "128", "255"]) {
            let source = text.replace(",UPDATE,UNSPECIFIED,", &format!(",UPDATE,{kind},"));
            assert_eq!(
                extract(&[source.as_bytes()]).unwrap()["snapshots"],
                base["snapshots"]
            );
        }
    }

    #[test]
    fn zero_fid_status_is_census_only_and_preserves_source_field_state() {
        let input = include_bytes!("../tests/fixtures/raw_ll2_status.csv");
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/raw_ll2_status.expected.json"
        ))
        .unwrap();
        assert_eq!(extract(&[input]).unwrap(), expected);
        assert_eq!(expected["census"]["message_classes"]["STATUS"], 1);
        assert_eq!(expected["census"]["depth_messages"], 3);
        let source = String::from_utf8(input.to_vec()).unwrap();
        let status = source.lines().find(|s| s.contains(",STATUS,")).unwrap();
        let invalid = format!(
            "{HEADER}\n{}\n,,,,FID,400,,BEST_BID1,1,\n",
            status.strip_suffix(",0").unwrap().to_owned() + ",1"
        );
        assert!(extract(&[invalid.as_bytes()]).is_err());
    }

    #[test]
    fn refuses_truncated_messages_or_orphan_fids() {
        let input = format!("{HEADER}\nESH24,Market Price,2024-01-02T00:00:00Z,-6,Legacy Level 2,UPDATE,UNSPECIFIED,,,,115,,1,2\n,,,,FID,436,,BEST_BID1,1,\n");
        assert!(extract(&[input.as_bytes()]).is_err());
        assert!(extract(&[format!("{HEADER}\n,,,,FID,436,,BEST_BID1,1,\n").as_bytes()]).is_err());
    }

    #[test]
    fn rejects_time_reversal_and_unsupported_message_classes() {
        let input =
            String::from_utf8(include_bytes!("../tests/fixtures/raw_ll2_small.csv").to_vec())
                .unwrap();
        assert!(extract(&[input
            .replace("00:05:00.000000000Z", "00:00:00.000000000Z")
            .as_bytes()])
        .is_err());
        assert!(extract(&[input.replace(",UPDATE,", ",UNSUPPORTED,").as_bytes()]).is_err());
    }
}
