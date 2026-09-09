use crate::quote_replay::source_location;
use crate::raw::{read_messages, read_parsed_messages, write_parsed_message, RawMessage};
use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use zstd::stream::{read::Decoder, write::Encoder};

pub const PARSED_MANIFEST: &str = "parsed_manifest.json";
const PARSED_SCHEMA: &str = "lseg-usstock-raw-parsed-by-ric-v1";
const MAGIC: &[u8; 8] = b"USRPAR01";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParsedSegment {
    pub file: String,
    pub ric: String,
    pub original_part: u16,
    pub shard_index: u16,
    pub messages: u64,
    pub first_source_row: u64,
    pub last_source_row: u64,
    pub encoded_bytes: u64,
    pub compressed_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParsedManifest {
    pub schema: String,
    pub complete: bool,
    pub source_messages: u64,
    pub encoded_bytes: u64,
    pub compressed_bytes: u64,
    pub segments: Vec<ParsedSegment>,
}

impl ParsedManifest {
    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join(PARSED_MANIFEST);
        let manifest: Self = serde_json::from_reader(
            File::open(&path).with_context(|| format!("open {}", path.display()))?,
        )?;
        manifest.validate_inner(root, true)?;
        Ok(manifest)
    }

    pub fn validate(&self, root: &Path) -> Result<()> {
        self.validate_inner(root, true)
    }

    fn validate_inner(&self, root: &Path, verify_digest: bool) -> Result<()> {
        if self.schema != PARSED_SCHEMA || !self.complete || self.segments.is_empty() {
            bail!("parsed RAW manifest is incomplete or unsupported");
        }
        let mut messages = 0_u64;
        let mut encoded = 0_u64;
        let mut compressed = 0_u64;
        let mut previous = BTreeMap::<&str, (u16, u16, u64)>::new();
        for segment in &self.segments {
            if segment.messages == 0
                || segment.first_source_row > segment.last_source_row
                || segment.sha256.len() != 64
            {
                bail!("invalid parsed segment {}", segment.file);
            }
            let path = root.join(&segment.file);
            let size = fs::metadata(&path)
                .with_context(|| format!("stat {}", path.display()))?
                .len();
            if size != segment.compressed_bytes {
                bail!("parsed segment size mismatch {}", segment.file);
            }
            if let Some(prior) = previous.insert(
                &segment.ric,
                (
                    segment.original_part,
                    segment.shard_index,
                    segment.last_source_row,
                ),
            ) {
                if (
                    segment.original_part,
                    segment.shard_index,
                    segment.first_source_row,
                ) <= prior
                {
                    bail!("parsed segment order is not increasing for {}", segment.ric);
                }
            }
            messages = messages
                .checked_add(segment.messages)
                .context("parsed message total overflow")?;
            encoded = encoded
                .checked_add(segment.encoded_bytes)
                .context("parsed byte total overflow")?;
            compressed = compressed
                .checked_add(segment.compressed_bytes)
                .context("parsed compressed total overflow")?;
        }
        if (messages, encoded, compressed)
            != (
                self.source_messages,
                self.encoded_bytes,
                self.compressed_bytes,
            )
        {
            bail!("parsed manifest totals disagree");
        }
        if verify_digest {
            self.segments.par_iter().try_for_each(|segment| {
                let path = root.join(&segment.file);
                if digest(&path)? != segment.sha256 {
                    bail!("parsed segment SHA-256 mismatch {}", segment.file);
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    pub fn by_ric(&self, root: &Path) -> BTreeMap<String, Vec<(u16, u16, PathBuf)>> {
        let mut result = BTreeMap::new();
        for s in &self.segments {
            result.entry(s.ric.clone()).or_insert_with(Vec::new).push((
                s.original_part,
                s.shard_index,
                root.join(&s.file),
            ));
        }
        result
    }
}

struct OpenSegment {
    ric: String,
    file: String,
    encoder: Encoder<'static, BufWriter<File>>,
    messages: u64,
    first_source_row: u64,
    last_source_row: u64,
    encoded_bytes: u64,
}

fn digest(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}

fn partition_one(path: &Path, building: &Path) -> Result<Vec<ParsedSegment>> {
    let (part, shard) = source_location(path)?;
    let mut open = HashMap::<String, OpenSegment>::new();
    let mut ordinal = 0_u16;
    let file = File::open(path)?;
    let reader = BufReader::with_capacity(16 * 1024 * 1024, file);
    let decoder = Decoder::new(reader)?;
    read_messages(decoder, |message| {
        let entry = match open.entry(message.ric.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let name =
                    format!("parsed-part-{part:06}-shard-{shard:06}-segment-{ordinal:04}.bin.zst");
                ordinal = ordinal
                    .checked_add(1)
                    .context("too many RIC segments in one shard")?;
                let mut encoder =
                    Encoder::new(BufWriter::new(File::create(building.join(&name))?), 1)?;
                encoder.write_all(MAGIC)?;
                let ric_bytes = message.ric.as_bytes();
                encoder.write_all(&u16::try_from(ric_bytes.len())?.to_le_bytes())?;
                encoder.write_all(ric_bytes)?;
                encoder.write_all(&part.to_le_bytes())?;
                encoder.write_all(&shard.to_le_bytes())?;
                entry.insert(OpenSegment {
                    ric: message.ric.clone(),
                    file: name,
                    encoder,
                    messages: 0,
                    first_source_row: message.source_row,
                    last_source_row: message.source_row,
                    encoded_bytes: 0,
                })
            }
        };
        entry.encoded_bytes += write_parsed_message(&mut entry.encoder, &message)?;
        entry.messages += 1;
        entry.last_source_row = message.source_row;
        Ok(())
    })
    .with_context(|| format!("partition parsed messages from {}", path.display()))?;
    let mut result = Vec::with_capacity(open.len());
    for (_, segment) in open {
        segment.encoder.finish()?.flush()?;
        let segment_path = building.join(&segment.file);
        result.push(ParsedSegment {
            file: segment.file,
            ric: segment.ric,
            original_part: part,
            shard_index: shard,
            messages: segment.messages,
            first_source_row: segment.first_source_row,
            last_source_row: segment.last_source_row,
            encoded_bytes: segment.encoded_bytes,
            compressed_bytes: fs::metadata(&segment_path)?.len(),
            sha256: digest(&segment_path)?,
        });
    }
    result.sort_by(|a, b| a.ric.cmp(&b.ric));
    Ok(result)
}

pub fn partition(inputs: &[PathBuf], output: &Path, workers: usize) -> Result<ParsedManifest> {
    if inputs.is_empty() || workers == 0 {
        bail!("parsed partition inputs/workers must be nonempty");
    }
    let building = output.with_extension("building");
    if output.exists() || building.exists() {
        bail!(
            "parsed output already exists: {} or {}",
            output.display(),
            building.display()
        );
    }
    fs::create_dir_all(&building)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|i| format!("usstock-raw-parser-{i}"))
        .build()?;
    let completed = AtomicUsize::new(0);
    let total_messages = AtomicU64::new(0);
    let started = Instant::now();
    let results = pool.install(|| {
        inputs
            .par_iter()
            .map(|path| {
                let result = partition_one(path, &building);
                if let Ok(segments) = &result {
                    let messages = segments.iter().map(|s| s.messages).sum::<u64>();
                    let messages = total_messages.fetch_add(messages, Ordering::Relaxed) + messages;
                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!(
                        "parsed RAW progress shards={done}/{} messages={messages} elapsed_s={:.1} last={}",
                        inputs.len(), started.elapsed().as_secs_f64(), path.display()
                    );
                }
                result
            })
            .collect::<Vec<_>>()
    });
    let mut segments = Vec::new();
    for result in results {
        segments.extend(result?);
    }
    segments.sort_by_key(|s| (s.ric.clone(), s.original_part, s.shard_index));
    let manifest = ParsedManifest {
        schema: PARSED_SCHEMA.into(),
        complete: true,
        source_messages: segments.iter().map(|s| s.messages).sum(),
        encoded_bytes: segments.iter().map(|s| s.encoded_bytes).sum(),
        compressed_bytes: segments.iter().map(|s| s.compressed_bytes).sum(),
        segments,
    };
    serde_json::to_writer_pretty(File::create(building.join(PARSED_MANIFEST))?, &manifest)?;
    manifest.validate_inner(&building, false)?;
    fs::rename(&building, output)?;
    Ok(manifest)
}

pub fn read_segment<F>(
    path: &Path,
    expected_ric: &str,
    expected_part: u16,
    expected_shard: u16,
    on_message: F,
) -> Result<u64>
where
    F: FnMut(RawMessage) -> Result<()>,
{
    let mut decoder = Decoder::new(BufReader::new(File::open(path)?))?;
    let mut magic = [0; 8];
    decoder.read_exact(&mut magic)?;
    if &magic != MAGIC {
        bail!("bad parsed segment magic in {}", path.display());
    }
    let mut len = [0; 2];
    decoder.read_exact(&mut len)?;
    let mut ric = vec![0; usize::from(u16::from_le_bytes(len))];
    decoder.read_exact(&mut ric)?;
    let ric = std::str::from_utf8(&ric)?;
    let mut part = [0; 2];
    let mut shard = [0; 2];
    decoder.read_exact(&mut part)?;
    decoder.read_exact(&mut shard)?;
    if ric != expected_ric
        || u16::from_le_bytes(part) != expected_part
        || u16::from_le_bytes(shard) != expected_shard
    {
        bail!("parsed segment identity mismatch in {}", path.display());
    }
    read_parsed_messages(decoder, expected_ric, on_message)
}
