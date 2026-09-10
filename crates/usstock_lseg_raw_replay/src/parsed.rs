use crate::quote_replay::source_location;
use crate::raw::{
    read_messages, read_messages_without_header, read_parsed_messages, write_parsed_message,
    RawMessage,
};
use anyhow::{bail, Context, Result};
use flate2::read::MultiGzDecoder;
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
// The RocksDB source-order ABI reserves its low 32 bits for a physical source
// row. Direct LSEG gzip deliveries can exceed that in one part, so preserve
// the established part|shard|row encoding by introducing logical shards.
const LOGICAL_SHARD_ROWS: u64 = 250_000_000;

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
    shard: u16,
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

fn finish_segment(segment: OpenSegment, part: u16, building: &Path) -> Result<ParsedSegment> {
    let OpenSegment {
        ric,
        file,
        shard,
        encoder,
        messages,
        first_source_row,
        last_source_row,
        encoded_bytes,
    } = segment;
    let mut writer = encoder.finish()?;
    writer.flush()?;
    let path = building.join(&file);
    Ok(ParsedSegment {
        file,
        ric,
        original_part: part,
        shard_index: shard,
        messages,
        first_source_row,
        last_source_row,
        encoded_bytes,
        compressed_bytes: fs::metadata(&path)?.len(),
        sha256: digest(&path)?,
    })
}

fn partition_one(
    path: &Path,
    building: &Path,
    logical_shard_rows: u64,
) -> Result<Vec<ParsedSegment>> {
    if logical_shard_rows == 0 || logical_shard_rows > u64::from(u32::MAX) {
        bail!("invalid logical RAW shard row limit {logical_shard_rows}");
    }
    let (part, input_shard) = source_location(path)?;
    let direct_multipart_gzip =
        path.extension().is_some_and(|extension| extension == "gz") && input_shard == 0;
    let mut open = HashMap::<(String, u16), OpenSegment>::new();
    let mut ordinal = 0_u16;
    let file = File::open(path)?;
    let reader = BufReader::with_capacity(16 * 1024 * 1024, file);
    let mut write_message = |mut message: RawMessage| {
        let shard = if direct_multipart_gzip {
            u16::try_from((message.source_row - 1) / logical_shard_rows)
                .context("too many logical RAW shards in one source part")?
        } else {
            input_shard
        };
        if direct_multipart_gzip {
            message.source_row -= u64::from(shard) * logical_shard_rows;
        }
        if message.source_row > u64::from(u32::MAX) {
            bail!(
                "parsed RAW source row {} exceeds 32-bit source-order slot for {}",
                message.source_row,
                path.display()
            );
        }
        let entry = match open.entry((message.ric.clone(), shard)) {
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
                    shard,
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
    };
    if path.extension().is_some_and(|extension| extension == "gz") {
        let decoder = MultiGzDecoder::new(reader);
        if !direct_multipart_gzip || part == 0 {
            read_messages(decoder, &mut write_message)
        } else {
            read_messages_without_header(decoder, &mut write_message)
        }
    } else {
        read_messages(Decoder::new(reader)?, &mut write_message)
    }
    .with_context(|| format!("partition parsed messages from {}", path.display()))?;
    let mut result = Vec::with_capacity(open.len());
    for (_, segment) in open {
        result.push(finish_segment(segment, part, building)?);
    }
    result.sort_by_key(|segment| (segment.ric.clone(), segment.shard_index));
    Ok(result)
}

fn partition_with_logical_shard_rows(
    inputs: &[PathBuf],
    output: &Path,
    workers: usize,
    logical_shard_rows: u64,
) -> Result<ParsedManifest> {
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
                let result = partition_one(path, &building, logical_shard_rows);
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

pub fn partition(inputs: &[PathBuf], output: &Path, workers: usize) -> Result<ParsedManifest> {
    partition_with_logical_shard_rows(inputs, output, workers, LOGICAL_SHARD_ROWS)
}

fn repartition_one(
    source_root: &Path,
    source: &ParsedSegment,
    output_root: &Path,
    ordinal: usize,
    logical_shard_rows: u64,
) -> Result<Vec<ParsedSegment>> {
    if source.shard_index != 0 {
        bail!(
            "parsed repartition only accepts direct RAW source shard zero, got {}",
            source.file
        );
    }
    let mut current: Option<OpenSegment> = None;
    let mut result = Vec::new();
    let count = read_segment(
        &source_root.join(&source.file),
        &source.ric,
        source.original_part,
        source.shard_index,
        |mut message| {
            let shard = u16::try_from((message.source_row - 1) / logical_shard_rows)
                .context("too many logical RAW shards in one source part")?;
            message.source_row -= u64::from(shard) * logical_shard_rows;
            if message.source_row > u64::from(u32::MAX) {
                bail!("rebased RAW source row does not fit source-order slot");
            }
            if current
                .as_ref()
                .is_some_and(|segment| segment.shard != shard)
            {
                result.push(finish_segment(
                    current.take().expect("checked current segment"),
                    source.original_part,
                    output_root,
                )?);
            }
            if current.is_none() {
                let file = format!(
                    "parsed-part-{:06}-shard-{shard:06}-repacked-{ordinal:04}.bin.zst",
                    source.original_part
                );
                let mut encoder =
                    Encoder::new(BufWriter::new(File::create(output_root.join(&file))?), 1)?;
                encoder.write_all(MAGIC)?;
                let ric = message.ric.as_bytes();
                encoder.write_all(&u16::try_from(ric.len())?.to_le_bytes())?;
                encoder.write_all(ric)?;
                encoder.write_all(&source.original_part.to_le_bytes())?;
                encoder.write_all(&shard.to_le_bytes())?;
                current = Some(OpenSegment {
                    ric: message.ric.clone(),
                    file,
                    shard,
                    encoder,
                    messages: 0,
                    first_source_row: message.source_row,
                    last_source_row: message.source_row,
                    encoded_bytes: 0,
                });
            }
            let segment = current.as_mut().expect("initialized current segment");
            segment.encoded_bytes += write_parsed_message(&mut segment.encoder, &message)?;
            segment.messages += 1;
            segment.last_source_row = message.source_row;
            Ok(())
        },
    )?;
    if count != source.messages {
        bail!(
            "parsed message count changed while repartitioning {}",
            source.file
        );
    }
    if let Some(segment) = current {
        result.push(finish_segment(segment, source.original_part, output_root)?);
    }
    Ok(result)
}

/// Rewrites completed direct-delivery parsed segments into source-order-safe
/// logical shards without rereading the original gzip data.
fn repartition_with_logical_shard_rows(
    source_root: &Path,
    output: &Path,
    workers: usize,
    logical_shard_rows: u64,
) -> Result<ParsedManifest> {
    if workers == 0 || logical_shard_rows == 0 || logical_shard_rows > u64::from(u32::MAX) {
        bail!("parsed repartition workers and logical shard rows must be valid");
    }
    let source = ParsedManifest::load(source_root)?;
    if source
        .segments
        .iter()
        .any(|segment| segment.shard_index != 0)
    {
        bail!("parsed repartition requires direct RAW source segments with shard zero");
    }
    let building = output.with_extension("building");
    if output.exists() || building.exists() {
        bail!(
            "parsed repartition output already exists: {} or {}",
            output.display(),
            building.display()
        );
    }
    fs::create_dir_all(&building)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|i| format!("usstock-raw-repartition-{i}"))
        .build()?;
    let started = Instant::now();
    let completed = AtomicUsize::new(0);
    let total_messages = AtomicU64::new(0);
    let results = pool.install(|| {
        source
            .segments
            .par_iter()
            .enumerate()
            .map(|(ordinal, segment)| {
                let result = repartition_one(
                    source_root,
                    segment,
                    &building,
                    ordinal,
                    logical_shard_rows,
                );
                if let Ok(segments) = &result {
                    let messages = segments.iter().map(|segment| segment.messages).sum::<u64>();
                    let messages = total_messages.fetch_add(messages, Ordering::Relaxed) + messages;
                    let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                    eprintln!(
                        "repartition RAW progress segments={done}/{} messages={messages} elapsed_s={:.1} last={}",
                        source.segments.len(),
                        started.elapsed().as_secs_f64(),
                        segment.file
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
    segments.sort_by_key(|segment| {
        (
            segment.ric.clone(),
            segment.original_part,
            segment.shard_index,
        )
    });
    let manifest = ParsedManifest {
        schema: PARSED_SCHEMA.into(),
        complete: true,
        source_messages: segments.iter().map(|segment| segment.messages).sum(),
        encoded_bytes: segments.iter().map(|segment| segment.encoded_bytes).sum(),
        compressed_bytes: segments
            .iter()
            .map(|segment| segment.compressed_bytes)
            .sum(),
        segments,
    };
    serde_json::to_writer_pretty(File::create(building.join(PARSED_MANIFEST))?, &manifest)?;
    manifest.validate_inner(&building, false)?;
    fs::rename(&building, output)?;
    Ok(manifest)
}

pub fn repartition_parsed(
    source_root: &Path,
    output: &Path,
    workers: usize,
) -> Result<ParsedManifest> {
    repartition_with_logical_shard_rows(source_root, output, workers, LOGICAL_SHARD_ROWS)
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

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use std::fs::OpenOptions;
    use tempfile::tempdir;

    fn write_gzip(path: &Path, input: &[u8]) {
        let mut encoder = GzEncoder::new(File::create(path).unwrap(), Compression::default());
        encoder.write_all(input).unwrap();
        encoder.finish().unwrap();
    }

    fn append_gzip_member(path: &Path, input: &[u8]) {
        let file = OpenOptions::new().append(true).open(path).unwrap();
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(input).unwrap();
        encoder.finish().unwrap();
    }

    #[test]
    fn partition_accepts_single_merged_gzip_period() {
        let temporary = tempdir().unwrap();
        let input = temporary.path().join("merged-Data.csv.gz");
        write_gzip(&input, include_bytes!("../tests/fixtures/raw_small.csv"));

        let output = temporary.path().join("parsed");
        let manifest = partition(&[input], &output, 1).unwrap();
        assert!(manifest.complete);
        assert!(!manifest.segments.is_empty());
        assert!(manifest
            .segments
            .iter()
            .all(|segment| (segment.original_part, segment.shard_index) == (0, 0)));
        ParsedManifest::load(&output).unwrap();
    }

    #[test]
    fn partition_accepts_headerless_gzip_continuation_part() {
        let temporary = tempdir().unwrap();
        let part_zero = temporary.path().join("merged-Data-part-000000.csv.gz");
        let part_one = temporary.path().join("merged-Data-part-000001.csv.gz");
        let fixture = include_bytes!("../tests/fixtures/raw_small.csv");
        let first_newline = fixture.iter().position(|byte| *byte == b'\n').unwrap();
        write_gzip(&part_zero, fixture);
        write_gzip(&part_one, &fixture[first_newline + 1..]);

        let output = temporary.path().join("parsed");
        let manifest = partition(&[part_zero, part_one], &output, 2).unwrap();
        assert_eq!(manifest.source_messages, 6);
        assert!(manifest
            .segments
            .iter()
            .any(|segment| segment.original_part == 1 && segment.first_source_row == 1));
        ParsedManifest::load(&output).unwrap();
    }

    #[test]
    fn partition_reads_every_member_of_a_concatenated_gzip() {
        let temporary = tempdir().unwrap();
        let input = temporary.path().join("merged-Data-part-000000.csv.gz");
        let fixture = include_bytes!("../tests/fixtures/raw_small.csv");
        let first_newline = fixture.iter().position(|byte| *byte == b'\n').unwrap();
        write_gzip(&input, fixture);
        append_gzip_member(&input, &fixture[first_newline + 1..]);

        let output = temporary.path().join("parsed");
        let manifest = partition(&[input], &output, 1).unwrap();
        assert_eq!(manifest.source_messages, 6);
        ParsedManifest::load(&output).unwrap();
    }

    #[test]
    fn direct_gzip_logical_shards_rebase_source_rows() {
        let temporary = tempdir().unwrap();
        let input = temporary.path().join("merged-Data-part-000000.csv.gz");
        write_gzip(&input, include_bytes!("../tests/fixtures/raw_small.csv"));
        let building = temporary.path().join("building");
        fs::create_dir(&building).unwrap();

        let segments = partition_one(&input, &building, 3).unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(
            segments
                .iter()
                .map(|segment| segment.shard_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        for segment in segments {
            assert!(segment.last_source_row <= 3);
            read_segment(
                &building.join(&segment.file),
                &segment.ric,
                segment.original_part,
                segment.shard_index,
                |message| {
                    assert!(message.source_row <= 3);
                    Ok(())
                },
            )
            .unwrap();
        }
    }

    #[test]
    fn repartition_rebases_completed_direct_parsed_segments() {
        let temporary = tempdir().unwrap();
        let input = temporary.path().join("merged-Data-part-000000.csv.gz");
        write_gzip(&input, include_bytes!("../tests/fixtures/raw_small.csv"));
        let original = temporary.path().join("original");
        partition_with_logical_shard_rows(&[input], &original, 1, u64::from(u32::MAX)).unwrap();

        let output = temporary.path().join("repartitioned");
        let manifest = repartition_with_logical_shard_rows(&original, &output, 2, 3).unwrap();
        assert_eq!(manifest.source_messages, 3);
        assert_eq!(manifest.segments.len(), 3);
        assert_eq!(
            manifest
                .segments
                .iter()
                .map(|segment| segment.shard_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(manifest
            .segments
            .iter()
            .all(|segment| segment.last_source_row <= 3));
        ParsedManifest::load(&output).unwrap();
    }
}
