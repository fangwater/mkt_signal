use anyhow::{Context, Result};
use cn_futures_pbs::sources::redis::{Event, Message, RedisFeed, DATA_TOPICS};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    net::SocketAddrV4,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub fn run(
    endpoint: SocketAddrV4,
    directory: Option<&Path>,
    seconds: Option<u64>,
    max_messages: Option<u64>,
) -> Result<()> {
    let mut feed = RedisFeed::new(endpoint, &DATA_TOPICS);
    let mut archive = directory.map(Archive::open).transpose()?;
    let mut counts: BTreeMap<_, _> = DATA_TOPICS.into_iter().map(|topic| (topic, 0u64)).collect();
    let mut total = 0u64;
    let mut resets = 0u64;
    let start = Instant::now();
    let mut last_flush = start;
    let mut last_stats = start;
    eprintln!(
        "cn-futures-pbs: receiving {} Redis channels from {endpoint}",
        DATA_TOPICS.len()
    );
    while seconds.is_none_or(|limit| start.elapsed() < Duration::from_secs(limit))
        && max_messages.is_none_or(|limit| total < limit)
    {
        match feed.poll() {
            Some(Event::Message(message)) => {
                if let Some(archive) = archive.as_mut() {
                    archive.append(&message)?;
                }
                *counts
                    .get_mut(message.topic.as_str())
                    .context("unexpected Redis topic")? += 1;
                total += 1;
            }
            Some(Event::Reset) => {
                if let Some(archive) = archive.as_mut() {
                    archive.reset()?;
                }
                resets += 1;
            }
            None => {}
        }
        if last_flush.elapsed() >= Duration::from_secs(1) {
            if let Some(archive) = archive.as_mut() {
                archive.flush()?;
            }
            last_flush = Instant::now();
        }
        if last_stats.elapsed() >= Duration::from_secs(60) {
            eprintln!("cn-futures-pbs: Redis received={total} continuity_resets={resets}");
            last_stats = Instant::now();
        }
    }
    if let Some(archive) = archive.as_mut() {
        archive.flush()?;
    }
    for (topic, count) in counts {
        println!("{topic} {count}");
    }
    println!("received={total} continuity_resets={resets}");
    Ok(())
}

struct Archive {
    directory: PathBuf,
    hour: i64,
    file: Option<BufWriter<File>>,
    resets: File,
    frame: Vec<u8>,
}

impl Archive {
    fn open(directory: &Path) -> Result<Self> {
        fs::create_dir_all(directory).context("create Redis archive directory")?;
        let resets = OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join("resets.log"))
            .context("open Redis continuity journal")?;
        resets.try_lock().context("lock Redis archive directory")?;
        Ok(Self {
            directory: directory.to_owned(),
            hour: i64::MIN,
            file: None,
            resets,
            frame: Vec::new(),
        })
    }

    fn append(&mut self, message: &Message) -> Result<()> {
        let now = chrono::Utc::now();
        let hour = now.timestamp().div_euclid(3600);
        if hour != self.hour {
            self.flush()?;
            // Rotate by Shanghai receipt time. Native trading day, action day
            // and exchange time remain exactly as supplied in the payload.
            let local = now.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap());
            let day = self.directory.join(local.format("%Y%m%d").to_string());
            fs::create_dir_all(&day).context("create Redis archive day")?;
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(day.join(local.format("%H.mdl").to_string()))
                .context("open Redis hourly archive")?;
            self.file = Some(BufWriter::with_capacity(256 * 1024, file));
            self.hour = hour;
        }
        let frame = message.mdl_frame(&mut self.frame)?;
        let frame = frame.strip_suffix(b"\n").unwrap_or(frame);
        let frame = frame.strip_suffix(b"\r").unwrap_or(frame);
        let file = self.file.as_mut().context("missing Redis hourly archive")?;
        file.write_all(frame).context("write Redis MDL frame")?;
        file.write_all(b"\n").context("write Redis MDL newline")
    }

    fn reset(&mut self) -> Result<()> {
        // Keep resets in a sidecar so each data line stays a valid MDL
        // frame, and preserve file order up to the continuity boundary.
        self.flush()?;
        writeln!(self.resets, "{}", chrono::Utc::now().to_rfc3339())
            .context("record Redis continuity reset")
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.flush().context("flush Redis hourly archive")?;
        }
        Ok(())
    }
}
