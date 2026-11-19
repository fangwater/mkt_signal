//! Last confirmed account observation for display during a cold restart.
use crate::{service_name, Adapter, RuntimeStatus, Snapshot, Text, PAYLOAD};
use anyhow::{ensure, Context, Result};
use bincode::Options;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    namespace: String,
    snapshot: Snapshot,
}

pub fn validate_path(path: &Path) -> Result<()> {
    ensure!(
        path.is_absolute()
            && path.file_name().is_some()
            && !path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir)),
        "account snapshot path must be an absolute file path"
    );
    Ok(())
}

pub fn confirmed(snapshot: &Snapshot) -> bool {
    snapshot.runtime_status == RuntimeStatus::Running
        && snapshot.position_ready
        && snapshot.connected
        && snapshot.transport_ready
        && (snapshot.ready
            || snapshot.reason.as_str()
                == "settlement not confirmed; read-only queries remain available")
}

pub fn for_display(mut snapshot: Snapshot) -> Snapshot {
    snapshot.runtime_status = RuntimeStatus::Stopped;
    snapshot.connected = false;
    snapshot.transport_ready = false;
    snapshot.ready = false;
    snapshot.can_trade = false;
    snapshot.l1_connected = false;
    snapshot.reason = Text::default();
    snapshot
}

fn options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(PAYLOAD as u64)
        .reject_trailing_bytes()
}

pub fn load(
    path: &Path,
    namespace: &str,
    adapter: Adapter,
    account_hash: &str,
) -> Result<Option<Snapshot>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("open account snapshot"),
    };
    let mut bytes = Vec::new();
    file.take(PAYLOAD as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= PAYLOAD,
        "account snapshot exceeds size limit"
    );
    let file: Checkpoint = options()
        .deserialize(&bytes)
        .context("decode account snapshot")?;
    let snapshot = file.snapshot;
    snapshot.validate()?;
    ensure!(
        file.namespace == namespace
            && snapshot.adapter == adapter
            && snapshot.account_hash.as_str() == account_hash,
        "account snapshot identity mismatch"
    );
    ensure!(
        snapshot.position_ready && snapshot.observed_us <= crate::publisher::now_us(),
        "invalid account snapshot observation"
    );
    Ok(Some(for_display(snapshot)))
}

pub(crate) fn save(path: &Path, namespace: &str, snapshot: Snapshot) -> Result<()> {
    validate_path(path)?;
    service_name(namespace)?;
    snapshot.validate()?;
    let bytes = options().serialize(&Checkpoint {
        namespace: namespace.to_owned(),
        snapshot: for_display(snapshot),
    })?;
    let dir = path
        .parent()
        .context("account snapshot has no parent directory")?;
    let name = path
        .file_name()
        .context("account snapshot has no file name")?;
    let mut temp_name = name.to_owned();
    temp_name.push(format!(
        ".{}.{}.tmp",
        std::process::id(),
        crate::publisher::now_us()
    ));
    let temp_path = dir.join(temp_name);
    let mut open = OpenOptions::new();
    open.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        open.mode(0o600);
    }
    let mut file = open
        .open(&temp_path)
        .context("create temporary account snapshot")?;
    let result = (|| -> Result<()> {
        file.write_all(&bytes).context("write account snapshot")?;
        file.sync_all().context("sync account snapshot")?;
        fs::rename(&temp_path, path).context("install account snapshot")?;
        File::open(dir)?
            .sync_all()
            .context("sync account snapshot directory")?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}
