//! Synchronous, bounded tmpfs logging on the market-data thread. No worker,
//! queue, fsync, or disk fallback; stdout/stderr also point at the memory file.
use anyhow::{Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const MAX_FILE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_RECORD_BYTES: usize = 64 * 1024;

fn suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn open_memory_file(path: &Path, truncate: bool) -> io::Result<File> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { stat.assume_init() }.f_type != libc::TMPFS_MAGIC {
        return Err(io::Error::other("market-data logs require tmpfs"));
    }
    if truncate {
        file.set_len(0)?;
    }
    Ok(file)
}

fn redirect_stdio(file: &File) -> io::Result<()> {
    for fd in [libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        if unsafe { libc::dup2(file.as_raw_fd(), fd) } == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

struct MemoryLog {
    path: PathBuf,
    file: File,
    _lock: File,
    max_bytes: u64,
    redirect: bool,
    dropped: u64,
    write_errors: u64,
    reported: (u64, u64),
}

impl MemoryLog {
    fn open(path: PathBuf, max_bytes: u64, redirect: bool) -> io::Result<Self> {
        let lock = open_memory_file(&suffix(&path, ".lock"), false)?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let file = open_memory_file(&path, false)?;
        if redirect {
            redirect_stdio(&file)?;
        }
        Ok(Self {
            path,
            file,
            _lock: lock,
            max_bytes,
            redirect,
            dropped: 0,
            write_errors: 0,
            reported: (0, 0),
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        let next = suffix(&self.path, ".next");
        // Create the replacement before disturbing the current file. ENOSPC
        // drops diagnostics; it must never switch the receiver back to ext4.
        let replacement = open_memory_file(&next, true)?;
        let previous = suffix(&self.path, ".1");
        match fs::remove_file(&previous) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        fs::hard_link(&self.path, &previous)?;
        fs::rename(&next, &self.path)?;
        if self.redirect {
            redirect_stdio(&replacement)?;
        }
        self.file = replacement;
        Ok(())
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut before = self.file.metadata()?.len();
        if before.saturating_add(bytes.len() as u64) > self.max_bytes {
            self.rotate()?;
            before = 0;
        }
        if let Err(error) = self.file.write_all(bytes) {
            // A full tmpfs may accept only a prefix. Do not leave half a JSON
            // summary that would poison the independent collector's cursor.
            let _ = self.file.set_len(before);
            return Err(error);
        }
        Ok(())
    }
}

impl Write for MemoryLog {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_RECORD_BYTES || bytes.len() as u64 > self.max_bytes {
            self.dropped = self.dropped.saturating_add(1);
            return Ok(bytes.len());
        }
        let totals = (self.dropped, self.write_errors);
        if self.reported != totals {
            let notice = format!(
                "spread_log_loss dropped_total={} write_errors_total={}\n",
                totals.0, totals.1
            );
            if self.append(notice.as_bytes()).is_ok() {
                self.reported = totals;
            }
        }
        if self.append(bytes).is_err() {
            self.dropped = self.dropped.saturating_add(1);
            self.write_errors = self.write_errors.saturating_add(1);
        }
        // Avoid retries, panics, or an implicit stderr/disk fallback on ENOSPC.
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn init(role: &str, test: bool) -> Result<()> {
    let root = PathBuf::from(format!("/dev/shm/spread_pbs-{}", unsafe {
        libc::geteuid()
    }));
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&root)
        .context("create tmpfs market-data log directory")?;
    let path = root.join(format!(
        "binance-futures-{role}{}.log",
        if test { "-test" } else { "" }
    ));
    let writer = MemoryLog::open(path.clone(), MAX_FILE_BYTES, true)
        .context("open exclusive tmpfs market-data log")?;
    env_logger::Builder::from_default_env()
        .target(env_logger::Target::Pipe(Box::new(writer)))
        .try_init()
        .context("install synchronous tmpfs logger")?;
    log::info!(
        "spread_pbs tmpfs_log path={} max_file_bytes={} retained_files=2 worker_threads=0",
        path.display(),
        MAX_FILE_BYTES
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Directory(PathBuf);
    impl Directory {
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn directory() -> Directory {
        let dir = Directory(PathBuf::from(format!(
            "/dev/shm/spread-log-test-{}",
            uuid::Uuid::new_v4()
        )));
        fs::create_dir(&dir.0).unwrap();
        dir
    }

    #[test]
    fn rotation_preserves_previous_tail_and_changes_inode() {
        use std::os::unix::fs::MetadataExt;
        let dir = directory();
        let path = dir.path().join("market.log");
        let mut log = MemoryLog::open(path.clone(), 12, false).unwrap();
        log.write_all(b"first\n").unwrap();
        let inode = fs::metadata(&path).unwrap().ino();
        log.write_all(b"second\n").unwrap();
        assert_ne!(inode, fs::metadata(&path).unwrap().ino());
        assert_eq!(fs::read(suffix(&path, ".1")).unwrap(), b"first\n");
        assert_eq!(fs::read(&path).unwrap(), b"second\n");
        log.write_all(b"third\n").unwrap();
        assert_eq!(fs::read(suffix(&path, ".1")).unwrap(), b"second\n");
        assert_eq!(fs::read(&path).unwrap(), b"third\n");
    }

    #[test]
    fn only_one_process_can_own_a_role_log() {
        let dir = directory();
        let path = dir.path().join("market.log");
        let writer = MemoryLog::open(path.clone(), 1024, false).unwrap();
        assert!(MemoryLog::open(path.clone(), 1024, false).is_err());
        drop(writer);
        assert!(MemoryLog::open(path, 1024, false).is_ok());
    }

    #[test]
    fn stdout_stderr_follow_memory_log_rotation() {
        const CHILD: &str = "SPREAD_TMPFS_LOG_TEST_CHILD";
        if let Some(path) = std::env::var_os(CHILD) {
            let path = PathBuf::from(path);
            let mut log = MemoryLog::open(path.clone(), 32, true).unwrap();
            for fd in [1, 2] {
                assert_eq!(fs::read_link(format!("/proc/self/fd/{fd}")).unwrap(), path);
                assert_eq!(unsafe { libc::write(fd, b"stdio\n".as_ptr().cast(), 6) }, 6);
            }
            log.write_all(b"replacement-record-123456\n").unwrap();
            for fd in [1, 2] {
                assert_eq!(fs::read_link(format!("/proc/self/fd/{fd}")).unwrap(), path);
            }
            assert_eq!(fs::read(suffix(&path, ".1")).unwrap(), b"stdio\nstdio\n");
            return;
        }
        let dir = directory();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tmpfs_log::tests::stdout_stderr_follow_memory_log_rotation",
                "--nocapture",
            ])
            .env(CHILD, dir.path().join("stdio.log"))
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[test]
    fn rotation_failure_drops_and_reports_without_blocking_or_disk_fallback() {
        let dir = directory();
        let path = dir.path().join("market.log");
        let mut log = MemoryLog::open(path.clone(), 256, false).unwrap();
        log.write_all(&[b'x'; 250]).unwrap();
        // A replacement that cannot be opened simulates a failed rotation.
        fs::create_dir(suffix(&path, ".next")).unwrap();
        log.write_all(b"dropped\n").unwrap();
        assert_eq!((log.dropped, log.write_errors), (1, 1));
        fs::remove_dir(suffix(&path, ".next")).unwrap();
        log.write_all(b"recovered\n").unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("spread_log_loss dropped_total=1 write_errors_total=1"));
        assert!(text.ends_with("recovered\n"));
    }
}
