use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;

use chrono::{Duration, Local, NaiveDate};

/// 初始化日志：RUST_LOG 控制级别，默认 info。
/// 设置 ARB_LOG_DIR 后按本地日期滚动写入 `<dir>/<ARB_LOG_PREFIX>.YYYY-MM-DD.log`，
/// 只保留最近 ARB_LOG_KEEP_DAYS 天(默认 3)；未设置则输出到 stderr。
pub fn init_logging() {
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
    if let Ok(dir) = std::env::var("ARB_LOG_DIR") {
        let prefix = std::env::var("ARB_LOG_PREFIX").unwrap_or_else(|_| "arb-scanner".into());
        let keep_days = std::env::var("ARB_LOG_KEEP_DAYS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|d| *d >= 1)
            .unwrap_or(3);
        match DailyFileWriter::new(PathBuf::from(dir), prefix, keep_days) {
            Ok(w) => {
                builder.target(env_logger::Target::Pipe(Box::new(w)));
            }
            Err(e) => eprintln!("无法初始化滚动日志，回退到 stderr: {e}"),
        }
    }
    builder.init();
}

struct DailyFileWriter {
    dir: PathBuf,
    prefix: String,
    keep_days: i64,
    date: NaiveDate,
    file: File,
}

impl DailyFileWriter {
    fn new(dir: PathBuf, prefix: String, keep_days: i64) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;
        let date = Local::now().date_naive();
        let file = Self::open(&dir, &prefix, date)?;
        let w = Self { dir, prefix, keep_days, date, file };
        w.cleanup();
        Ok(w)
    }

    fn open(dir: &PathBuf, prefix: &str, date: NaiveDate) -> io::Result<File> {
        let path = dir.join(format!("{prefix}.{}.log", date.format("%Y-%m-%d")));
        OpenOptions::new().create(true).append(true).open(path)
    }

    fn cleanup(&self) {
        let cutoff = self.date - Duration::days(self.keep_days - 1);
        let head = format!("{}.", self.prefix);
        let Ok(entries) = fs::read_dir(&self.dir) else { return };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(rest) = name.strip_prefix(&head) else { continue };
            let Some(date_str) = rest.strip_suffix(".log") else { continue };
            if let Ok(d) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                if d < cutoff {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }
}

impl Write for DailyFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let today = Local::now().date_naive();
        if today != self.date {
            if let Ok(f) = Self::open(&self.dir, &self.prefix, today) {
                self.file = f;
                self.date = today;
                self.cleanup();
            }
        }
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}
