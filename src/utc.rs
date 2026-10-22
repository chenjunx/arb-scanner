//! UTC 时间戳(毫秒)和 `2026-10-20T23:00Z` 这种字符串互转。不引日期库，只处理
//! UTC、只到秒。

/// 公历日期 → 1970-01-01 起的天数(Howard Hinnant 的 days_from_civil)。
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// 1970-01-01 起的天数 → 公历 (年, 月, 日)(civil_from_days)。
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(month <= 2), month, day)
}

/// 毫秒 → `2026-10-20 23:00Z`(日志/报表用，精确到分钟)。
pub fn format_minute(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    format!("{year:04}-{month:02}-{day:02} {:02}:{:02}Z", sod / 3600, sod % 3600 / 60)
}

/// 解析 `YYYY-MM-DD`、`YYYY-MM-DDTHH:MM`、`YYYY-MM-DDTHH:MM:SS`(`T` 也可以是空格，
/// 末尾的 `Z` 可省略，一律按 UTC)，或者纯数字的毫秒时间戳。
pub fn parse(text: &str) -> anyhow::Result<u64> {
    let s = text.trim();
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        return s.parse().map_err(|e| anyhow::anyhow!("invalid timestamp '{text}': {e}"));
    }
    let bad = || anyhow::anyhow!("invalid UTC time '{text}', expected e.g. 2026-10-20T23:00Z");
    let s = s.strip_suffix('Z').or_else(|| s.strip_suffix('z')).unwrap_or(s);
    let (date, time) = match s.split_once(['T', 't', ' ']) {
        Some((date, time)) => (date, time),
        None => (s, "00:00"),
    };
    let num = |part: &str, max: i64| -> anyhow::Result<i64> {
        part.parse::<i64>().ok().filter(|v| (0..=max).contains(v)).ok_or_else(bad)
    };
    let date: Vec<&str> = date.split('-').collect();
    let time: Vec<&str> = time.split(':').collect();
    if date.len() != 3 || !(2..=3).contains(&time.len()) || date[0].len() != 4 {
        return Err(bad());
    }
    let (year, month, day) = (num(date[0], 9999)?, num(date[1], 12)?, num(date[2], 31)?);
    if year < 1970 || month == 0 || day == 0 {
        return Err(bad());
    }
    let (hour, minute) = (num(time[0], 23)?, num(time[1], 59)?);
    let second = if time.len() == 3 { num(time[2], 59)? } else { 0 };
    let days = days_from_civil(year, month, day);
    if civil_from_days(days) != (year, month, day) {
        return Err(bad());
    }
    Ok(((days * 86_400 + hour * 3600 + minute * 60 + second) * 1000) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_formats_utc() {
        // 2026-10-20T23:00:00Z = 1792537200
        assert_eq!(parse("2026-10-20T23:00Z").unwrap(), 1_792_537_200_000);
        assert_eq!(parse("2026-10-20 23:00:00").unwrap(), 1_792_537_200_000);
        assert_eq!(parse("2026-10-21").unwrap(), 1_792_540_800_000);
        assert_eq!(parse("1792537200000").unwrap(), 1_792_537_200_000);
        assert_eq!(format_minute(1_792_537_200_000), "2026-10-20 23:00Z");
        assert_eq!(parse("2024-02-29T00:00Z").map(format_minute).unwrap(), "2024-02-29 00:00Z");
    }

    #[test]
    fn rejects_malformed_times() {
        for bad in ["", "2026-13-01", "2026-02-30", "2026-10-20T24:00", "26-10-20", "2026-10-20T23", "abc"] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }
}
