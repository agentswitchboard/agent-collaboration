use std::{
    future::Future,
    path::Path,
    process::{Command, Stdio},
};
use tokio::io::AsyncReadExt;

pub(super) async fn process_started_at(
    process_id: u32,
    deadline: impl Future<Output = ()>,
) -> Option<i64> {
    let mut child = spawn_process_start(Path::new("/bin/ps"), process_id)?;
    read_process_start(&mut child, deadline).await
}

fn spawn_process_start(command_path: &Path, process_id: u32) -> Option<tokio::process::Child> {
    let mut command = Command::new(command_path);
    command
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .args(["-p", &process_id.to_string(), "-o", "lstart="])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    tokio::process::Command::from(command)
        .kill_on_drop(true)
        .spawn()
        .ok()
}

async fn read_process_start(
    child: &mut tokio::process::Child,
    deadline: impl Future<Output = ()>,
) -> Option<i64> {
    let mut stdout = child.stdout.take()?.take(128);
    let mut bytes = Vec::new();
    let finished = async {
        let (status, _) = tokio::try_join!(child.wait(), stdout.read_to_end(&mut bytes)).ok()?;
        status.success().then_some(())
    };
    tokio::select! {
        biased;
        result = finished => result?,
        () = deadline => {
            // Tokio's kill waits for the child after sending the signal.
            let _ = child.kill().await;
            return None;
        }
    }
    parse_ps_start(std::str::from_utf8(&bytes).ok()?)
}

/// Convert the C locale's `%a %b %e %T %Y` in UTC to Unix milliseconds.
fn parse_ps_start(output: &str) -> Option<i64> {
    let fields: Vec<_> = output.trim().split_ascii_whitespace().collect();
    let [weekday, month, day, time, year] = fields.as_slice() else {
        return None;
    };
    let weekday = i64::try_from(
        ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
            .iter()
            .position(|name| name == weekday)?,
    )
    .ok()?;
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|name| name == month)?;
    let year = year
        .parse::<i64>()
        .ok()
        .filter(|year| (1970..=9999).contains(year))?;
    let day = day.parse::<i64>().ok()?;
    let leap = i64::from(year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    let month_lengths = [31, 28 + leap, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if !(1..=*month_lengths.get(month)?).contains(&day) {
        return None;
    }
    let leap_days_before = |year: i64| {
        let last_year = year - 1;
        last_year / 4 - last_year / 100 + last_year / 400
    };
    let days = (year - 1970) * 365 + leap_days_before(year) - leap_days_before(1970)
        + month_lengths.get(..month)?.iter().sum::<i64>()
        + day
        - 1;
    if (days + 4) % 7 != weekday {
        return None;
    }
    let time: Vec<_> = time.split(':').collect();
    let [hour, minute, second] = time.as_slice() else {
        return None;
    };
    let hour = hour
        .parse::<i64>()
        .ok()
        .filter(|hour| (0..24).contains(hour))?;
    let minute = minute
        .parse::<i64>()
        .ok()
        .filter(|minute| (0..60).contains(minute))?;
    let second = second
        .parse::<i64>()
        .ok()
        .filter(|second| (0..60).contains(second))?;
    Some((((days * 24 + hour) * 60 + minute) * 60 + second) * 1_000)
}

#[cfg(test)]
#[path = "registry_process_start_test.rs"]
mod tests;
