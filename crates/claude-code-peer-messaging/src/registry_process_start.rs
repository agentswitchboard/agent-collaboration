use std::{
    future::Future,
    path::Path,
    process::{Command, Stdio},
};
use tokio::io::AsyncReadExt;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ProcessStartObservation {
    DisplayedSecond(i64),
    Unavailable,
    Interrupted,
    ReapDeferred,
}

pub(super) async fn process_started_at<TReap: Future<Output = ()>>(
    process_id: u32,
    stop: impl Future<Output = ()>,
    reap_deadline: impl FnOnce() -> TReap,
) -> ProcessStartObservation {
    let Some(mut child) = spawn_process_start(Path::new("/bin/ps"), process_id) else {
        return ProcessStartObservation::Unavailable;
    };
    read_process_start(&mut child, stop, reap_deadline).await
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

async fn read_process_start<TReap: Future<Output = ()>>(
    child: &mut tokio::process::Child,
    stop: impl Future<Output = ()>,
    reap_deadline: impl FnOnce() -> TReap,
) -> ProcessStartObservation {
    let mut stdout = child.stdout.take().map(|stdout| stdout.take(128));
    let mut bytes = Vec::new();
    let finished = async {
        let read = async {
            match &mut stdout {
                Some(stdout) => stdout.read_to_end(&mut bytes).await.is_ok(),
                None => false,
            }
        };
        tokio::join!(child.wait(), read)
    };
    let finished = tokio::select! {
        biased;
        () = stop => None,
        result = finished => Some(result),
    };
    match finished {
        Some((Ok(status), true)) if status.success() => std::str::from_utf8(&bytes)
            .ok()
            .and_then(parse_ps_start)
            .map_or(
                ProcessStartObservation::Unavailable,
                ProcessStartObservation::DisplayedSecond,
            ),
        Some((Ok(_), _)) => ProcessStartObservation::Unavailable,
        Some((Err(_), _)) => ProcessStartObservation::ReapDeferred,
        None => reap_interrupted_probe(child, reap_deadline()).await,
    }
}

async fn reap_interrupted_probe(
    child: &mut tokio::process::Child,
    deadline: impl Future<Output = ()>,
) -> ProcessStartObservation {
    if child.start_kill().is_err() {
        return ProcessStartObservation::ReapDeferred;
    }
    wait_for_reap(child.wait(), deadline).await
}

async fn wait_for_reap(
    wait: impl Future<Output = std::io::Result<std::process::ExitStatus>>,
    deadline: impl Future<Output = ()>,
) -> ProcessStartObservation {
    tokio::select! {
        biased;
        result = wait => match result {
            Ok(_) => ProcessStartObservation::Interrupted,
            Err(_) => ProcessStartObservation::ReapDeferred,
        },
        () = deadline => ProcessStartObservation::ReapDeferred,
    }
}

/// Convert the C locale's `%a %b %e %T %Y` in UTC to Unix milliseconds.
pub(super) fn parse_ps_start(output: &str) -> Option<i64> {
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
