use central_link_client::WelcomeClockAnchor;
use router_clock::ClockInstant;
use runtime_primitives::SyncTime;
use std::time::Duration;

/// Projects the minute grid using elapsed time since the socket owner observed Welcome.
#[must_use]
pub fn shared_time_at(anchor: WelcomeClockAnchor, now: ClockInstant) -> SyncTime {
    let elapsed = now.saturating_duration_since(anchor.observed_at);
    let elapsed_ms = i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX);
    SyncTime::from_unix_millis(
        anchor
            .central_time
            .timestamp_millis()
            .saturating_add(elapsed_ms),
    )
}

/// Arms the shared-time mark and its selected offset on the elapsed clock.
#[must_use]
pub fn wake_for_mark(anchor: WelcomeClockAnchor, mark: SyncTime, jitter_ms: i64) -> ClockInstant {
    let limit = crate::flush_marks::FLUSH_JITTER_MILLIS;
    let send_at_ms = mark
        .unix_millis()
        .saturating_add(jitter_ms.clamp(-limit, limit));
    let delay_ms = send_at_ms.saturating_sub(anchor.central_time.timestamp_millis());
    anchor
        .observed_at
        .after(Duration::from_millis(u64::try_from(delay_ms).unwrap_or(0)))
}

#[cfg(test)]
#[path = "flush_clock_test.rs"]
mod tests;
