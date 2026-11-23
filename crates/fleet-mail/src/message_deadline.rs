//! A direct send keeps a 15-minute delivery window, clamped at Central admission.
//! Deadline settlement is due on the first grid mark at or after cutoff.
use chrono::{DateTime, Duration, Utc};

/// The one delivery window, from when the agent sent the message.
pub(crate) const DELIVERY_WINDOW: Duration = Duration::minutes(15);
const SYSTEM_MESSAGE_DELIVERY_WINDOW: Duration = Duration::hours(24);
pub(crate) const EXPIRED_24_HOURS: &str = "expired24Hours";
/// Due items run on this grid, like wakes.
const DUE_GRID_SECONDS: u32 = 5 * 60;

/// A notice or delivery outcome expires on the first grid mark at or after 24 hours.
pub(crate) fn system_message_deadline(created_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let earliest = created_at.checked_add_signed(SYSTEM_MESSAGE_DELIVERY_WINDOW)?;
    central_engine_model::aligned_at_or_after(earliest, DUE_GRID_SECONDS)
}

/// Return the stored receipt mark for a direct send.
/// A sender clock ahead of Central cannot extend the delivery window. A send
/// already at its cutoff is admitted with zero time left. The mark is the first
/// grid point at or after cutoff.
pub fn direct_receipt_mark(
    sent_at: DateTime<Utc>,
    accepted_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    direct_deadline_occurrence(delivery_window_ends_at(sent_at, accepted_at)?)
}

pub(crate) fn direct_deadline_occurrence(cutoff: DateTime<Utc>) -> Option<DateTime<Utc>> {
    central_engine_model::aligned_at_or_after(cutoff, DUE_GRID_SECONDS)
}

pub(crate) fn delivery_window_ends_at(
    sent_at: DateTime<Utc>,
    accepted_at: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let earliest_send = accepted_at.checked_sub_signed(DELIVERY_WINDOW)?;
    let cutoff = sent_at
        .clamp(earliest_send, accepted_at)
        .checked_add_signed(DELIVERY_WINDOW)?;
    // Central stores millisecond timestamps. Round up once so fractional
    // wire timestamps cannot bring the deadline forward or skip a mark.
    let round_up = i64::from(cutoff.timestamp_subsec_nanos() % 1_000_000 != 0);
    DateTime::from_timestamp_millis(cutoff.timestamp_millis().checked_add(round_up)?)
}

/// Whether the message's delivery window has run out at `now`: central never
/// pushes it after this, not even on reconnect.
#[cfg(test)]
#[must_use]
fn delivery_window_open(
    sent_at: DateTime<Utc>,
    accepted_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> bool {
    delivery_window_ends_at(sent_at, accepted_at).is_some_and(|cutoff| now < cutoff)
}

#[cfg(test)]
#[path = "message_deadline_test.rs"]
mod tests;
