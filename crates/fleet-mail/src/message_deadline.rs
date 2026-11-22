//! A direct send keeps a 15-minute delivery window, clamped at Central admission.
//! The receipt mark falls on the five-minute grid at least two minutes after
//! the cutoff. The margin gives the recipient time to report delivery evidence
//! before a due batch can select that message.
use chrono::{DateTime, Duration, Utc};

/// The one delivery window, from when the agent sent the message.
pub(crate) const DELIVERY_WINDOW: Duration = Duration::minutes(15);
const SYSTEM_MESSAGE_DELIVERY_WINDOW: Duration = Duration::hours(24);
pub(crate) const EXPIRED_24_HOURS: &str = "expired24Hours";
/// Grace for the recipient to report delivery before the mark.
pub(crate) const RECEIPT_MARGIN: Duration = Duration::minutes(2);
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
/// grid point at or after cutoff plus the receipt margin.
#[must_use]
pub fn direct_receipt_mark(sent_at: DateTime<Utc>, accepted_at: DateTime<Utc>) -> DateTime<Utc> {
    round_up_to_grid(delivery_window_ends_at(sent_at, accepted_at) + RECEIPT_MARGIN)
}

pub(crate) fn delivery_window_ends_at(
    sent_at: DateTime<Utc>,
    accepted_at: DateTime<Utc>,
) -> DateTime<Utc> {
    sent_at.clamp(accepted_at - DELIVERY_WINDOW, accepted_at) + DELIVERY_WINDOW
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
    now < delivery_window_ends_at(sent_at, accepted_at)
}

fn round_up_to_grid(at: DateTime<Utc>) -> DateTime<Utc> {
    let millis = at.timestamp_millis();
    let grid = i64::from(DUE_GRID_SECONDS) * 1000;
    let rounded = millis.div_euclid(grid) * grid
        + if millis.rem_euclid(grid) == 0 {
            0
        } else {
            grid
        };
    DateTime::from_timestamp_millis(rounded).unwrap_or(at)
}

#[cfg(test)]
#[path = "message_deadline_test.rs"]
mod tests;
