use super::*;
use hegel::{TestCase, generators as gs};

/// 2030-01-01T00:00:00Z, on the 5-minute grid.
const BASE: i64 = 1_893_456_000_000;

fn at(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(BASE + millis).unwrap()
}
const MINUTE: i64 = 60_000;

#[test]
fn an_aligned_delivery_cutoff_is_the_deadline_occurrence_without_extra_margin() {
    assert_eq!(direct_receipt_mark(at(0), at(0)), at(15 * MINUTE));
}

#[test]
fn the_deadline_occurrence_is_the_first_grid_mark_at_or_after_cutoff() {
    let cases = [
        // (sent, accepted, deadline occurrence), in ms from BASE.
        (0, 0, 15 * MINUTE),
        (3 * MINUTE, 3 * MINUTE, 20 * MINUTE),
        (5 * MINUTE + 1, 5 * MINUTE + 1, 25 * MINUTE),
        (0, 4 * MINUTE, 15 * MINUTE),
        (0, 15 * MINUTE, 15 * MINUTE),
        (0, 20 * MINUTE, 20 * MINUTE),
        (10 * MINUTE, 0, 15 * MINUTE),
    ];
    for (sent, accepted, expected) in cases {
        assert_eq!(
            direct_receipt_mark(at(sent), at(accepted)),
            at(expected),
            "sent {sent} accepted {accepted}"
        );
    }
}

#[test]
fn the_window_closes_exactly_15_minutes_after_the_clamped_send() {
    assert!(delivery_window_open(at(0), at(0), at(15 * MINUTE - 1)));
    assert!(!delivery_window_open(at(0), at(0), at(15 * MINUTE)));
    // A fast Mac clock cannot keep the window open longer.
    assert!(!delivery_window_open(
        at(10 * MINUTE),
        at(0),
        at(15 * MINUTE)
    ));
}

#[test]
fn system_expiry_uses_the_first_grid_mark_at_or_after_24_hours() {
    let day = 24 * 60 * MINUTE;
    assert_eq!(system_message_deadline(at(0)), Some(at(day)));
    assert_eq!(system_message_deadline(at(1)), Some(at(day + 5 * MINUTE)));
    assert_eq!(
        system_message_deadline(at(5 * MINUTE - 1)),
        Some(at(day + 5 * MINUTE))
    );
}

#[hegel::test]
fn the_deadline_occurrence_is_on_grid_within_one_step_of_the_cutoff(tc: TestCase) {
    let accepted = tc.draw(
        gs::integers::<i64>()
            .min_value(0)
            .max_value(10_000 * MINUTE),
    );
    let skew = tc.draw(
        gs::integers::<i64>()
            .min_value(-60 * MINUTE)
            .max_value(60 * MINUTE),
    );
    let sent = accepted + skew;

    let due = direct_receipt_mark(at(sent), at(accepted));

    let clamped = sent.clamp(accepted - 15 * MINUTE, accepted);
    let due_ms = due.timestamp_millis() - BASE;
    assert_eq!(due_ms.rem_euclid(5 * MINUTE), 0, "on the 5-minute grid");
    assert!(
        due_ms >= clamped + 15 * MINUTE,
        "never before the delivery cutoff"
    );
    assert!(
        due_ms < clamped + 20 * MINUTE,
        "within one grid step of cutoff"
    );
    assert!(
        !delivery_window_open(at(sent), at(accepted), due),
        "the window is closed at the deadline occurrence"
    );
}
