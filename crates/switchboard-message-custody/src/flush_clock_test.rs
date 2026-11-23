use super::{ClockInstant, Duration, SyncTime, WelcomeClockAnchor, shared_time_at, wake_for_mark};
use router_clock::{ManualClock, RuntimeClock};

const MINUTE_MARK_MS: i64 = 1_763_380_800_000;

#[test]
fn a_wall_clock_jump_neither_moves_the_shared_minute_nor_rearms_its_wake() {
    let clock = ManualClock::starting_at(SyncTime::from_unix_millis(MINUTE_MARK_MS)).unwrap();
    clock.advance(Duration::from_secs(10)).unwrap();
    let anchor = WelcomeClockAnchor {
        central_time: chrono::DateTime::from_timestamp_millis(MINUTE_MARK_MS - 7_000).unwrap(),
        observed_at: clock.monotonic_now(),
    };
    let mark = SyncTime::from_unix_millis(MINUTE_MARK_MS);
    let wake = ClockInstant::ZERO.after(Duration::from_secs(15));

    clock.set_utc(chrono::DateTime::from_timestamp_millis(MINUTE_MARK_MS + 3_600_000).unwrap());
    clock.advance(Duration::from_secs(5)).unwrap();

    assert_eq!(
        shared_time_at(anchor, clock.monotonic_now()),
        SyncTime::from_unix_millis(MINUTE_MARK_MS - 2_000)
    );
    assert_eq!(wake_for_mark(anchor, mark, -2_000), wake);
}
