use super::{ClockInstant, Duration, SyncTime, WelcomeClockAnchor, shared_time_at, wake_for_mark};
use super::{FlushJitter, MachineFlushJitter};
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

#[test]
fn two_machines_spread_the_same_mark_inside_the_allowed_window() {
    let first = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let second = "019f0000-0000-7000-8000-000000000012".parse().unwrap();
    let mark = SyncTime::from_unix_millis(MINUTE_MARK_MS);
    let first_offset = MachineFlushJitter::new(&first).jitter_millis(mark);
    let second_offset = MachineFlushJitter::new(&second).jitter_millis(mark);
    assert_ne!(first_offset, second_offset);
    assert!((-3_000..=3_000).contains(&first_offset));
    assert!((-3_000..=3_000).contains(&second_offset));
}
