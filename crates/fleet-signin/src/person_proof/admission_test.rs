use super::FactorStanding;
use fleet_identity_protocol::UnixSeconds;

#[test]
fn only_factor_times_inside_the_applying_window_are_fresh() {
    let at = 1_900_000_000;
    let time_ms = at * 1000;
    for now_ms in [time_ms, time_ms + 900_000] {
        assert_eq!(
            FactorStanding::at(Some(at), now_ms),
            FactorStanding::FreshWithin15Minutes {
                at: UnixSeconds::try_from(at).unwrap()
            },
        );
    }

    for (factor, now_ms) in [
        (None, time_ms),
        (Some(at), time_ms - 1),
        (Some(at), time_ms + 900_001),
        (Some(i64::MAX), time_ms),
    ] {
        assert_eq!(FactorStanding::at(factor, now_ms), FactorStanding::NotFresh);
    }
}
