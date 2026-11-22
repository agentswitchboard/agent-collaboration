use super::{POSITION, assert_wire_refused, position, target};
use switchboard_session_calls::WatchStart;

const FRESH: &str = r#"{"kind":"fresh","target":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000001","sessionId":"native/thread"}}"#;
const RESUME: &str = r#"{"kind":"resume","position":{"target":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000001","sessionId":"native/thread"},"routerRunId":"01890f2e-7b4c-7cc0-98c4-000000000003","historyEpoch":"0","after":"18446744073709551615"}}"#;

#[test]
fn watch_start_has_one_target_and_a_closed_kind() {
    for (literal, expected) in [
        (FRESH, WatchStart::Fresh { target: target() }),
        (
            RESUME,
            WatchStart::Resume {
                position: position(),
            },
        ),
    ] {
        let start: WatchStart = serde_json::from_str(literal).unwrap();
        assert_eq!(start, expected);
        assert_eq!(serde_json::to_string(&start).unwrap(), literal);
    }

    assert_wire_refused::<WatchStart>(
        &FRESH.replace(
            r#""target":"#,
            &format!(r#""position":{POSITION},"target":"#),
        ),
        "position",
        "unknown field",
    );
    assert_wire_refused::<WatchStart>(
        &RESUME.replace(
            r#""position":"#,
            &format!(
                r#""target":{},"position":"#,
                serde_json::to_string(&target()).unwrap()
            ),
        ),
        "target",
        "unknown field",
    );
    assert_wire_refused::<WatchStart>(r#"{"kind":"fresh"}"#, "target", "missing field");
    assert_wire_refused::<WatchStart>(r#"{"kind":"resume"}"#, "position", "missing field");
    assert_wire_refused::<WatchStart>(&FRESH.replace("fresh", "live"), "kind", "unknown variant");
    assert_wire_refused::<WatchStart>(
        &RESUME.replace(r#""sessionId":"#, r#""endpoint":"legacy","sessionId":"#),
        "endpoint",
        "unknown field",
    );
}
