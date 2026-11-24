#![expect(
    clippy::unwrap_used,
    reason = "Invalid literal fixtures and unexpectedly accepted wire values must fail the test at their call site."
)]

mod position_wire;
mod watch_wire;

use std::fmt::Debug;

use serde::de::DeserializeOwned;

const MACHINE_ID: &str = "01890f2e-7b4c-7cc0-98c4-000000000001";
const RUN_ID: &str = "01890f2e-7b4c-7cc0-98c4-000000000003";
const POSITION: &str = r#"{"target":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000001","sessionId":"native/thread"},"routerRunId":"01890f2e-7b4c-7cc0-98c4-000000000003","historyEpoch":"0","after":"18446744073709551615"}"#;

fn target() -> workspace_ids::SessionRef {
    workspace_ids::SessionRef {
        machine_id: MACHINE_ID.parse().unwrap(),
        session_id: "native/thread".parse().unwrap(),
    }
}

fn position() -> switchboard_session_calls::Continuation {
    switchboard_session_calls::Continuation {
        target: target(),
        router_run_id: RUN_ID.parse().unwrap(),
        history_epoch: 0.into(),
        after: u64::MAX.into(),
    }
}

fn assert_wire_refused<T: DeserializeOwned + Debug>(wire: &str, field: &str, reason: &str) {
    let mut reader = serde_json::Deserializer::from_str(wire);
    let result = serde_path_to_error::deserialize::<_, T>(&mut reader);
    assert!(result.is_err(), "must refuse {field}: {reason}");
    let error = result.unwrap_err();
    let message = error.inner().to_string();
    assert!(
        error.path().to_string().contains(field) || message.contains(field),
        "must name {field}, got {error}"
    );
    assert!(message.contains(reason), "must carry {reason}, got {error}");
}
