#![expect(
    clippy::unwrap_used,
    reason = "Invalid literal fixtures and unexpectedly accepted wire values must fail the test at their call site."
)]

use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};
use switchboard_session_payloads::{LoadQuery, LoadRequest};
use workspace_ids::{SessionId, SessionIdError, SessionRef};

const MACHINE_ID: &str = "01890f2e-7b4c-7cc0-98c4-000000000001";
const OPERATION_ID: &str = "01890f2e-7b4c-7cc0-98c4-000000000002";
const PAIR: &str =
    r#"{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000001","sessionId":"native/thread"}"#;
const LOAD: &str = r#"{"target":{"machineId":"01890f2e-7b4c-7cc0-98c4-000000000001","sessionId":"native/thread"},"operationId":"01890f2e-7b4c-7cc0-98c4-000000000002"}"#;

fn target() -> SessionRef {
    SessionRef {
        machine_id: MACHINE_ID.parse().unwrap(),
        session_id: "native/thread".parse().unwrap(),
    }
}

#[test]
fn load_request_uses_exact_private_wire_shape() {
    let pair: SessionRef = serde_json::from_str(PAIR).unwrap();
    assert_eq!(pair, target());
    assert_eq!(serde_json::to_string(&pair).unwrap(), PAIR);
    assert_shape_refused::<SessionRef>(
        &PAIR.replace(
            r#""sessionId":"native/thread""#,
            r#""sessionId":"native/thread","endpoint":"legacy""#,
        ),
        "endpoint",
    );

    assert_load_contract(LoadRequest {
        target: target(),
        operation_id: OPERATION_ID.parse().unwrap(),
    });
}

#[test]
fn load_query_uses_exact_private_wire_shape() {
    assert_load_contract(LoadQuery {
        target: target(),
        operation_id: OPERATION_ID.parse().unwrap(),
    });
}

fn assert_load_contract<T: DeserializeOwned + Serialize + Debug + PartialEq>(expected: T) {
    let load: T = serde_json::from_str(LOAD).unwrap();
    assert_eq!(load, expected);
    assert_eq!(serde_json::to_string(&load).unwrap(), LOAD);

    assert_shape_refused::<T>(
        &LOAD.replace(r#""operationId":"#, r#""extra":null,"operationId":"#),
        "extra",
    );
    assert_shape_refused::<T>(
        &LOAD.replace(r#""sessionId":"#, r#""extra":null,"sessionId":"#),
        "extra",
    );
    assert_shape_refused::<T>(
        &LOAD.replace(r#""sessionId":"#, r#""endpoint":"legacy","sessionId":"#),
        "endpoint",
    );
    assert_shape_refused::<T>(&format!(r#"{{"target":{PAIR}}}"#), "operationId");
    assert_shape_refused::<T>(
        &format!(r#"{{"target":false,"operationId":"{OPERATION_ID}"}}"#),
        "target",
    );
    assert_shape_refused::<T>(
        &format!(r#"{{"target":{PAIR},"operationId":7}}"#),
        "operationId",
    );

    let longest = LOAD.replace("native/thread", &"x".repeat(4096));
    let accepted: T = serde_json::from_str(&longest).unwrap();
    assert_eq!(serde_json::to_string(&accepted).unwrap(), longest);
    let too_long = "x".repeat(4097);
    assert_eq!(SessionId::try_from(too_long.clone()), Err(SessionIdError));
    assert_shape_refused::<T>(
        &LOAD.replace("native/thread", &too_long),
        &SessionIdError.to_string(),
    );
}

fn assert_shape_refused<T: DeserializeOwned + Debug>(wire: &str, field_or_error: &str) {
    let mut reader = serde_json::Deserializer::from_str(wire);
    let result = serde_path_to_error::deserialize::<_, T>(&mut reader);
    assert!(result.is_err(), "must refuse {field_or_error}");
    let error = result.unwrap_err();
    assert!(
        error.path().to_string().contains(field_or_error)
            || error.inner().to_string().contains(field_or_error),
        "must name {field_or_error}, got {error}"
    );
}
