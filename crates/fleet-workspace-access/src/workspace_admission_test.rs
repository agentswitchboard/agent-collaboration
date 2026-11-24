use super::decode_workspace_state;
use central_storage::{ReadOutcome, SqlValue};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_runtime_ports::workspace_is_servable;

#[test]
fn workspace_admission_decodes_only_known_states() {
    let mut outcome = ReadOutcome {
        columns: vec!["workspace_state".to_owned()],
        rows: Vec::new(),
    };
    assert_eq!(decode_workspace_state(&outcome).unwrap(), None);

    for (value, state) in [
        (SqlValue::Null, None),
        (SqlValue::Text("active".to_owned()), Some("active")),
        (SqlValue::Text("archived".to_owned()), Some("archived")),
    ] {
        outcome.rows = vec![vec![value]];
        assert_eq!(decode_workspace_state(&outcome).unwrap(), state);
    }

    outcome.rows = vec![vec![SqlValue::Text("enabled".to_owned())]];
    assert_eq!(
        workspace_is_servable(true, decode_workspace_state(&outcome).unwrap()),
        Err(corrupt("directory_workspaces.state"))
    );

    outcome.rows = vec![vec![SqlValue::Text("active".to_owned())]; 2];
    assert_eq!(
        decode_workspace_state(&outcome),
        Err(corrupt("directory_workspaces.workspace_id"))
    );
}
