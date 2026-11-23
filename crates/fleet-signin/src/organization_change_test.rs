use super::*;

fn record(kind: OrganizationChangeKind) -> OrganizationChangeRecord {
    let target = "018f1111-2222-7000-8000-000000000002";
    OrganizationChangeRecord {
        id: Uuid::parse_str("018f1111-2222-7000-8000-000000000001").unwrap(),
        request_id: format!(
            "018f1111-2222-7000-8000-000000000003:{}:{target}",
            kind.as_str()
        )
        .try_into()
        .unwrap(),
        kind,
        origin: OrganizationChangeOrigin::Automatic {},
        target_id: target.to_owned(),
        created_at_millis: UnixMillis::try_from(1_800_000_000_000).unwrap(),
    }
}
fn rows(record: &OrganizationChangeRecord) -> ReadOutcome {
    let statement = insert(record).unwrap().statement;
    ReadOutcome {
        columns: [
            "id",
            "request_id",
            "kind",
            "origin",
            "actor_person_id",
            "permission_checked",
            "target_id",
            "created_at",
            "updated_at",
            "workspace_id",
            "allowance_state",
        ]
        .map(str::to_owned)
        .to_vec(),
        rows: vec![statement.args],
    }
}

#[test]
fn machine_kinds_and_automatic_origin_round_trip_through_the_production_row_codec() {
    for kind in [
        OrganizationChangeKind::MachineEnrolled,
        OrganizationChangeKind::MachineRevoked,
        OrganizationChangeKind::MachineExpired,
    ] {
        let expected = record(kind);
        let outcome = rows(&expected);
        assert_eq!(outcome.rows[0][3], text("automatic"));
        assert_eq!(outcome.rows[0][4], SqlValue::Null);
        assert_eq!(outcome.rows[0][5], SqlValue::Null);
        assert_eq!(
            decode(&outcome, &outcome.rows[0]).unwrap(),
            StoredOrganizationChange::TargetedChange(expected)
        );
    }
}

#[test]
fn automatic_origin_rejects_person_or_permission_cells_instead_of_dropping_them() {
    for column in [4, 5] {
        let mut outcome = rows(&record(OrganizationChangeKind::MachineExpired));
        outcome.rows[0][column] = text("unexpected");
        assert_eq!(
            decode(&outcome, &outcome.rows[0]),
            Err(corrupt("origin", ""))
        );
    }
    let mut outcome = rows(&record(OrganizationChangeKind::MachineExpired));
    outcome.rows[0][3] = text("unknown");
    assert_eq!(
        decode(&outcome, &outcome.rows[0]),
        Err(corrupt("origin", ""))
    );
}

#[test]
fn malformed_allowance_cells_are_reported_by_the_checked_row_decoder() {
    let mut outcome = rows(&record(OrganizationChangeKind::WorkspaceCreated));
    let row = &mut outcome.rows[0];
    row[2] = text(MACHINE_ALLOWANCE_CHANGE_KIND);
    row[9] = text("018f1111-2222-7000-8000-000000000010");
    row[10] = text("withdrawn");
    let StoredOrganizationChange::MachineAllowanceChanged(change) =
        decode(&outcome, &outcome.rows[0]).unwrap()
    else {
        panic!("stored allowance did not project as a typed record");
    };
    assert_eq!(change.state, MachineAllowanceState::Withdrawn);

    for (column, invalid, field) in [
        (6, text("bad machine"), "target_id"),
        (9, SqlValue::Null, "workspace_id"),
        (9, text("bad workspace"), "workspace_id"),
        (10, SqlValue::Null, "allowance_state"),
        (10, text("unknown"), "allowance_state"),
        (2, text("workspaceCreated"), "workspace_id"),
    ] {
        let mut malformed = outcome.rows[0].clone();
        malformed[column] = invalid;
        assert_eq!(decode(&outcome, &malformed), Err(corrupt(field, "")));
    }
}

struct ReadOnlyRows(ReadOutcome);
impl CentralStore for ReadOnlyRows {
    async fn read_timed_due_rows(
        &self,
        _: fleet_runtime_ports::DueListQuery,
    ) -> Result<Vec<fleet_runtime_ports::DueRow>, StoreFailure> {
        Err(StoreFailure::Unavailable)
    }

    async fn engine_alarm_inputs(&self) -> Result<central_storage::EngineAlarmTimes, StoreFailure> {
        Err(StoreFailure::Unavailable)
    }

    async fn engine_due_times(&self) -> Result<fleet_runtime_ports::EngineDueTimes, StoreFailure> {
        Ok(fleet_runtime_ports::EngineDueTimes::default())
    }
    async fn published_alarm(
        &self,
    ) -> Result<Option<central_engine_model::EngineTime>, central_storage::StoreFailure> {
        panic!("this test double never reads the physical alarm")
    }
    async fn read(&self, statement: Statement) -> Result<ReadOutcome, StoreFailure> {
        assert_eq!(
            statement.args,
            vec![text("018f1111-2222-7000-8000-000000000003")]
        );
        Ok(self.0.clone())
    }
    async fn run_batch(
        &self,
        _: central_storage::StatementBatch,
    ) -> Result<central_storage::BatchOutcome, StoreFailure> {
        panic!("the supersession lookup cannot write")
    }
}

#[tokio::test]
async fn two_valid_supersession_records_are_corrupt_instead_of_selecting_one() {
    // Arrange: either independently valid record could otherwise be decoded.
    let first = record(OrganizationChangeKind::MachineRevoked);
    let second = OrganizationChangeRecord {
        id: Uuid::parse_str("018f1111-2222-7000-8000-000000000004").unwrap(),
        request_id: "018f1111-2222-7000-8000-000000000003:machineRevoked:018f1111-2222-7000-8000-000000000005"
            .to_owned().try_into().unwrap(),
        target_id: "018f1111-2222-7000-8000-000000000005".to_owned(),
        ..first.clone()
    };
    let mut outcome = rows(&first);
    outcome.rows.extend(rows(&second).rows);
    for row in &outcome.rows {
        assert!(decode(&outcome, row).is_ok());
    }

    // Act/Assert: ambiguity is corruption even when both candidates are valid.
    assert_eq!(
        read_machine_supersession(
            &ReadOnlyRows(outcome),
            "018f1111-2222-7000-8000-000000000003"
        )
        .await,
        Err(bad_value("signin_change_records.machine_supersession"))
    );
}
