use super::*;
use central_storage::{BatchOutcome, ReadOutcome, SqlValue, StatementBatch};
use std::sync::Mutex;

struct ReadSequence(Mutex<Vec<(&'static str, ReadOutcome)>>);
impl CentralStore for ReadSequence {
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
        let (sql, result) = self.0.lock().unwrap().remove(0);
        assert_eq!(statement.sql.trim_end(), sql.trim_end());
        Ok(result)
    }
    async fn run_batch(&self, _: StatementBatch) -> Result<BatchOutcome, StoreFailure> {
        panic!("reading machine-login facts must never write")
    }
}
fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}
fn row(fields: Vec<(&str, SqlValue)>) -> ReadOutcome {
    ReadOutcome {
        columns: fields.iter().map(|(name, _)| (*name).to_owned()).collect(),
        rows: vec![fields.into_iter().map(|(_, value)| value).collect()],
    }
}

#[tokio::test]
async fn retained_approval_returns_before_reading_any_mutable_decision_facts() {
    // Arrange: only the retained approval can be read; any further read fails loudly.
    let stored = row(vec![
        ("approval_id", text("019a3dce-2000-7000-8000-000000000001")),
        ("request_id", text("019a3dce-2000-7000-8000-000000000004")),
        ("machine_id", text("019a3dce-2000-7000-8000-000000000003")),
        ("label", text("First approval")),
        ("allowed_workspaces", text("[]")),
        (
            "approved_by_person_id",
            text("019a3dce-2000-7000-8000-000000000005"),
        ),
        ("permission_checked", text("machine:enroll")),
        (
            "base_generation",
            SqlValue::Integer(0_i64.try_into().unwrap()),
        ),
        ("code_expires_at", text("2030-03-17T17:56:40Z")),
        ("created_at", text("2030-03-17T17:46:40Z")),
        ("updated_at", text("2030-03-17T17:46:40Z")),
        ("state", text("approved")),
        ("end_reason", SqlValue::Null),
        ("machine_client_id", SqlValue::Null),
        ("last_grant_at", SqlValue::Null),
        ("ended_at", SqlValue::Null),
    ]);
    let host = ReadSequence(Mutex::new(vec![(
        include_str!("queries/read_approval_by_request.sql"),
        stored,
    )]));
    let query = EnrollmentQuery {
        request: Some(
            "019a3dce-2000-7000-8000-000000000004"
                .to_owned()
                .try_into()
                .unwrap(),
        ),
        machine_id: Some(
            "019a3dce-2000-7000-8000-000000000099"
                .to_owned()
                .try_into()
                .unwrap(),
        ),
        workspaces: vec![
            "019a3dce-2000-7000-8000-000000000098"
                .to_owned()
                .try_into()
                .unwrap(),
        ],
        ..EnrollmentQuery::default()
    };

    // Act: mutable machine_id/workspace choices are deliberately different.
    let found = facts(
        &host,
        "019a3dce-2000-7000-8000-000000000006"
            .to_owned()
            .try_into()
            .unwrap(),
        &query,
    )
    .await
    .unwrap();

    // Assert: the owning request row is sufficient for N03's immutable answer.
    assert_eq!(
        found.prior_approval.unwrap().label.as_str(),
        "First approval"
    );
    assert!(
        found.machine.is_none() && found.current.is_none() && found.missing_workspaces.is_empty()
    );
    assert!(host.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn history_scalar_must_be_present_and_boolean() {
    for value in [
        SqlValue::Null,
        text("1"),
        SqlValue::Integer(2_i64.try_into().unwrap()),
    ] {
        let host = ReadSequence(Mutex::new(vec![(
            "SELECT EXISTS(SELECT 1 FROM login_machine_approvals) AS has_enrollment",
            row(vec![("has_enrollment", value)]),
        )]));
        assert!(
            facts(
                &host,
                "019a3dce-2000-7000-8000-000000000006"
                    .to_owned()
                    .try_into()
                    .unwrap(),
                &EnrollmentQuery::default()
            )
            .await
            .is_err()
        );
    }
}

const MACHINE: &str = "019a3dce-2000-7000-8000-000000000003";
const APPROVAL: &str = "019a3dce-2000-7000-8000-000000000001";
const ACTIVE_WORKSPACE: &str = "019a3dce-2000-7000-8000-000000000007";
const WITHDRAWN_WORKSPACE: &str = "019a3dce-2000-7000-8000-000000000008";
const STORED_TIME: &str = "2030-03-17T17:46:40.000Z";

fn current_rows(generation: i64, machine_state: &str) -> Vec<(&'static str, ReadOutcome)> {
    let approval = row(vec![
        ("approval_id", text(APPROVAL)),
        ("request_id", text("019a3dce-2000-7000-8000-000000000004")),
        ("machine_id", text(MACHINE)),
        ("label", text("First approval")),
        (
            "allowed_workspaces",
            text(&serde_json::json!([ACTIVE_WORKSPACE, WITHDRAWN_WORKSPACE]).to_string()),
        ),
        (
            "approved_by_person_id",
            text("019a3dce-2000-7000-8000-000000000005"),
        ),
        ("permission_checked", text("machine:enroll")),
        (
            "base_generation",
            SqlValue::Integer(0_i64.try_into().unwrap()),
        ),
        ("code_expires_at", text("2030-03-17T17:56:40Z")),
        ("created_at", text(STORED_TIME)),
        ("updated_at", text(STORED_TIME)),
        ("state", text("enrolled")),
        ("end_reason", SqlValue::Null),
        ("machine_client_id", text(APPROVAL)),
        ("last_grant_at", text(STORED_TIME)),
        ("ended_at", SqlValue::Null),
    ]);
    let machine = row(vec![
        ("machine_id", text(MACHINE)),
        ("label", text("First approval")),
        ("state", text(machine_state)),
        (
            "enrollment_generation",
            SqlValue::Integer(generation.try_into().unwrap()),
        ),
        ("created_at", text(STORED_TIME)),
        ("updated_at", text(STORED_TIME)),
    ]);
    vec![
        (
            include_str!("queries/read_approval_by_id.sql"),
            approval.clone(),
        ),
        (include_str!("queries/read_machine_by_id.sql"), machine),
        (include_str!("queries/read_active_approval.sql"), approval),
    ]
}

fn current_link_rows(generation: i64, machine_state: &str) -> Vec<(&'static str, ReadOutcome)> {
    let mut full = current_rows(generation, machine_state).into_iter();
    let (_, approval) = full.next().unwrap();
    let (_, machine) = full.next().unwrap();
    fn narrow(row: ReadOutcome, columns: &[&str]) -> ReadOutcome {
        let (projected_columns, projected_values) = row
            .columns
            .into_iter()
            .zip(row.rows.into_iter().next().unwrap())
            .filter(|(name, _)| columns.contains(&name.as_str()))
            .unzip();
        ReadOutcome {
            columns: projected_columns,
            rows: vec![projected_values],
        }
    }
    let approval = narrow(
        approval,
        &[
            "approval_id",
            "machine_id",
            "base_generation",
            "state",
            "end_reason",
            "machine_client_id",
            "last_grant_at",
            "ended_at",
        ],
    );
    let machine = narrow(machine, &["machine_id", "state", "enrollment_generation"]);
    vec![
        (
            include_str!("queries/read_link_approval.sql"),
            approval.clone(),
        ),
        (include_str!("queries/read_link_machine.sql"), machine),
        (
            include_str!("queries/read_current_link_approval.sql"),
            approval,
        ),
    ]
}

#[tokio::test]
async fn authority_rejects_the_inconsistent_current_pair_without_reading_allowances() {
    for (generation, state, consistent) in [
        (2, "enrolled", false),
        (1, "revoked", false),
        (1, "enrolled", true),
    ] {
        let host = ReadSequence(Mutex::new(current_link_rows(generation, state)));
        let found = read_authority(
            &host,
            &MACHINE.to_owned().try_into().unwrap(),
            &APPROVAL.to_owned().try_into().unwrap(),
        )
        .await;
        if consistent {
            assert_eq!(found.unwrap().machine().unwrap().generation.get(), 1);
        } else {
            assert!(matches!(
                found,
                Err(error) if error == corrupt("login_machine_approvals.current_enrollment")
            ));
        }
        assert!(host.0.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn terminal_plan_holds_the_withdrawn_selection_and_withdraws_the_active_pair() {
    fn allowance(id: &str, workspace: &str, state: &str) -> ReadOutcome {
        row(vec![
            ("allowance_id", text(id)),
            ("workspace_id", text(workspace)),
            ("state", text(state)),
            (
                "change_request_id",
                text("earlier:machineAllowanceChanged:test"),
            ),
            ("change_origin", text("person")),
            ("created_at", text(STORED_TIME)),
            ("updated_at", text(STORED_TIME)),
        ])
    }

    let active_id = "019a3dce-2000-7000-8000-000000000009";
    let withdrawn_id = "019a3dce-2000-7000-8000-000000000010";
    let mut reads = current_link_rows(1, "enrolled");
    reads.extend(current_rows(1, "enrolled").into_iter().take(2).rev());
    reads.extend([
        (
            include_str!("queries/read_machine_allowances.sql"),
            allowance(active_id, ACTIVE_WORKSPACE, "active"),
        ),
        (
            include_str!("queries/read_machine_allowance_pair.sql"),
            allowance(withdrawn_id, WITHDRAWN_WORKSPACE, "withdrawn"),
        ),
    ]);
    let host = ReadSequence(Mutex::new(reads));
    let authority = read_authority(
        &host,
        &MACHINE.to_owned().try_into().unwrap(),
        &APPROVAL.to_owned().try_into().unwrap(),
    )
    .await
    .unwrap();
    let ready = facts_for_ending(
        &host,
        "019a3dce-2000-7000-8000-000000000006"
            .to_owned()
            .try_into()
            .unwrap(),
        authority,
    )
    .await
    .unwrap();

    struct Ids(std::sync::atomic::AtomicU32);
    impl fleet_runtime_ports::IdSource for Ids {
        fn new_id(&self) -> uuid::Uuid {
            let next = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            format!("019a3dce-2000-7000-8000-{next:012x}")
                .parse()
                .unwrap()
        }
    }
    let applying_time = crate::policy::DecisionTime {
        now_ms: 1_900_000_000_000 + 60 * 24 * 60 * 60 * 1000,
    };
    let decided = crate::policy::expire_current_if_idle(&ready, applying_time, &Ids(100.into()))
        .unwrap()
        .unwrap();
    let plan = crate::enrollment_writes::statements(&ready, &decided.changes, applying_time.now_ms)
        .unwrap();

    let allowances: Vec<_> = decided
        .changes
        .iter()
        .filter_map(|change| match change {
            crate::policy::EnrollmentChange::RecordAllowance { record, effect } => {
                Some((record.workspace_id.as_str(), *effect))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        allowances,
        [(
            ACTIVE_WORKSPACE,
            crate::policy::AllowanceEffect::Withdraw(active_id.parse().unwrap())
        )]
    );
    let held = plan
        .iter()
        .find(|step| step.statement.sql == include_str!("queries/guard_held_machine_allowance.sql"))
        .unwrap();
    assert!(held.statement.args.contains(&text(withdrawn_id)));
    assert!(host.0.lock().unwrap().is_empty());
}
