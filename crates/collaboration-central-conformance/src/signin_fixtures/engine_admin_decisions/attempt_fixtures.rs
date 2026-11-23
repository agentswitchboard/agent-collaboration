//! Stored facts for engine admin HTTP cases and their independent write oracles.

use super::{ENGINE_ATTEMPT, EngineAdminCase, EngineAdminHarness};
use crate::ConformanceResult;
use central_storage::SqlValue;
use serde_json::{Value, json};

pub(super) const LAST_HIDDEN_ATTEMPT: &str = "019f0000-0000-7000-8000-000000000104";
const FIRST_FAILURE: &str = "2027-01-15T08:00:00.123456789Z";
const RETIREMENT: &str = "2027-01-15T08:05:00.654321Z";

#[derive(Clone, Copy)]
enum AttemptStanding {
    Failing,
    Retired,
}

pub(super) struct AttemptFixture {
    wake: Option<String>,
}

#[derive(Debug)]
pub(super) struct AdminFacts {
    pub(super) proofs: Vec<Vec<SqlValue>>,
    pub(super) receipts: Vec<Vec<SqlValue>>,
    pub(super) actors: Vec<Vec<SqlValue>>,
    attempts: Vec<Vec<SqlValue>>,
    wakes: Vec<Vec<SqlValue>>,
    due: Vec<Vec<SqlValue>>,
    alarm_millis: Option<i64>,
}

pub(super) async fn read_admin_facts(
    host: &impl EngineAdminHarness,
) -> ConformanceResult<AdminFacts> {
    Ok(AdminFacts {
        proofs: host
            .read_admin_rows("SELECT id FROM admin_used_requests ORDER BY id", vec![])
            .await?,
        receipts: host
            .read_admin_rows("SELECT request_id,answer FROM runtime_request_log ORDER BY request_id", vec![])
            .await?,
        actors: host
            .read_admin_rows("SELECT actor_identity_id,identity FROM access_actor_identities WHERE identity_kind='person' ORDER BY actor_identity_id", vec![])
            .await?,
        attempts: host
            .read_admin_rows("SELECT * FROM engine_work_attempts ORDER BY attempt_id", vec![])
            .await?,
        wakes: host
            .read_admin_rows("SELECT wake_id,state FROM automation_wakes ORDER BY wake_id", vec![])
            .await?,
        due: host
            .read_admin_rows("SELECT * FROM engine_due_entries ORDER BY work_kind,subject_key", vec![])
            .await?,
        alarm_millis: host.published_alarm_millis().await,
    })
}

pub(super) async fn seed_attempt(
    host: &impl EngineAdminHarness,
    case: EngineAdminCase,
) -> ConformanceResult<AttemptFixture> {
    use AttemptStanding::{Failing, Retired};
    use EngineAdminCase as Case;
    let wake = match case {
        Case::HealthSnapshot | Case::RetiredPage | Case::Retried | Case::Stale => {
            insert_attempt(host, ENGINE_ATTEMPT, "wake_timer", ENGINE_ATTEMPT, Retired).await?;
            None
        }
        Case::RetiredEmptyContinuation => {
            for attempt in [
                ENGINE_ATTEMPT,
                "019f0000-0000-7000-8000-000000000102",
                "019f0000-0000-7000-8000-000000000103",
                LAST_HIDDEN_ATTEMPT,
            ] {
                insert_attempt(host, attempt, "engine_probe", attempt, Retired).await?;
            }
            let visible = "019f0000-0000-7000-8000-000000000105";
            insert_attempt(host, visible, "wake_timer", visible, Retired).await?;
            None
        }
        Case::NotRetired => {
            insert_attempt(host, ENGINE_ATTEMPT, "wake_timer", ENGINE_ATTEMPT, Failing).await?;
            None
        }
        Case::DiscardUnsupported => {
            insert_attempt(
                host,
                ENGINE_ATTEMPT,
                "legacy_notice_windows",
                "all",
                Retired,
            )
            .await?;
            None
        }
        Case::Discarded | Case::SubjectUnreadable => {
            let wake_id = host.create_wake_for_discard().await?;
            insert_attempt(host, ENGINE_ATTEMPT, "wake_timer", &wake_id, Retired).await?;
            if matches!(case, Case::SubjectUnreadable) {
                host.apply_admin_fixture(
                    "UPDATE automation_wakes SET state='unreadable' WHERE wake_id=?1",
                    vec![SqlValue::Text(wake_id.clone())],
                )
                .await?;
            }
            Some(wake_id)
        }
        Case::RetiredEnd
        | Case::NotPermitted
        | Case::SecondFactorRequired
        | Case::RefreshRequired
        | Case::AuthorizationSyncRequired
        | Case::Expired
        | Case::RequestTime
        | Case::RequestReused
        | Case::GuardChanged
        | Case::OperationConflict
        | Case::RetainedReplay
        | Case::NotFound
        | Case::WrongMethod
        | Case::MissingProof
        | Case::BadAddress
        | Case::InvalidIntent
        | Case::Unavailable => None,
    };
    Ok(AttemptFixture { wake })
}

async fn insert_attempt(
    host: &impl EngineAdminHarness,
    attempt_id: &str,
    kind: &str,
    subject: &str,
    standing: AttemptStanding,
) -> ConformanceResult<()> {
    let (state, next, retired) = match standing {
        AttemptStanding::Failing => (
            "failing",
            SqlValue::Text("2027-01-15T08:10:00Z".into()),
            SqlValue::Null,
        ),
        AttemptStanding::Retired => ("retired", SqlValue::Null, SqlValue::Text(RETIREMENT.into())),
    };
    host.apply_admin_fixture(
        "INSERT INTO engine_work_attempts(attempt_id,machine_kind,subject_key,state,failure_count,next_try_at,last_failure,first_failed_at,last_failed_at,retired_at,created_at,updated_at) VALUES (?1,?2,?3,?4,2,?5,'statement_rejected',?6,?6,?7,?6,?6)",
        vec![
            SqlValue::Text(attempt_id.into()),
            SqlValue::Text(kind.into()),
            SqlValue::Text(subject.into()),
            SqlValue::Text(state.into()),
            next,
            SqlValue::Text(FIRST_FAILURE.into()),
            retired,
        ],
    )
    .await
}

pub(super) fn expected_health() -> Value {
    json!({
        "result":"engineHealth","alarm":null,"alarmMissing":false,
        "kinds":[
            {"kind":"wake_timer","failing":0,"retired":1,"oldestFirstFailedAt":FIRST_FAILURE,"lastFailure":"statement_rejected"},
            {"kind":"automation_timer","failing":0,"retired":0,"oldestFirstFailedAt":null,"lastFailure":null},
            {"kind":"direct_message","failing":0,"retired":0,"oldestFirstFailedAt":null,"lastFailure":null},
            {"kind":"system_message_expiry","failing":0,"retired":0,"oldestFirstFailedAt":null,"lastFailure":null},
            {"kind":"legacy_notice_windows","failing":0,"retired":0,"oldestFirstFailedAt":null,"lastFailure":null},
            {"kind":"legacy_workspace_drains","failing":0,"retired":0,"oldestFirstFailedAt":null,"lastFailure":null},
            {"kind":"engine_source","failing":0,"retired":0,"oldestFirstFailedAt":null,"lastFailure":null}
        ]
    })
}

pub(super) fn expected_retired_page() -> Value {
    json!({
        "result":"engineRetired","next":null,"items":[{
            "attemptId":ENGINE_ATTEMPT,"kind":"wake_timer","subjectKey":ENGINE_ATTEMPT,
            "failureClass":"statement_rejected","retiredAt":RETIREMENT,"lastFailure":null,"count":2
        }]
    })
}

pub(super) fn check_domain_writes(
    case: EngineAdminCase,
    subject: &AttemptFixture,
    before: &AdminFacts,
    after: &AdminFacts,
) {
    use EngineAdminCase as Case;
    match case {
        Case::Retried => {
            assert_eq!(before.attempts.len(), 1);
            assert!(after.attempts.is_empty());
            assert_eq!(after.wakes, before.wakes);
        }
        Case::Discarded => {
            assert_eq!(before.attempts.len(), 1);
            assert!(after.attempts.is_empty());
            assert!(subject.wake.is_some(), "discard needs a stored wake");
            let wake_state = |state: &str| {
                subject
                    .wake
                    .as_ref()
                    .map(|wake| vec![SqlValue::Text(wake.clone()), SqlValue::Text(state.into())])
            };
            assert_eq!(before.wakes, wake_state("active").as_slice());
            assert_eq!(after.wakes, wake_state("disabled").as_slice());
            assert!(
                !before.due.is_empty(),
                "the wake starts with scheduled work"
            );
            assert!(after.due.is_empty(), "discard retires the wake's due entry");
        }
        Case::HealthSnapshot
        | Case::RetiredPage
        | Case::RetiredEmptyContinuation
        | Case::RetiredEnd
        | Case::NotPermitted
        | Case::SecondFactorRequired
        | Case::RefreshRequired
        | Case::AuthorizationSyncRequired
        | Case::Expired
        | Case::RequestTime
        | Case::RequestReused
        | Case::GuardChanged
        | Case::OperationConflict
        | Case::RetainedReplay
        | Case::NotFound
        | Case::NotRetired
        | Case::DiscardUnsupported
        | Case::Stale
        | Case::SubjectUnreadable
        | Case::WrongMethod
        | Case::MissingProof
        | Case::BadAddress
        | Case::InvalidIntent
        | Case::Unavailable => {
            assert_eq!(after.attempts, before.attempts, "{case:?} attempts");
            assert_eq!(after.wakes, before.wakes, "{case:?} subjects");
            assert_eq!(after.due, before.due, "{case:?} due entries");
            assert_eq!(after.alarm_millis, before.alarm_millis, "{case:?} alarm");
        }
    }
}
