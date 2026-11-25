//! Real organization and engine operations for the host behavior tests.
use super::super::workerd_host::RemoteRuntime;
use super::support::*;
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, SqlValue, Statement,
    StatementBatch, StoreFailure,
};
use collaboration_central_conformance::signin_fixtures as signed;
use collaboration_central_core::{EngineAdminControl, EngineAttemptAction};
use router_clock::RuntimeClock;
use serde_json::{Value, json};
use std::sync::Arc;
use workspace_model as m;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
};

pub(super) type Time = chrono::DateTime<chrono::Utc>;

pub(super) fn text(value: impl ToString) -> SqlValue {
    SqlValue::Text(value.to_string())
}
pub(super) fn minutes(count: i64) -> chrono::Duration {
    chrono::Duration::minutes(count)
}
pub(super) fn stamp(at: Time) -> SqlValue {
    text(at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}
pub(super) fn integer(value: i64) -> SqlValue {
    SqlValue::Integer(value.try_into().unwrap())
}
/// The first 5-minute mark strictly after `at`.
pub(super) fn next_mark(at: Time) -> Time {
    let millis = at.timestamp_millis();
    chrono::DateTime::from_timestamp_millis((millis.div_euclid(300_000) + 1) * 300_000).unwrap()
}

pub(super) struct EngineOrg {
    pub f: Fixture,
    pub caller: super::super::support::FixtureCaller,
    pub workspace: m::WorkspaceId,
    serial: std::cell::Cell<u32>,
}
impl EngineOrg {
    pub async fn new() -> Self {
        let f = Fixture::new().await;
        assert_eq!(f.provision().await.0, 200);
        f.floors().await;
        let caller = f
            .enrolled_session_caller("Engine admin fixture", "engine-admin")
            .await;
        let mut engine = Self {
            f,
            caller,
            workspace: "019f0000-0000-7000-8000-00000000b301"
                .to_owned()
                .try_into()
                .unwrap(),
            serial: std::cell::Cell::new(1),
        };
        let created = engine
            .request(
                None,
                m::WorkspaceOperation::CreateWorkspace {
                    name: "Engine admin".to_owned().try_into().unwrap(),
                },
            )
            .await;
        let m::WorkspaceResult::Done(m::WorkspaceAnswer::WorkspaceCreated { workspace }) =
            created.result
        else {
            panic!("workspace creation refused: {created:?}")
        };
        engine.workspace = workspace.workspace_id;
        engine
            .f
            .execute(
                collaboration_central_conformance::REPLACE_FIXTURE_ALLOWANCES_SQL,
                vec![
                    text(serde_json::to_string(&vec![engine.workspace.clone()]).unwrap()),
                    text(FIXTURE_MACHINE),
                ],
            )
            .await;
        engine
    }
    pub fn now(&self) -> Time {
        self.f.clock.utc_now()
    }
    pub fn set(&self, at: Time) {
        self.f.clock.set_utc(at);
    }
    fn now_seconds(&self) -> i64 {
        self.now().timestamp()
    }
    pub fn fresh(&self) -> String {
        signed::fresh_request_id(self.now_seconds()).unwrap()
    }
    /// The organization's runtime, with the held time pushed to the Worker.
    pub async fn runtime(&self) -> Arc<RemoteRuntime> {
        self.f.runtime().await
    }
    pub async fn rows(&self, sql: &'static str, args: Vec<SqlValue>) -> Vec<Vec<SqlValue>> {
        self.runtime()
            .await
            .read(Statement { sql, args })
            .await
            .unwrap()
            .rows
    }
    pub async fn count(&self, sql: &'static str, args: Vec<SqlValue>) -> i64 {
        match &self.rows(sql, args).await[0][0] {
            SqlValue::Integer(count) => count.get(),
            other => panic!("count: {other:?}"),
        }
    }
    pub async fn raw(&self, sql: &'static str, args: Vec<SqlValue>) {
        assert!(matches!(
            self.runtime()
                .await
                .run_batch(StatementBatch {
                    statements: vec![GuardedStatement {
                        statement: Statement { sql, args },
                        expect: RowExpectation::Any,
                    }],
                    alarm: AlarmUpdate::Unchanged,
                })
                .await
                .unwrap(),
            BatchOutcome::Applied { .. }
        ));
    }
    pub async fn after_event(&self) {
        self.runtime().await.after_event().await.unwrap();
    }
    pub async fn alarm_ms(&self) -> Option<i64> {
        self.runtime()
            .await
            .control::<Option<i64>>("raw-alarm-time", &())
            .await
            .unwrap()
    }
    pub async fn invocations(&self) -> fleet_runtime::LegacyUnitInvocations {
        self.runtime().await.legacy_unit_invocations().await
    }
    pub async fn request(
        &self,
        workspace: Option<m::WorkspaceId>,
        operation: m::WorkspaceOperation,
    ) -> m::WorkspaceReply {
        // Engine tests move the clock far past one token; mint one at the clock's now.
        let caller = super::super::support::fresh_auth(&self.f.host, &self.caller);
        self.f
            .host
            .request(
                &caller,
                m::WorkspaceRequest {
                    request_id: operation
                        .is_write()
                        .then(|| self.fresh().try_into().unwrap()),
                    workspace_id: workspace,
                    operation,
                },
            )
            .await
            .unwrap()
    }
    pub async fn write(&self, operation: m::WorkspaceOperation) -> m::ItemRef {
        match self
            .request(Some(self.workspace.clone()), operation)
            .await
            .result
        {
            m::WorkspaceResult::Done(m::WorkspaceAnswer::Created { item, .. }) => item,
            other => panic!("not created: {other:?}"),
        }
    }
    pub async fn thread(&self) -> m::ThreadId {
        let serial = self.serial.get();
        self.serial.set(serial + 1);
        let m::ItemRef::Area(area_id) = self
            .write(m::WorkspaceOperation::CreateArea {
                name: format!("Area {serial}").try_into().unwrap(),
                description: "Test description".to_owned().try_into().unwrap(),
            })
            .await
        else {
            panic!("area")
        };
        let m::ItemRef::Thread(thread) = self
            .write(m::WorkspaceOperation::CreateThread {
                area_id,
                title: "Engine thread".to_owned().try_into().unwrap(),
                body: "First post".to_owned().try_into().unwrap(),
                addressees: vec![],
            })
            .await
        else {
            panic!("thread")
        };
        thread
    }
    pub async fn observer(&self, thread: &m::ThreadId) -> m::ActorIdentityId {
        let mut observer = super::super::support::fresh_auth(&self.f.host, &self.caller);
        let m::Caller::Session(address) = &mut observer.caller else {
            panic!("session caller")
        };
        address.session_id = "engine-observer".to_owned().try_into().unwrap();
        let joined = self
            .f
            .host
            .request(
                &observer,
                m::WorkspaceRequest {
                    request_id: Some(self.fresh().try_into().unwrap()),
                    workspace_id: Some(self.workspace.clone()),
                    operation: m::WorkspaceOperation::JoinThread {
                        thread_id: thread.clone(),
                        role: m::MemberRole::Observer,
                    },
                },
            )
            .await
            .unwrap();
        assert!(
            matches!(joined.result, m::WorkspaceResult::Done(_)),
            "{joined:?}"
        );
        let actors = self
            .rows(
                "SELECT actor_identity_id,identity FROM access_actor_identities",
                vec![],
            )
            .await;
        actors
            .iter()
            .find_map(|row| match (&row[0], &row[1]) {
                (SqlValue::Text(id), SqlValue::Text(identity))
                    if identity.contains("engine-observer") =>
                {
                    Some(id.parse().unwrap())
                }
                _ => None,
            })
            .unwrap()
    }
    pub async fn post(&self, thread: &m::ThreadId) {
        self.write(m::WorkspaceOperation::ThreadPost {
            thread_id: thread.clone(),
            body: "Notice activity".to_owned().try_into().unwrap(),
            addressees: vec![],
        })
        .await;
    }
    async fn central(&self, operation: CentralMailOperation) -> CentralMailResult {
        let caller = super::super::support::fresh_auth(&self.f.host, &self.caller);
        self.f
            .host
            .request_central(
                &caller,
                CentralMailRequest {
                    request_id: self.fresh().try_into().unwrap(),
                    operation,
                },
            )
            .await
            .unwrap()
    }
    pub fn target(&self) -> workspace_ids::SessionRef {
        let m::Caller::Session(address) = &self.caller.caller else {
            panic!("session caller")
        };
        address.clone()
    }
    pub async fn wake(&self, due: Time, label: &str) -> String {
        let result = self
            .central(CentralMailOperation::SetWake {
                target: self.target(),
                due_at: due,
                text: label.to_owned().try_into().unwrap(),
            })
            .await;
        let CentralMailResult::Done(CentralMailAnswer::WakeSet { wake_id, .. }) = result else {
            panic!("wake refused: {result:?}")
        };
        wake_id.to_string()
    }
    pub async fn automation(&self, at: Time, label: &str) -> String {
        let result = self
            .central(CentralMailOperation::SetAutomation {
                target: self.target(),
                timing: serde_json::from_value(json!({"kind":"at","at":at})).unwrap(),
                text: label.to_owned().try_into().unwrap(),
            })
            .await;
        let CentralMailResult::Done(CentralMailAnswer::AutomationSet { automation_id, .. }) =
            result
        else {
            panic!("automation refused: {result:?}")
        };
        automation_id.to_string()
    }
    pub async fn wake_messages(&self, wake: &str) -> i64 {
        self.count(
            "SELECT COUNT(*) FROM mail_messages WHERE message_kind='wake' AND json_extract(body,'$.wakeId')=?",
            vec![text(wake)],
        )
        .await
    }
    pub async fn record(&self, kind: &str, key: &str) -> Vec<Vec<SqlValue>> {
        self.rows(
            "SELECT state,failure_count,next_try_at,last_failure FROM engine_work_attempts WHERE machine_kind=? AND subject_key=?",
            vec![text(kind), text(key)],
        )
        .await
    }
    pub async fn attempt_id(&self, kind: &str, key: &str) -> String {
        match &self
            .rows(
                "SELECT attempt_id FROM engine_work_attempts WHERE machine_kind=? AND subject_key=?",
                vec![text(kind), text(key)],
            )
            .await[..]
        {
            [row] => match &row[0] {
                SqlValue::Text(id) => id.clone(),
                other => panic!("attempt id: {other:?}"),
            },
            other => panic!("one record expected for {kind}/{key}: {other:?}"),
        }
    }
    /// The Worker's logged failure counts for one subject, in order.
    pub fn observed(&self, key: &str) -> Vec<(i64, bool)> {
        self.f
            .host
            .captured_logs()
            .iter()
            .filter_map(|line| {
                let rest = line.split("central engine: kind=").nth(1)?;
                let (_, rest) = rest.split_once(" key=")?;
                let (logged, rest) = rest.split_once(" class=")?;
                (logged == key).then_some(())?;
                let (_, rest) = rest.split_once(" count=")?;
                let (count, rest) = rest.split_once(" retired=")?;
                let (retired, _) = rest.split_once(" workspaces=")?;
                Some((count.parse().ok()?, retired == "true"))
            })
            .collect()
    }
    pub async fn until_observed(&self, key: &str, class: &str, count: i64) {
        self.f
            .host
            .until_log(&format!("key={key} class={class} count={count} "))
            .await;
    }

    pub async fn engine_admin(&self, control: EngineAdminControl) -> Result<Value, StoreFailure> {
        self.runtime()
            .await
            .control("engine-admin-control", &control)
            .await?
    }

    pub async fn health(&self) -> Value {
        self.engine_admin(EngineAdminControl::Health {})
            .await
            .unwrap()
    }

    pub async fn retired_result(
        &self,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Value, StoreFailure> {
        self.engine_admin(EngineAdminControl::Retired {
            after: after.map(|id| id.parse().unwrap()),
            limit: limit.try_into().unwrap(),
        })
        .await
    }

    pub async fn retired(&self, after: Option<&str>, limit: u32) -> Value {
        self.retired_result(after, limit).await.unwrap()
    }

    pub async fn change(
        &self,
        action: EngineAttemptAction,
        operation_id: &str,
        attempt: &str,
    ) -> Value {
        self.engine_admin(EngineAdminControl::Change {
            operation_id: operation_id.parse().unwrap(),
            attempt_id: attempt.parse().unwrap(),
            person_id: signed::OWNER.parse().unwrap(),
            action,
        })
        .await
        .unwrap()
    }

    pub async fn retry(&self, attempt: &str) -> Value {
        self.change(EngineAttemptAction::Retry, &self.fresh(), attempt)
            .await
    }

    pub async fn discard(&self, attempt: &str) -> Value {
        self.change(EngineAttemptAction::Discard, &self.fresh(), attempt)
            .await
    }
    pub async fn close(self) {
        self.f.close().await;
    }
}

pub(super) fn refused(reason: &str) -> Value {
    json!({"result":"refused","reason":reason})
}
pub(super) fn kind_health<'a>(health: &'a Value, kind: &str) -> &'a Value {
    health["kinds"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["kind"] == kind)
        .unwrap_or_else(|| panic!("{kind} missing from {health}"))
}
