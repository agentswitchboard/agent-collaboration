//! Real organization and engine operations for the host behavior tests.
// Test-only module: clippy's test allowances (clippy.toml) apply to its helpers.
#![cfg(test)]
use super::support::*;
use central_storage::{SqlValue, StoreFailure};
use collaboration_central_core::WorkspaceCaller;
use collaboration_central_core::{EngineAdminControl, EngineAttemptAction};

use router_clock::RuntimeClock;
use serde_json::{Value, json};
use workspace_model as m;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
};

pub(super) type Time = chrono::DateTime<chrono::Utc>;

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
    pub caller: WorkspaceCaller,
    pub workspace: m::WorkspaceId,
    serial: std::cell::Cell<u32>,
}
impl EngineOrg {
    pub async fn new() -> Self {
        let f = Fixture::new().await.unwrap();
        f.ready().await.unwrap();
        f.seed_machine().await.unwrap();
        let caller = f.machine_caller("engine-admin").await.unwrap();
        let mut engine = Self {
            f,
            caller,
            workspace: WORKSPACE.to_owned().try_into().unwrap(),
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
            .apply(
                A,
                collaboration_central_conformance::REPLACE_FIXTURE_ALLOWANCES_SQL,
                vec![
                    text(serde_json::to_string(&vec![engine.workspace.clone()]).unwrap()),
                    text(INSTALLATION),
                ],
            )
            .await
            .unwrap();
        engine
    }
    pub fn now(&self) -> Time {
        self.f.clock.utc_now()
    }
    pub async fn set(&self, at: Time) {
        self.f.clock.set_utc(at).await;
    }
    /// A fresh UUIDv7 for requests and operation IDs at the held time.
    pub fn fresh(&self) -> String {
        signed::fresh_request_id(self.f.clock_time()).unwrap()
    }
    pub async fn runtime(&self) -> std::sync::Arc<Runtime> {
        self.f.runtime(A).await.unwrap()
    }
    pub async fn rows(&self, sql: &'static str, args: Vec<SqlValue>) -> Vec<Vec<SqlValue>> {
        self.f.rows(A, sql, args).await.unwrap()
    }
    pub async fn count(&self, sql: &'static str, args: Vec<SqlValue>) -> i64 {
        match &self.rows(sql, args).await[0][0] {
            SqlValue::Integer(count) => count.get(),
            other => panic!("count: {other:?}"),
        }
    }
    pub async fn raw(&self, sql: &'static str, args: Vec<SqlValue>) {
        self.f.apply(A, sql, args).await.unwrap();
    }
    pub async fn after_event(&self) {
        self.runtime().await.after_event().await.unwrap();
    }
    pub async fn alarm_ms(&self) -> Option<i64> {
        self.runtime()
            .await
            .runtime()
            .statement_host()
            .alarm()
            .map(|time| time.unix_millis())
    }
    pub async fn invocations(&self) -> fleet_runtime::LegacyUnitInvocations {
        self.runtime().await.runtime().legacy_unit_invocations()
    }
    pub async fn request(
        &self,
        workspace: Option<m::WorkspaceId>,
        operation: m::WorkspaceOperation,
    ) -> m::WorkspaceReply {
        let caller = self.f.machine_caller("engine-admin").await.unwrap();
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
        let m::ItemRef::Area(area_id) = self
            .write(m::WorkspaceOperation::CreateArea {
                name: format!("Area {}", self.bump()).try_into().unwrap(),
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
    fn bump(&self) -> u32 {
        let next = self.serial.get();
        self.serial.set(next + 1);
        next
    }
    /// Joins a second session as observer and returns its actor identity.
    pub async fn observer(&self, thread: &m::ThreadId) -> m::ActorIdentityId {
        let observer = self.f.machine_caller("engine-observer").await.unwrap();
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
    pub async fn central(&self, operation: CentralMailOperation) -> CentralMailResult {
        let caller = self.f.machine_caller("engine-admin").await.unwrap();
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
        let m::Caller::Session(address) = self.caller.caller() else {
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
    /// (state, failure_count, next_try_at, last_failure)
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

    pub async fn engine_admin(
        &self,
        control: EngineAdminControl,
    ) -> std::result::Result<Value, StoreFailure> {
        collaboration_central_core::fixture_engine_admin(self.runtime().await.as_ref(), control)
            .await
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
    ) -> std::result::Result<Value, StoreFailure> {
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
        self.f.close().await.unwrap();
    }
}

pub(super) fn refused(reason: &str) -> Value {
    json!({"result":"refused","reason":reason})
}
/// The health entry for one kind.
pub(super) fn kind_health<'a>(health: &'a Value, kind: &str) -> &'a Value {
    health["kinds"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["kind"] == kind)
        .unwrap_or_else(|| panic!("{kind} missing from {health}"))
}
