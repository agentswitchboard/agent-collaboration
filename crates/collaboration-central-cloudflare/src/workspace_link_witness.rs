//! Verification-only events; no hook or binding exists in a production bundle.
use super::CentralObject;
use central_storage::{
    AlarmPublication, AlarmUpdate, EngineAlarmDecider, EngineAlarmTimes, ReadOutcome,
    StatementBatch, StoreFailure,
};
use serde::Serialize;
use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::Rc,
    sync::{Arc, Mutex, PoisonError},
};
use worker::{Env, Method, RequestInit, Result, send::SendWrapper};

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum EnrollmentQuery {
    ByEnrollment,
    MachineById,
    ActiveEnrollment,
}
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum ReadResult {
    Read,
    Unavailable,
}
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum RestoreOutcome {
    Admitted,
    Refused,
    Unavailable,
}
#[derive(Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum Event {
    EnrollmentRead {
        object_id: String,
        query: EnrollmentQuery,
        outcome: ReadResult,
    },
    RestoreResult {
        object_id: String,
        correlation_id: central_link_protocol::CorrelationId,
        outcome: RestoreOutcome,
    },
    AlarmSnapshot {
        object_id: String,
        correlation_id: central_link_protocol::CorrelationId,
        earliest_due_ms: Option<i64>,
        alarm_ms: Option<i64>,
    },
}

// The observer borrows the actual Engine inputs and only keeps the pair once
// the host confirms the same transaction committed the physical alarm change.
pub(super) struct ApplyingAlarmInputs(Arc<Mutex<Option<EngineAlarmTimes>>>);

pub(super) fn capture_alarm_inputs(
    env: &Env,
    decider: EngineAlarmDecider,
    batch: &StatementBatch,
) -> (EngineAlarmDecider, Option<ApplyingAlarmInputs>) {
    if !matches!(batch.alarm, AlarmUpdate::Engine(_)) || !enabled(env).unwrap_or(false) {
        return (decider, None);
    }
    let inputs = Arc::new(Mutex::new(None));
    let applying = inputs.clone();
    let observed: EngineAlarmDecider = Arc::new(move |times, now, write| {
        let decision = decider(times, now, write)?;
        *applying.lock().unwrap_or_else(PoisonError::into_inner) = Some(times);
        Ok(decision)
    });
    (observed, Some(ApplyingAlarmInputs(inputs)))
}

#[derive(Clone, Copy)]
struct CommittedAlarmObservation {
    inputs: EngineAlarmTimes,
    alarm_ms: Option<i64>,
}

// Every object records observations in capture order. The detached drain may wait
// for acknowledgements without affecting a statement, reply, or later batch member.
#[derive(Clone, Default)]
pub(super) struct PendingWitnesses(SendWrapper<Rc<RefCell<WitnessQueue>>>);

#[derive(Default)]
struct WitnessQueue {
    events: VecDeque<Event>,
    draining: bool,
    committed_alarm: Option<CommittedAlarmObservation>,
}

impl PendingWitnesses {
    pub(super) fn alarm_committed(
        &self,
        applying: ApplyingAlarmInputs,
        publication: &AlarmPublication,
    ) {
        let alarm_ms = match publication {
            AlarmPublication::Set { unix_millis } => Some(*unix_millis),
            AlarmPublication::Cleared => None,
            AlarmPublication::Unchanged | AlarmPublication::Failed { .. } => return,
        };
        if let Some(inputs) = *applying.0.lock().unwrap_or_else(PoisonError::into_inner) {
            self.0.borrow_mut().committed_alarm =
                Some(CommittedAlarmObservation { inputs, alarm_ms });
        }
    }

    fn enqueue(&self, env: &Env, state: &worker::State, event: Event) -> Result<()> {
        if !enabled(env)? {
            return Ok(());
        }
        let mut queue = self.0.borrow_mut();
        queue.events.push_back(event);
        if queue.draining {
            return Ok(());
        }
        queue.draining = true;
        let pending = self.clone();
        let env = env.clone();
        state.wait_until(async move {
            loop {
                let event = {
                    let mut queue = pending.0.borrow_mut();
                    let Some(event) = queue.events.pop_front() else {
                        queue.draining = false;
                        break;
                    };
                    event
                };
                if let Err(error) = record(&env, event).await {
                    worker::console_warn!("C2 witness delivery failed: {error}");
                }
            }
        });
        Ok(())
    }

    pub(super) fn read_completed(
        &self,
        env: &Env,
        state: &worker::State,
        sql: &str,
        result: &std::result::Result<ReadOutcome, StoreFailure>,
    ) {
        let Some(query) = classify(sql) else {
            return;
        };
        if let Err(error) = self.enqueue(
            env,
            state,
            Event::EnrollmentRead {
                object_id: state.id().to_string(),
                query,
                outcome: if result.is_ok() {
                    ReadResult::Read
                } else {
                    ReadResult::Unavailable
                },
            },
        ) {
            worker::console_warn!("C2 enrollment diagnostic failed: {error}");
        }
    }
}

fn enabled(env: &Env) -> Result<bool> {
    worker::js_sys::Reflect::has(
        env,
        &worker::wasm_bindgen::JsValue::from_str("TEST_C2_WITNESS"),
    )
    .map_err(worker::Error::from)
}
async fn record(env: &Env, event: Event) -> Result<()> {
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_body(Some(serde_json::to_string(&event)?.into()));
    let response = env
        .service("TEST_C2_WITNESS")?
        .fetch("https://c2-witness.test/record", Some(init))
        .await?;
    if response.status().as_u16() != 204 {
        return Err(worker::Error::RustError(
            "C2 witness acknowledgement failed".into(),
        ));
    }
    Ok(())
}

fn classify(sql: &str) -> Option<EnrollmentQuery> {
    // The independent observer must fail closed when these owning queries change.
    match sql {
        "SELECT a.approval_id,a.request_id,a.label,a.allowed_workspaces,a.state,a.end_reason,a.approved_by_person_id,a.permission_checked,a.base_generation,a.code_expires_at,a.machine_client_id,a.last_grant_at,a.ended_at,a.created_at,a.updated_at,m.machine_id FROM login_machine_approvals a LEFT JOIN login_machines m ON m.machine_id=a.machine_id WHERE a.approval_id=?" => {
            Some(EnrollmentQuery::ByEnrollment)
        }
        "SELECT a.approval_id,a.request_id,a.label,a.allowed_workspaces,a.state,a.end_reason,a.approved_by_person_id,a.permission_checked,a.base_generation,a.code_expires_at,a.machine_client_id,a.last_grant_at,a.ended_at,a.created_at,a.updated_at,m.machine_id FROM login_machine_approvals a LEFT JOIN login_machines m ON m.machine_id=a.machine_id WHERE a.machine_id=? AND a.state='enrolled'" => {
            Some(EnrollmentQuery::ActiveEnrollment)
        }
        "SELECT machine_id,label,state,allowed_workspaces,enrollment_generation,created_at,updated_at FROM login_machines WHERE machine_id=?" => {
            Some(EnrollmentQuery::MachineById)
        }
        _ => None,
    }
}
impl CentralObject {
    pub(super) fn witness_restored(
        &self,
        correlation_id: &central_link_protocol::CorrelationId,
        outcome: RestoreOutcome,
    ) {
        if let Err(error) = self.witnesses.enqueue(
            &self.env,
            &self.state,
            Event::RestoreResult {
                object_id: self.state.id().to_string(),
                correlation_id: correlation_id.clone(),
                outcome,
            },
        ) {
            worker::console_warn!("C2 authority diagnostic failed: {error}");
        }
    }
    pub(super) fn witness_alarm_outcome(
        &self,
        correlation_id: &central_link_protocol::CorrelationId,
    ) -> Result<()> {
        if !enabled(&self.env)? {
            return Ok(());
        }
        // A member that made no successful publication retains the last one.
        // Cold runtime initialization publishes before accepting any member.
        let observed =
            self.witnesses.0.borrow().committed_alarm.ok_or_else(|| {
                worker::Error::RustError("C2 has no committed alarm outcome".into())
            })?;
        let earliest_due_ms = [
            observed.inputs.earliest_on_grid_due_unix_millis,
            observed.inputs.earliest_legacy_due_unix_millis,
        ]
        .into_iter()
        .flatten()
        .min();
        if earliest_due_ms.is_some_and(|value| value < 0) {
            return Err(worker::Error::RustError("C2 invalid due value".into()));
        }
        let alarm_ms = observed.alarm_ms;
        if alarm_ms
            .is_some_and(|value| value < 0 || central_storage::SafeInteger::new(value).is_err())
        {
            return Err(worker::Error::RustError("C2 invalid alarm value".into()));
        }
        self.witnesses.enqueue(
            &self.env,
            &self.state,
            Event::AlarmSnapshot {
                object_id: self.state.id().to_string(),
                correlation_id: correlation_id.clone(),
                earliest_due_ms,
                alarm_ms,
            },
        )
    }
}
