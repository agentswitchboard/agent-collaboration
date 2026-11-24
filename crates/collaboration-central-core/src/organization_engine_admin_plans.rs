//! P8's four admin operations on the existing organization-admin owner: the
//! reads answer from storage; retry and discard retain their answer under the
//! operation ID, so a replay returns it as CreateWorkspace's does.
use crate::{
    AdminRefusal, WorkspaceRuntime,
    engine_admin_types::{
        EngineAdminResult, EngineAttemptAction, EngineAttemptCommand, EngineKindHealth,
        EngineRetiredItem,
    },
    organization_admin_contract::RetainedOrganizationAdminAnswer,
    workspace_runtime::engine_admin_dispatch::KIND_REGISTRY,
};
use central_storage::{AlarmUpdate, BatchOutcome, GuardedStatement, StatementBatch, StoreFailure};
use fleet_engine::{AttemptRefusal, DiscardOutcome, FleetEngine};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_runtime_ports::{
    stored_values::{json, string},
    workspace_decode_error::corrupt,
};
use workspace_model::{ActorIdentityId, Identity, RequestId, Timestamp};

/// What the admin batch carries for one engine operation.
pub(crate) struct EngineAdminPlan {
    pub result: EngineAdminResult,
    statements: Vec<GuardedStatement>,
    alarm: AlarmUpdate,
    retained: Option<RetainedOrganizationAdminAnswer>,
    /// Discard already committed its batch through `handle`.
    committed: bool,
}
impl EngineAdminPlan {
    fn read(result: EngineAdminResult) -> Self {
        Self {
            result,
            statements: vec![],
            alarm: AlarmUpdate::Unchanged,
            retained: None,
            committed: false,
        }
    }
}

enum OperationReceipt {
    Replay(EngineAdminPlan),
    Fresh {
        operation: RequestId,
        actor: ActorIdentityId,
        statements: Vec<GuardedStatement>,
        cutoff: String,
    },
}

fn refusal(refusal: AttemptRefusal) -> EngineAdminResult {
    EngineAdminResult::Refused {
        reason: match refusal {
            AttemptRefusal::NotFound => AdminRefusal::NotFound,
            AttemptRefusal::NotRetired => AdminRefusal::NotRetired,
            AttemptRefusal::DiscardUnsupported => AdminRefusal::DiscardUnsupported,
            AttemptRefusal::DiscardSuperseded => AdminRefusal::Stale,
            AttemptRefusal::SubjectUnreadable => AdminRefusal::SubjectUnreadable,
        },
    }
}

fn retained_engine_result(encoded: &str) -> Result<EngineAdminResult, StoreFailure> {
    let answer: EngineAdminResult =
        serde_json::from_value(json(encoded, "runtime_request_log.answer")?)
            .map_err(|_| corrupt("runtime_request_log.answer"))?;
    match answer {
        EngineAdminResult::EngineRetried {}
        | EngineAdminResult::EngineDiscarded {}
        | EngineAdminResult::Refused { .. } => Ok(answer),
        EngineAdminResult::EngineHealth { .. } | EngineAdminResult::EngineRetired { .. } => {
            Err(corrupt("runtime_request_log.answer"))
        }
    }
}

impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    pub(crate) async fn engine_health_result(&self) -> Result<EngineAdminPlan, StoreFailure> {
        let health = FleetEngine::new(self)
            .read_engine_attempt_health(&KIND_REGISTRY)
            .await?;
        Ok(EngineAdminPlan::read(EngineAdminResult::EngineHealth {
            kinds: health
                .health_by_machine_kind
                .into_iter()
                .map(|kind| EngineKindHealth {
                    kind: kind.machine_kind,
                    failing: kind.failing_attempt_count,
                    retired: kind.retired_attempt_count,
                    oldest_first_failed_at: kind.oldest_first_failed_at,
                    last_failure: kind.latest_failure_class,
                })
                .collect(),
            alarm: health.published_alarm_at,
            alarm_missing: health.alarm_missing,
        }))
    }

    pub(crate) async fn engine_retired_result(
        &self,
        after: Option<uuid::Uuid>,
        limit: u16,
    ) -> Result<EngineAdminPlan, StoreFailure> {
        let (items, next) = FleetEngine::new(self)
            .read_retired_attempt_page(&KIND_REGISTRY, after, limit)
            .await?;
        Ok(EngineAdminPlan::read(EngineAdminResult::EngineRetired {
            items: items
                .into_iter()
                .map(|item| EngineRetiredItem {
                    attempt_id: item.attempt_id,
                    kind: item.machine_kind,
                    subject_key: item.subject_key,
                    failure_class: item.failure_class,
                    count: item.failure_count,
                    retired_at: item.retired_at,
                    last_failure: item.last_failure_detail,
                })
                .collect(),
            next,
        }))
    }

    async fn engine_operation_receipt(
        &self,
        command: &EngineAttemptCommand,
        now: Timestamp,
    ) -> Result<OperationReceipt, StoreFailure> {
        if &command.organization_id != self.execution.organization() {
            return Err(StoreFailure::Unavailable);
        }
        let operation = command.operation_id.clone();
        let identity = Identity::Person {
            person_id: command.person_id.clone(),
        };
        let actor = self.access().prepare_identity(&identity, now).await?;
        let cutoff = (now - chrono::Duration::hours(24)).to_rfc3339();
        let logged = self
            .execution
            .statement_host()
            .read(fleet_request_log::lookup_organization_request(
                &operation, &cutoff,
            ))
            .await?;
        if logged.rows.len() > 1 {
            return Err(corrupt("runtime_request_log.request_id"));
        }
        let Some(row) = logged.rows.first() else {
            return Ok(OperationReceipt::Fresh {
                operation,
                actor: actor.actor,
                statements: actor.statements,
                cutoff,
            });
        };
        let stored_actor: ActorIdentityId = string(&logged, row, "caller")?
            .parse()
            .map_err(|_| corrupt("runtime_request_log.caller"))?;
        if stored_actor != actor.actor
            || string(&logged, row, "content_digest")? != command.intent_digest
        {
            return Ok(OperationReceipt::Replay(EngineAdminPlan::read(
                EngineAdminResult::Refused {
                    reason: AdminRefusal::Conflict,
                },
            )));
        }
        let encoded = string(&logged, row, "answer")?;
        let mut plan = EngineAdminPlan::read(retained_engine_result(encoded)?);
        plan.retained = Some(RetainedOrganizationAdminAnswer::validated(
            encoded.to_owned(),
        ));
        Ok(OperationReceipt::Replay(plan))
    }

    /// Applies a repair for the future person link call and is currently exercised through test support.
    pub(crate) async fn apply_engine_attempt(
        &self,
        command: &EngineAttemptCommand,
        action: EngineAttemptAction,
    ) -> Result<EngineAdminResult, StoreFailure> {
        let _turn = self.execution.take_central_turn().await;
        let now = self.execution.now()?;
        let mut statements = Vec::new();
        let (planned, guard_failed) = match action {
            EngineAttemptAction::Retry => {
                let planned = self.prepare_engine_retry(command, now).await?;
                // A concurrent retry deleted the record first.
                (planned, AdminRefusal::NotFound)
            }
            EngineAttemptAction::Discard => {
                let planned = self
                    .prepare_engine_discard(command, &mut statements, now)
                    .await?;
                if planned.committed {
                    return Ok(planned.result);
                }
                (planned, AdminRefusal::Conflict)
            }
        };
        statements.extend(planned.statements);
        match self
            .execution
            .statement_host()
            .run_batch(StatementBatch {
                statements,
                alarm: planned.alarm,
            })
            .await?
        {
            BatchOutcome::Applied { .. } => match planned.retained {
                Some(answer) => retained_engine_result(&answer.into_text()),
                None => Ok(planned.result),
            },
            BatchOutcome::StatementRejected { .. } => Err(StoreFailure::Unavailable),
            BatchOutcome::GuardFailed { .. } | BatchOutcome::ConstraintViolated { .. } => {
                Ok(EngineAdminResult::Refused {
                    reason: guard_failed,
                })
            }
        }
    }

    /// Retry: the guarded delete and its retained answer ride in the admin batch,
    /// which re-arms the alarm so an idle object wakes for the subject.
    async fn prepare_engine_retry(
        &self,
        command: &EngineAttemptCommand,
        now: Timestamp,
    ) -> Result<EngineAdminPlan, StoreFailure> {
        let (operation, actor, mut statements, cutoff) =
            match self.engine_operation_receipt(command, now).await? {
                OperationReceipt::Replay(plan) => return Ok(plan),
                OperationReceipt::Fresh {
                    operation,
                    actor,
                    statements,
                    cutoff,
                } => (operation, actor, statements, cutoff),
            };
        let result = match FleetEngine::new(self)
            .plan_retired_attempt_retry(&KIND_REGISTRY, command.attempt_id, now)
            .await?
        {
            Ok(retry) => {
                statements.extend(retry);
                EngineAdminResult::EngineRetried {}
            }
            Err(reason) => refusal(reason),
        };
        statements.extend(fleet_request_log::retain_organization_request(
            &operation,
            &actor,
            &command.intent_digest,
            &result,
            &now.to_rfc3339(),
            &cutoff,
        )?);
        Ok(EngineAdminPlan {
            alarm: match result {
                EngineAdminResult::EngineRetried {} => {
                    AlarmUpdate::Engine(central_storage::EngineAlarmPlan::Recompute)
                }
                _ => AlarmUpdate::Unchanged,
            },
            result,
            statements,
            retained: None,
            committed: false,
        })
    }

    /// Discard: the admission writes, the retained answer and the guarded delete
    /// commit with the subject's Discard transition; a refusal falls back to the
    /// ordinary admin batch with the refusal retained.
    async fn prepare_engine_discard(
        &self,
        command: &EngineAttemptCommand,
        admission: &mut Vec<GuardedStatement>,
        now: Timestamp,
    ) -> Result<EngineAdminPlan, StoreFailure> {
        let (operation, actor, mut statements, cutoff) =
            match self.engine_operation_receipt(command, now).await? {
                OperationReceipt::Replay(plan) => return Ok(plan),
                OperationReceipt::Fresh {
                    operation,
                    actor,
                    statements,
                    cutoff,
                } => (operation, actor, statements, cutoff),
            };
        let retain = |result: &EngineAdminResult| {
            fleet_request_log::retain_organization_request(
                &operation,
                &actor,
                &command.intent_digest,
                result,
                &now.to_rfc3339(),
                &cutoff,
            )
        };
        let mut committed = admission.clone();
        committed.extend(statements.clone());
        committed.extend(retain(&EngineAdminResult::EngineDiscarded {})?);
        match FleetEngine::new(self)
            .discard_retired_attempt(command.attempt_id, committed)
            .await?
        {
            DiscardOutcome::Committed => {
                admission.clear();
                // The re-arm is this event's last step; a failed publication is
                // observed and the next event re-publishes.
                let _ = FleetEngine::new(self)
                    .publish_engine_alarm(central_storage::EngineAlarmPlan::Recompute)
                    .await;
                Ok(EngineAdminPlan {
                    result: EngineAdminResult::EngineDiscarded {},
                    statements: vec![],
                    alarm: AlarmUpdate::Unchanged,
                    retained: None,
                    committed: true,
                })
            }
            DiscardOutcome::Refused(reason) => {
                let result = refusal(reason);
                statements.extend(retain(&result)?);
                Ok(EngineAdminPlan {
                    result,
                    statements,
                    alarm: AlarmUpdate::Unchanged,
                    retained: None,
                    committed: false,
                })
            }
        }
    }
}
