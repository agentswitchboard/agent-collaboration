//! Test support for the existing engine methods after removal of the signed HTTP routes.

use crate::{
    AdminRefusal, WorkspaceRuntime,
    engine_admin_types::{EngineAdminResult, EngineAttemptCommand},
};
use central_storage::{BatchOutcome, StoreFailure};
use fleet_runtime_ports::{CentralStore, WorkspaceStores, workspace_decode_error::corrupt};
use serde::{Deserialize, Serialize};
use workspace_ids::{PersonId, RequestId};

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "operation",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum EngineAdminControl {
    Health {},
    Retired {
        after: Option<uuid::Uuid>,
        limit: u16,
    },
    Change {
        command: EngineAttemptFixture,
        action: EngineAttemptAction,
    },
}

#[derive(Deserialize, Serialize)]
pub struct EngineAttemptFixture {
    pub operation_id: RequestId,
    pub attempt_id: uuid::Uuid,
    pub person_id: PersonId,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EngineAttemptAction {
    Retry,
    Discard,
}

pub async fn fixture_engine_admin<Host: CentralStore, Stores: WorkspaceStores>(
    runtime: &WorkspaceRuntime<Host, Stores>,
    control: EngineAdminControl,
) -> Result<serde_json::Value, StoreFailure> {
    let answer = apply_engine_fixture(runtime, control).await;
    let _ = runtime.after_event().await;
    answer
}

async fn apply_engine_fixture<Host: CentralStore, Stores: WorkspaceStores>(
    runtime: &WorkspaceRuntime<Host, Stores>,
    control: EngineAdminControl,
) -> Result<serde_json::Value, StoreFailure> {
    let _turn = runtime.execution.take_central_turn().await;
    let result = match control {
        EngineAdminControl::Health {} => runtime.engine_health_result().await?.result,
        EngineAdminControl::Retired { after, limit } => {
            runtime.engine_retired_result(after, limit).await?.result
        }
        EngineAdminControl::Change { command, action } => {
            let now = runtime.execution.now()?;
            let intent_digest = fleet_signin::person_proof::sha256_url(
                format!("engine-fixture:{action:?}:{}", command.attempt_id).as_bytes(),
            );
            let command = EngineAttemptCommand {
                organization_id: runtime.execution.organization().clone(),
                operation_id: command.operation_id,
                attempt_id: command.attempt_id,
                person_id: command.person_id,
                intent_digest,
            };
            let mut statements = Vec::new();
            let plan = match action {
                EngineAttemptAction::Retry => runtime.prepare_engine_retry(&command, now).await?,
                EngineAttemptAction::Discard => {
                    runtime
                        .prepare_engine_discard(&command, &mut statements, now)
                        .await?
                }
            };
            if let Some(retained) = plan.retained {
                return serde_json::from_str(&retained.into_text())
                    .map_err(|_| corrupt("runtime_request_log.answer"));
            }
            if !plan.committed {
                statements.extend(plan.statements);
                match runtime
                    .execution
                    .statement_host()
                    .run_batch(central_storage::StatementBatch {
                        statements,
                        alarm: plan.alarm,
                    })
                    .await?
                {
                    BatchOutcome::Applied { .. } => {}
                    BatchOutcome::StatementRejected { .. } => {
                        return Err(StoreFailure::Unavailable);
                    }
                    BatchOutcome::GuardFailed { .. } | BatchOutcome::ConstraintViolated { .. } => {
                        return serde_json::to_value(EngineAdminResult::Refused {
                            reason: AdminRefusal::NotFound,
                        })
                        .map_err(|_| StoreFailure::Unavailable);
                    }
                }
            }
            plan.result
        }
    };
    serde_json::to_value(result).map_err(|_| StoreFailure::Unavailable)
}
