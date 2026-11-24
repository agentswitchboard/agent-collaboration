//! Test control for the engine methods on each host.

use crate::{
    WorkspaceRuntime,
    engine_admin_types::{EngineAttemptAction, EngineAttemptCommand},
};
use central_storage::StoreFailure;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
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
        operation_id: RequestId,
        attempt_id: uuid::Uuid,
        person_id: PersonId,
        action: EngineAttemptAction,
    },
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
    let result = match control {
        EngineAdminControl::Health {} => {
            let _turn = runtime.execution.take_central_turn().await;
            runtime.engine_health_result().await?.result
        }
        EngineAdminControl::Retired { after, limit } => {
            let _turn = runtime.execution.take_central_turn().await;
            runtime.engine_retired_result(after, limit).await?.result
        }
        EngineAdminControl::Change {
            operation_id,
            attempt_id,
            person_id,
            action,
        } => {
            let intent_digest = fleet_signin::person_proof::sha256_url(
                format!("engine-fixture:{action:?}:{attempt_id}").as_bytes(),
            );
            let command = EngineAttemptCommand {
                organization_id: runtime.execution.organization().clone(),
                operation_id,
                attempt_id,
                person_id,
                intent_digest,
            };
            runtime.apply_engine_attempt(&command, action).await?
        }
    };
    serde_json::to_value(result).map_err(|_| StoreFailure::Unavailable)
}
