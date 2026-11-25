//! Verified setup decisions share one org-owned directory, ledger and audit batch.
use crate::{
    AdminRefusal, OrganizationAdminResult, WorkspaceRuntime,
    organization_admin_contract::RetainedOrganizationAdminAnswer,
    person_request::{AdminOperation, CreateWorkspaceRequest},
};
use central_storage::{AlarmUpdate, BatchOutcome, GuardedStatement, StatementBatch, StoreFailure};
use fleet_contracts::{DirectoryWorkspace, WorkspaceDirectory};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_runtime_ports::{
    stored_values::{json, string},
    workspace_decode_error::corrupt,
};
use fleet_signin::organization_change::{
    self, ChangeRequestId, OrganizationChangeKind, OrganizationChangeOrigin,
    OrganizationChangeRecord,
};
use fleet_workspace_directory::CentralWorkspaceDirectory;
use workspace_model::{
    ActorIdentityId, Description, Identity, PersonId, RequestId, ResourceName, Timestamp,
    WorkspaceId, WorkspaceState,
};

struct WorkspaceCreatePlan {
    pub result: OrganizationAdminResult,
    pub statements: Vec<GuardedStatement>,
    pub operation: RequestId,
    pub retained: Option<RetainedOrganizationAdminAnswer>,
}

pub(crate) struct WorkspaceCreateAnswer {
    pub result: OrganizationAdminResult,
    pub retained: Option<RetainedOrganizationAdminAnswer>,
}

/// Both ingress paths admit their person and reserve their guards under the owning organization turn.
pub(crate) async fn apply_workspace_create<H: CentralStore, S: WorkspaceStores>(
    runtime: &WorkspaceRuntime<H, S>,
    person: &PersonId,
    request: &CreateWorkspaceRequest,
    now: Timestamp,
    mut statements: Vec<GuardedStatement>,
) -> Result<WorkspaceCreateAnswer, StoreFailure> {
    // Both paths must use the existing ledger digest, including for receipts committed via HTTP.
    let intent_digest = AdminOperation::CreateWorkspace(request.clone())
        .intent_digest()
        .map_err(|_| StoreFailure::Unavailable)?;
    let created = runtime
        .prepare_workspace_create(person, &intent_digest, request, now)
        .await?;
    statements.extend(created.statements);
    #[cfg(feature = "test-support")]
    runtime
        .execution
        .pause_points()
        .reached_central(fleet_runtime::PausePoint::AfterSignInSample)
        .await;
    match runtime
        .execution
        .statement_host()
        .run_batch(StatementBatch {
            statements,
            alarm: AlarmUpdate::Unchanged,
        })
        .await?
    {
        BatchOutcome::Applied { .. } => {
            #[cfg(feature = "test-support")]
            if runtime.execution.take_lost_reply(&created.operation) {
                return Err(StoreFailure::AnswerLost);
            }
            #[cfg(not(feature = "test-support"))]
            let _operation = created.operation;
            Ok(WorkspaceCreateAnswer {
                result: created.result,
                retained: created.retained,
            })
        }
        BatchOutcome::StatementRejected { .. } => Err(StoreFailure::Unavailable),
        BatchOutcome::GuardFailed { .. } | BatchOutcome::ConstraintViolated { .. } => {
            Ok(WorkspaceCreateAnswer {
                result: refused(AdminRefusal::Conflict),
                retained: None,
            })
        }
    }
}

fn refused(reason: AdminRefusal) -> OrganizationAdminResult {
    OrganizationAdminResult::Refused { reason }
}

fn retained_result(
    encoded: &str,
    request: &CreateWorkspaceRequest,
) -> Result<OrganizationAdminResult, StoreFailure> {
    let value: serde_json::Value = json(encoded, "runtime_request_log.answer")?;
    let answer: OrganizationAdminResult =
        serde_json::from_value(value.clone()).map_err(|_| corrupt("runtime_request_log.answer"))?;
    match &answer {
        OrganizationAdminResult::Applied { workspace }
            if workspace.workspace_id.as_str() == request.operation_id.as_str()
                && workspace.name.as_str() == request.name.as_str()
                && value
                    .pointer("/workspace/name")
                    .and_then(serde_json::Value::as_str)
                    == Some(workspace.name.as_str())
                && workspace.description.as_str() == request.description
                && workspace.state == message_board::WorkspaceState::Active =>
        {
            Ok(answer)
        }
        OrganizationAdminResult::Refused {
            reason: AdminRefusal::NameInUse,
        } => Ok(answer),
        _ => Err(corrupt("runtime_request_log.answer")),
    }
}

impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    async fn prepare_workspace_create(
        &self,
        person: &PersonId,
        intent_digest: &str,
        request: &CreateWorkspaceRequest,
        now: Timestamp,
    ) -> Result<WorkspaceCreatePlan, StoreFailure> {
        let operation: RequestId = request
            .operation_id
            .as_str()
            .to_owned()
            .try_into()
            .map_err(|_| corrupt("operation_id"))?;
        let identity = Identity::Person {
            person_id: person.clone(),
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
        if let Some(row) = logged.rows.first() {
            let stored_actor: ActorIdentityId = string(&logged, row, "caller")?
                .parse()
                .map_err(|_| corrupt("runtime_request_log.caller"))?;
            let (result, retained) = if stored_actor != actor.actor
                || string(&logged, row, "content_digest")? != intent_digest
            {
                (refused(AdminRefusal::Conflict), None)
            } else {
                let encoded = string(&logged, row, "answer")?;
                (
                    retained_result(encoded, request)?,
                    Some(RetainedOrganizationAdminAnswer::validated(
                        encoded.to_owned(),
                    )),
                )
            };
            // Replay consumes only its new admitted proof. Neither the retained
            // operation answer nor actor mapping is written a second time.
            return Ok(WorkspaceCreatePlan {
                result,
                statements: vec![],
                operation,
                retained,
            });
        }
        let name: ResourceName = request
            .name
            .as_str()
            .to_owned()
            .try_into()
            .map_err(|_| corrupt("workspace.name"))?;
        let description: Description = request
            .description
            .clone()
            .try_into()
            .map_err(|_| corrupt("workspace.description"))?;
        let mut statements = actor.statements;
        let directory = CentralWorkspaceDirectory::new(self.execution.statement_host());
        let result = if directory.read_by_name(&name).await?.is_some() {
            refused(AdminRefusal::NameInUse)
        } else {
            let workspace_id: WorkspaceId = request
                .operation_id
                .as_str()
                .to_owned()
                .try_into()
                .map_err(|_| corrupt("workspace_id"))?;
            statements.push(directory.plan_guarded_insert(
                &workspace_id,
                &name,
                &description,
                now,
            )?);
            statements.push(organization_change::insert(&OrganizationChangeRecord {
                id: self.execution.ids().new_id(),
                request_id: ChangeRequestId::try_from(request.operation_id.as_str().to_owned())?,
                kind: OrganizationChangeKind::WorkspaceCreated,
                origin: OrganizationChangeOrigin::Person {
                    person_id: person.to_string(),
                    permission_checked: fleet_signin::FleetPermission::CreateWorkspace
                        .as_str()
                        .to_owned(),
                },
                target_id: workspace_id.to_string(),
                created_at_millis: now
                    .timestamp_millis()
                    .try_into()
                    .map_err(|_| corrupt("signin_change_records.created_at"))?,
            })?);
            OrganizationAdminResult::Applied {
                workspace: directory.workspace_view(DirectoryWorkspace {
                    workspace_id,
                    name,
                    description,
                    state: WorkspaceState::Active,
                })?,
            }
        };
        statements.extend(fleet_request_log::retain_organization_request(
            &operation,
            &actor.actor,
            intent_digest,
            &result,
            &now.to_rfc3339(),
            &cutoff,
        )?);
        Ok(WorkspaceCreatePlan {
            result,
            statements,
            operation,
            retained: None,
        })
    }
}

#[cfg(test)]
#[path = "organization_workspace_plans_test.rs"]
mod tests;
