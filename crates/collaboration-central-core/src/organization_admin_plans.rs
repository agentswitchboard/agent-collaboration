//! Person proof admission and read operations on modern org central.
use crate::{
    AdminRefusal, ListWorkspacesResult, OrganizationAdminAnswer, OrganizationAdminResult,
    WorkspaceRuntime,
    organization_admin_contract::OrganizationChangeRecord,
    organization_authority_cache::StampStanding,
    organization_workspace_plans::apply_workspace_create,
    person_admission_plans::prepare_person_admission,
    person_request::{
        AdminOperation, AdminRoute, AdmissionRefusal, PersonRequestInput, PersonRequestRefusal,
    },
};
use central_storage::{BatchOutcome, StatementBatch, StoreFailure};
use fleet_contracts::{
    MachineEnrollmentRevocationRefusal, MachineEnrollmentRevocationResult, WorkspaceDirectory,
};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_workspace_directory::CentralWorkspaceDirectory;
impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    pub(crate) async fn list_organization_workspaces(
        &self,
        after: Option<&message_board::WorkspaceId>,
        limit: u16,
    ) -> Result<fleet_workspace_directory::WorkspaceDirectoryPage, StoreFailure> {
        CentralWorkspaceDirectory::new(self.execution.statement_host())
            .read_workspace_page(after, limit)
            .await
    }

    pub async fn admin_person_request(
        &self,
        input: &PersonRequestInput,
    ) -> Result<OrganizationAdminAnswer, StoreFailure> {
        // An unstamped org reveals nothing about a presented person proof.
        {
            let _turn = self.execution.take_central_turn().await;
            self.prepare_signin_authority().await?;
            if self.signin_authority.standing(self.execution.clock().now()) != StampStanding::Valid
            {
                return Err(StoreFailure::Unavailable);
            }
        }
        let check = self
            .signin_authority
            .check()
            .ok_or(StoreFailure::Unavailable)?;
        if check.sign_in_service().is_none() {
            return Err(StoreFailure::Unavailable);
        }
        let verified = match check.verify_person_request(input).await {
            Ok(verified) => verified,
            Err(PersonRequestRefusal::VerificationUnavailable | PersonRequestRefusal::Policy) => {
                return Err(StoreFailure::Unavailable);
            }
            Err(_) => return Ok(OrganizationAdminAnswer::Unauthenticated),
        };
        if &verified.organization_id != self.execution.organization() {
            return Ok(OrganizationAdminAnswer::Unauthenticated);
        }
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeSignInSample)
            .await;
        let _turn = self.execution.take_central_turn().await;
        self.prepare_signin_authority().await?;
        let now = self.execution.now()?;
        if self
            .signin_authority
            .standing(fleet_runtime_ports::SyncTime::from_unix_millis(
                now.timestamp_millis(),
            ))
            != StampStanding::Valid
        {
            return Err(StoreFailure::Unavailable);
        }
        let plan = match prepare_person_admission(self.execution.statement_host(), &verified, now)
            .await?
        {
            Ok(plan) => plan,
            Err(reason) => {
                if matches!(
                    verified.operation,
                    AdminOperation::RevokeMachineEnrollment(_)
                ) {
                    return Ok(revocation_admission_refusal(reason));
                }
                return Ok(admin_refusal_answer(
                    verified.operation.route(),
                    reason.into(),
                ));
            }
        };
        let statements = plan.statements;
        let result = match &verified.operation {
            AdminOperation::RevokeMachineEnrollment(revocation) => {
                return self
                    .revoke_machine_enrollment_from_admin(
                        &verified,
                        &revocation.request.enrollment_id,
                        statements,
                        now.timestamp_millis(),
                    )
                    .await;
            }
            AdminOperation::CreateWorkspace(request) => {
                let person = verified.person_id.try_into().map_err(|_| {
                    fleet_runtime_ports::workspace_decode_error::corrupt("person_id")
                })?;
                let answer =
                    apply_workspace_create(self, &person, request, now, statements).await?;
                return Ok(match answer.retained {
                    Some(retained) => OrganizationAdminAnswer::Retained(retained),
                    None => OrganizationAdminAnswer::Answered(answer.result),
                });
            }
            AdminOperation::WorkspaceLookup(request) => {
                let workspace = request
                    .workspace_id
                    .as_str()
                    .to_owned()
                    .try_into()
                    .map_err(|_| {
                        fleet_runtime_ports::workspace_decode_error::corrupt("workspace_id")
                    })?;
                let directory = CentralWorkspaceDirectory::new(self.execution.statement_host());
                let found = directory
                    .read_by_id(&workspace)
                    .await?
                    .map(|row| directory.workspace_view(row))
                    .transpose()?;
                OrganizationAdminAnswer::Answered(OrganizationAdminResult::Found {
                    workspace: found,
                })
            }
            AdminOperation::ListWorkspaces(request) => {
                let page = self
                    .list_organization_workspaces(request.after.as_ref(), request.limit.get())
                    .await?;
                OrganizationAdminAnswer::WorkspaceList(ListWorkspacesResult::Workspaces {
                    workspaces: page
                        .workspaces
                        .try_into()
                        .map_err(|_| StoreFailure::Unavailable)?,
                    next: page.next,
                })
            }
            AdminOperation::ReadChangeRecords(request) => {
                let records = fleet_signin::organization_change::read_page(
                    self.execution.statement_host(),
                    request.after,
                    request.limit.get(),
                )
                .await?;
                let records = records
                    .into_iter()
                    .map(OrganizationChangeRecord::try_from)
                    .collect::<Result<_, _>>()?;
                OrganizationAdminAnswer::Answered(OrganizationAdminResult::Records { records })
            }
        };
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::AfterSignInSample)
            .await;
        match self
            .execution
            .statement_host()
            .run_batch(StatementBatch {
                statements,
                alarm: central_storage::AlarmUpdate::Unchanged,
            })
            .await?
        {
            BatchOutcome::Applied { .. } => Ok(result),
            // A rejected statement keeps main's store-outage answer on this path.
            BatchOutcome::StatementRejected { .. } => Err(StoreFailure::Unavailable),
            _ => Ok(admin_refusal_answer(
                verified.operation.route(),
                AdminRefusal::Conflict,
            )),
        }
    }
}

fn admin_refusal_answer(route: AdminRoute, reason: AdminRefusal) -> OrganizationAdminAnswer {
    if route == AdminRoute::ListWorkspaces {
        OrganizationAdminAnswer::WorkspaceList(ListWorkspacesResult::Refused { reason })
    } else {
        OrganizationAdminAnswer::Answered(OrganizationAdminResult::Refused { reason })
    }
}

fn revocation_admission_refusal(reason: AdmissionRefusal) -> OrganizationAdminAnswer {
    let reason = match reason {
        AdmissionRefusal::NotPermitted => MachineEnrollmentRevocationRefusal::NotPermitted,
        AdmissionRefusal::SecondFactorRequired => {
            MachineEnrollmentRevocationRefusal::FactorNotRecent
        }
        _ => return OrganizationAdminAnswer::Unauthenticated,
    };
    OrganizationAdminAnswer::MachineEnrollmentRevocation(
        MachineEnrollmentRevocationResult::Refused { reason },
    )
}
