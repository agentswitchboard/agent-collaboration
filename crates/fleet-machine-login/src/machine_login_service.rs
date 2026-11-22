//! Prepare machine-login effects under the caller's existing organization turn.
use crate::{
    enrollment_reads, enrollment_writes,
    policy::{
        self, Decided, DecisionTime, EnrollmentChange, EnrollmentFacts, EnrollmentQuery,
        StoredAllowanceState, StoredEnrollmentState,
    },
};
use central_storage::{GuardedStatement, StoreFailure};
use fleet_contracts::{
    AdmittedRevoke, DecideDeviceLogin, DecideMachineToken, DeviceLoginDecision, EndedMachineClient,
    EnrollmentInvalidation, EnrollmentRevocationReason, MachineTokenDecision,
    mail_link::MachineLinkCloseCode,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_runtime_ports::{CentralStore, IdSource, workspace_decode_error::corrupt};
use workspace_ids::{MachineId, WorkspaceId};

pub struct MachineLoginService<'turn, Host> {
    pub(crate) store: &'turn Host,
    pub(crate) organization: &'turn OrganizationId,
    ids: &'turn dyn IdSource,
}

pub struct MachineLoginPlan<Answer> {
    answer: Answer,
    statements: Vec<GuardedStatement>,
    invalidation: Option<EnrollmentInvalidation>,
}

impl<Answer> MachineLoginPlan<Answer> {
    pub fn into_applying(
        self,
    ) -> (
        Answer,
        Vec<GuardedStatement>,
        Option<EnrollmentInvalidation>,
    ) {
        (self.answer, self.statements, self.invalidation)
    }
}

pub enum PreparedEnrollmentRevocation {
    NotFound,
    AlreadyEnded,
    Revoked(PreparedOwnerRevocation),
}

pub struct PreparedOwnerRevocation {
    ended: EndedMachineClient,
    invalidation: EnrollmentInvalidation,
    statements: Vec<GuardedStatement>,
}

impl PreparedOwnerRevocation {
    #[must_use]
    pub fn ended_machine(&self) -> &MachineId {
        &self.invalidation.machine_id
    }

    pub fn into_applying(
        self,
    ) -> (
        EndedMachineClient,
        EnrollmentInvalidation,
        Vec<GuardedStatement>,
    ) {
        (self.ended, self.invalidation, self.statements)
    }
}

impl<'turn, Host: CentralStore> MachineLoginService<'turn, Host> {
    pub fn for_organization(
        store: &'turn Host,
        organization: &'turn OrganizationId,
        ids: &'turn dyn IdSource,
    ) -> Self {
        Self {
            store,
            organization,
            ids,
        }
    }

    pub async fn prepare_device_login(
        &self,
        request: &DecideDeviceLogin,
        received_at_ms: i64,
        time: DecisionTime,
    ) -> Result<MachineLoginPlan<DeviceLoginDecision>, StoreFailure> {
        let facts = self
            .read_facts(&EnrollmentQuery {
                machine_id: Some(request.machine_id.clone()),
                request: Some(request.request_id.clone()),
                workspaces: request.allowed_workspaces.as_slice().to_vec(),
                ..EnrollmentQuery::default()
            })
            .await?;
        let decided = policy::decide_device_login(request, &facts, received_at_ms, time, self.ids)?;
        prepare_plan(&facts, decided, time)
    }

    pub async fn prepare_machine_token(
        &self,
        request: &DecideMachineToken,
        time: DecisionTime,
    ) -> Result<MachineLoginPlan<MachineTokenDecision>, StoreFailure> {
        let mut facts = self
            .read_facts(&EnrollmentQuery {
                enrollment: Some(request.enrollment_id.clone()),
                ..EnrollmentQuery::default()
            })
            .await?;
        let ends_current = facts.current.as_ref().is_some_and(|current| {
            request.organization_id == facts.organization_id
                && current.enrollment().id == request.enrollment_id
                && matches!(&current.enrollment().state,
                    StoredEnrollmentState::Enrolled { client_id, last_grant_at }
                    if client_id == &request.client_id
                        && policy::idle_limit_reached(time, *last_grant_at))
        });
        if ends_current {
            enrollment_reads::load_terminal_allowances(self.store, &mut facts).await?;
        }
        let decided = policy::decide_machine_token(request, &facts, time, self.ids)?;
        prepare_plan(&facts, decided, time)
    }

    pub async fn prepare_revoke_enrollment(
        &self,
        authority: AdmittedRevoke,
        enrollment_id: &EnrollmentId,
        time: DecisionTime,
    ) -> Result<PreparedEnrollmentRevocation, StoreFailure> {
        let mut facts = self
            .read_facts(&EnrollmentQuery {
                enrollment: Some(enrollment_id.clone()),
                ..EnrollmentQuery::default()
            })
            .await?;
        let Some(enrollment) = &facts.enrollment else {
            return Ok(PreparedEnrollmentRevocation::NotFound);
        };
        match &enrollment.state {
            StoredEnrollmentState::Approved => Ok(PreparedEnrollmentRevocation::NotFound),
            StoredEnrollmentState::Lapsed
            | StoredEnrollmentState::Revoked { .. }
            | StoredEnrollmentState::Expired { .. } => {
                Ok(PreparedEnrollmentRevocation::AlreadyEnded)
            }
            StoredEnrollmentState::Enrolled { .. } => {
                if facts
                    .current
                    .as_ref()
                    .map(|current| &current.enrollment().id)
                    != Some(enrollment_id)
                {
                    return Err(corrupt("login_machine_approvals.enrolled"));
                }
                enrollment_reads::load_terminal_allowances(self.store, &mut facts).await?;
                let decided = policy::revoke_machine_enrollment(
                    &facts,
                    &authority.actor,
                    &authority.proof_request_id,
                    authority.permission.as_str(),
                    time,
                    self.ids,
                )?;
                let plan = prepare_plan(&facts, decided, time)?;
                Ok(PreparedEnrollmentRevocation::Revoked(
                    PreparedOwnerRevocation {
                        invalidation: plan
                            .invalidation
                            .ok_or_else(|| corrupt("login_machine_approvals.enrolled"))?,
                        ended: plan
                            .answer
                            .ok_or_else(|| corrupt("login_machine_approvals.enrolled"))?,
                        statements: plan.statements,
                    },
                ))
            }
        }
    }

    async fn read_facts(&self, query: &EnrollmentQuery) -> Result<EnrollmentFacts, StoreFailure> {
        enrollment_reads::facts(self.store, self.organization.clone(), query).await
    }

    pub async fn active_workspaces(
        &self,
        machine: &MachineId,
    ) -> Result<Vec<WorkspaceId>, StoreFailure> {
        enrollment_reads::read_active_allowances(self.store, machine)
            .await?
            .into_iter()
            .map(|id| {
                id.as_str()
                    .parse()
                    .map_err(|_| corrupt("login_machine_allowances.workspace_id"))
            })
            .collect()
    }

    pub async fn allows_workspace(
        &self,
        machine: &MachineId,
        workspace: &WorkspaceId,
    ) -> Result<bool, StoreFailure> {
        let workspace = crate::enrollment_row_values::identifier(
            workspace.as_str(),
            "login_machine_allowances.workspace_id",
        )?;
        let allowance = enrollment_reads::read_allowance(self.store, machine, &workspace).await?;
        Ok(allowance.is_some_and(|pair| pair.state == StoredAllowanceState::Active))
    }

    pub(crate) fn prepare_idle_expiry(
        &self,
        facts: &EnrollmentFacts,
        time: DecisionTime,
    ) -> Result<Option<MachineLoginPlan<()>>, StoreFailure> {
        policy::expire_current_if_idle(facts, time, self.ids)?
            .map(|decided| prepare_plan(facts, decided, time))
            .transpose()
    }
}

fn prepare_plan<Answer>(
    facts: &EnrollmentFacts,
    decided: Decided<Answer>,
    time: DecisionTime,
) -> Result<MachineLoginPlan<Answer>, StoreFailure> {
    if match decided.written_at_ms {
        Some(at) => at != time.now_ms || decided.changes.is_empty(),
        None => !decided.changes.is_empty(),
    } {
        return Err(corrupt("machine_login.applying_time"));
    }
    let invalidation = decided.changes.iter().find_map(|change| {
        let EnrollmentChange::ReplaceEnrollment(enrollment) = change else {
            return None;
        };
        let cause = match enrollment.state {
            StoredEnrollmentState::Revoked {
                reason: EnrollmentRevocationReason::Owner {},
                ..
            } => MachineLinkCloseCode::Revoked,
            StoredEnrollmentState::Revoked {
                reason: EnrollmentRevocationReason::Superseded {},
                ..
            }
            | StoredEnrollmentState::Expired { .. } => MachineLinkCloseCode::Inactive,
            StoredEnrollmentState::Approved
            | StoredEnrollmentState::Enrolled { .. }
            | StoredEnrollmentState::Lapsed => return None,
        };
        Some(EnrollmentInvalidation {
            machine_id: enrollment.machine_id.clone(),
            enrollment_id: enrollment.id.clone(),
            cause,
        })
    });
    Ok(MachineLoginPlan {
        answer: decided.answer,
        statements: enrollment_writes::statements(facts, &decided.changes, time.now_ms)?,
        invalidation,
    })
}
