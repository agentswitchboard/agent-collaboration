//! Prepare machine-login effects under the caller's existing organization turn.
use crate::{
    enrollment_reads, enrollment_writes,
    policy::{
        self, Decided, DecisionTime, EnrollmentFacts, EnrollmentQuery, StoredEnrollmentState,
    },
};
use central_storage::{GuardedStatement, StoreFailure};
use fleet_contracts::{
    AdmittedRevoke, DecideDeviceLogin, DecideMachineToken, DeviceLoginDecision, EndedMachineClient,
    MachineTokenDecision,
};
use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_runtime_ports::{CentralStore, IdSource, workspace_decode_error::corrupt};
use workspace_ids::{MachineId, WorkspaceId};

pub struct MachineLoginService<'turn, Host> {
    store: &'turn Host,
    organization: &'turn OrganizationId,
    ids: &'turn dyn IdSource,
}

pub struct MachineLoginPlan<Answer> {
    pub answer: Answer,
    pub statements: Vec<GuardedStatement>,
}

pub enum PreparedEnrollmentRevocation {
    NotFound,
    AlreadyEnded,
    Revoked {
        machine_id: MachineId,
        ended: EndedMachineClient,
        statements: Vec<GuardedStatement>,
    },
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
        let facts = self
            .read_facts(&EnrollmentQuery {
                enrollment: Some(request.enrollment_id.clone()),
                ..EnrollmentQuery::default()
            })
            .await?;
        let decided = policy::decide_machine_token(request, &facts, time, self.ids)?;
        prepare_plan(&facts, decided, time)
    }

    pub async fn prepare_revoke_enrollment(
        &self,
        authority: AdmittedRevoke,
        enrollment_id: &EnrollmentId,
        time: DecisionTime,
    ) -> Result<PreparedEnrollmentRevocation, StoreFailure> {
        let facts = self
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
                if facts.active.as_ref().map(|active| &active.id) != Some(enrollment_id) {
                    return Err(corrupt("login_machine_approvals.enrolled"));
                }
                let decided = policy::revoke_machine_enrollment(
                    &facts,
                    &authority.actor,
                    &authority.proof_request_id,
                    authority.permission.as_str(),
                    time,
                    self.ids,
                )?;
                let plan = prepare_plan(&facts, decided, time)?;
                Ok(PreparedEnrollmentRevocation::Revoked {
                    machine_id: enrollment.machine_id.clone(),
                    ended: plan
                        .answer
                        .ok_or_else(|| corrupt("login_machine_approvals.enrolled"))?,
                    statements: plan.statements,
                })
            }
        }
    }

    pub async fn read_facts(
        &self,
        query: &EnrollmentQuery,
    ) -> Result<EnrollmentFacts, StoreFailure> {
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

    pub fn prepare_idle_expiry(
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
    Ok(MachineLoginPlan {
        answer: decided.answer,
        statements: enrollment_writes::statements(facts, &decided.changes, time.now_ms)?,
    })
}
