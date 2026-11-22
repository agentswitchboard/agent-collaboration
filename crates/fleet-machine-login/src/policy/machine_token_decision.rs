//! Activation is generation-fenced; renewal rechecks live client and idle state.
use super::allowance_transitions::{AllowanceEvent, allowance_effect};
use super::decision_support::{DecisionIds, DecisionTime, idle_limit_reached};
use super::enrollment_records::{
    AllowanceInputs, CurrentEnrollment, Decided, EnrollmentChange, EnrollmentFacts,
    StoredEnrollment, StoredEnrollmentState,
};
use super::enrollment_transitions::{
    EnrollmentEnding, EnrollmentEvent, activate_enrollment, end_current_enrollment,
    replace_enrollment,
};
use central_storage::StoreFailure;
use fleet_contracts::{
    DecideMachineToken, EndedMachineClient, EnrollmentRevocationReason, MachineTokenDecision,
    MachineTokenRefusal,
};
use fleet_identity_protocol::UnixSeconds;
use fleet_runtime_ports::{IdSource, workspace_decode_error::corrupt};
use fleet_signin::machine_token::MACHINE_TOKEN_LIFETIME_SECONDS;
use fleet_signin::organization_change::{
    MachineAllowanceState, OrganizationChangeKind, OrganizationChangeOrigin,
};
use workspace_ids::MachineId;

pub fn decide_machine_token(
    request: &DecideMachineToken,
    facts: &EnrollmentFacts,
    time: DecisionTime,
    ids: &dyn IdSource,
) -> Result<Decided<MachineTokenDecision>, StoreFailure> {
    if request.organization_id != facts.organization_id {
        return Ok(refuse(MachineTokenRefusal::EnrollmentNotActive {}));
    }
    let Some(enrollment) = &facts.enrollment else {
        return Ok(refuse(MachineTokenRefusal::EnrollmentNotActive {}));
    };
    let ids = DecisionIds(ids);
    match &enrollment.state {
        StoredEnrollmentState::Approved => first_grant(request, enrollment, facts, time, &ids),
        StoredEnrollmentState::Enrolled {
            client_id,
            last_grant_at,
        } => {
            let current = facts
                .current
                .as_ref()
                .filter(|active| active.enrollment() == enrollment)
                .ok_or_else(|| corrupt("login_machine_approvals.current_enrollment"))?;
            if client_id != &request.client_id {
                return Ok(refuse(MachineTokenRefusal::WrongClient {}));
            }
            if idle_limit_reached(time, *last_grant_at) {
                return idle_expiry(current, facts, time, &ids);
            }
            Ok(Decided::writing(
                grant(enrollment, time, facts.superseded_client.clone())?,
                vec![replace_enrollment(
                    enrollment,
                    EnrollmentEvent::ActiveGrant(time),
                )?],
                time.now_ms,
            ))
        }
        StoredEnrollmentState::Revoked {
            reason: EnrollmentRevocationReason::Superseded {},
            ..
        } => Ok(refuse(MachineTokenRefusal::Superseded {})),
        StoredEnrollmentState::Lapsed
        | StoredEnrollmentState::Revoked { .. }
        | StoredEnrollmentState::Expired { .. } => {
            Ok(refuse(MachineTokenRefusal::EnrollmentNotActive {}))
        }
    }
}

fn first_grant(
    request: &DecideMachineToken,
    enrollment: &StoredEnrollment,
    facts: &EnrollmentFacts,
    time: DecisionTime,
    ids: &DecisionIds<'_>,
) -> Result<Decided<MachineTokenDecision>, StoreFailure> {
    let deadline = enrollment
        .code_expires_at
        .0
        .checked_mul(1000)
        .ok_or(StoreFailure::Unavailable)?;
    if time.now_ms >= deadline {
        return Ok(Decided::writing(
            MachineTokenDecision::Refuse {
                reason: MachineTokenRefusal::EnrollmentNotActive {},
            },
            vec![replace_enrollment(
                enrollment,
                EnrollmentEvent::ApprovalDeadlineElapsed(time),
            )?],
            time.now_ms,
        ));
    }
    if request.client_id.as_str() != enrollment.id.as_str() {
        return Ok(refuse(MachineTokenRefusal::WrongClient {}));
    }
    let machine = facts.machine.as_ref().ok_or(StoreFailure::Unavailable)?;
    if machine.generation != enrollment.base_generation {
        return Ok(refuse(MachineTokenRefusal::Superseded {}));
    }
    let origin = OrganizationChangeOrigin::Person {
        person_id: enrollment.approved_by.as_str().to_owned(),
        permission_checked: "machine:enroll".to_owned(),
    };
    let mut changes = Vec::new();
    let previous = facts.current.as_ref().map(CurrentEnrollment::enrollment);
    let superseded = previous.and_then(StoredEnrollment::ended_client);
    if let Some(previous) = previous {
        // End the old enrollment before entering the partial unique index.
        changes.push(replace_enrollment(
            previous,
            EnrollmentEvent::Superseded(time),
        )?);
        changes.push(EnrollmentChange::Record(ids.record(
            enrollment.request_id.as_str(),
            OrganizationChangeKind::MachineRevoked,
            &origin,
            previous.id.as_str().to_owned(),
            time,
        )?));
    }
    changes.extend(activate_enrollment(
        machine,
        enrollment,
        request.client_id.clone(),
        time,
    )?);
    changes.push(EnrollmentChange::Record(ids.record(
        enrollment.request_id.as_str(),
        OrganizationChangeKind::MachineEnrolled,
        &origin,
        enrollment.id.as_str().to_owned(),
        time,
    )?));
    Ok(Decided::writing(
        grant(enrollment, time, superseded)?,
        changes,
        time.now_ms,
    ))
}

fn grant(
    enrollment: &StoredEnrollment,
    time: DecisionTime,
    superseded: Option<EndedMachineClient>,
) -> Result<MachineTokenDecision, StoreFailure> {
    let decided_at = time.seconds();
    let expires = decided_at
        .0
        .checked_add(MACHINE_TOKEN_LIFETIME_SECONDS)
        .ok_or(StoreFailure::Unavailable)?;
    Ok(MachineTokenDecision::Grant {
        machine_id: enrollment.machine_id.clone(),
        enrollment_id: enrollment.id.clone(),
        decided_at,
        grant_expires_at: UnixSeconds(expires),
        superseded,
    })
}

fn idle_expiry(
    current: &CurrentEnrollment,
    facts: &EnrollmentFacts,
    time: DecisionTime,
    ids: &DecisionIds<'_>,
) -> Result<Decided<MachineTokenDecision>, StoreFailure> {
    let enrollment = current.enrollment();
    let automatic = OrganizationChangeOrigin::Automatic {};
    let mut changes = Vec::from(end_current_enrollment(
        current,
        EnrollmentEnding::Idle(time),
    )?);
    changes.push(EnrollmentChange::Record(ids.record(
        enrollment.id.as_str(),
        OrganizationChangeKind::MachineExpired,
        &automatic,
        enrollment.id.as_str().to_owned(),
        time,
    )?));
    append_allowance_records(
        &mut changes,
        &facts.allowances,
        &current.machine().machine_id,
        enrollment.id.as_str(),
        &automatic,
        time,
        ids,
    )?;
    Ok(Decided::writing(
        MachineTokenDecision::Refuse {
            reason: MachineTokenRefusal::IdleExpired {},
        },
        changes,
        time.now_ms,
    ))
}

pub(crate) fn expire_current_if_idle(
    facts: &EnrollmentFacts,
    time: DecisionTime,
    ids: &dyn IdSource,
) -> Result<Option<Decided<()>>, StoreFailure> {
    let current = facts
        .current
        .as_ref()
        .filter(|current| Some(current.enrollment()) == facts.enrollment.as_ref())
        .ok_or_else(|| corrupt("login_machine_approvals.current_enrollment"))?;
    let StoredEnrollmentState::Enrolled { last_grant_at, .. } = &current.enrollment().state else {
        return Err(corrupt("login_machine_approvals.current_enrollment"));
    };
    if !idle_limit_reached(time, *last_grant_at) {
        return Ok(None);
    }
    let decided = idle_expiry(current, facts, time, &DecisionIds(ids))?;
    Ok(Some(Decided {
        answer: (),
        changes: decided.changes,
        written_at_ms: decided.written_at_ms,
    }))
}

pub(super) fn append_allowance_records(
    changes: &mut Vec<EnrollmentChange>,
    allowances: &AllowanceInputs,
    machine_id: &MachineId,
    originating: &str,
    origin: &OrganizationChangeOrigin,
    time: DecisionTime,
    ids: &DecisionIds<'_>,
) -> Result<(), StoreFailure> {
    let AllowanceInputs::Ending(pairs) = allowances else {
        return Err(corrupt("login_machine_allowances.ending_inputs"));
    };
    // Missing pairs are outside the persisted set, and do not acquire a row on ending.
    // Allowance ID order preserves the audit sequence across the active-only read.
    let mut ordered: Vec<_> = pairs.values().collect();
    ordered.sort_by_key(|pair| pair.id);
    for pair in ordered {
        if let Some(effect) = allowance_effect(Some(pair), AllowanceEvent::EnrollmentEnded)? {
            changes.push(EnrollmentChange::RecordAllowance {
                record: ids.record_allowance_change(
                    originating,
                    origin,
                    machine_id,
                    &pair.workspace_id,
                    MachineAllowanceState::Withdrawn,
                    time,
                )?,
                effect,
            });
        }
    }
    Ok(())
}
fn refuse(reason: MachineTokenRefusal) -> Decided<MachineTokenDecision> {
    Decided::unchanged(MachineTokenDecision::Refuse { reason })
}

#[cfg(test)]
#[path = "machine_token_decision_test.rs"]
mod tests;
