//! Approval reserves a snapshot; it never activates a login.
use super::allowance_transitions::{AllowanceEvent, allowance_effect};
use super::decision_support::{DecisionIds, DecisionTime};
use super::enrollment_records::{
    AllowanceInputs, Decided, EnrollmentChange, EnrollmentFacts, StoredEnrollment,
};
use super::enrollment_transitions::reserve_enrollment;
use central_storage::StoreFailure;
use fleet_contracts::PERSON_SECOND_FACTOR_WINDOW_SECONDS;
use fleet_contracts::{DecideDeviceLogin, DeviceLoginDecision, DeviceLoginRefusal};
use fleet_identity_protocol::EnrollmentId;
use fleet_runtime_ports::{IdSource, workspace_decode_error::corrupt};
use fleet_signin::organization_change::{MachineAllowanceState, OrganizationChangeOrigin};
use std::{collections::BTreeSet, num::NonZeroU32};

pub const SD8_MILLIS: i64 = 5_000;

pub fn decide_device_login(
    request: &DecideDeviceLogin,
    facts: &EnrollmentFacts,
    received_at_ms: i64,
    time: DecisionTime,
    ids: &dyn IdSource,
) -> Result<Decided<DeviceLoginDecision>, StoreFailure> {
    let refuse = |reason| Ok(Decided::unchanged(DeviceLoginDecision::Refused { reason }));
    if request.organization_id != facts.organization_id {
        return refuse(DeviceLoginRefusal::NotPermitted {});
    }
    let machine_id = request.machine_id.clone();
    // A retained request ID belongs to its original machine and bound inputs.
    // Compare workspace sets while returning the original ordered snapshot.
    // Receipt age and factor freshness are predicates of a new approval.
    if let Some(earlier) = &facts.prior_approval {
        if earlier.machine_id != machine_id {
            return refuse(DeviceLoginRefusal::NotPermitted {});
        }
        if earlier.label != request.label
            || earlier.approved_by != request.approver
            || earlier.code_expires_at != request.code_expires_at
            || earlier
                .allowed_workspaces
                .as_slice()
                .iter()
                .collect::<BTreeSet<_>>()
                != request
                    .allowed_workspaces
                    .as_slice()
                    .iter()
                    .collect::<BTreeSet<_>>()
        {
            return refuse(DeviceLoginRefusal::RequestConflict {});
        }
        return Ok(Decided::unchanged(approved(earlier)));
    }
    if time.now_ms.saturating_sub(received_at_ms) > SD8_MILLIS {
        return refuse(DeviceLoginRefusal::NotPermitted {});
    }
    let Some(deadline_ms) = request.code_expires_at.0.checked_mul(1000) else {
        return refuse(DeviceLoginRefusal::NotPermitted {});
    };
    if deadline_ms <= time.now_ms {
        return refuse(DeviceLoginRefusal::NotPermitted {});
    }
    if !facts.missing_workspaces.is_empty() {
        return refuse(DeviceLoginRefusal::UnknownWorkspace {});
    }
    if facts.organization_has_enrollment
        && !second_factor_fresh(
            request.approver_second_factor_at.map(|at| at.0),
            time.now_ms,
        )
    {
        let within_seconds = u32::try_from(PERSON_SECOND_FACTOR_WINDOW_SECONDS)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(StoreFailure::Unavailable)?;
        return refuse(DeviceLoginRefusal::SecondFactorRequired { within_seconds });
    }
    let ids = DecisionIds(ids);
    let (machine, enrollment) = reserve_enrollment(
        facts.machine.as_ref(),
        request,
        EnrollmentId::try_from(ids.next()?.to_string()).map_err(|_| StoreFailure::Unavailable)?,
        time,
    )?;
    // The owning machine must precede its approval's foreign key.
    let mut changes = vec![
        EnrollmentChange::PutMachine(machine),
        EnrollmentChange::InsertEnrollment(enrollment.clone()),
    ];
    let origin = OrganizationChangeOrigin::Person {
        person_id: request.approver.as_str().to_owned(),
        permission_checked: "machine:enroll".to_owned(),
    };
    let AllowanceInputs::Selected(selected) = &facts.allowances else {
        return Err(corrupt("login_machine_allowances.approval_inputs"));
    };
    let mut touched = BTreeSet::new();
    for workspace in request.allowed_workspaces.as_slice() {
        if !touched.insert(workspace) {
            continue;
        }
        let observed = selected
            .get(workspace)
            .ok_or_else(|| corrupt("login_machine_allowances.workspace_id"))?;
        if let Some(effect) =
            allowance_effect(observed.as_ref(), AllowanceEvent::ApprovalSelectedWorkspace)?
        {
            changes.push(EnrollmentChange::RecordAllowance {
                record: ids.record_allowance_change(
                    request.request_id.as_str(),
                    &origin,
                    &enrollment.machine_id,
                    workspace,
                    MachineAllowanceState::Active,
                    time,
                )?,
                effect,
            });
        }
    }
    Ok(Decided::writing(
        approved(&enrollment),
        changes,
        time.now_ms,
    ))
}

fn approved(enrollment: &StoredEnrollment) -> DeviceLoginDecision {
    DeviceLoginDecision::Approved {
        enrollment_id: enrollment.id.clone(),
        label: enrollment.label.clone(),
        allowed_workspaces: enrollment.allowed_workspaces.clone(),
    }
}
fn second_factor_fresh(second_factor_at: Option<i64>, now_ms: i64) -> bool {
    second_factor_at
        .and_then(|at| at.checked_mul(1000))
        .and_then(|at| now_ms.checked_sub(at))
        .is_some_and(|age| (0..=PERSON_SECOND_FACTOR_WINDOW_SECONDS * 1000).contains(&age))
}

#[cfg(test)]
#[path = "device_login_decision_test.rs"]
mod tests;
