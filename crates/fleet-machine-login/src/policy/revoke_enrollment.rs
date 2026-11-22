//! Private revocation policy; routing and connection close delivery belong to C.
use super::decision_support::{DecisionIds, DecisionTime};
use super::enrollment_records::{Decided, EnrollmentChange, EnrollmentFacts};
use super::enrollment_transitions::{EnrollmentEnding, end_current_enrollment};
use super::machine_token_decision::append_allowance_records;
use central_storage::StoreFailure;
use fleet_contracts::{EndedMachineClient, PersonId};
use fleet_runtime_ports::IdSource;
use fleet_signin::organization_change::{OrganizationChangeKind, OrganizationChangeOrigin};
use message_board::RecordId;

/// None means the machine_id has no currently enrolled login.
pub fn revoke_machine_enrollment(
    facts: &EnrollmentFacts,
    person: &PersonId,
    operation: &RecordId,
    permission_checked: &'static str,
    time: DecisionTime,
    ids: &dyn IdSource,
) -> Result<Decided<Option<EndedMachineClient>>, StoreFailure> {
    let Some(current) = &facts.current else {
        return Ok(Decided::unchanged(None));
    };
    let active = current.enrollment();
    let ids = DecisionIds(ids);
    let origin = OrganizationChangeOrigin::Person {
        person_id: person.as_str().to_owned(),
        permission_checked: permission_checked.to_owned(),
    };
    let mut changes = Vec::from(end_current_enrollment(
        current,
        EnrollmentEnding::Owner(time),
    )?);
    changes.push(EnrollmentChange::Record(ids.record(
        operation.as_str(),
        OrganizationChangeKind::MachineRevoked,
        &origin,
        active.id.as_str().to_owned(),
        time,
    )?));
    append_allowance_records(
        &mut changes,
        &facts.allowances,
        &current.machine().machine_id,
        operation.as_str(),
        &origin,
        time,
        &ids,
    )?;
    Ok(Decided::writing(
        active.ended_client(),
        changes,
        time.now_ms,
    ))
}

#[cfg(test)]
#[path = "revoke_enrollment_test.rs"]
mod tests;
