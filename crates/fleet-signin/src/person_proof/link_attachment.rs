//! Admitted facts carried by the host across a person socket’s lifetime.

use super::{
    CheckedPersonLink, OrganizationFloor, OrganizationFloorState, PersonFloor, PersonFloorState,
    PersonLinkFacts, PersonLinkSecondFactor,
};
use crate::{FenceRevision, FleetPermission};
use fleet_identity_protocol::{OrganizationId, UnixMillis};
use workspace_ids::PersonId;

/// The private connection state accepted only from signed admission or the owning socket.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PersonSocketAttachment {
    person_id: PersonId,
    organization_id: OrganizationId,
    permissions: Vec<FleetPermission>,
    authorization_snapshot_at_ms: UnixMillis,
    authorization_snapshot_valid_until_ms: UnixMillis,
    person_revision: FenceRevision,
    organization_revision: FenceRevision,
    session_expires_at_ms: UnixMillis,
    #[serde(deserialize_with = "Option::deserialize")]
    second_factor: Option<PersonLinkSecondFactor>,
}

impl PersonSocketAttachment {
    pub fn admit(
        checked: &CheckedPersonLink,
        organization: &OrganizationId,
        now_ms: i64,
        organization_floor: &OrganizationFloor,
        person_floor: &PersonFloor,
    ) -> Option<Self> {
        let facts = checked.facts();
        (now_ms < checked.upgrade_expires_at_ms()
            && &facts.organization_id == organization
            && accepts_floors(facts, organization_floor, person_floor))
        .then(|| Self {
            person_id: facts.person_id.clone(),
            organization_id: facts.organization_id.clone(),
            permissions: facts.permissions.clone(),
            authorization_snapshot_at_ms: facts.authorization_snapshot_at_ms,
            authorization_snapshot_valid_until_ms: facts.authorization_snapshot_valid_until_ms,
            person_revision: facts.person_revision,
            organization_revision: facts.organization_revision,
            session_expires_at_ms: facts.session_expires_at_ms,
            second_factor: facts.second_factor.clone(),
        })
    }

    pub fn organization(&self) -> &OrganizationId {
        &self.organization_id
    }

    pub fn person(&self) -> &PersonId {
        &self.person_id
    }

    pub fn organization_revision(&self) -> i64 {
        self.organization_revision.get()
    }

    pub fn person_revision(&self) -> i64 {
        self.person_revision.get()
    }

    pub fn permits(&self, permission: FleetPermission) -> bool {
        self.permissions.contains(&permission)
    }

    pub fn has_fresh_second_factor(&self, now_ms: i64) -> bool {
        super::admission::fresh_admin_second_factor(
            self.second_factor.as_ref().map(|factor| factor.at.0),
            now_ms,
        )
        .is_some()
    }

    pub fn is_current(&self, now_ms: i64) -> bool {
        now_ms
            < self
                .authorization_snapshot_valid_until_ms
                .0
                .min(self.session_expires_at_ms.0)
    }

    pub fn still_matches(&self, organization: &OrganizationFloor, person: &PersonFloor) -> bool {
        organization.state == OrganizationFloorState::Active
            && organization.revision.get() == self.organization_revision.get()
            && person.state == PersonFloorState::Active
            && person.revision.get() == self.person_revision.get()
    }

    pub fn is_well_formed(&self) -> bool {
        let snapshot = self.authorization_snapshot_at_ms.0;
        let snapshot_expiry = self.authorization_snapshot_valid_until_ms.0;
        snapshot >= 0
            && self.session_expires_at_ms.0 <= 9_007_199_254_740_991
            && snapshot_expiry <= 9_007_199_254_740_991
            && snapshot_expiry
                .checked_sub(snapshot)
                .is_some_and(|life| (1..=300_000).contains(&life))
            && self.session_expires_at_ms.0 > snapshot
            && self.second_factor.as_ref().is_none_or(|factor| {
                !factor.method.is_empty() && (0..=9_007_199_254_740).contains(&factor.at.0)
            })
    }
}

fn accepts_floors(
    facts: &PersonLinkFacts,
    organization_floor: &OrganizationFloor,
    person_floor: &PersonFloor,
) -> bool {
    organization_floor.state == OrganizationFloorState::Active
        && organization_floor.revision.get() == facts.organization_revision.get()
        && organization_floor.policy_digest == facts.permission_policy_digest
        && person_floor.state == PersonFloorState::Active
        && person_floor.revision.get() == facts.person_revision.get()
        && person_floor.subject_id == facts.authorization_subject_id
}
