use fleet_identity_protocol::{EnrollmentId, OrganizationId};
use fleet_machine_login::link_authority::ValidatedMachineConnection;
use workspace_ids::{CentralSessionId, MachineId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedSession {
    pub session_id: CentralSessionId,
    pub machine_id: MachineId,
    pub enrollment_id: EnrollmentId,
}

/// A live session whose presented secret matched its authenticated machine's enrollment.
///
/// ```compile_fail,E0451
/// use fleet_actor_identity::{AuthenticatedSession, ScopedSession};
/// use fleet_identity_protocol::OrganizationId;
/// fn forge(organization_id: OrganizationId, session: ScopedSession) -> AuthenticatedSession {
///     AuthenticatedSession { organization_id, session }
/// }
/// ```
///
/// ```compile_fail,E0277
/// fn require_wire_input<Value: serde::de::DeserializeOwned>() {}
/// require_wire_input::<fleet_actor_identity::AuthenticatedSession>();
/// ```
#[derive(Clone)]
pub struct AuthenticatedSession {
    organization_id: OrganizationId,
    session: ScopedSession,
}

impl AuthenticatedSession {
    pub(crate) fn from_checked_connection(
        machine: &ValidatedMachineConnection,
        session_id: CentralSessionId,
    ) -> Self {
        Self {
            organization_id: machine.organization_id().clone(),
            session: ScopedSession {
                session_id,
                machine_id: machine.machine_id().clone(),
                enrollment_id: machine.enrollment_id().clone(),
            },
        }
    }

    pub fn organization_id(&self) -> &OrganizationId {
        &self.organization_id
    }

    pub fn scoped_session(&self) -> &ScopedSession {
        &self.session
    }
}
