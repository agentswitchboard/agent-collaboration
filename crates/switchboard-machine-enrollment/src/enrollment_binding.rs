use url::{Host, Url};
use workspace_ids::MachineId;

use crate::{EnrollmentEnd, OrganizationId, RefreshCredential};

/// The store starts at zero and advances this version on every binding transition.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct BindingGeneration(u64);

impl BindingGeneration {
    pub const EMPTY: Self = Self(0);

    #[must_use]
    pub const fn from_persisted(generation: u64) -> Self {
        Self(generation)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn successor(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

/// Composition validates a trusted sign-in origin before building the client or a binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SignInDeployment(Url);

/// The deployment parser rejects an invalid origin without echoing its input.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("sign-in requires an HTTPS origin")]
pub struct InvalidDeployment;

impl SignInDeployment {
    pub fn new(origin: Url) -> Result<Self, InvalidDeployment> {
        let loopback = match origin.host() {
            Some(Host::Ipv4(ip)) => ip.is_loopback(),
            Some(Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        // Scripted sign-in can use HTTP on a numeric loopback address.
        if !(origin.scheme() == "https" || origin.scheme() == "http" && loopback)
            || !origin.has_host()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
        {
            return Err(InvalidDeployment);
        }
        Ok(Self(origin))
    }

    #[must_use]
    pub fn origin(&self) -> &Url {
        &self.0
    }
}

/// Composition supplies the machine, organization and sign-in deployment for its enrollment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrollmentContext {
    pub organization_id: OrganizationId,
    pub deployment: SignInDeployment,
    pub machine_id: MachineId,
}

/// Enrollment and renewal assemble this complete binding for one atomic store transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrollmentBinding {
    pub generation: BindingGeneration,
    pub context: EnrollmentContext,
    pub credential: RefreshCredential,
}

/// A store exposes a whole binding or its durable ending, even after secret removal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoredEnrollment {
    Empty,
    Active(EnrollmentBinding),
    Ended {
        generation: BindingGeneration,
        why: EnrollmentEnd,
    },
}

impl StoredEnrollment {
    #[must_use]
    pub fn generation(&self) -> BindingGeneration {
        match self {
            Self::Empty => BindingGeneration::EMPTY,
            Self::Active(binding) => binding.generation,
            Self::Ended { generation, .. } => *generation,
        }
    }
}

#[cfg(test)]
#[path = "enrollment_binding_test.rs"]
mod tests;
