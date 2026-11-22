use url::Url;
use workspace_ids::MachineId;

use crate::{CentralEndpointIds, EnrollmentEnd, EnrollmentId, OrganizationId, RefreshCredential};

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
pub enum InvalidDeployment {
    #[error("sign-in requires an HTTPS origin")]
    InvalidOrigin,
}

impl SignInDeployment {
    pub fn new(origin: Url) -> Result<Self, InvalidDeployment> {
        if origin.scheme() != "https" {
            return Err(InvalidDeployment::InvalidOrigin);
        }
        Self::validate_origin(origin)
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn loopback_for_test(origin: Url) -> Result<Self, InvalidDeployment> {
        let loopback = match origin.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        if origin.scheme() != "http" || !loopback {
            return Err(InvalidDeployment::InvalidOrigin);
        }
        Self::validate_origin(origin)
    }

    fn validate_origin(origin: Url) -> Result<Self, InvalidDeployment> {
        if !origin.has_host()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
        {
            return Err(InvalidDeployment::InvalidOrigin);
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

impl EnrollmentBinding {
    #[must_use]
    pub fn key(&self) -> EnrollmentKey {
        EnrollmentKey {
            organization_id: self.context.organization_id,
            machine_id: self.context.machine_id.clone(),
            enrollment_id: self.credential.enrollment_id,
        }
    }
}

/// The owner and a link caller name the enrollment independently of its current stored version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrollmentKey {
    pub organization_id: OrganizationId,
    pub machine_id: MachineId,
    pub enrollment_id: EnrollmentId,
}

/// A store exposes a whole binding or its durable ending, even after secret removal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoredEnrollment {
    Empty,
    Active(EnrollmentBinding),
    Ended {
        generation: BindingGeneration,
        why: EnrollmentEnd,
        key: EnrollmentKey,
    },
}

impl StoredEnrollment {
    #[must_use]
    pub fn key(&self) -> Option<EnrollmentKey> {
        match self {
            Self::Empty => None,
            Self::Active(binding) => Some(binding.key()),
            Self::Ended { key, .. } => Some(key.clone()),
        }
    }

    #[must_use]
    pub fn generation(&self) -> BindingGeneration {
        match self {
            Self::Empty => BindingGeneration::EMPTY,
            Self::Active(binding) => binding.generation,
            Self::Ended { generation, .. } => *generation,
        }
    }
}

/// The command receives this receipt only after sign-in's new binding has been stored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnrollmentReceipt {
    pub generation: BindingGeneration,
    pub enrollment_id: EnrollmentId,
    pub central: CentralEndpointIds,
}

#[cfg(test)]
#[path = "enrollment_binding_test.rs"]
mod tests;
