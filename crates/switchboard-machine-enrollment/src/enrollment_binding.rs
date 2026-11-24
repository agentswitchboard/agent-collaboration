use url::Url;
use workspace_ids::MachineId;

use crate::{EnrollmentEnd, EnrollmentId, OrganizationId, RefreshCredential};

/// The store starts at zero and advances this version on every binding transition.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct BindingGeneration(u64);

impl BindingGeneration {
    pub const EMPTY: Self = Self(0);
    const MAX: Self = Self(i64::MAX as u64);

    #[must_use]
    pub const fn from_persisted(generation: u64) -> Option<Self> {
        if generation <= Self::MAX.0 {
            Some(Self(generation))
        } else {
            None
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn successor(self) -> Option<Self> {
        Self::from_persisted(self.0 + 1)
    }

    /// A live binding leaves a generation available for its durable ending.
    #[must_use]
    pub fn binding_successor(self) -> Option<Self> {
        self.successor().filter(|next| *next < Self::MAX)
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

impl EnrollmentContext {
    /// Whether a binding can be replaced within its existing sign-in scope.
    #[must_use]
    pub fn matches_scope(
        &self,
        organization: OrganizationId,
        deployment: &SignInDeployment,
    ) -> bool {
        self.organization_id == organization && &self.deployment == deployment
    }
}

/// Enrollment and renewal assemble this complete binding for one atomic store transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrollmentBinding {
    pub generation: BindingGeneration,
    pub context: EnrollmentContext,
    pub credential: RefreshCredential,
}

impl EnrollmentBinding {
    pub(crate) fn matches_replacement(&self, stored: &EnrollmentBinding) -> bool {
        stored.generation == self.generation
            && stored.key() == self.key()
            && stored.credential == self.credential
    }

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
        deployment: SignInDeployment,
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

/// Enrollment state available without accessing a refresh credential.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnrollmentMetadata {
    Empty,
    Active {
        key: EnrollmentKey,
        deployment: SignInDeployment,
        generation: BindingGeneration,
    },
    Ended {
        key: EnrollmentKey,
        why: EnrollmentEnd,
        generation: BindingGeneration,
    },
}

impl From<StoredEnrollment> for EnrollmentMetadata {
    fn from(stored: StoredEnrollment) -> Self {
        match stored {
            StoredEnrollment::Empty => Self::Empty,
            StoredEnrollment::Active(binding) => Self::Active {
                key: binding.key(),
                deployment: binding.context.deployment,
                generation: binding.generation,
            },
            StoredEnrollment::Ended {
                key,
                why,
                generation,
                deployment: _,
            } => Self::Ended {
                key,
                why,
                generation,
            },
        }
    }
}

/// The command receives this receipt only after sign-in's new binding has been stored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnrollmentReceipt {
    pub generation: BindingGeneration,
    pub enrollment_id: EnrollmentId,
}

#[cfg(test)]
#[path = "enrollment_binding_test.rs"]
mod tests;
