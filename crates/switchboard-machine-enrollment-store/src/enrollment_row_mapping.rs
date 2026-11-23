use codex_router_secret_store::{
    enrollment_row_sealer::EnrollmentRowContext, model::SecretStoreError,
};
use switchboard_machine_core_store::{
    EnrollmentRowCorruption, StoredEnrollmentEnd, StoredEnrollmentRow,
    StoredInvalidEnrollmentReason,
};
use switchboard_machine_enrollment::{
    BindingGeneration, EnrollmentEnd, EnrollmentKey, InvalidReason, MachineId, SignInDeployment,
};

#[derive(Eq, PartialEq)]
pub(crate) enum EnrollmentSnapshot {
    Empty,
    Active {
        key: EnrollmentKey,
        deployment: SignInDeployment,
        generation: BindingGeneration,
        sealed: Vec<u8>,
    },
    Ended {
        key: EnrollmentKey,
        generation: BindingGeneration,
        why: EnrollmentEnd,
    },
}

impl EnrollmentSnapshot {
    pub(crate) fn from_row(
        row: StoredEnrollmentRow,
        machine_id: &MachineId,
    ) -> Result<Self, EnrollmentRowCorruption> {
        match row {
            StoredEnrollmentRow::Empty => Ok(Self::Empty),
            StoredEnrollmentRow::Active {
                generation,
                organization_id,
                deployment,
                enrollment_id,
                sealed,
            } => {
                let organization_id = organization_id
                    .parse()
                    .map_err(|_| EnrollmentRowCorruption::EnrollmentId)?;
                let saved_deployment = deployment;
                let deployment = SignInDeployment::new(
                    saved_deployment
                        .parse()
                        .map_err(|_| EnrollmentRowCorruption::Deployment)?,
                )
                .map_err(|_| EnrollmentRowCorruption::Deployment)?;
                if deployment.origin().as_str() != saved_deployment {
                    return Err(EnrollmentRowCorruption::Deployment);
                }
                let enrollment_id = enrollment_id
                    .parse()
                    .map_err(|_| EnrollmentRowCorruption::EnrollmentId)?;
                let key = EnrollmentKey {
                    machine_id: machine_id.clone(),
                    organization_id,
                    enrollment_id,
                };
                Ok(Self::Active {
                    key,
                    deployment,
                    generation: binding_generation(generation)?,
                    sealed,
                })
            }
            StoredEnrollmentRow::Ended {
                generation,
                organization_id,
                enrollment_id,
                why,
            } => Ok(Self::Ended {
                key: EnrollmentKey {
                    machine_id: machine_id.clone(),
                    organization_id: organization_id
                        .parse()
                        .map_err(|_| EnrollmentRowCorruption::EnrollmentId)?,
                    enrollment_id: enrollment_id
                        .parse()
                        .map_err(|_| EnrollmentRowCorruption::EnrollmentId)?,
                },
                generation: binding_generation(generation)?,
                why: enrollment_end(why),
            }),
        }
    }

    pub(crate) fn generation(&self) -> BindingGeneration {
        match self {
            Self::Empty => BindingGeneration::EMPTY,
            Self::Active { generation, .. } | Self::Ended { generation, .. } => *generation,
        }
    }
}

fn binding_generation(generation: i64) -> Result<BindingGeneration, EnrollmentRowCorruption> {
    u64::try_from(generation)
        .ok()
        .and_then(BindingGeneration::from_persisted)
        .ok_or(EnrollmentRowCorruption::Generation)
}

pub(crate) fn seal_context(
    key: &EnrollmentKey,
    deployment: &SignInDeployment,
    generation: BindingGeneration,
) -> Result<EnrollmentRowContext, SecretStoreError> {
    EnrollmentRowContext::new(
        key.machine_id.as_str(),
        &key.organization_id.to_string(),
        &key.enrollment_id.to_string(),
        deployment.origin().as_str(),
        generation.get(),
    )
}

pub(crate) const fn stored_end(why: EnrollmentEnd) -> StoredEnrollmentEnd {
    match why {
        EnrollmentEnd::Denied => StoredEnrollmentEnd::Denied {},
        EnrollmentEnd::Expired => StoredEnrollmentEnd::Expired {},
        EnrollmentEnd::Invalid(reason) => StoredEnrollmentEnd::Invalid {
            reason: stored_invalid_reason(reason),
        },
        EnrollmentEnd::RefreshRefused => StoredEnrollmentEnd::RefreshRefused {},
        EnrollmentEnd::RefreshClientDisabled => StoredEnrollmentEnd::RefreshClientDisabled {},
        EnrollmentEnd::EndedByCentral => StoredEnrollmentEnd::EndedByCentral {},
        EnrollmentEnd::LocalCredentialLost => StoredEnrollmentEnd::LocalCredentialLost {},
        EnrollmentEnd::Revoked => StoredEnrollmentEnd::Revoked {},
        EnrollmentEnd::Inactive => StoredEnrollmentEnd::Inactive {},
    }
}

const fn enrollment_end(why: StoredEnrollmentEnd) -> EnrollmentEnd {
    match why {
        StoredEnrollmentEnd::Denied {} => EnrollmentEnd::Denied,
        StoredEnrollmentEnd::Expired {} => EnrollmentEnd::Expired,
        StoredEnrollmentEnd::Invalid { reason } => EnrollmentEnd::Invalid(invalid_reason(reason)),
        StoredEnrollmentEnd::RefreshRefused {} => EnrollmentEnd::RefreshRefused,
        StoredEnrollmentEnd::RefreshClientDisabled {} => EnrollmentEnd::RefreshClientDisabled,
        StoredEnrollmentEnd::EndedByCentral {} => EnrollmentEnd::EndedByCentral,
        StoredEnrollmentEnd::LocalCredentialLost {} => EnrollmentEnd::LocalCredentialLost,
        StoredEnrollmentEnd::Revoked {} => EnrollmentEnd::Revoked,
        StoredEnrollmentEnd::Inactive {} => EnrollmentEnd::Inactive,
    }
}

const fn stored_invalid_reason(reason: InvalidReason) -> StoredInvalidEnrollmentReason {
    match reason {
        InvalidReason::GrantRefused => StoredInvalidEnrollmentReason::GrantRefused,
        InvalidReason::InvalidClient => StoredInvalidEnrollmentReason::InvalidClient,
        InvalidReason::InvalidRequest => StoredInvalidEnrollmentReason::InvalidRequest,
        InvalidReason::UnknownError => StoredInvalidEnrollmentReason::UnknownError,
        InvalidReason::MalformedAnswer => StoredInvalidEnrollmentReason::MalformedAnswer,
        InvalidReason::UnexpectedStatus => StoredInvalidEnrollmentReason::UnexpectedStatus,
        InvalidReason::WrongEnrollment => StoredInvalidEnrollmentReason::WrongEnrollment,
        InvalidReason::OversizedAnswer => StoredInvalidEnrollmentReason::OversizedAnswer,
    }
}

const fn invalid_reason(reason: StoredInvalidEnrollmentReason) -> InvalidReason {
    match reason {
        StoredInvalidEnrollmentReason::GrantRefused => InvalidReason::GrantRefused,
        StoredInvalidEnrollmentReason::InvalidClient => InvalidReason::InvalidClient,
        StoredInvalidEnrollmentReason::InvalidRequest => InvalidReason::InvalidRequest,
        StoredInvalidEnrollmentReason::UnknownError => InvalidReason::UnknownError,
        StoredInvalidEnrollmentReason::MalformedAnswer => InvalidReason::MalformedAnswer,
        StoredInvalidEnrollmentReason::UnexpectedStatus => InvalidReason::UnexpectedStatus,
        StoredInvalidEnrollmentReason::WrongEnrollment => InvalidReason::WrongEnrollment,
        StoredInvalidEnrollmentReason::OversizedAnswer => InvalidReason::OversizedAnswer,
    }
}
