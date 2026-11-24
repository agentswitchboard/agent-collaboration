//! Durable enrollment format owned by the machine core store.

use serde::{Deserialize, Serialize};
use uuid::{Uuid, Variant};

const SEALED_FORMAT_VERSION: u8 = 1;
const MINIMUM_SEALED_LENGTH: usize = 1 + 12 + 16 + 1;

/// A structurally valid enrollment snapshot. Active ciphertext is opaque to this store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoredEnrollmentRow {
    Empty,
    Active {
        generation: i64,
        organization_id: String,
        deployment: String,
        enrollment_id: String,
        sealed: Vec<u8>,
    },
    Ended {
        generation: i64,
        organization_id: String,
        deployment: String,
        enrollment_id: String,
        why: StoredEnrollmentEnd,
    },
}

impl StoredEnrollmentRow {
    #[must_use]
    pub const fn generation(&self) -> i64 {
        match self {
            Self::Empty => 0,
            Self::Active { generation, .. } | Self::Ended { generation, .. } => *generation,
        }
    }

    fn validate(&self) -> Result<(), EnrollmentRowCorruption> {
        match self {
            Self::Empty => Ok(()),
            Self::Active {
                generation,
                organization_id,
                enrollment_id,
                sealed,
                ..
            } => {
                if !(1..i64::MAX).contains(generation) {
                    return Err(EnrollmentRowCorruption::Generation);
                }
                validate_ids(organization_id, enrollment_id)?;
                if sealed.len() < MINIMUM_SEALED_LENGTH
                    || sealed.first() != Some(&SEALED_FORMAT_VERSION)
                {
                    return Err(EnrollmentRowCorruption::SealedFormat);
                }
                Ok(())
            }
            Self::Ended {
                generation,
                organization_id,
                enrollment_id,
                ..
            } => {
                if *generation <= 0 {
                    return Err(EnrollmentRowCorruption::Generation);
                }
                validate_ids(organization_id, enrollment_id)
            }
        }
    }

    pub(crate) fn to_columns(&self) -> Result<EnrollmentColumns<'_>, EnrollmentRowCorruption> {
        self.validate()?;
        let (metadata, sealed) = match self {
            Self::Empty => return Ok((None, None)),
            Self::Active {
                organization_id,
                deployment,
                enrollment_id,
                sealed,
                ..
            } => (
                EnrollmentMetadata::Active {
                    organization_id: organization_id.as_str().into(),
                    deployment: deployment.as_str().into(),
                    enrollment_id: enrollment_id.as_str().into(),
                },
                Some(sealed.as_slice()),
            ),
            Self::Ended {
                organization_id,
                deployment,
                enrollment_id,
                why,
                ..
            } => (
                EnrollmentMetadata::Ended {
                    organization_id: organization_id.as_str().into(),
                    deployment: deployment.as_str().into(),
                    enrollment_id: enrollment_id.as_str().into(),
                    why: *why,
                },
                None,
            ),
        };
        let encoded =
            serde_json::to_string(&metadata).map_err(|_| EnrollmentRowCorruption::Json)?;
        Ok((Some(encoded), sealed))
    }

    pub(crate) fn from_columns(
        generation: i64,
        json: Option<&[u8]>,
        sealed: Option<Vec<u8>>,
    ) -> Result<Self, EnrollmentRowCorruption> {
        let metadata = StoredEnrollmentRowMetadata::from_columns(generation, json)?;
        match (metadata, sealed) {
            (StoredEnrollmentRowMetadata::Empty, None) => Ok(Self::Empty),
            (
                StoredEnrollmentRowMetadata::Active {
                    organization_id,
                    deployment,
                    enrollment_id,
                    ..
                },
                Some(sealed),
            ) => {
                if sealed.len() < MINIMUM_SEALED_LENGTH
                    || sealed.first() != Some(&SEALED_FORMAT_VERSION)
                {
                    return Err(EnrollmentRowCorruption::SealedFormat);
                }
                Ok(Self::Active {
                    generation,
                    organization_id,
                    deployment,
                    enrollment_id,
                    sealed,
                })
            }
            (
                StoredEnrollmentRowMetadata::Ended {
                    organization_id,
                    deployment,
                    enrollment_id,
                    why,
                    ..
                },
                None,
            ) => Ok(Self::Ended {
                generation,
                organization_id,
                deployment,
                enrollment_id,
                why,
            }),
            _ => Err(EnrollmentRowCorruption::Shape),
        }
    }
}

/// Decoded enrollment row state, independent of the credential column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoredEnrollmentRowMetadata {
    Empty,
    Active {
        generation: i64,
        organization_id: String,
        deployment: String,
        enrollment_id: String,
    },
    Ended {
        generation: i64,
        organization_id: String,
        deployment: String,
        enrollment_id: String,
        why: StoredEnrollmentEnd,
    },
}

impl StoredEnrollmentRowMetadata {
    #[must_use]
    pub const fn generation(&self) -> i64 {
        match self {
            Self::Empty => 0,
            Self::Active { generation, .. } | Self::Ended { generation, .. } => *generation,
        }
    }

    pub(crate) fn from_columns(
        generation: i64,
        json: Option<&[u8]>,
    ) -> Result<Self, EnrollmentRowCorruption> {
        if generation < 0 {
            return Err(EnrollmentRowCorruption::Generation);
        }
        if generation == 0 {
            return if json.is_none() {
                Ok(Self::Empty)
            } else {
                Err(EnrollmentRowCorruption::Shape)
            };
        }
        let decoded: EnrollmentMetadata<'_> =
            serde_json::from_slice(json.ok_or(EnrollmentRowCorruption::Shape)?)
                .map_err(|_| EnrollmentRowCorruption::Json)?;
        match decoded {
            EnrollmentMetadata::Active {
                organization_id,
                deployment,
                enrollment_id,
            } => {
                if generation == i64::MAX {
                    return Err(EnrollmentRowCorruption::Generation);
                }
                validate_ids(&organization_id, &enrollment_id)?;
                Ok(Self::Active {
                    generation,
                    organization_id: organization_id.into_owned(),
                    deployment: deployment.into_owned(),
                    enrollment_id: enrollment_id.into_owned(),
                })
            }
            EnrollmentMetadata::Ended {
                organization_id,
                deployment,
                enrollment_id,
                why,
            } => {
                validate_ids(&organization_id, &enrollment_id)?;
                Ok(Self::Ended {
                    generation,
                    organization_id: organization_id.into_owned(),
                    deployment: deployment.into_owned(),
                    enrollment_id: enrollment_id.into_owned(),
                    why,
                })
            }
        }
    }
}

type EnrollmentColumns<'row> = (Option<String>, Option<&'row [u8]>);

fn validate_ids(organization_id: &str, enrollment_id: &str) -> Result<(), EnrollmentRowCorruption> {
    for id in [organization_id, enrollment_id] {
        let parsed = Uuid::parse_str(id).map_err(|_| EnrollmentRowCorruption::EnrollmentId)?;
        if parsed.get_version_num() != 7
            || parsed.get_variant() != Variant::RFC4122
            || parsed.hyphenated().to_string() != id
        {
            return Err(EnrollmentRowCorruption::EnrollmentId);
        }
    }
    Ok(())
}

#[derive(Deserialize, Serialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum EnrollmentMetadata<'row> {
    Active {
        organization_id: std::borrow::Cow<'row, str>,
        deployment: std::borrow::Cow<'row, str>,
        enrollment_id: std::borrow::Cow<'row, str>,
    },
    Ended {
        organization_id: std::borrow::Cow<'row, str>,
        deployment: std::borrow::Cow<'row, str>,
        enrollment_id: std::borrow::Cow<'row, str>,
        why: StoredEnrollmentEnd,
    },
}

/// Closed set of ending reasons saved by the core store.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum StoredEnrollmentEnd {
    Denied {},
    Expired {},
    Invalid {
        reason: StoredInvalidEnrollmentReason,
    },
    RefreshRefused {},
    RefreshClientDisabled {},
    EndedByCentral {},
    LocalCredentialLost {},
    Revoked {},
    Inactive {},
}

/// Closed set of reasons attached to an invalid sign-in outcome.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum StoredInvalidEnrollmentReason {
    GrantRefused,
    InvalidClient,
    InvalidRequest,
    UnknownError,
    MalformedAnswer,
    UnexpectedStatus,
    WrongEnrollment,
    OversizedAnswer,
}

/// Stored row damage without including the rejected metadata or ciphertext.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnrollmentRowCorruption {
    #[error("machine identity row is missing")]
    MissingMachine,
    #[error("machine identity table contains multiple rows")]
    MultipleMachines,
    #[error("machine identity is invalid")]
    MachineId,
    #[error("machine identity belongs to another device")]
    WrongMachine,
    #[error("enrollment columns have invalid SQLite storage types")]
    StorageType,
    #[error("enrollment generation is invalid")]
    Generation,
    #[error("enrollment columns do not form a valid row")]
    Shape,
    #[error("enrollment metadata is invalid JSON")]
    Json,
    #[error("enrollment metadata contains an invalid identifier")]
    EnrollmentId,
    #[error("enrollment metadata contains an invalid deployment")]
    Deployment,
    #[error("enrollment ciphertext has an invalid structure")]
    SealedFormat,
}
