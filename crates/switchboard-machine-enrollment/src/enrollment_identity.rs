use std::str::FromStr;

use serde::Deserialize;
use uuid::{Uuid, Variant};

/// An enrollment identifier parser refuses noncanonical or non-v7 input without echoing it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnrollmentIdentifierError {
    #[error("{field} must be a canonical lowercase RFC UUIDv7")]
    Invalid { field: &'static str },
}

macro_rules! enrollment_identifier {
    ($(#[$document:meta])* $name:ident) => {
        $(#[$document])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
        #[serde(try_from = "String")]
        pub struct $name(Uuid);

        impl FromStr for $name {
            type Err = EnrollmentIdentifierError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                let invalid = || EnrollmentIdentifierError::Invalid { field: stringify!($name) };
                let uuid = Uuid::try_parse(input).map_err(|_| invalid())?;
                if uuid.get_version_num() != 7
                    || uuid.get_variant() != Variant::RFC4122
                    || uuid.hyphenated().to_string() != input
                {
                    return Err(invalid());
                }
                Ok(Self(uuid))
            }
        }

        impl TryFrom<String> for $name {
            type Error = EnrollmentIdentifierError;

            fn try_from(input: String) -> Result<Self, Self::Error> {
                input.parse()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

enrollment_identifier!(
    /// The public device-login client configured and returned by sign-in discovery.
    RouterClientId
);
enrollment_identifier!(
    /// The organization in sign-in that the person supplies when enrolling a Mac.
    OrganizationId
);
enrollment_identifier!(
    /// The enrollment client id sign-in issues for this machine's refreshes.
    EnrollmentId
);
enrollment_identifier!(
    /// A link endpoint id configured and returned by sign-in.
    CentralEndpointId
);

/// The two central endpoint ids sign-in supplies in a successful token response.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CentralEndpointIds {
    pub receiver: CentralEndpointId,
    pub relay: CentralEndpointId,
}
