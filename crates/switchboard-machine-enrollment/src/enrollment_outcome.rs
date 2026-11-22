/// The sign-in decoder classifies the refusal codes that callers can act on safely.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidReason {
    GrantRefused,
    InvalidClient,
    InvalidRequest,
    UnknownError,
    MalformedAnswer,
    UnexpectedStatus,
}

/// The enrollment reducer or refresh owner reports one of these final reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnrollmentEnd {
    Denied,
    Expired,
    Invalid(InvalidReason),
    RefreshRefused,
    LocalCredentialLost,
    Revoked,
    Inactive,
}
