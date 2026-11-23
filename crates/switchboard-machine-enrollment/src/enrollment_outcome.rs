/// The sign-in decoder reports invalid replies without preserving untrusted diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DecodeAnswerError {
    #[error("sign-in returned an invalid answer")]
    Malformed,
}

/// The sign-in decoder classifies the refusal codes that callers can act on safely.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidReason {
    GrantRefused,
    InvalidClient,
    InvalidRequest,
    UnknownError,
    MalformedAnswer,
    UnexpectedStatus,
    WrongEnrollment,
    OversizedAnswer,
}

/// The enrollment reducer or refresh owner reports one of these final reasons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnrollmentEnd {
    Denied,
    Expired,
    Invalid(InvalidReason),
    RefreshRefused,
    RefreshClientDisabled,
    LocalCredentialLost,
    Revoked,
    Inactive,
}
