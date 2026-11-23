use std::time::Duration;

use serde::Deserialize;
use url::Url;

use crate::{DecodeAnswerError, EnrollmentId, InvalidReason, RefreshCredential, SecretValue};

/// Sign-in supplies these two human-facing values when a device attempt starts.
#[derive(Debug)]
pub struct DeviceInstructions {
    pub user_code: String,
    pub verification_link: Url,
}

/// The sign-in decoder assembles a device attempt, including its secret proof and lifetime.
#[derive(Debug)]
pub struct DeviceAuthorization {
    pub instructions: DeviceInstructions,
    pub device_code: SecretValue,
    pub expires_in: Duration,
    pub interval: Duration,
}

/// Sign-in issues one token grant. The store receives only the refresh credential.
#[derive(Debug)]
pub struct TokenGrant {
    pub credential: RefreshCredential,
    pub access_token: SecretValue,
    pub expires_in: Duration,
}

/// The sign-in decoder produces one of these outcomes for a device-token poll.
#[derive(Debug)]
pub enum PollAnswer {
    Pending,
    SlowDown,
    Denied,
    Expired,
    Invalid { reason: InvalidReason },
    Unavailable { retry_after: Option<Duration> },
    Enrolled(TokenGrant),
}

/// The sign-in decoder produces one of these outcomes for a credential refresh.
#[derive(Debug)]
pub enum RefreshAnswer {
    Rotated(TokenGrant),
    OversizedReply,
    Refused { reason: InvalidReason },
    Unavailable { retry_after: Option<Duration> },
}

/// A discovery or device-start call supplies no success value on these outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SignInFailure {
    #[error("sign-in refused the enrollment request")]
    Invalid { reason: InvalidReason },
    #[error("sign-in is temporarily unavailable")]
    Unavailable { retry_after: Option<Duration> },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceBody {
    device_code: String,
    user_code: String,
    verification_uri: Url,
    verification_uri_complete: Url,
    expires_in: u64,
    interval: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenBody {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
    #[serde(rename = "expires_at")]
    _expires_at: crate::response_object::DiscardTimestamp,
    token_type: BearerTokenType,
    scope: OfflineScope,
    machine_client_id: EnrollmentId,
}

#[derive(Deserialize)]
enum BearerTokenType {
    Bearer,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum OfflineScope {
    OfflineAccess,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefusalBody {
    error: String,
    #[serde(default, rename = "reason")]
    _reason: crate::response_object::DiscardText,
}

pub fn decode_device_authorization(body: &[u8]) -> Result<DeviceAuthorization, DecodeAnswerError> {
    let DeviceBody {
        device_code,
        user_code,
        verification_uri,
        verification_uri_complete,
        expires_in,
        interval,
    } = crate::response_object::from_json(body)?;
    let device_code = SecretValue::new(device_code).ok_or(DecodeAnswerError::Malformed)?;
    let secret = device_code.expose_secret();
    let mut complete = verification_uri_complete.clone();
    complete.set_query(None);
    let mut params = verification_uri_complete.query_pairs();
    if expires_in == 0
        || interval == 0
        || user_code.is_empty()
        || user_code.chars().any(char::is_control)
        || user_code.contains(secret)
        || verification_uri.path() != "/device"
        || verification_uri.fragment().is_some()
        || !verification_uri.username().is_empty()
        || verification_uri.password().is_some()
        || !matches!(verification_uri.scheme(), "http" | "https")
        || verification_uri != complete
        || verification_uri_complete.as_str().contains(secret)
        || !matches!(params.next(), Some((key, code)) if key == "user_code" && code == user_code)
        || params.next().is_some()
    {
        return Err(DecodeAnswerError::Malformed);
    }
    Ok(DeviceAuthorization {
        instructions: DeviceInstructions {
            user_code,
            verification_link: verification_uri_complete,
        },
        device_code,
        expires_in: Duration::from_secs(expires_in),
        interval: Duration::from_secs(interval),
    })
}

fn decode_grant(body: &[u8]) -> Result<TokenGrant, DecodeAnswerError> {
    let TokenBody {
        access_token,
        refresh_token,
        expires_in,
        _expires_at: _,
        token_type: BearerTokenType::Bearer,
        scope: OfflineScope::OfflineAccess,
        machine_client_id,
    } = crate::response_object::from_json(body)?;
    let access_token = SecretValue::new(access_token).ok_or(DecodeAnswerError::Malformed)?;
    let refresh_token = SecretValue::new(refresh_token).ok_or(DecodeAnswerError::Malformed)?;
    if !(1..=3_600).contains(&expires_in) {
        return Err(DecodeAnswerError::Malformed);
    }
    Ok(TokenGrant {
        credential: RefreshCredential {
            enrollment_id: machine_client_id,
            refresh_token,
        },
        access_token,
        expires_in: Duration::from_secs(expires_in),
    })
}

fn invalid_reason(error: &str) -> InvalidReason {
    match error {
        "invalid_grant" => InvalidReason::GrantRefused,
        "invalid_client" => InvalidReason::InvalidClient,
        "invalid_request" => InvalidReason::InvalidRequest,
        _ => InvalidReason::UnknownError,
    }
}

pub fn decode_poll(status: u16, retry_after: Option<Duration>, body: &[u8]) -> PollAnswer {
    if status == 200 {
        return match decode_grant(body) {
            Ok(grant) => PollAnswer::Enrolled(grant),
            Err(_) => PollAnswer::Invalid {
                reason: InvalidReason::MalformedAnswer,
            },
        };
    }
    if status == 429 {
        return PollAnswer::Unavailable { retry_after };
    }
    if !matches!(status, 400 | 503) {
        return PollAnswer::Invalid {
            reason: InvalidReason::UnexpectedStatus,
        };
    }
    match crate::response_object::from_json::<RefusalBody>(body) {
        Ok(reply) => match (status, reply.error.as_str()) {
            (400, "authorization_pending") => PollAnswer::Pending,
            (400, "slow_down") => PollAnswer::SlowDown,
            (400, "access_denied") => PollAnswer::Denied,
            (400, "expired_token") => PollAnswer::Expired,
            (503, "temporarily_unavailable") => PollAnswer::Unavailable { retry_after },
            (400, error) => PollAnswer::Invalid {
                reason: invalid_reason(error),
            },
            _ => PollAnswer::Invalid {
                reason: InvalidReason::UnexpectedStatus,
            },
        },
        Err(_) => PollAnswer::Invalid {
            reason: InvalidReason::MalformedAnswer,
        },
    }
}

pub fn decode_refresh(status: u16, retry_after: Option<Duration>, body: &[u8]) -> RefreshAnswer {
    match decode_poll(status, retry_after, body) {
        PollAnswer::Enrolled(grant) => RefreshAnswer::Rotated(grant),
        PollAnswer::Unavailable { retry_after } => RefreshAnswer::Unavailable { retry_after },
        PollAnswer::Invalid { reason } => RefreshAnswer::Refused { reason },
        PollAnswer::Pending | PollAnswer::SlowDown | PollAnswer::Denied | PollAnswer::Expired => {
            RefreshAnswer::Refused {
                reason: InvalidReason::UnexpectedStatus,
            }
        }
    }
}

pub fn decode_start_failure(
    status: u16,
    retry_after: Option<Duration>,
    body: &[u8],
) -> SignInFailure {
    match decode_poll(status, retry_after, body) {
        PollAnswer::Unavailable { retry_after } => SignInFailure::Unavailable { retry_after },
        PollAnswer::Invalid { reason } => SignInFailure::Invalid { reason },
        _ => SignInFailure::Invalid {
            reason: InvalidReason::UnexpectedStatus,
        },
    }
}

#[cfg(test)]
#[path = "sign_in_answers_test.rs"]
mod tests;
