use std::time::Duration;

use serde::{Deserialize, de::IgnoredAny};
use url::Url;

use crate::{CentralAddresses, EnrollmentId, RefreshCredential, RouterClientId, SecretValue};

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
}

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
struct DiscoveryBody {
    client_id: RouterClientId,
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
    _expires_at: IgnoredAny,
    token_type: BearerTokenType,
    scope: OfflineScope,
    machine_client_id: EnrollmentId,
    fleet_central: CentralAddresses,
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
    _reason: Option<IgnoredAny>,
}

fn json_body<'body, Body: Deserialize<'body>>(
    body: &'body [u8],
) -> Result<Body, DecodeAnswerError> {
    serde_json::from_slice(body).map_err(|_| DecodeAnswerError::Malformed)
}

pub fn decode_discovery(body: &[u8]) -> Result<RouterClientId, DecodeAnswerError> {
    Ok(json_body::<DiscoveryBody>(body)?.client_id)
}

pub fn decode_device_authorization(body: &[u8]) -> Result<DeviceAuthorization, DecodeAnswerError> {
    let DeviceBody {
        device_code,
        user_code,
        verification_uri,
        verification_uri_complete,
        expires_in,
        interval,
    } = json_body(body)?;
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
        fleet_central,
    } = json_body(body)?;
    let access_token = SecretValue::new(access_token).ok_or(DecodeAnswerError::Malformed)?;
    let refresh_token = SecretValue::new(refresh_token).ok_or(DecodeAnswerError::Malformed)?;
    if expires_in == 0 {
        return Err(DecodeAnswerError::Malformed);
    }
    Ok(TokenGrant {
        credential: RefreshCredential {
            enrollment_id: machine_client_id,
            central: fleet_central,
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
    if (200..300).contains(&status) {
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
    match json_body::<RefusalBody>(body) {
        Ok(reply) => match reply.error.as_str() {
            "authorization_pending" => PollAnswer::Pending,
            "slow_down" => PollAnswer::SlowDown,
            "access_denied" => PollAnswer::Denied,
            "expired_token" => PollAnswer::Expired,
            "temporarily_unavailable" if status == 503 => PollAnswer::Unavailable { retry_after },
            error => PollAnswer::Invalid {
                reason: invalid_reason(error),
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
