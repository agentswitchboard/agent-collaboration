use std::time::Duration;

use super::{
    DecodeAnswerError, InvalidReason, PollAnswer, RefreshAnswer, SignInFailure,
    decode_device_authorization, decode_discovery, decode_poll, decode_refresh,
    decode_start_failure,
};

const DISCOVERY: &[u8] = br#"{"client_id":"019a0000-0000-7000-8000-000000000001"}"#;
const DEVICE: &[u8] = br#"{
  "device_code":"private-device-code",
  "user_code":"MACH-TEST",
  "verification_uri":"https://signin.example/device",
  "verification_uri_complete":"https://signin.example/device?user_code=MACH-TEST",
  "expires_in":600,
  "interval":5
}"#;
const GRANT: &str = r#"{
  "access_token":"opaque-access-token-not-jwt",
  "refresh_token":"private-refresh-token",
  "expires_in":3600,
  "expires_at":1795382400,
  "token_type":"Bearer",
  "scope":"offline_access",
  "machine_client_id":"019a0000-0000-7000-8000-000000000002",
  "fleet_central":{
    "receiver":"019a0000-0000-7000-8000-000000000003",
    "relay":"019a0000-0000-7000-8000-000000000004"
  }
}"#;

#[test]
fn discovery_and_device_start_read_only_the_contract_fields() {
    assert_eq!(
        decode_discovery(DISCOVERY).map(|id| id.to_string()),
        Ok("019a0000-0000-7000-8000-000000000001".to_owned())
    );
    let device = decode_device_authorization(DEVICE).expect("valid sign-in response");
    assert_eq!(device.instructions.user_code, "MACH-TEST");
    assert_eq!(
        device.instructions.verification_link.as_str(),
        "https://signin.example/device?user_code=MACH-TEST"
    );
    assert_eq!(
        (device.expires_in, device.interval),
        (Duration::from_secs(600), Duration::from_secs(5))
    );
    let leaked = br#"{
      "device_code":"private-device-code",
      "user_code":"private-device-code",
      "verification_uri":"https://signin.example/device",
      "verification_uri_complete":"https://signin.example/device?user_code=private-device-code",
      "expires_in":600,
      "interval":5
    }"#;
    assert!(matches!(
        decode_device_authorization(leaked),
        Err(DecodeAnswerError::Malformed)
    ));
}

#[test]
fn poll_decodes_every_answer_and_rejects_unknown_error() {
    assert!(matches!(
        decode_poll(400, None, br#"{"error":"authorization_pending"}"#),
        PollAnswer::Pending
    ));
    assert!(matches!(
        decode_poll(400, None, br#"{"error":"slow_down"}"#),
        PollAnswer::SlowDown
    ));
    assert!(matches!(
        decode_poll(400, None, br#"{"error":"access_denied"}"#),
        PollAnswer::Denied
    ));
    assert!(matches!(
        decode_poll(400, None, br#"{"error":"expired_token"}"#),
        PollAnswer::Expired
    ));
    for (body, expected) in [
        (
            r#"{"error":"invalid_grant","reason":"spent"}"#,
            InvalidReason::GrantRefused,
        ),
        (
            r#"{"error":"invalid_client"}"#,
            InvalidReason::InvalidClient,
        ),
        (
            r#"{"error":"invalid_request"}"#,
            InvalidReason::InvalidRequest,
        ),
        (r#"{"error":"new_error"}"#, InvalidReason::UnknownError),
    ] {
        let PollAnswer::Invalid { reason } = decode_poll(400, None, body.as_bytes()) else {
            panic!("expected invalid for {body}");
        };
        assert_eq!(reason, expected);
    }
    let retry = Duration::from_secs(11);
    assert!(matches!(
        decode_poll(503, Some(retry), br#"{"error":"temporarily_unavailable"}"#),
        PollAnswer::Unavailable { retry_after: Some(delay) } if delay == retry
    ));
    assert!(matches!(
        decode_poll(429, Some(retry), b""),
        PollAnswer::Unavailable { retry_after: Some(delay) } if delay == retry
    ));
}

#[test]
fn token_grant_trusts_the_body_and_keeps_it_closed() {
    let PollAnswer::Enrolled(grant) = decode_poll(200, None, GRANT.as_bytes()) else {
        panic!("expected a valid grant");
    };
    assert_eq!(
        grant.access_token.expose_secret(),
        "opaque-access-token-not-jwt"
    );
    assert_eq!(
        grant.credential.refresh_token.expose_secret(),
        "private-refresh-token"
    );
    assert_eq!(
        grant.credential.enrollment_id.to_string(),
        "019a0000-0000-7000-8000-000000000002"
    );
    assert_eq!(
        grant.credential.central.receiver.to_string(),
        "019a0000-0000-7000-8000-000000000003"
    );
    assert_eq!(
        grant.credential.central.relay.to_string(),
        "019a0000-0000-7000-8000-000000000004"
    );

    let extra = GRANT.replacen('{', r#"{"extra":"unexpected","#, 1);
    assert!(matches!(
        decode_poll(200, None, extra.as_bytes()),
        PollAnswer::Invalid {
            reason: InvalidReason::MalformedAnswer
        }
    ));
}

#[test]
fn refresh_and_start_classify_their_own_failures() {
    assert!(matches!(
        decode_refresh(200, None, GRANT.as_bytes()),
        RefreshAnswer::Rotated(_)
    ));
    assert!(matches!(
        decode_refresh(400, None, br#"{"error":"invalid_grant"}"#),
        RefreshAnswer::Refused {
            reason: InvalidReason::GrantRefused
        }
    ));
    assert!(matches!(
        decode_refresh(503, None, br#"{"error":"temporarily_unavailable"}"#),
        RefreshAnswer::Unavailable { retry_after: None }
    ));
    assert_eq!(
        decode_start_failure(400, None, br#"{"error":"invalid_request"}"#),
        SignInFailure::Invalid {
            reason: InvalidReason::InvalidRequest
        }
    );
}
