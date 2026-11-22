use std::time::Duration;

use super::{
    DecodeAnswerError, InvalidReason, PollAnswer, RefreshAnswer, SignInFailure,
    decode_device_authorization, decode_poll, decode_refresh, decode_start_failure,
};

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
fn device_start_reads_only_the_contract_fields() {
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

#[test]
fn positional_arrays_are_rejected_at_every_response_object() {
    let discovery = br#"["019a0000-0000-7000-8000-000000000001","https://central.example/{organization_id}/link"]"#;
    assert!(matches!(
        crate::decode_discovery(discovery),
        Err(DecodeAnswerError::Malformed)
    ));
    let start = br#"["private-code","USER","https://signin.example/device","https://signin.example/device?user_code=USER",600,5]"#;
    assert!(matches!(
        decode_device_authorization(start),
        Err(DecodeAnswerError::Malformed)
    ));
    assert!(matches!(
        decode_poll(400, None, br#"["authorization_pending"]"#),
        PollAnswer::Invalid {
            reason: InvalidReason::MalformedAnswer
        }
    ));

    let envelope: serde_json::Value = serde_json::from_str(GRANT).expect("literal grant");
    let token_array = serde_json::json!([
        envelope["access_token"],
        envelope["refresh_token"],
        envelope["expires_in"],
        envelope["expires_at"],
        envelope["token_type"],
        envelope["scope"],
        envelope["machine_client_id"],
        envelope["fleet_central"]
    ]);
    assert!(matches!(
        decode_poll(200, None, token_array.to_string().as_bytes()),
        PollAnswer::Invalid {
            reason: InvalidReason::MalformedAnswer
        }
    ));
    let mut wrong_nested = envelope;
    wrong_nested["fleet_central"] = serde_json::json!([
        "019a0000-0000-7000-8000-000000000003",
        "019a0000-0000-7000-8000-000000000004"
    ]);
    assert!(matches!(
        decode_poll(200, None, wrong_nested.to_string().as_bytes()),
        PollAnswer::Invalid {
            reason: InvalidReason::MalformedAnswer
        }
    ));
}

#[test]
fn timestamps_reasons_and_access_lifetime_are_checked_before_use() {
    let mut grant: serde_json::Value = serde_json::from_str(GRANT).expect("literal grant");
    grant["expires_at"] = serde_json::json!("2026-11-22T12:00:00Z");
    assert!(matches!(
        decode_refresh(200, None, grant.to_string().as_bytes()),
        RefreshAnswer::Rotated(_)
    ));

    for malformed in [
        serde_json::json!(null),
        serde_json::json!(true),
        serde_json::json!([]),
        serde_json::json!({}),
    ] {
        grant["expires_at"] = malformed.clone();
        assert!(matches!(
            decode_refresh(200, None, grant.to_string().as_bytes()),
            RefreshAnswer::Refused {
                reason: InvalidReason::MalformedAnswer
            }
        ));
        let refused = serde_json::json!({"error": "authorization_pending", "reason": malformed});
        assert!(matches!(
            decode_poll(400, None, refused.to_string().as_bytes()),
            PollAnswer::Invalid {
                reason: InvalidReason::MalformedAnswer
            }
        ));
    }
    grant["expires_at"] = serde_json::json!(1795382400);
    for lifetime in [0, 3601] {
        grant["expires_in"] = serde_json::json!(lifetime);
        assert!(matches!(
            decode_refresh(200, None, grant.to_string().as_bytes()),
            RefreshAnswer::Refused {
                reason: InvalidReason::MalformedAnswer
            }
        ));
    }
}
