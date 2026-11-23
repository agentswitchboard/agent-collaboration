use std::{collections::BTreeMap, fs, path::PathBuf, time::Duration};

use axum::http::{HeaderMap, StatusCode, header};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;
use switchboard_machine_enrollment::{
    AccessRequest, EnrollmentEnd, EnrollmentRequest, InvalidReason, LeaseUnavailable, PollAnswer,
    RefreshAnswer, SignInFailure, TokenGrant, decode_device_authorization, decode_discovery,
    decode_poll, decode_refresh, decode_start_failure,
};

use super::scripted_sign_in::{Observation, Reply, ScriptStore, SignInScript};

const CLIENT: &str = "019a3dce-2000-7000-8000-0000000000a1";
const MACHINE: &str = "019a3dce-2000-7000-8000-0000000000a2";
const ORGANIZATION: &str = "019a3dce-2000-7000-8000-0000000000a3";
const ENROLLMENT: &str = "019a3dce-2000-7000-8000-0000000000a4";

#[derive(Deserialize)]
struct ResponseFixture {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Value,
}

impl ResponseFixture {
    fn retry_after(&self) -> Option<Duration> {
        self.headers.get("retry-after").map(|value| {
            Duration::from_secs(value.parse().expect("retry-after in fixture seconds"))
        })
    }

    fn reply(&self, body: &Value) -> Reply {
        let headers: HeaderMap = self
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name.parse::<header::HeaderName>()
                        .expect("valid fixture header name"),
                    value
                        .parse::<header::HeaderValue>()
                        .expect("valid fixture header value"),
                )
            })
            .collect();
        Reply::new(
            StatusCode::from_u16(self.status).expect("fixture status"),
            body.to_string(),
        )
        .with_headers(headers)
    }
}

#[derive(Deserialize)]
struct RequestFixture {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: BTreeMap<String, String>,
}

impl RequestFixture {
    fn assert_on_wire(&self, observed: Observation) {
        assert_eq!(observed.method.as_str(), self.method);
        assert_eq!(observed.target, self.path);
        assert_eq!(
            observed.content_type.as_ref(),
            self.headers.get(header::CONTENT_TYPE.as_str())
        );
        assert_eq!(
            observed.authorization.as_ref(),
            self.headers.get(header::AUTHORIZATION.as_str())
        );
        assert!(
            observed.form == self.body,
            "exact URL-encoded form fields on {}",
            self.path
        );
    }
}

fn fixtures<T: DeserializeOwned>(kind: &str) -> BTreeMap<String, T> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/sign-in-c5")
        .join(kind);
    fs::read_dir(root)
        .expect("pinned fixtures")
        .map(|entry| {
            let path = entry.expect("fixture path").path();
            assert_eq!(
                path.extension().and_then(|value| value.to_str()),
                Some("json")
            );
            let key = path
                .file_stem()
                .expect("fixture name")
                .to_string_lossy()
                .into_owned();
            let fixture = serde_json::from_slice(&fs::read(&path).expect("fixture contents"))
                .expect("well-formed fixture envelope");
            (key, fixture)
        })
        .collect()
}

fn assert_token(grant: &TokenGrant, body: &Value) {
    assert_eq!(grant.credential.enrollment_id.to_string(), ENROLLMENT);
    assert!(
        body["access_token"]
            .as_str()
            .is_some_and(|value| value == grant.access_token.expose_secret())
    );
    assert!(
        body["refresh_token"]
            .as_str()
            .is_some_and(|value| value == grant.credential.refresh_token.expose_secret())
    );
    assert_eq!(
        Some(grant.expires_in.as_secs()),
        body["expires_in"].as_u64()
    );
}

fn check_response(name: &str, fixture: &ResponseFixture) {
    assert_eq!(
        fixture.headers.get("cache-control").map(String::as_str),
        Some("no-store")
    );
    let body = fixture.body.to_string();
    let body_bytes = body.as_bytes();
    match name {
        "discovery" => {
            assert_eq!(fixture.status, 200);
            let answer = decode_discovery(body_bytes).expect("pinned discovery response");
            assert_eq!(answer.client_id.to_string(), CLIENT);
            let organization = ORGANIZATION.parse().expect("fixture organization");
            assert_eq!(
                answer
                    .link_url
                    .for_organization(organization)
                    .expect("fixture dial address")
                    .as_str(),
                format!("https://sign-in.example.invalid/api/fleet/link/{ORGANIZATION}"),
            );
        }
        "device-code-start" => {
            assert_eq!(fixture.status, 200);
            let challenge = decode_device_authorization(body_bytes).expect("pinned start response");
            assert_eq!(challenge.instructions.user_code, "ABCD-EFGH");
            assert!(challenge.device_code.expose_secret() == "fixture-only-device-code");
            assert_eq!(
                challenge.instructions.verification_link.as_str(),
                "https://sign-in.example.invalid/device?user_code=ABCD-EFGH"
            );
            assert_eq!(
                Some(challenge.expires_in.as_secs()),
                fixture.body.get("expires_in").and_then(Value::as_u64)
            );
            assert_eq!(
                Some(challenge.interval.as_secs()),
                fixture.body.get("interval").and_then(Value::as_u64)
            );
        }
        "token-success" => {
            let grant = match decode_poll(fixture.status, fixture.retry_after(), body_bytes) {
                PollAnswer::Enrolled(grant) => Some(grant),
                _ => None,
            };
            assert_token(
                &grant.expect("pinned token response must enroll"),
                &fixture.body,
            );
        }
        "refresh-success" => {
            let grant = match decode_refresh(fixture.status, fixture.retry_after(), body_bytes) {
                RefreshAnswer::Rotated(grant) => Some(grant),
                _ => None,
            };
            assert_token(
                &grant.expect("pinned refresh response must rotate"),
                &fixture.body,
            );
        }
        "refresh-invalid_grant" => assert!(matches!(
            decode_refresh(fixture.status, fixture.retry_after(), body_bytes),
            RefreshAnswer::Refused {
                reason: InvalidReason::GrantRefused
            }
        )),
        _ => {
            let answer = decode_poll(fixture.status, fixture.retry_after(), body_bytes);
            let correct = match name {
                "poll-access_denied" => matches!(answer, PollAnswer::Denied),
                "poll-authorization_pending" => matches!(answer, PollAnswer::Pending),
                "poll-expired_token" => matches!(answer, PollAnswer::Expired),
                "poll-invalid_client" => matches!(
                    answer,
                    PollAnswer::Invalid {
                        reason: InvalidReason::InvalidClient
                    }
                ),
                "poll-invalid_grant" => matches!(
                    answer,
                    PollAnswer::Invalid {
                        reason: InvalidReason::GrantRefused
                    }
                ),
                "poll-invalid_request" => matches!(
                    answer,
                    PollAnswer::Invalid {
                        reason: InvalidReason::InvalidRequest
                    }
                ),
                "poll-slow_down" => matches!(answer, PollAnswer::SlowDown),
                "poll-temporarily_unavailable" => {
                    matches!(answer, PollAnswer::Unavailable { retry_after: Some(delay) } if delay == Duration::from_secs(5))
                }
                _ => false,
            };
            assert!(correct, "typed response for {name} was {answer:?}");
        }
    }
}

#[tokio::test]
async fn replays_every_pinned_sign_in_response_and_request() {
    let responses = fixtures::<ResponseFixture>("responses");
    assert_eq!(responses.len(), 13, "response fixture inventory");
    for (name, response) in &responses {
        check_response(name, response);
    }
    let mut requests = fixtures::<RequestFixture>("requests");
    let mut script = SignInScript::from_replies(|origin| {
        let start = responses.get("device-code-start").expect("pinned start");
        let mut loopback_challenge = start.body.clone();
        // The original fixture above is checked untouched. Only the transport copy
        // is redirected to the isolated server so the same-origin guard still runs.
        loopback_challenge["verification_uri"] = Value::String(format!("{origin}/device"));
        loopback_challenge["verification_uri_complete"] =
            Value::String(format!("{origin}/device?user_code=ABCD-EFGH"));
        let discovery = responses.get("discovery").expect("pinned discovery");
        let token = responses.get("token-success").expect("pinned token grant");
        let refresh = responses.get("refresh-success").expect("pinned refresh");
        [
            discovery.reply(&discovery.body),
            start.reply(&loopback_challenge),
            token.reply(&token.body),
            refresh.reply(&refresh.body),
        ]
        .into_iter()
        .collect()
    })
    .await;
    let store = ScriptStore::default();
    let machine: switchboard_machine_enrollment::MachineId =
        MACHINE.parse().expect("fixture machine");
    let organization_id = ORGANIZATION.parse().expect("fixture organization");
    let enrollment = script.spawn_attempt({
        let client = script.client.clone();
        let store = store.clone();
        let machine = machine.clone();
        async move {
            client
                .enroll(
                    EnrollmentRequest {
                        machine_id: &machine,
                        organization_id,
                        machine_label: "Fixture Mac",
                    },
                    &store,
                    |_| Ok(()),
                )
                .await
        }
    });
    script.next_request().await.assert_discovery();
    requests
        .remove("device-code-start")
        .expect("pinned start request")
        .assert_on_wire(script.next_request().await);
    script.wait_for_poll(5, 5).await;
    requests
        .remove("poll")
        .expect("pinned poll request")
        .assert_on_wire(script.next_request().await);
    let receipt = enrollment
        .await
        .expect("enrollment task")
        .expect("fixture enrollment");
    assert_eq!(receipt.enrollment_id.to_string(), ENROLLMENT);

    let owner = script.client.refresh_owner(store, machine, organization_id);
    let lease = owner
        .lease(AccessRequest::Usable)
        .await
        .expect("fixture rotation");
    assert!(lease.token.expose_secret() == "fixture-only-rotated-access-token");
    requests
        .remove("refresh")
        .expect("pinned refresh request")
        .assert_on_wire(
            script
                .observations
                .try_recv()
                .expect("fixture refresh on the wire"),
        );
    assert!(requests.is_empty(), "every request fixture was replayed");
    script.assert_exhausted();
}

#[test]
fn invalid_client_is_only_terminal_for_refresh() {
    let body = br#"{"error":"invalid_client"}"#;
    assert_eq!(
        decode_start_failure(400, None, body),
        SignInFailure::Invalid {
            reason: InvalidReason::InvalidClient
        }
    );
    assert!(matches!(
        decode_poll(400, None, body),
        PollAnswer::Invalid {
            reason: InvalidReason::InvalidClient
        }
    ));
}

#[tokio::test]
async fn a_disabled_refresh_client_ends_the_enrollment_without_retry() {
    let mut script = SignInScript::start(
        20,
        [
            super::scripted_sign_in::token_reply(
                super::scripted_sign_in::INITIAL_ACCESS,
                super::scripted_sign_in::INITIAL_REFRESH,
            ),
            Reply::failure(StatusCode::BAD_REQUEST, "invalid_client"),
        ],
    )
    .await;
    let store = ScriptStore::default();
    let enrollment = script.spawn_attempt({
        let client = script.client.clone();
        let store = store.clone();
        async move {
            client
                .enroll(
                    super::scripted_sign_in::enrollment_request(
                        &super::scripted_sign_in::machine_id(),
                    ),
                    &store,
                    |_| Ok(()),
                )
                .await
        }
    });
    script.next_request().await.assert_discovery();
    script.next_request().await.assert_start();
    script.wait_for_poll(5, 5).await;
    script.next_request().await.assert_poll();
    enrollment
        .await
        .expect("enrollment task")
        .expect("initial enrollment");
    let owner = script.client.refresh_owner(
        store.clone(),
        super::scripted_sign_in::machine_id(),
        super::scripted_sign_in::organization_id(),
    );

    let ended = Err(LeaseUnavailable::Ended {
        why: EnrollmentEnd::RefreshClientDisabled,
    });
    assert_eq!(owner.lease(AccessRequest::Usable).await, ended);
    assert_eq!(owner.lease(AccessRequest::Usable).await, ended);
    assert!(matches!(
        store.snapshot(),
        switchboard_machine_enrollment::StoredEnrollment::Ended {
            why: EnrollmentEnd::RefreshClientDisabled,
            ..
        }
    ));
    script
        .observations
        .try_recv()
        .expect("one refresh")
        .assert_refresh(super::scripted_sign_in::INITIAL_REFRESH);
    script.assert_exhausted();
}
