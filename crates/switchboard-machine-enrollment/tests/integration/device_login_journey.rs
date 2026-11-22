use std::time::Duration;

use axum::http::StatusCode;
use switchboard_machine_enrollment::{EnrollmentEnd, EnrollmentFailure, InvalidReason};

use super::scripted_sign_in::{
    ENROLLMENT_ID, INITIAL_ACCESS, INITIAL_REFRESH, RECEIVER, RELAY, Reply, ScriptStore,
    SignInScript, USER_CODE, enrollment_request, machine_id, token_reply,
};

#[tokio::test]
async fn polling_over_http_obeys_each_wait_and_uses_the_sign_in_body() {
    let mut script = SignInScript::start(
        100,
        [
            Reply::failure(StatusCode::BAD_REQUEST, "authorization_pending"),
            Reply::failure(StatusCode::BAD_REQUEST, "slow_down"),
            Reply::failure(StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable").after("4"),
            Reply::rate_limit("Thu, 01 Jan 1970 00:00:31 GMT"),
            Reply::failure(StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable"),
            token_reply(INITIAL_ACCESS, INITIAL_REFRESH),
        ],
    )
    .await;
    let store = ScriptStore::default();
    let enrollment = tokio::spawn({
        let client = script.client.clone();
        let store = store.clone();
        async move {
            client
                .enroll(enrollment_request(&machine_id()), &store, |instructions| {
                    assert_eq!(instructions.user_code, USER_CODE);
                    assert!(instructions.verification_link.path().ends_with("/device"));
                    Ok(())
                })
                .await
        }
    });

    script.next_request().await.assert_discovery();
    script.next_request().await.assert_start();
    for (delay, due) in [(5, 5), (5, 10), (10, 20), (4, 24), (7, 31), (10, 41)] {
        script.wait_for_poll(delay, due).await;
        let poll = script.next_request().await;
        poll.assert_at(due);
        poll.assert_poll();
    }
    let receipt = enrollment
        .await
        .expect("enrollment task")
        .expect("approved enrollment");
    let stored = store.current().expect("saved credential");
    assert_eq!(receipt.enrollment_id.to_string(), ENROLLMENT_ID);
    assert_eq!(receipt.central.receiver.to_string(), RECEIVER);
    assert_eq!(receipt.central.relay.to_string(), RELAY);
    assert_eq!(stored.credential.enrollment_id, receipt.enrollment_id);
    assert_eq!(stored.credential.central, receipt.central);
    assert!(stored.credential.refresh_token.expose_secret() == INITIAL_REFRESH);
    script.assert_exhausted();
}

#[tokio::test]
async fn terminal_http_responses_never_trigger_another_poll() {
    for (code, expected) in [
        ("access_denied", EnrollmentEnd::Denied),
        ("expired_token", EnrollmentEnd::Expired),
        (
            "future_error",
            EnrollmentEnd::Invalid(InvalidReason::UnknownError),
        ),
    ] {
        let mut script =
            SignInScript::start(80, [Reply::failure(StatusCode::BAD_REQUEST, code)]).await;
        let store = ScriptStore::default();
        let enrollment = tokio::spawn({
            let client = script.client.clone();
            let store = store.clone();
            async move {
                client
                    .enroll(enrollment_request(&machine_id()), &store, |_| Ok(()))
                    .await
            }
        });
        script.next_request().await.assert_discovery();
        script.next_request().await.assert_start();
        script.wait_for_poll(5, 5).await;
        script.next_request().await.assert_poll();
        let result = tokio::select! {
            result = enrollment => Some(result.expect("enrollment task")),
            () = script.clock.not_due_check_now(super::scripted_sign_in::at(10)) => None,
        };
        assert_eq!(result, Some(Err(EnrollmentFailure::Ended(expected))));
        script
            .clock
            .advance(Duration::from_secs(80))
            .expect("advance manual clock");
        assert!(store.current().is_none());
        script.assert_exhausted();
    }
}

#[tokio::test]
async fn device_expiry_stops_the_attempt_even_after_a_server_retry_delay() {
    let mut script = SignInScript::start(
        8,
        [Reply::failure(StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable").after("15")],
    )
    .await;
    let store = ScriptStore::default();
    let enrollment = tokio::spawn({
        let client = script.client.clone();
        let store = store.clone();
        async move {
            client
                .enroll(enrollment_request(&machine_id()), &store, |_| Ok(()))
                .await
        }
    });
    script.next_request().await.assert_discovery();
    script.next_request().await.assert_start();
    script.wait_for_poll(5, 5).await;
    script.next_request().await.assert_poll();
    script.wait_for_poll(3, 8).await;
    assert_eq!(
        enrollment.await.expect("enrollment task"),
        Err(EnrollmentFailure::Ended(EnrollmentEnd::Expired))
    );
    assert!(store.current().is_none());
    script.assert_exhausted();
}
