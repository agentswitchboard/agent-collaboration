use std::{sync::Arc, time::Duration};

use axum::http::StatusCode;
use switchboard_machine_enrollment::{
    EnrollmentEnd, EnrollmentFailure, InvalidReason, SignInClient, SignInClientError, SignInFailure,
};

use super::scripted_sign_in::{
    INITIAL_ACCESS, INITIAL_REFRESH, Reply, ScriptStore, SignInScript, enrollment_request,
    machine_id, token_reply,
};

#[tokio::test]
async fn shipping_constructor_refuses_all_plain_loopback_origins() {
    let mut script = SignInScript::token_responses([]).await;
    let ipv4 = script.client.deployment().origin().clone();
    for origin in [
        ipv4,
        "http://[::1]:1234/".parse().expect("IPv6"),
        "http://[0:0:0:0:0:0:0:1]:1234/"
            .parse()
            .expect("expanded IPv6"),
    ] {
        let client = SignInClient::new(
            origin,
            Arc::clone(&script.clock) as Arc<dyn router_clock::RuntimeClock + Send + Sync>,
        );
        assert!(matches!(client, Err(SignInClientError::InvalidIssuer)));
    }
    script.assert_exhausted();
}

#[tokio::test]
async fn declared_oversized_reply_is_invalid_before_collecting_the_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener");
    let origin = format!("http://{}/", listener.local_addr().expect("bound address"));
    let clock = Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let client =
        SignInClient::new_loopback_for_test(origin.parse().expect("origin"), clock, |_| {})
            .expect("script client");
    // Emit headers before intentionally truncating the response body. A body-only
    // limit sees the transport failure, while a declared-length guard sees the cap.
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("incoming connection");
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") && request.len() < 4096 {
            request.push(stream.read_u8().await.expect("HTTP request"));
        }
        assert!(request.starts_with(b"GET /.well-known/fleet-login HTTP/1.1\r\n"));
        stream.write_all(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 65537\r\nRetry-After: 60\r\nConnection: close\r\n\r\npartial")
            .await.expect("raw response headers");
        stream.shutdown().await.expect("close truncated response");
    });
    let answer = client.discover().await;
    assert_eq!(
        answer,
        Err(SignInFailure::Invalid {
            reason: InvalidReason::OversizedAnswer
        })
    );
    server.await.expect("scripted server");
}

#[tokio::test]
async fn chunked_oversized_reply_is_invalid_without_a_declared_length() {
    let padded_json = format!(
        "{{\"client_id\":\"{}\",\"link_url\":\"https://central.example/{{organization_id}}/link\"}}",
        super::scripted_sign_in::ROUTER_CLIENT
    );
    let oversized = Reply::chunked(
        StatusCode::OK,
        vec![padded_json, " ".repeat(32_768), " ".repeat(32_768)],
    );
    let mut script = SignInScript::token_responses([oversized]).await;
    assert_eq!(
        script.client.discover().await,
        Err(SignInFailure::Invalid {
            reason: InvalidReason::OversizedAnswer
        })
    );
    script
        .observations
        .try_recv()
        .expect("discovery reached real socket")
        .assert_discovery();
    script.assert_exhausted();
}

#[tokio::test]
async fn redirect_with_a_pending_body_does_not_poll_again() {
    let mut script = SignInScript::start(
        100,
        [Reply::failure(
            StatusCode::TEMPORARY_REDIRECT,
            "authorization_pending",
        )],
    )
    .await;
    let store = ScriptStore::default();
    let enrollment = script.spawn_attempt({
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
    let result = script.finish_before_more_polling(enrollment).await;

    assert_eq!(
        result,
        Some(Err(EnrollmentFailure::Ended(EnrollmentEnd::Invalid(
            InvalidReason::UnexpectedStatus
        ))))
    );
    assert!(store.current().is_none());
    script.assert_exhausted();
}

#[tokio::test]
async fn delayed_start_never_extends_the_issued_lifetime() {
    let (mut script, release) = SignInScript::hold_start(
        12,
        [
            Reply::failure(StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable").after("25"),
            token_reply(INITIAL_ACCESS, INITIAL_REFRESH),
        ],
    )
    .await;
    let store = ScriptStore::default();
    let enrollment = script.spawn_attempt({
        let client = script.client.clone();
        let store = store.clone();
        async move {
            client
                .enroll(enrollment_request(&machine_id()), &store, |_| Ok(()))
                .await
        }
    });
    script.next_request().await.assert_discovery();
    let start = script.next_request().await;
    start.assert_start();
    start.assert_at(0);
    script
        .clock
        .advance(Duration::from_secs(6))
        .expect("held start response");
    release.send(()).expect("release start");
    script.wait_for_poll(5, 11).await;
    let poll = script.next_request().await;
    poll.assert_poll();
    poll.assert_at(11);

    script.observe_poll_wait(12).await;
    script
        .clock
        .advance(Duration::from_secs(1))
        .expect("issued expiry");
    assert_eq!(
        script.finish_before_more_polling(enrollment).await,
        Some(Err(EnrollmentFailure::Ended(EnrollmentEnd::Expired)))
    );
    assert!(store.current().is_none());
    assert!(script.observations.try_recv().is_err());
}

#[tokio::test]
async fn a_delayed_start_clamps_the_first_wait_before_any_poll() {
    let (mut script, release) = SignInScript::hold_start(12, []).await;
    let store = ScriptStore::default();
    let enrollment = script.spawn_attempt({
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
    script
        .clock
        .advance(Duration::from_secs(8))
        .expect("held start response");
    release.send(()).expect("release start");

    script.wait_for_poll(4, 12).await;
    assert_eq!(
        script.finish_before_more_polling(enrollment).await,
        Some(Err(EnrollmentFailure::Ended(EnrollmentEnd::Expired)))
    );
    assert!(store.current().is_none());
    script.assert_exhausted();
}
