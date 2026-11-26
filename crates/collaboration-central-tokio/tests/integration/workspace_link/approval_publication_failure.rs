use std::cell::{Cell, RefCell};

use central_storage::AlarmPublicationFailure;
use collaboration_central_conformance::{
    ConformanceResult,
    approval_link_scenarios::{read_request, receive_decision, receive_decision_before},
    signin_fixtures::{
        approval_routes::check_socket_holder_delivery_after_publication_failure,
        approval_socket::PersonApprovalEndpoint,
    },
};
use collaboration_central_tokio::test_support::start_session_link_fixture;

use super::{NativeApprovalFixture, signed_clock};
use crate::workspace_link::fixture::{receive, send};

#[expect(
    clippy::panic_in_result_fn,
    reason = "The socket assertions name the failed observation."
)]
#[tokio::test]
async fn tokio_socket_releases_committed_person_ending_when_alarm_publication_fails()
-> ConformanceResult<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = async {
        let runtime = fixture
            .link
            .host
            .central(&fixture.link.organization_id)
            .await?;
        let store = runtime.runtime().statement_host();
        let held_write = RefCell::new(None);
        let client = reqwest::Client::builder()
            .no_proxy()
            .unix_socket(fixture.link.signin_socket.clone())
            .build()?;
        let public = fixture.link.client()?;
        let origin = fixture.link.origin();
        check_socket_holder_delivery_after_publication_failure(
            &fixture,
            PersonApprovalEndpoint {
                client: &public,
                origin: &origin,
                now_seconds: collaboration_central_conformance::signin_fixtures::NOW_SECONDS,
            },
            async |path, body, headers| {
                let mut request = client
                    .post(format!("http://central.local{path}"))
                    .json(body);
                for (name, value) in headers {
                    request = request.header(name, value);
                }
                let response = request.send().await?;
                Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
            },
            async || {
                held_write.replace(Some(
                    runtime
                        .runtime()
                        .pause_points()
                        .hold_central(fleet_runtime::PausePoint::BeforeApprovalEndingWrite),
                ));
                store.fail_next_alarm_publication(AlarmPublicationFailure::NoLaterGridMark);
                Ok(())
            },
            async |socket, target, request, ending| {
                if !fixture.link.host.approval_delivery_is_tracked() {
                    return receive_decision_before(
                        &fixture,
                        socket,
                        target,
                        request,
                        ending,
                        std::future::ready(Ok(())),
                    )
                    .await;
                }
                let mut writer = held_write.take().ok_or("ending writer was not armed")?;
                writer.until_held().await;
                // The writer already owns the link pump. A queued Pong follows
                // its write, so an omitted ending fails on the real wire.
                send(socket, &central_link_protocol::LinkFrame::Ping).await?;
                let release = async {
                    for _ in 0..129 {
                        read_request(&fixture, request).await?;
                    }
                    writer.release();
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                };
                let (observed, release) = futures_util::join!(
                    receive_decision(&fixture, socket, target, request, ending),
                    release
                );
                release?;
                observed?;
                assert_eq!(
                    receive(socket).await?,
                    central_link_protocol::LinkFrame::Pong
                );
                Ok(())
            },
        )
        .await?;
        if store.take_alarm_publication_failures() != [AlarmPublicationFailure::NoLaterGridMark] {
            return Err(
                "the real native alarm publisher did not report the injected failure".into(),
            );
        }
        Ok(())
    }
    .await;
    fixture.link.close().await?;
    observed
}
