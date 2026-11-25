use std::time::Duration;

use central_link_client::{EnrollmentEnd, ReadyLinkLane};
use central_link_protocol::{
    CorrelationId, LinkCallRefusal, MachineLinkCloseCode,
    approval_rpc::{ApprovalServiceTarget, RegisterRunReply},
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::json;
use switchboard_central_link::{
    DialAddress,
    link_policy::{LinkStatus, RegistrationFailure},
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};

use super::{
    Fixture, registration_request, reply_registration, text, tick, welcome_before_registration,
};

async fn framed_refusal(
    socket: &mut WebSocketStream<TcpStream>,
    correlation: &CorrelationId,
    refusal: LinkCallRefusal,
) {
    send_json(
        socket,
        json!({
            "kind": "centralCallReply",
            "correlationId": correlation,
            "result": { "kind": "refused", "reason": refusal },
        }),
    )
    .await;
}

async fn send_json(socket: &mut WebSocketStream<TcpStream>, value: serde_json::Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn pong(socket: &mut WebSocketStream<TcpStream>) {
    assert_eq!(text(socket).await, "ping");
    socket.send(Message::Text("pong".into())).await.unwrap();
}

fn assert_open_and_pending(fixture: &Fixture) {
    assert_eq!(*fixture.status.borrow(), LinkStatus::Registering);
    assert!(
        fixture
            .inbox_events
            .lane
            .watch_registered()
            .borrow()
            .is_none()
    );
}

#[derive(Clone, Copy)]
enum Mismatch {
    Correlation,
    KnownCall,
    Target,
}

async fn mismatched_reply(
    socket: &mut WebSocketStream<TcpStream>,
    correlation: &CorrelationId,
    target: &ApprovalServiceTarget,
    kind: Mismatch,
) {
    let wrong = "01890f2e-7b4c-7cc0-98c4-0000000000ff";
    let wrong_correlation = wrong.parse::<CorrelationId>().unwrap();
    let correlation_id = match kind {
        Mismatch::Correlation => &wrong_correlation,
        Mismatch::KnownCall | Mismatch::Target => correlation,
    };
    let reply_body = match kind {
        Mismatch::Correlation => json!({
            "call": "registerRun", "envelope": { "target": target, "body": { "kind": "registered" } },
        }),
        Mismatch::Target => json!({
            "call": "registerRun", "envelope": {
                "target": { "organizationId": wrong }, "body": { "kind": "registered" }
            },
        }),
        Mismatch::KnownCall => json!({
            "call": "stop",
            "envelope": { "target": target, "body": { "kind": "authorityRefused", "refusal": { "kind": "runRetired" } } },
        }),
    };
    let reply = json!({
        "kind": "centralCallReply",
        "correlationId": correlation_id,
        "result": { "kind": "answered", "reply": reply_body },
    });
    assert!(
        central_link_protocol::decode_text(
            &reply.to_string(),
            central_link_protocol::FrameDirection::ToRouter
        )
        .is_ok()
    );
    send_json(socket, reply).await;
}

async fn verify_mismatch(kind: Mismatch, answer_before_limit: bool) {
    let before = super::credential_diagnostics::registration_mismatches();
    let mut fixture = Fixture::new().await;
    let mut socket = fixture.accept().await;
    welcome_before_registration(&mut socket).await;
    let (correlation, target, run) = registration_request(&mut socket).await;
    assert_open_and_pending(&fixture);
    mismatched_reply(&mut socket, &correlation, &target, kind).await;
    // The real socket must stay live and continue housekeeping while the reply is non-settling.
    socket
        .send(Message::Text(super::DELIVER_FRAME.into()))
        .await
        .unwrap();
    assert!(fixture.inbox_events.recv().await.is_some());
    assert_eq!(
        super::credential_diagnostics::registration_mismatches(),
        before + 1
    );
    fixture.clock.advance(Duration::from_secs(20)).unwrap();
    pong(&mut socket).await;
    fixture.clock.not_due_check_now(tick(30)).await;
    assert_open_and_pending(&fixture);

    if answer_before_limit {
        reply_registration(
            &mut socket,
            correlation,
            target,
            RegisterRunReply::Registered {},
        )
        .await;
    } else {
        fixture.clock.advance(Duration::from_secs(10)).unwrap();
        fixture
            .status
            .wait_for(|status| {
                *status
                    == LinkStatus::RegistrationFailed {
                        reason: RegistrationFailure::TimedOut,
                    }
            })
            .await
            .unwrap();
        fixture.clock.not_due_check_now(tick(31)).await;
        assert!(socket.next().now_or_never().is_none());
        fixture.clock.advance(Duration::from_secs(1)).unwrap();
        let (retry, retry_target, retry_run) = registration_request(&mut socket).await;
        assert_ne!(retry, correlation);
        assert_eq!(retry_run, run);
        assert_eq!(retry_target, target);
        reply_registration(
            &mut socket,
            correlation,
            target.clone(),
            RegisterRunReply::Registered {},
        )
        .await;
        // Put a custody offer behind the stale reply and wait until it is read.
        // At that point the old acknowledgement has definitely been processed.
        socket
            .send(Message::Text(super::DELIVER_FRAME.into()))
            .await
            .unwrap();
        assert!(fixture.inbox_events.recv().await.is_some());
        assert_open_and_pending(&fixture);
        reply_registration(&mut socket, retry, target, RegisterRunReply::Registered {}).await;
    }
    fixture
        .status
        .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
        .await
        .unwrap();
    let registration = fixture
        .inbox_events
        .lane
        .watch_registered()
        .borrow()
        .clone()
        .unwrap();
    assert_eq!(registration.router_run(), &run);
    fixture.stop().await;
}

#[tokio::test]
async fn wrong_correlations_kinds_and_targets_cannot_complete_the_pending_registration() {
    let _diagnostics = super::credential_diagnostics::capture_trace();
    for kind in [Mismatch::Correlation, Mismatch::KnownCall, Mismatch::Target] {
        for answer_before_limit in [true, false] {
            verify_mismatch(kind, answer_before_limit).await;
        }
    }
}

#[tokio::test]
async fn parked_answers_survive_same_binding_address_change_and_a_new_binding_mints_a_new_run() {
    for (answer, terminal_status) in [
        (RegisterRunReply::RunRetired {}, LinkStatus::RunRetired),
        (
            RegisterRunReply::EnrollmentEnded {},
            LinkStatus::RegistrationFailed {
                reason: RegistrationFailure::CentralEnrollmentEnded,
            },
        ),
    ] {
        let mut fixture = Fixture::new().await;
        let mut socket = fixture.accept().await;
        assert_eq!(fixture.acquired.recv().await, Some(None));
        welcome_before_registration(&mut socket).await;
        let (correlation, target, run) = registration_request(&mut socket).await;
        reply_registration(&mut socket, correlation, target, answer).await;
        fixture
            .status
            .wait_for(|status| status == &terminal_status)
            .await
            .unwrap();
        assert!(matches!(
            socket.next().await,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None
        ));
        assert!(
            fixture
                .inbox_events
                .lane
                .watch_registered()
                .borrow()
                .is_none()
        );

        let replacement = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = DialAddress::test_loopback(&format!(
            "ws://{}/organization/link",
            replacement.local_addr().unwrap()
        ));
        let poll_before = *fixture.driver_polls.borrow_and_update();
        fixture
            .settings
            .send_modify(|settings| settings.address = address.clone());
        fixture.clock.advance(Duration::from_secs(180)).unwrap();
        fixture
            .driver_polls
            .wait_for(|poll| *poll > poll_before)
            .await
            .unwrap();
        assert_eq!(*fixture.status.borrow(), terminal_status);
        assert!(fixture.acquired.try_recv().is_err());
        assert!(fixture.ended.try_recv().is_err());
        assert!(replacement.accept().now_or_never().is_none());

        fixture
            .settings
            .send_modify(|settings| settings.binding = Some(2));
        assert_eq!(fixture.acquired.recv().await, Some(None));
        let (stream, _) = replacement.accept().await.unwrap();
        let mut next = tokio_tungstenite::accept_async(stream).await.unwrap();
        welcome_before_registration(&mut next).await;
        let (correlation, target, replacement_run) = registration_request(&mut next).await;
        assert_ne!(replacement_run, run);
        reply_registration(
            &mut next,
            correlation,
            target,
            RegisterRunReply::Registered {},
        )
        .await;
        fixture
            .status
            .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
            .await
            .unwrap();
        assert!(fixture.ended.try_recv().is_err());
        fixture.stop().await;
    }
}

#[tokio::test]
async fn authority_lapsed_refusal_keeps_the_socket_open_until_its_typed_close() {
    for cause in [
        MachineLinkCloseCode::Revoked,
        MachineLinkCloseCode::Inactive,
        MachineLinkCloseCode::Expired,
    ] {
        let mut fixture = Fixture::new().await;
        let mut socket = fixture.accept().await;
        assert_eq!(fixture.acquired.recv().await, Some(None));
        welcome_before_registration(&mut socket).await;
        let (correlation, _, _) = registration_request(&mut socket).await;
        let refusal = LinkCallRefusal::AuthorityLapsed { cause };
        framed_refusal(&mut socket, &correlation, refusal).await;
        fixture
            .status
            .wait_for(|status| {
                *status
                    == LinkStatus::RegistrationFailed {
                        reason: RegistrationFailure::Refused(refusal),
                    }
            })
            .await
            .unwrap();
        assert!(socket.next().now_or_never().is_none());
        assert!(fixture.ended.try_recv().is_err());

        socket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(cause.code()),
                reason: "".into(),
            })))
            .await
            .unwrap();
        match cause {
            MachineLinkCloseCode::Revoked => assert_eq!(
                fixture.ended.recv().await,
                Some((1, EnrollmentEnd::Revoked))
            ),
            MachineLinkCloseCode::Inactive => assert_eq!(
                fixture.ended.recv().await,
                Some((1, EnrollmentEnd::Inactive))
            ),
            MachineLinkCloseCode::Expired => {
                fixture
                    .status
                    .wait_for(|status| *status == LinkStatus::Connecting)
                    .await
                    .unwrap();
                assert!(fixture.ended.try_recv().is_err());
            }
        }
        fixture.stop().await;
    }
}

#[tokio::test]
async fn each_framed_refusal_waits_for_its_due_tick_and_reuses_the_live_socket() {
    for (refusal, long_wait) in [
        (LinkCallRefusal::CentralFault, false),
        (
            LinkCallRefusal::AuthorityLapsed {
                cause: MachineLinkCloseCode::Expired,
            },
            false,
        ),
        (
            LinkCallRefusal::AuthorityLapsed {
                cause: MachineLinkCloseCode::Revoked,
            },
            false,
        ),
        (
            LinkCallRefusal::AuthorityLapsed {
                cause: MachineLinkCloseCode::Inactive,
            },
            false,
        ),
        (LinkCallRefusal::UnknownCall, true),
        (LinkCallRefusal::WrongTarget, true),
        (LinkCallRefusal::NotPermitted, true),
        (LinkCallRefusal::SessionProofNotAccepted, true),
    ] {
        let mut fixture = Fixture::new().await;
        let mut socket = fixture.accept().await;
        welcome_before_registration(&mut socket).await;
        let (first, target, run) = registration_request(&mut socket).await;
        framed_refusal(&mut socket, &first, refusal).await;
        let expected = if long_wait {
            LinkStatus::RegistrationFault { reason: refusal }
        } else {
            LinkStatus::RegistrationFailed {
                reason: RegistrationFailure::Refused(refusal),
            }
        };
        fixture
            .status
            .wait_for(|status| *status == expected)
            .await
            .unwrap();
        let due = if long_wait {
            for heartbeat in [20, 40] {
                fixture.clock.not_due_check_now(tick(heartbeat)).await;
                fixture.clock.advance(Duration::from_secs(20)).unwrap();
                pong(&mut socket).await;
            }
            60
        } else {
            1
        };
        let now = if long_wait { 40 } else { 0 };
        fixture.clock.not_due_check_now(tick(due)).await;
        fixture
            .clock
            .advance(Duration::from_secs(due - now) - Duration::from_nanos(1))
            .unwrap();
        fixture.clock.not_due_check_now(tick(due)).await;
        assert!(
            socket.next().now_or_never().is_none(),
            "no early registration"
        );
        assert_eq!(*fixture.status.borrow(), expected);
        assert!(fixture.ended.try_recv().is_err());
        fixture.clock.advance(Duration::from_nanos(1)).unwrap();
        let (retry, retry_target, retry_run) = registration_request(&mut socket).await;
        assert_ne!(retry, first);
        assert_eq!(retry_run, run);
        assert_eq!(retry_target, target);
        reply_registration(&mut socket, retry, target, RegisterRunReply::Registered {}).await;
        fixture
            .status
            .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
            .await
            .unwrap();
        assert!(fixture.ended.try_recv().is_err());
        fixture.stop().await;
    }
}

#[tokio::test]
async fn registration_backoff_survives_pre_success_redial_and_resets_only_on_registered() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.accept().await;
    welcome_before_registration(&mut original).await;
    let (first, target, run) = registration_request(&mut original).await;
    reply_registration(
        &mut original,
        first.clone(),
        target.clone(),
        RegisterRunReply::RunNotRegistered {},
    )
    .await;
    assert!(matches!(
        original.next().await,
        Some(Ok(Message::Close(_))) | Some(Err(_)) | None
    ));
    assert_eq!(*fixture.status.borrow(), LinkStatus::Connecting);
    assert!(fixture.ended.try_recv().is_err());

    fixture.clock.not_due_check_now(tick(1)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let mut replacement = fixture.accept().await;
    welcome_before_registration(&mut replacement).await;
    let (second, _, same_run) = registration_request(&mut replacement).await;
    assert_eq!(same_run, run);
    assert_ne!(second, first);
    let failure = LinkCallRefusal::CentralFault;
    let failure_status = LinkStatus::RegistrationFailed {
        reason: RegistrationFailure::Refused(failure),
    };
    framed_refusal(&mut replacement, &second, failure).await;
    fixture
        .status
        .wait_for(|status| *status == failure_status)
        .await
        .unwrap();
    await_due(&fixture, &mut replacement, 2, 3).await;
    let (third, third_target, same_run) = registration_request(&mut replacement).await;
    assert_eq!(same_run, run);
    assert_ne!(third, second);
    reply_registration(
        &mut replacement,
        third,
        third_target,
        RegisterRunReply::Registered {},
    )
    .await;
    fixture
        .status
        .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
        .await
        .unwrap();

    replacement.send(Message::Close(None)).await.unwrap();
    fixture
        .status
        .wait_for(|status| *status == LinkStatus::Connecting)
        .await
        .unwrap();
    fixture.clock.not_due_check_now(tick(4)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    let mut after_success = fixture.accept().await;
    welcome_before_registration(&mut after_success).await;
    let (fourth, _, same_run) = registration_request(&mut after_success).await;
    assert_eq!(same_run, run);
    framed_refusal(&mut after_success, &fourth, failure).await;
    fixture
        .status
        .wait_for(|status| *status == failure_status)
        .await
        .unwrap();
    await_due(&fixture, &mut after_success, 1, 5).await;
    let (fifth, _, same_run) = registration_request(&mut after_success).await;
    assert_ne!(fifth, fourth);
    assert_eq!(same_run, run);
    assert!(fixture.ended.try_recv().is_err());
    fixture.stop().await;
}

async fn await_due(
    fixture: &Fixture,
    socket: &mut WebSocketStream<TcpStream>,
    wait: u64,
    due: u64,
) {
    fixture.clock.not_due_check_now(tick(due)).await;
    fixture
        .clock
        .advance(Duration::from_secs(wait) - Duration::from_nanos(1))
        .unwrap();
    fixture.clock.not_due_check_now(tick(due)).await;
    assert!(socket.next().now_or_never().is_none());
    fixture.clock.advance(Duration::from_nanos(1)).unwrap();
}

#[tokio::test]
async fn changing_the_address_or_binding_fences_the_old_registration_and_accepts_a_fresh_one() {
    for change_binding in [false, true] {
        let mut fixture = Fixture::new().await;
        let mut original = fixture.accept().await;
        welcome_before_registration(&mut original).await;
        let (old, old_target, old_run) = registration_request(&mut original).await;
        let replacement = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = DialAddress::test_loopback(&format!(
            "ws://{}/organization/link",
            replacement.local_addr().unwrap()
        ));
        fixture.settings.send_modify(|settings| {
            settings.address = address.clone();
            if change_binding {
                settings.binding = Some(2);
            }
        });
        assert!(matches!(
            original.next().await,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None
        ));
        if !change_binding {
            fixture.clock.not_due_check_now(tick(1)).await;
            fixture.clock.advance(Duration::from_secs(1)).unwrap();
        }
        let (stream, _) = replacement.accept().await.unwrap();
        let mut next = tokio_tungstenite::accept_async(stream).await.unwrap();
        welcome_before_registration(&mut next).await;
        let (current, target, run) = registration_request(&mut next).await;
        assert_ne!(current, old);
        assert_eq!(run != old_run, change_binding);
        reply_registration(&mut next, old, old_target, RegisterRunReply::Registered {}).await;
        next.send(Message::Text(super::DELIVER_FRAME.into()))
            .await
            .unwrap();
        assert!(fixture.inbox_events.recv().await.is_some());
        assert_open_and_pending(&fixture);
        reply_registration(&mut next, current, target, RegisterRunReply::Registered {}).await;
        fixture
            .status
            .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
            .await
            .unwrap();
        assert_eq!(
            fixture
                .inbox_events
                .lane
                .watch_registered()
                .borrow()
                .as_ref()
                .unwrap()
                .router_run(),
            &run
        );
        fixture.stop().await;
    }
}

#[tokio::test]
async fn a_typed_close_takes_effect_when_the_refusal_is_already_queued_behind_it() {
    use tokio::io::AsyncWriteExt as _;
    use tokio_tungstenite::tungstenite::protocol::frame::{
        Frame,
        coding::{Data, OpCode},
    };

    for cause in [
        MachineLinkCloseCode::Revoked,
        MachineLinkCloseCode::Inactive,
        MachineLinkCloseCode::Expired,
    ] {
        let mut fixture = Fixture::new().await;
        let mut socket = fixture.accept().await;
        assert_eq!(fixture.acquired.recv().await, Some(None));
        welcome_before_registration(&mut socket).await;
        let (correlation, _, _) = registration_request(&mut socket).await;

        // Queue both while the peer still owns the transport. Using the raw
        // sink here allows the fixture to represent a reply racing a close.
        let mut frames = Vec::new();
        Frame::close(Some(CloseFrame {
            code: CloseCode::from(cause.code()),
            reason: "".into(),
        }))
        .format(&mut frames)
        .unwrap();
        let refusal = json!({
            "kind": "centralCallReply",
            "correlationId": correlation,
            "result": {
                "kind": "refused",
                "reason": LinkCallRefusal::AuthorityLapsed { cause },
            },
        });
        Frame::message(refusal.to_string(), OpCode::Data(Data::Text), true)
            .format(&mut frames)
            .unwrap();
        socket.get_mut().write_all(&frames).await.unwrap();
        let ending = match cause {
            MachineLinkCloseCode::Revoked => Some(EnrollmentEnd::Revoked),
            MachineLinkCloseCode::Inactive => Some(EnrollmentEnd::Inactive),
            MachineLinkCloseCode::Expired => None,
        };
        let expected = ending.map_or(LinkStatus::Connecting, |cause| {
            LinkStatus::EnrollmentEnded { cause }
        });
        fixture
            .status
            .wait_for(|status| *status == expected)
            .await
            .unwrap();
        assert!(matches!(
            socket.next().await,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None
        ));
        assert_eq!(*fixture.status.borrow(), expected);
        assert!(
            fixture
                .inbox_events
                .lane
                .watch_registered()
                .borrow()
                .is_none()
        );
        if let Some(cause) = ending {
            assert_eq!(fixture.ended.recv().await, Some((1, cause)));
        } else {
            assert!(fixture.ended.try_recv().is_err());
        }
        fixture.stop().await;
    }
}
