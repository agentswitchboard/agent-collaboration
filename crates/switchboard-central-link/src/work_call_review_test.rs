use std::sync::atomic::Ordering;
use std::time::Duration;

use central_link_client::ReadyLinkLane;
use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, FrameDirection, LinkFrame, TokenRenewalResult,
    TypedCallResult, encode_text,
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use router_clock::{ClockInstant, RuntimeClock};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

use super::{
    CallNotReached, CallSettlement, CentralCallSubmission, LeaseVersion, WorkSocket,
    WriteCheckpoint, elapsed_tick, end_call, end_call_for_session, poll_driver, read_frame, submit,
};

const CANCELLED_SESSION: &str = "019f0000-0000-7000-8000-000000000013";

async fn send_frame(fixture: &mut WorkSocket, frame: LinkFrame) {
    let text = encode_text(&frame, FrameDirection::ToRouter).unwrap();
    fixture.peer.send(Message::Text(text.into())).await.unwrap();
}

fn next_work_frame(fixture: &mut WorkSocket) -> (CorrelationId, String) {
    let Some(LinkFrame::Data(DataFrame::CallCentral {
        correlation_id,
        request,
    })) = read_frame(&mut fixture.peer).now_or_never()
    else {
        panic!("the original socket must receive a complete work frame");
    };
    let CentralCall::EndFleetSession(request) = *request else {
        panic!("the caller offered EndFleetSession");
    };
    let body = serde_json::to_value(request.body).unwrap();
    let target = body["target"]["sessionId"].as_str().unwrap().to_owned();
    (correlation_id, target)
}

async fn answer_work(fixture: &mut WorkSocket, correlation_id: CorrelationId) {
    send_frame(fixture, LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result: Box::new(TypedCallResult::Answered { reply: serde_json::from_value(json!({
            "call":"endFleetSession", "envelope": {
                "target":"019f0000-0000-7000-8000-000000000001", "body":{"result":"noSuchStart"}
            }
        })).unwrap() }),
    })).await;
}

async fn heartbeat_after(fixture: &mut WorkSocket, seconds: u64) {
    fixture.clock.advance(Duration::from_secs(seconds)).unwrap();
    assert!(poll_driver(&fixture.turns).await);
    assert_eq!(
        read_frame(&mut fixture.peer).now_or_never(),
        Some(LinkFrame::Ping)
    );
    send_frame(fixture, LinkFrame::Pong).await;
    assert!(poll_driver(&fixture.turns).await);
}

#[tokio::test]
async fn work_one_tick_before_renewal_waits_and_answers_on_the_original_socket_after_expiry() {
    for raw_budget in [0, 1] {
        let mut fixture = WorkSocket::registered_with_lease_lifetime(120).await;
        heartbeat_after(&mut fixture, 20).await;
        heartbeat_after(&mut fixture, 20).await;
        fixture.clock.advance(Duration::from_secs(19)).unwrap();
        fixture.write_budget.store(raw_budget, Ordering::Relaxed);
        fixture.crossed.store(0, Ordering::Relaxed);
        let polls_before_work = fixture.write_polls.load(Ordering::Relaxed);
        let accepted = submit(&fixture.lane, elapsed_tick(150));
        let connection = accepted.connection().clone();
        let mut answer = Box::pin(accepted.resolve());
        assert!(poll_driver(&fixture.turns).await);
        let near_renewal_write_polls = fixture.write_polls.load(Ordering::Relaxed);
        let near_renewal_bytes = fixture.crossed.load(Ordering::Relaxed);
        assert!(answer.as_mut().now_or_never().is_none());
        assert!(fixture.renewed_from.try_recv().is_err());

        fixture.write_budget.store(4096, Ordering::Relaxed);
        fixture.clock.advance(Duration::from_secs(1)).unwrap();
        assert!(
            poll_driver(&fixture.turns).await,
            "queued work must leave the original socket alive at renewal"
        );
        assert_eq!(
            near_renewal_write_polls, polls_before_work,
            "work must stay queued before renewal"
        );
        assert_eq!(near_renewal_bytes, 0);
        assert_eq!(fixture.renewed_from.try_recv().unwrap(), LeaseVersion(1));
        assert_eq!(
            read_frame(&mut fixture.peer).now_or_never(),
            Some(LinkFrame::Ping)
        );
        let Some(LinkFrame::Data(DataFrame::RenewMachineToken { correlation_id, .. })) =
            read_frame(&mut fixture.peer).now_or_never()
        else {
            panic!("renewal must precede the deferred work frame");
        };
        send_frame(&mut fixture, LinkFrame::Pong).await;
        let central_time = fixture.clock.utc_now();
        send_frame(
            &mut fixture,
            LinkFrame::Data(DataFrame::MachineTokenRenewal {
                correlation_id,
                result: TokenRenewalResult::Renewed {
                    central_time,
                    effective_expires_at: central_time + Duration::from_secs(300),
                },
            }),
        )
        .await;
        assert!(poll_driver(&fixture.turns).await);
        let (correlation_id, _) = next_work_frame(&mut fixture);
        assert!(answer.as_mut().now_or_never().is_none());
        for _ in 0..3 {
            heartbeat_after(&mut fixture, 20).await;
        }
        fixture.clock.advance(Duration::from_secs(1)).unwrap();
        answer_work(&mut fixture, correlation_id).await;
        assert!(poll_driver(&fixture.turns).await);
        assert!(matches!(
            answer.as_mut().now_or_never(),
            Some(CallSettlement::Answered(_))
        ));
        assert_eq!(fixture.clock.monotonic_now(), elapsed_tick(121));
        let registered = fixture.lane.watch_registered();
        assert_eq!(
            registered.borrow().as_ref().unwrap().admitted().connection,
            connection
        );
        assert!(fixture.peer.next().now_or_never().is_none());
        fixture.stop().await;
    }
}

#[tokio::test]
async fn dropping_a_queued_caller_frees_the_slot_without_writing_its_frame() {
    let mut fixture = WorkSocket::registered().await;
    let work = fixture.lane.central_calls();
    let deadline = elapsed_tick(30);
    let CentralCallSubmission::Accepted(cancelled) =
        work.submit(end_call_for_session(CANCELLED_SESSION), deadline)
    else {
        panic!("the call to cancel must hold the first slot");
    };
    let connection = cancelled.connection().clone();
    let waiting: Vec<_> = (0..7).map(|_| submit(&fixture.lane, deadline)).collect();
    assert!(matches!(
        work.submit(end_call(), deadline),
        CentralCallSubmission::NotReached(CallNotReached::LaneFull)
    ));

    drop(cancelled);
    let replacement = submit(&fixture.lane, deadline);
    assert_eq!(replacement.connection(), &connection);
    fixture.write_budget.store(4096, Ordering::Relaxed);
    for _ in 0..8 {
        assert!(poll_driver(&fixture.turns).await);
        assert_ne!(next_work_frame(&mut fixture).1, CANCELLED_SESSION);
    }
    assert!(fixture.peer.next().now_or_never().is_none());
    assert_eq!(fixture.clock.monotonic_now(), ClockInstant::ZERO);
    fixture.stop().await;
    drop((waiting, replacement));
}

#[tokio::test]
async fn dropping_a_caller_during_a_partial_write_releases_the_slot_at_disposition() {
    let mut fixture = WorkSocket::registered().await;
    let work = fixture.lane.central_calls();
    let deadline = elapsed_tick(30);
    let CentralCallSubmission::Accepted(cancelled) =
        work.submit(end_call_for_session(CANCELLED_SESSION), deadline)
    else {
        panic!("the writing call must hold the first slot");
    };
    let connection = cancelled.connection().clone();
    fixture.write_budget.store(1, Ordering::Relaxed);
    assert!(poll_driver(&fixture.turns).await);
    assert_eq!(fixture.crossed.load(Ordering::Relaxed), 1);
    let waiting: Vec<_> = (0..7).map(|_| submit(&fixture.lane, deadline)).collect();

    drop(cancelled);
    assert!(matches!(
        work.submit(end_call(), deadline),
        CentralCallSubmission::NotReached(CallNotReached::LaneFull)
    ));
    // The active frame is already flushing. Hold the next frame at readiness
    // so the replacement can only reuse the cancelled call's slot.
    fixture.gate.hold.set(WriteCheckpoint::Readiness).unwrap();
    fixture.write_budget.store(4096, Ordering::Relaxed);
    assert!(poll_driver(&fixture.turns).await);
    assert!(fixture.gate.observed.load(Ordering::Relaxed));
    assert_eq!(next_work_frame(&mut fixture).1, CANCELLED_SESSION);
    let replacement = submit(&fixture.lane, deadline);
    assert_eq!(replacement.connection(), &connection);
    assert_eq!(fixture.clock.monotonic_now(), ClockInstant::ZERO);
    fixture.stop().await;
    drop((waiting, replacement));
}
