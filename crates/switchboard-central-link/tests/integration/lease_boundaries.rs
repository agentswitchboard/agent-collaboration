use std::time::Duration;

use central_link_client::EnrollmentEnd;
use futures_util::{FutureExt, SinkExt};
use switchboard_central_link::{
    lease_source::LeaseFailure,
    link_policy::{LeaseVersion, LinkStatus},
};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;

use super::{DELIVER_FRAME, Fixture, text, tick, welcome};

#[tokio::test]
async fn held_initial_lease_does_not_spend_the_welcome_budget() {
    let (release, gate) = oneshot::channel();
    let mut fixture = Fixture::with_initial_lease(Some(gate)).await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    assert!(
        !fixture.clock.has_not_due_check_now(tick(10)),
        "a held lease must not arm the Welcome timer"
    );
    assert!(fixture.listener.accept().now_or_never().is_none());
    fixture.clock.advance(Duration::from_secs(40)).unwrap();
    release.send(Ok(())).unwrap();
    let mut socket = fixture.accept().await;
    let outcome = tokio::select! {
        () = fixture.clock.not_due_check_now(tick(50)) => 50,
        () = fixture.clock.not_due_check_now(tick(49)) => 49,
    };
    assert_eq!(
        outcome, 50,
        "the dial's Welcome timer starts after lease acquisition"
    );
    welcome(&mut socket).await;
    fixture
        .status
        .wait_for(|status| matches!(status, LinkStatus::Welcomed { .. }))
        .await
        .unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn final_lease_source_refusal_ends_the_exact_binding_without_dialing() {
    let (release, gate) = oneshot::channel();
    let mut fixture = Fixture::with_initial_lease(Some(gate)).await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    let previous_poll = *fixture.driver_polls.borrow_and_update();
    release.send(Err(LeaseFailure::EnrollmentEnded)).unwrap();
    fixture
        .driver_polls
        .wait_for(|poll| *poll > previous_poll)
        .await
        .unwrap();
    assert_eq!(
        *fixture.status.borrow(),
        LinkStatus::EnrollmentEnded {
            cause: EnrollmentEnd::LeaseRefused
        }
    );
    assert_eq!(
        fixture.ended.recv().await,
        Some((1, EnrollmentEnd::LeaseRefused))
    );
    assert!(fixture.listener.accept().now_or_never().is_none());
    fixture.shutdown.send_replace(true);
    fixture.task.await.unwrap();
    assert_eq!(fixture.ended.recv().await, None);
}

#[tokio::test]
async fn wall_clock_changes_in_both_directions_leave_elapsed_lease_timers_unchanged() {
    let mut fixture = Fixture::new().await;
    let mut socket = fixture.accept().await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    welcome(&mut socket).await;
    fixture
        .status
        .wait_for(|status| matches!(status, LinkStatus::Welcomed { .. }))
        .await
        .unwrap();
    fixture.clock.not_due_check_now(tick(20)).await;

    for utc in ["2070-04-01T00:00:00Z", "1990-01-01T00:00:00Z"] {
        fixture.clock.set_utc(utc.parse().unwrap());
        socket
            .send(Message::Text(DELIVER_FRAME.into()))
            .await
            .unwrap();
        let still_welcomed = tokio::select! {
            event = fixture.inbox_events.recv() => event.is_some(),
            _ = fixture.status.wait_for(|status| !matches!(status, LinkStatus::Welcomed { .. })) => false,
        };
        assert!(
            still_welcomed,
            "changing UTC alone must leave the same socket welcomed"
        );
        assert!(
            fixture.acquired.try_recv().is_err(),
            "changing UTC alone must not acquire a new lease"
        );
    }
    fixture.clock.advance(Duration::from_secs(60)).unwrap();
    assert_eq!(text(&mut socket).await, "ping");
    assert_eq!(fixture.acquired.recv().await, Some(Some(LeaseVersion(1))));
    assert!(text(&mut socket).await.contains("renewMachineToken"));
    fixture.stop().await;
}
