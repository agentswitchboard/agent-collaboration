use std::time::Duration;

use central_link_client::{LinkFault, ReadyLinkLane};
use switchboard_central_link::{
    DialAddress,
    link_policy::{LeaseVersion, LinkStatus},
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{ErrorResponse, Request, Response},
        http::StatusCode,
    },
};

use super::{Fixture, tick, welcome};

#[expect(
    clippy::result_large_err,
    reason = "The handshake failure includes the fixture HTTP response."
)]
async fn reject_upgrade(fixture: &Fixture, status: StatusCode) {
    let (stream, _) = fixture.listener.accept().await.unwrap();
    let rejection = accept_hdr_async(stream, |_request: &Request, _response: Response| {
        let mut rejection = ErrorResponse::new(None);
        *rejection.status_mut() = status;
        Err(rejection)
    })
    .await;
    assert!(rejection.is_err());
}

async fn replay_settings(fixture: &mut Fixture) {
    let last_poll = *fixture.driver_polls.borrow_and_update();
    let same_settings = fixture.settings.borrow().clone();
    fixture.settings.send_replace(same_settings);
    fixture
        .driver_polls
        .wait_for(|poll| *poll > last_poll)
        .await
        .unwrap();
}

#[tokio::test]
async fn replayed_bad_address_stays_parked_until_the_address_changes() {
    let mut fixture = Fixture::new().await;
    reject_upgrade(&fixture, StatusCode::BAD_REQUEST).await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    let fault = LinkStatus::Fault {
        kind: LinkFault::MalformedAddress,
    };
    fixture
        .status
        .wait_for(|status| status == &fault)
        .await
        .unwrap();

    replay_settings(&mut fixture).await;
    assert_eq!(
        *fixture.status.borrow(),
        fault,
        "same address cannot rearm the 400 fault"
    );
    assert!(fixture.acquired.try_recv().is_err());

    fixture.settings.send_modify(|settings| {
        settings.address = DialAddress::test_loopback(&format!(
            "ws://{}/corrected/link",
            fixture.listener.local_addr().unwrap()
        ));
    });
    let mut socket = fixture.accept().await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    welcome(&mut socket).await;
    fixture
        .status
        .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
        .await
        .unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn replayed_settings_preserve_first_401_backoff_and_lease_floor() {
    let mut fixture = Fixture::new().await;
    reject_upgrade(&fixture, StatusCode::UNAUTHORIZED).await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    fixture.clock.not_due_check_now(tick(1)).await;

    replay_settings(&mut fixture).await;
    assert!(
        fixture.acquired.try_recv().is_err(),
        "same settings cannot acquire a lease during the 401 backoff"
    );
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    assert_eq!(
        fixture.acquired.recv().await,
        Some(Some(LeaseVersion(1))),
        "401 retry requires a newer lease"
    );
    let mut replacement = fixture.accept().await;
    welcome(&mut replacement).await;
    fixture
        .status
        .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
        .await
        .unwrap();
    assert!(
        fixture
            .inbox_events
            .lane
            .watch_registered()
            .borrow()
            .is_some()
    );
    fixture.stop().await;
}

#[tokio::test]
async fn replayed_settings_preserve_second_401_fault_until_elapsed_rearm() {
    let mut fixture = Fixture::new().await;
    reject_upgrade(&fixture, StatusCode::UNAUTHORIZED).await;
    assert_eq!(fixture.acquired.recv().await, Some(None));
    fixture.clock.not_due_check_now(tick(1)).await;
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    reject_upgrade(&fixture, StatusCode::UNAUTHORIZED).await;
    assert_eq!(fixture.acquired.recv().await, Some(Some(LeaseVersion(1))));
    fixture.clock.not_due_check_now(tick(61)).await;

    replay_settings(&mut fixture).await;
    assert_eq!(
        *fixture.status.borrow(),
        LinkStatus::Fault {
            kind: LinkFault::FreshCredentialRefused
        },
        "same settings cannot rearm the second 401 fault"
    );
    assert!(fixture.acquired.try_recv().is_err());
    fixture.clock.advance(Duration::from_secs(60)).unwrap();
    assert_eq!(fixture.acquired.recv().await, Some(None));
    fixture.stop().await;
}
