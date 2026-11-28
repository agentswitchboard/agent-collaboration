//! A Welcome naming another machine ends dialing; a late Welcome closes and retries.

use std::time::Duration;

use central_link_client::{ClientAction, LinkClient, LinkFault, LinkStatus as ClientLinkStatus};
use central_link_protocol::{DataFrame, LinkFrame};
use hyper_util::client::legacy::connect::HttpConnector;
use router_clock::RuntimeClock;
use workspace_ids::MachineId;

use super::{HeartbeatFixture, NoLeaseSource, WelcomedDriver, elapsed_tick};
use crate::{
    AddressError,
    admitted_lane_socket::AdmittedSocketLane,
    link_driver::{BindingDriver, receive::SocketReceive},
    link_lane::link_lane_channel,
    link_policy::{LeaseVersion, LeaseWindow, LinkInput, LinkState, LinkStatus},
    run_registration::RunRegistration,
    wire_write_fence::{WireWriteFence, default_http_connector},
};

const OTHER_MACHINE: &str = "01890f2e-7b4c-7cc0-98c4-000000000099";

fn dialing_driver(
    fixture: &HeartbeatFixture,
) -> (
    WelcomedDriver<'_, NoLeaseSource>,
    central_link_client::ConnectionAttempt,
) {
    let mut client = LinkClient::new(fixture.machine.clone(), fixture.clock.clone());
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("first connection should be due");
    };
    client.dial_started(&attempt).unwrap();
    let mut driver = BindingDriver {
        lease_source: &NoLeaseSource,
        make_connector: &(default_http_connector as fn() -> HttpConnector),
        clock: fixture.clock.clone(),
        status: &fixture.status,
        address: Err(AddressError::InvalidLocation),
        machine_id: fixture.machine.clone(),
        state: LinkState::new(fixture.machine.clone()),
        registration: RunRegistration::new(1, client.mint_router_run().unwrap()),
        client,
        lease_retry_at: None,
    };
    assert!(driver.apply(LinkInput::BindingAvailable(1)).is_some());
    assert!(
        driver
            .apply(LinkInput::DialStarted {
                attempt: attempt.clone(),
                lease: LeaseWindow {
                    binding: 1,
                    version: LeaseVersion(1),
                    usable_until: elapsed_tick(200),
                },
                now: elapsed_tick(0),
            })
            .is_some()
    );
    (driver, attempt)
}

fn welcome_naming(fixture: &HeartbeatFixture, machine_id: MachineId) -> LinkFrame {
    LinkFrame::Data(DataFrame::Welcome {
        machine_id,
        central_time: fixture.clock.utc_now(),
        effective_expires_at: "2030-01-01T00:05:00Z".parse().unwrap(),
    })
}

/// Receive one frame, then close the carrier the way the socket loop does on `Close`.
fn receive_then_close(
    driver: &mut WelcomedDriver<'_, NoLeaseSource>,
    attempt: &central_link_client::ConnectionAttempt,
    frame: LinkFrame,
) {
    let (_user, owner) = link_lane_channel();
    let write_fence = WireWriteFence::new(driver.clock.clone());
    let mut lane = AdmittedSocketLane::new(&owner, &write_fence);
    let received = driver.receive_frame(attempt, frame, elapsed_tick(0), None, &mut lane, None);
    assert!(matches!(received, SocketReceive::Close));
    driver.close(attempt, None);
}

#[test]
fn a_welcome_for_another_machine_parks_both_policies_until_the_address_changes() {
    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = dialing_driver(&fixture);

    receive_then_close(
        &mut driver,
        &attempt,
        welcome_naming(&fixture, OTHER_MACHINE.parse().unwrap()),
    );

    let faulted = LinkStatus::Fault {
        kind: LinkFault::WrongMachine,
    };
    assert_eq!(driver.state.status(), faulted);
    assert_eq!(*fixture.status.borrow(), faulted);
    assert_eq!(
        driver.client.status(),
        ClientLinkStatus::Fault {
            kind: LinkFault::WrongMachine
        }
    );
    fixture
        .clock
        .advance(Duration::from_secs(7 * 24 * 60 * 60))
        .unwrap();
    assert_eq!(driver.client.poll_time(), None);
    assert_eq!(driver.client.next_deadline(), None);
    assert_eq!(driver.state.next_deadline(), None);
    assert_eq!(driver.state.dial_requirement(), None);

    // A changed address is applied exactly as `run_binding` applies it.
    assert!(driver.apply(LinkInput::AddressCorrected).is_some());
    assert!(driver.client.retry_after_fault());
    assert!(matches!(
        driver.client.poll_time(),
        Some(ClientAction::Connect { .. })
    ));
}

#[test]
fn a_late_welcome_for_this_machine_closes_and_retries() {
    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = dialing_driver(&fixture);
    fixture.clock.advance(Duration::from_secs(10)).unwrap();

    receive_then_close(
        &mut driver,
        &attempt,
        welcome_naming(&fixture, fixture.machine.clone()),
    );

    assert_eq!(driver.state.status(), LinkStatus::Connecting);
    assert_eq!(driver.state.dial_requirement(), Some(None));
    assert_eq!(driver.client.status(), ClientLinkStatus::Connecting);
    assert_eq!(
        driver.client.next_deadline(),
        Some(fixture.clock.monotonic_now().after(Duration::from_secs(1)))
    );
}
