use std::{sync::Arc, time::Duration};

use central_link_client::{ClientAction, ConnectionAttempt, EnrollmentEnd, LinkClient};
use central_link_protocol::{DataFrame, LinkFrame};
use futures_util::{FutureExt, sink::unfold};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use tokio::sync::{oneshot, watch};
use tokio_tungstenite::tungstenite::Message;
use workspace_ids::MachineId;

use super::{BindingDriver, Control, DriverSettings, Interrupted, SocketWriteFailure};
use crate::{
    AddressError,
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource},
    link_policy::{LeaseVersion, LinkState, LinkStatus},
};

#[path = "renewal_lane_test.rs"]
mod renewal_lane;

struct NoLeaseSource;

impl LeaseSource<u8> for NoLeaseSource {
    fn acquire<'source>(
        &'source self,
        _binding: &'source u8,
        _newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'source, u8> {
        Box::pin(async { Err(LeaseFailure::Fault) })
    }

    fn end(&self, _binding: &u8, _cause: EnrollmentEnd) {}
}

struct HeartbeatFixture {
    machine: MachineId,
    clock: Arc<ManualClock>,
    status: watch::Sender<LinkStatus>,
}

impl HeartbeatFixture {
    fn new() -> Self {
        Self {
            machine: "01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap(),
            clock: Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap())),
            status: watch::channel(LinkStatus::Connecting).0,
        }
    }

    fn welcomed_driver<'fixture, Source>(
        &'fixture self,
        lease_source: &'fixture Source,
    ) -> (BindingDriver<'fixture, u8, Source>, ConnectionAttempt) {
        let mut client = LinkClient::new(self.machine.clone(), self.clock.clone());
        let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
            panic!("first connection should be due");
        };
        client.dial_started(&attempt).unwrap();
        client
            .receive(
                &attempt,
                LinkFrame::Data(DataFrame::Welcome {
                    machine_id: self.machine.clone(),
                    central_time: self.clock.utc_now(),
                    effective_expires_at: "2030-01-01T00:05:00Z".parse().unwrap(),
                }),
            )
            .unwrap();
        (
            BindingDriver {
                lease_source,
                clock: self.clock.as_ref(),
                status: &self.status,
                address: Err(AddressError::InvalidLocation),
                machine_id: self.machine.clone(),
                state: LinkState::new(self.machine.clone()),
                client,
                lease_retry_at: None,
            },
            attempt,
        )
    }
}

fn test_settings() -> DriverSettings<u8> {
    DriverSettings {
        binding: Some(1),
        address: Err(AddressError::InvalidLocation),
    }
}

fn elapsed_tick(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

#[tokio::test]
async fn both_heartbeat_windows_start_at_their_completed_writes() {
    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = fixture.welcomed_driver(&NoLeaseSource);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };

    for response_deadline in [43, 66] {
        fixture.clock.advance(Duration::from_secs(20)).unwrap();
        assert_eq!(driver.client.poll_time(), Some(ClientAction::SendPing));
        assert_eq!(
            driver.client.next_deadline(),
            None,
            "a queued ping has no response window"
        );
        let (release, wait) = oneshot::channel::<()>();
        let mut held = Some(wait);
        let mut socket = Box::pin(unfold((), |(), message| {
            let wait = held.take().unwrap();
            async move {
                assert_eq!(message, Message::Text("ping".into()));
                wait.await.unwrap();
                Ok::<(), ()>(())
            }
        }));
        let mut write = Box::pin(driver.write_heartbeat(&mut socket, &attempt, &mut control));
        assert!(write.as_mut().now_or_never().is_none());
        fixture.clock.advance(Duration::from_secs(3)).unwrap();
        assert!(write.as_mut().now_or_never().is_none());
        release.send(()).unwrap();
        assert!(matches!(write.await, Ok(true)));
        assert_eq!(
            driver.client.next_deadline(),
            Some(elapsed_tick(response_deadline))
        );
    }
    fixture.clock.advance(Duration::from_secs(19)).unwrap();
    assert_eq!(driver.client.poll_time(), None);
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    assert!(matches!(
        driver.client.poll_time(),
        Some(ClientAction::Reconnect { .. })
    ));
}

#[tokio::test]
async fn refused_heartbeat_write_retries_without_a_probe_window() {
    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = fixture.welcomed_driver(&NoLeaseSource);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    fixture.clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(driver.client.poll_time(), Some(ClientAction::SendPing));
    assert_eq!(
        driver.client.next_deadline(),
        None,
        "writer refusal cannot spend a probe window"
    );
    let mut socket = Box::pin(unfold((), |(), _message| async { Err::<(), ()>(()) }));
    assert!(matches!(
        driver
            .write_heartbeat(&mut socket, &attempt, &mut control)
            .await,
        Ok(false)
    ));
    assert_eq!(driver.client.next_deadline(), Some(elapsed_tick(21)));
}

#[tokio::test]
async fn shutdown_with_a_pending_heartbeat_leaves_no_probe_window() {
    let fixture = HeartbeatFixture::new();
    let (mut driver, attempt) = fixture.welcomed_driver(&NoLeaseSource);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    fixture.clock.advance(Duration::from_secs(20)).unwrap();
    assert_eq!(driver.client.poll_time(), Some(ClientAction::SendPing));
    let mut socket = Box::pin(unfold((), |(), _message| {
        std::future::pending::<Result<(), ()>>()
    }));
    let mut write = Box::pin(driver.write_heartbeat(&mut socket, &attempt, &mut control));
    assert!(write.as_mut().now_or_never().is_none());
    shutdown_sender.send_replace(true);
    assert!(matches!(write.await, Err(Interrupted::Stopped)));
    assert_eq!(
        driver.client.next_deadline(),
        None,
        "withdrawn write cannot spend a probe window"
    );
}

#[tokio::test]
async fn oversized_renewal_credential_is_rejected_before_writing_a_frame() {
    use crate::socket_upgrade::MAX_CREDENTIAL_BYTES;

    let fixture = HeartbeatFixture::new();
    let (mut driver, _) = fixture.welcomed_driver(&NoLeaseSource);
    let (_settings_sender, mut settings) = watch::channel(test_settings());
    let (_shutdown_sender, mut shutdown) = watch::channel(false);
    let mut control = Control {
        settings: &mut settings,
        shutdown: &mut shutdown,
        observed: test_settings(),
    };
    let mut socket = Box::pin(unfold((), |(), _message| async { Ok::<(), ()>(()) }));
    let frame = LinkFrame::Data(DataFrame::RenewMachineToken {
        correlation_id: "01890f2e-7b4c-7cc0-98c4-000000000099".parse().unwrap(),
        token: "a".repeat(MAX_CREDENTIAL_BYTES + 1).try_into().unwrap(),
    });
    assert!(
        matches!(
            driver.write(&mut socket, frame, &mut control).await,
            Ok(Err(SocketWriteFailure::SizeLimit))
        ),
        "a renewal credential over the bearer bound must fail before the sink"
    );
}
