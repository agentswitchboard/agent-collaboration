#![cfg(unix)]
#![expect(
    clippy::unwrap_used,
    reason = "The real-server tests fail at the exact unexpected setup or wire observation."
)]

use std::{sync::Arc, time::Duration};

use central_link_client::{
    CorrelatedRequestReply, CorrelatedStatusReply, EnrollmentEnd, ImmediateRequestCommand,
    LinkLaneDisposition, ReadyLinkLane, RequestStatusCommand,
};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, ReplyPayload, RequestScope, RequestStatusAnswer,
    RequestStatusQuery, decode_text,
};
use collaboration_central_conformance::signin_fixtures as signed;
use collaboration_central_tokio::test_support::{
    MACHINE_LINK_FIXTURE_MACHINE, MachineLinkFixture, start_machine_link_fixture,
};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use serde_json::json;
use switchboard_central_link::{
    DialAddress,
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource, LinkLease},
    link_driver::{DriverSettings, LinkDriver},
    link_lane::{LinkLaneHandle, link_lane_channel},
    link_policy::{LeaseVersion, LeaseWindow, LinkStatus},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

mod custody_journey;
#[cfg(debug_assertions)]
mod enrolled_lease_journey;
#[cfg(debug_assertions)]
mod first_message_local;
mod managed_prompt_fixture;
#[cfg(debug_assertions)]
mod message_unit_fixture;
#[cfg(debug_assertions)]
mod message_unit_journey;
#[cfg(debug_assertions)]
mod native_central_calls;
#[cfg(debug_assertions)]
mod native_work_carrier;
mod simulated_link;

struct RealLeases {
    central: Arc<MachineLinkFixture>,
    mac_clock: Arc<ManualClock>,
    acquisitions: mpsc::UnboundedSender<Option<LeaseVersion>>,
    endings: mpsc::UnboundedSender<(u8, EnrollmentEnd)>,
}

impl LeaseSource<u8> for RealLeases {
    fn registration_target(
        &self,
        _binding: &u8,
    ) -> Result<switchboard_central_link::lease_source::ApprovalServiceTarget, LeaseFailure> {
        let organization_id = self.central.organization_id.clone();
        Ok(switchboard_central_link::lease_source::ApprovalServiceTarget { organization_id })
    }

    fn acquire<'a>(
        &'a self,
        binding: &'a u8,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'a, u8> {
        Box::pin(async move {
            let token = if newer_than.is_some() {
                self.central
                    .mint_machine_token()
                    .await
                    .map_err(|_| LeaseFailure::Fault)?
            } else {
                self.central.token.clone()
            };
            let _ = self.acquisitions.send(newer_than);
            Ok(LinkLease {
                window: LeaseWindow {
                    binding: *binding,
                    version: LeaseVersion(newer_than.map_or(1, |old| old.0 + 1)),
                    usable_until: self
                        .mac_clock
                        .monotonic_now()
                        .after(Duration::from_secs(500)),
                },
                token: token.try_into().map_err(|_| LeaseFailure::Fault)?,
            })
        })
    }

    fn end(&self, binding: &u8, cause: EnrollmentEnd) {
        let _ = self.endings.send((*binding, cause));
    }
}

struct Journey {
    central: Arc<MachineLinkFixture>,
    mac_clock: Arc<ManualClock>,
    status: watch::Receiver<LinkStatus>,
    acquisitions: mpsc::UnboundedReceiver<Option<LeaseVersion>>,
    endings: mpsc::UnboundedReceiver<(u8, EnrollmentEnd)>,
    shutdown: watch::Sender<bool>,
    _settings: watch::Sender<DriverSettings<u8>>,
    lane: LinkLaneHandle,
    driver: JoinHandle<()>,
}

impl Journey {
    async fn start() -> Self {
        let signed_clock = "2030-03-17T17:46:40Z".parse().unwrap();
        let central = Arc::new(
            start_machine_link_fixture(Arc::new(ManualClock::new(signed_clock)))
                .await
                .unwrap(),
        );
        let mac_clock = Arc::new(ManualClock::new(signed_clock));
        let address = format!(
            "ws://{}/v1/organizations/{}/link",
            central.origin().strip_prefix("http://").unwrap(),
            central.organization_id.as_str()
        );
        let (settings, settings_rx) = watch::channel(DriverSettings {
            binding: Some(1),
            address: DialAddress::test_loopback(&address),
        });
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (status_tx, mut status) = watch::channel(LinkStatus::NotEnrolled);
        let (inbox, lane) = link_lane_channel();
        let (acquired_tx, mut acquisitions) = mpsc::unbounded_channel();
        let (ended_tx, endings) = mpsc::unbounded_channel();
        let driver = tokio::spawn(
            LinkDriver {
                machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
                clock: mac_clock.clone(),
                lease_source: Arc::new(RealLeases {
                    central: central.clone(),
                    mac_clock: mac_clock.clone(),
                    acquisitions: acquired_tx,
                    endings: ended_tx,
                }),
                settings: settings_rx,
                shutdown: shutdown_rx,
                status: status_tx,
                lane,
            }
            .run(),
        );
        assert_eq!(acquisitions.recv().await, Some(None));
        let actual = status
            .wait_for(|status| {
                matches!(
                    status,
                    LinkStatus::Connected { .. }
                        | LinkStatus::Fault { .. }
                        | LinkStatus::EnrollmentEnded { .. }
                )
            })
            .await
            .unwrap()
            .clone();
        assert_eq!(
            actual,
            LinkStatus::Connected {
                machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap()
            }
        );
        inbox
            .watch_admitted()
            .wait_for(Option::is_some)
            .await
            .unwrap();
        Self {
            central,
            mac_clock,
            status,
            acquisitions,
            endings,
            shutdown,
            _settings: settings,
            lane: inbox,
            driver,
        }
    }

    async fn close(self) {
        self.shutdown.send_replace(true);
        self.driver.await.unwrap();
        let central = Arc::into_inner(self.central);
        assert!(
            central.is_some(),
            "the driver must release its fixture lease source at shutdown"
        );
        central.unwrap().close().await.unwrap();
    }
}

#[tokio::test]
async fn real_machine_admission_allows_immediate_request_and_status_on_the_registered_socket() {
    let journey = Journey::start().await;
    let admitted_watch = journey.lane.watch_admitted();
    let admitted = admitted_watch.borrow().clone();
    assert!(
        admitted.is_some(),
        "Central's checked Welcome must enable the message lane"
    );
    let admitted = admitted.unwrap();
    assert_eq!(
        journey
            .lane
            .watch_registered()
            .borrow()
            .as_ref()
            .unwrap()
            .admitted(),
        &admitted
    );
    let wire = format!(
        r#"{{"kind":"request","request":{{"correlationId":"01890f2e-7b4c-7cc0-98c4-000000000031","caller":{{"kind":"session","value":{{"machineId":"{MACHINE_LINK_FIXTURE_MACHINE}","sessionId":"fixture-lane"}}}},"payload":{{"kind":"mail","request":{{"requestId":"01890f2e-7b4c-7cc0-98c4-000000000032","operation":{{"kind":"setNoticeCadence","cadence":"m25"}}}}}}}}}}"#
    );
    let LinkFrame::Data(DataFrame::Request { request }) =
        decode_text(&wire, FrameDirection::ToCentral).unwrap()
    else {
        panic!("fixture expected one immediate request");
    };
    let correlation_id = request.correlation_id().clone();
    let caller = request.caller().clone();
    let query = RequestStatusQuery {
        request_id: request.payload().write_request_id().unwrap().clone(),
        scope: RequestScope::Organization,
    };
    let result = journey
        .lane
        .submit_immediate_request(ImmediateRequestCommand {
            connection: admitted.connection.clone(),
            send_before: ClockInstant::ZERO.after(Duration::from_secs(3)),
            request,
        })
        .await;
    let expected: ReplyPayload = serde_json::from_value(json!({
        "kind":"mail", "reply":{"kind":"done", "value":{"kind":"noticeCadenceSet", "cadence":"m25"}}
    }))
    .unwrap();
    assert_eq!(
        result,
        LinkLaneDisposition::Answered {
            reply: CorrelatedRequestReply {
                correlation_id,
                result: Box::new(expected.clone()),
            },
            accepted_at: ClockInstant::ZERO,
        },
        "Central must successfully apply the submitted cadence"
    );
    let status_correlation = "01890f2e-7b4c-7cc0-98c4-000000000033"
        .parse::<central_link_protocol::CorrelationId>()
        .unwrap();
    let status = journey
        .lane
        .request_status(RequestStatusCommand {
            connection: admitted.connection,
            send_before: ClockInstant::ZERO.after(Duration::from_secs(3)),
            correlation_id: status_correlation.clone(),
            caller,
            query,
        })
        .await;
    assert_eq!(
        status,
        LinkLaneDisposition::Answered {
            reply: CorrelatedStatusReply {
                correlation_id: status_correlation,
                result: Box::new(RequestStatusAnswer::Answered(expected)),
            },
            accepted_at: ClockInstant::ZERO,
        },
        "status must read the exact stored cadence outcome for the same caller, scope and request ID"
    );
    journey.close().await;
}

#[tokio::test]
async fn real_central_renews_the_welcomed_socket_across_its_original_deadline() {
    let mut journey = Journey::start().await;
    journey
        .mac_clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(20)))
        .await;
    journey
        .central
        .clock
        .advance(Duration::from_secs(500))
        .unwrap();
    journey.mac_clock.advance(Duration::from_secs(540)).unwrap();
    assert_eq!(
        journey.acquisitions.recv().await,
        Some(Some(LeaseVersion(1)))
    );
    // Force a heartbeat after the renewal request. Its next deadline can reach
    // 610 only after Central has extended the original 600-second authority.
    journey.mac_clock.advance(Duration::from_secs(50)).unwrap();
    tokio::select! {
        () = journey.mac_clock.not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(610))) => {}
        status = journey.status.wait_for(|status| !matches!(status, LinkStatus::Connected { .. })) => {
            assert!(status.is_err(), "Central renewal must keep the original socket welcomed");
        }
    }
    journey.mac_clock.advance(Duration::from_secs(11)).unwrap();
    journey
        .mac_clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(610)))
        .await;
    assert!(matches!(
        &*journey.status.borrow(),
        LinkStatus::Connected { .. }
    ));
    assert!(journey.acquisitions.try_recv().is_err());
    journey.close().await;
}

#[tokio::test]
async fn real_expired_close_fetches_a_fresh_lease_without_ending_enrollment() {
    let mut journey = Journey::start().await;
    journey
        .mac_clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(20)))
        .await;
    journey
        .central
        .clock
        .advance(Duration::from_secs(601))
        .unwrap();
    journey.mac_clock.advance(Duration::from_secs(540)).unwrap();
    assert_eq!(
        journey.acquisitions.recv().await,
        Some(Some(LeaseVersion(1)))
    );
    journey
        .status
        .wait_for(|status| matches!(status, LinkStatus::Connecting))
        .await
        .unwrap();
    journey
        .mac_clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(541)))
        .await;
    journey.mac_clock.advance(Duration::from_secs(1)).unwrap();
    assert_eq!(
        journey.acquisitions.recv().await,
        Some(Some(LeaseVersion(2)))
    );
    assert!(journey.endings.try_recv().is_err());
    journey.close().await;
}

#[tokio::test]
async fn real_revocation_and_supersession_end_the_exact_lease_binding() {
    for expected in [EnrollmentEnd::Revoked, EnrollmentEnd::Inactive] {
        let mut journey = Journey::start().await;
        let organization = &journey.central.organization_id;
        let now = journey.central.clock.utc_now().timestamp();
        match expected {
            EnrollmentEnd::Revoked => {
                let client = reqwest::Client::builder()
                    .no_proxy()
                    .unix_socket(journey.central.signin_socket.clone())
                    .build()
                    .unwrap();
                let action = signed::RevocationJourney {
                    client: &client,
                    origin: "http://localhost",
                    organization: organization.as_str(),
                    now,
                };
                let owner = action.prepare().await.unwrap();
                action
                    .expect(
                        &owner,
                        journey.central.enrollment_id.as_str(),
                        json!({"result":"revoked"}),
                    )
                    .await
                    .unwrap();
            }
            EnrollmentEnd::Inactive => {
                let runtime = journey.central.host.central(organization).await.unwrap();
                let request = serde_json::from_value(json!({
                    "organizationId": organization, "approver": signed::OWNER,
                    "permissionChecked":"machine:enroll", "requestId":"019f0000-0000-7000-8000-00000000cf08",
                    "machineId": MACHINE_LINK_FIXTURE_MACHINE, "label":"Replacement binding",
                    "allowedWorkspaces":[], "approverSecondFactorAt":now-60, "codeExpiresAt":now+600,
                })).unwrap();
                let replacement =
                    serde_json::to_value(runtime.decide_device_login(&request).await.unwrap())
                        .unwrap();
                let enrollment = replacement
                    .get("enrollmentId")
                    .and_then(|id| id.as_str())
                    .unwrap();
                let request = serde_json::from_value(json!({
                    "organizationId": organization, "enrollmentId": enrollment, "clientId": enrollment,
                })).unwrap();
                let grant =
                    serde_json::to_value(runtime.decide_machine_token(&request).await.unwrap())
                        .unwrap();
                assert_eq!(grant.get("result"), Some(&json!("grant")));
            }
            EnrollmentEnd::LeaseRefused | EnrollmentEnd::ConnectionEnrollmentEnded => {}
        }
        assert_eq!(journey.endings.recv().await, Some((1, expected)));
        assert_eq!(
            *journey.status.borrow(),
            LinkStatus::EnrollmentEnded { cause: expected }
        );
        assert!(journey.acquisitions.try_recv().is_err());
        journey.close().await;
    }
}
