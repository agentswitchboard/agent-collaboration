use std::{sync::Arc, time::Duration};

use central_link_client::ReadyLinkLane;
use central_link_protocol::{DataFrame, LinkFrame};
use central_storage::{SqlValue, Statement};
use collaboration_central_conformance::signin_fixtures as signed;
use router_clock::RuntimeClock;
use switchboard_central_link::{
    DialAddress,
    link_driver::{DriverSettings, LinkDriver},
    link_lane::link_lane_channel,
    link_policy::LinkStatus,
};
use tokio::sync::watch;
use turmoil::net::TcpStream;

use super::{
    CENTRAL, LINK_PORT, MACHINE_LINK_FIXTURE_MACHINE, SimulatedLeases,
    packet_flight::{FrameKind, PacketFlight},
    run_native_central_case,
    simulation_clock::ObservedClock,
};

#[test]
fn a_valid_token_for_another_enrolled_machine_never_admits_the_local_driver() {
    run_native_central_case(
        PacketFlight::new(),
        None,
        |clock, network, central| async move {
            let other_machine = "019f0000-0000-7000-8000-00000000ce07";
            let private = reqwest::Client::builder()
                .no_proxy()
                .unix_socket(central.signin_socket.clone())
                .build()
                .unwrap();
            let other_token = signed::enroll_another_machine(
                &private,
                "http://localhost",
                central.organization_id.as_str(),
                other_machine,
                "019f0000-0000-7000-8000-00000000ce08",
                clock.utc_now().timestamp(),
            )
            .await
            .unwrap();
            let (_current_token, token) = watch::channel(other_token);
            let address = format!(
                "ws://127.0.0.1:{LINK_PORT}/v1/organizations/{}/link",
                central.organization_id.as_str()
            );
            let (_settings, settings) = watch::channel(DriverSettings {
                binding: Some(1),
                address: DialAddress::test_loopback(&address),
            });
            let (shutdown, stopping) = watch::channel(false);
            let (status, progress) = watch::channel(LinkStatus::NotEnrolled);
            let (lane, owner) = link_lane_channel();
            let driver_clock = Arc::new(ObservedClock::new(clock.clone()));
            let driver = tokio::spawn(
                LinkDriver {
                    machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
                    clock: driver_clock.clone(),
                    lease_source: Arc::new(SimulatedLeases {
                        central: central.clone(),
                        clock: clock.clone(),
                        token,
                    }),
                    settings,
                    shutdown: stopping,
                    status,
                    lane: owner,
                }
                .run_with_test_transport(|| TcpStream::connect((CENTRAL, LINK_PORT))),
            );

            let mut previous_port = None;
            for (attempt, delay) in [1, 2, 4, 8, 16, 30, 30].into_iter().enumerate() {
                let welcome = network.seen_nth(0, FrameKind::Welcome, attempt).await;
                assert!(
                    matches!(welcome.frame, LinkFrame::Data(DataFrame::Welcome { machine_id, .. })
                if machine_id.as_str() == other_machine)
                );
                assert_eq!(welcome.sent_at, clock.monotonic_now());
                assert_ne!(Some(welcome.connection_port), previous_port);
                previous_port = Some(welcome.connection_port);

                // The same injected time is held through Welcome and socket close.
                // The driver's next retry is observable only after it closes the carrier.
                let retry_at = welcome.sent_at + Duration::from_secs(delay);
                let mut admitted = lane.watch_admitted();
                tokio::select! {
                    () = driver_clock.wait_until_pending(retry_at) => {}
                    observed = admitted.wait_for(Option::is_some) => {
                        panic!("the driver admitted another machine's Welcome: {observed:?}");
                    }
                }
                assert_eq!(*progress.borrow(), LinkStatus::Connecting);
                assert!(lane.watch_admitted().borrow().is_none());
                assert!(lane.watch_registered().borrow().is_none());
                if attempt < 6 {
                    clock.advance(Duration::from_secs(delay)).unwrap();
                }
            }

            shutdown.send_replace(true);
            driver.await.unwrap();
            assert!(
                network
                    .frames()
                    .iter()
                    .all(|frame| frame.kind == FrameKind::Welcome),
                "the refused sockets must carry no capability advertisement, registration or application frame"
            );

            let runtime = central
                .host
                .central(&central.organization_id)
                .await
                .unwrap();
            let store = runtime.runtime().statement_host();
            let registered = store
                .read(Statement {
                    sql: "SELECT router_run_id FROM approval_router_runs WHERE machine_id = ?1",
                    args: vec![SqlValue::Text(MACHINE_LINK_FIXTURE_MACHINE.to_owned())],
                })
                .await
                .unwrap();
            assert!(
                registered.rows.is_empty(),
                "the local machine must have no Central run"
            );
            let writes = store
                .read(Statement {
                    sql: "SELECT request_id FROM runtime_request_log",
                    args: vec![],
                })
                .await
                .unwrap();
            assert!(
                writes.rows.is_empty(),
                "the isolated Central accepted no application writes"
            );
        },
    );
}
