use std::{sync::Arc, time::Duration};

use central_link_client::{ClientAction, EnrollmentEnd, LinkClient, LinkFault, WelcomeClockAnchor};
use router_clock::ManualClock;

use super::{
    AdmittedConnection, ApprovalServiceTarget, AuthorityEnd, ClockInstant, Exchange,
    LinkCallRefusal, LinkStatus, MachineId, PendingRunRegistration, RegistrationFailure,
    RunRegistration,
};

#[test]
fn connected_requires_ready_exchange_and_welcomed_transport_and_parked_overrides_both() {
    let machine: MachineId = "01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap();
    let utc = "2030-01-01T00:00:00Z".parse().unwrap();
    let clock = Arc::new(ManualClock::new(utc));
    let mut client = LinkClient::new(machine.clone(), clock);
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("expected first connection attempt");
    };
    let mut registration = RunRegistration::new(1, client.mint_router_run().unwrap());
    let admitted = AdmittedConnection {
        connection: attempt.clone(),
        clock_anchor: WelcomeClockAnchor {
            central_time: utc,
            observed_at: ClockInstant::ZERO,
        },
    };
    let target = ApprovalServiceTarget {
        organization_id: "01890f2e-7b4c-7cc0-98c4-000000000005"
            .to_owned()
            .try_into()
            .unwrap(),
    };
    assert!(registration.dial_started(attempt));
    assert!(registration.welcomed(admitted.clone()));
    let frame = registration
        .begin_write(target, ClockInstant::ZERO, utc.timestamp_millis())
        .unwrap();
    let Exchange::Writing { call, .. } = &registration.exchange else {
        panic!("expected a selected registration to write");
    };
    let call = call.clone();
    let deadline = ClockInstant::ZERO.after(Duration::from_secs(30));
    let awaiting =
        PendingRunRegistration::from_sent_frame(admitted.clone(), &frame, deadline).unwrap();

    let exchanges = [
        Exchange::AwaitingWelcome,
        Exchange::Due {
            admitted: admitted.clone(),
            retry: None,
        },
        Exchange::Due {
            admitted: admitted.clone(),
            retry: Some((deadline, RegistrationFailure::TimedOut)),
        },
        Exchange::Due {
            admitted: admitted.clone(),
            retry: Some((
                deadline,
                RegistrationFailure::Refused(LinkCallRefusal::WrongTarget),
            )),
        },
        Exchange::Writing {
            admitted: admitted.clone(),
            call: call.clone(),
        },
        Exchange::AwaitingReply {
            admitted,
            call,
            pending: awaiting,
        },
        Exchange::Ready,
    ];
    let statuses = [
        LinkStatus::NotEnrolled,
        LinkStatus::Connecting,
        LinkStatus::DiscoveryPending,
        LinkStatus::DiscoveryUnavailable,
        LinkStatus::Welcomed {
            machine_id: machine.clone(),
        },
        LinkStatus::Registering,
        LinkStatus::RegistrationFailed {
            reason: RegistrationFailure::TimedOut,
        },
        LinkStatus::RegistrationFault {
            reason: LinkCallRefusal::WrongTarget,
        },
        LinkStatus::RunRetired,
        LinkStatus::Connected {
            machine_id: machine.clone(),
        },
        LinkStatus::Fault {
            kind: LinkFault::LeaseSource,
        },
        LinkStatus::EnrollmentEnded {
            cause: EnrollmentEnd::Revoked,
        },
    ];

    for (exchange_index, exchange) in exchanges.into_iter().enumerate() {
        let ready = exchange_is_ready(&exchange);
        registration.exchange = exchange;
        for transport in &statuses {
            registration.parked = None;
            let reported = registration.status(transport.clone(), &machine);
            assert_eq!(
                matches!(reported, LinkStatus::Connected { .. }),
                ready && transport_is_welcomed(transport),
                "exchange {exchange_index}, transport {transport:?}"
            );

            registration.parked = Some(AuthorityEnd::RunRetired);
            assert_eq!(
                registration.status(transport.clone(), &machine),
                LinkStatus::RunRetired
            );
            registration.parked = Some(AuthorityEnd::EnrollmentEnded);
            assert_eq!(
                registration.status(transport.clone(), &machine),
                LinkStatus::RegistrationFailed {
                    reason: RegistrationFailure::CentralEnrollmentEnded
                }
            );
        }
    }
}

fn exchange_is_ready(exchange: &Exchange) -> bool {
    match exchange {
        Exchange::Ready => true,
        Exchange::AwaitingWelcome
        | Exchange::Due { .. }
        | Exchange::Writing { .. }
        | Exchange::AwaitingReply { .. } => false,
    }
}

fn transport_is_welcomed(status: &LinkStatus) -> bool {
    match status {
        LinkStatus::Welcomed { .. } => true,
        LinkStatus::NotEnrolled
        | LinkStatus::Connecting
        | LinkStatus::DiscoveryPending
        | LinkStatus::DiscoveryUnavailable
        | LinkStatus::Registering
        | LinkStatus::RegistrationFailed { .. }
        | LinkStatus::RegistrationFault { .. }
        | LinkStatus::RunRetired
        | LinkStatus::Connected { .. }
        | LinkStatus::Fault { .. }
        | LinkStatus::EnrollmentEnded { .. } => false,
    }
}
