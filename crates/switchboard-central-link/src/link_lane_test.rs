use central_link_client::{ClientAction, LinkClient, WelcomeClockAnchor};
use central_link_protocol::CorrelationId;
use hegel::generators as gs;
use router_clock::ManualClock;
use std::sync::Arc;

use super::*;
use crate::{admitted_lane_socket::AdmittedSocketLane, wire_write_fence::WireWriteFence};

fn admitted_connection() -> AdmittedConnection {
    let central_time = "2030-01-01T00:00:00Z".parse().unwrap();
    let mut client = LinkClient::new(
        "01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap(),
        Arc::new(ManualClock::new(central_time)),
    );
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("fixture expected its initial attempt");
    };
    AdmittedConnection {
        connection: attempt,
        clock_anchor: WelcomeClockAnchor {
            central_time,
            observed_at: ClockInstant::ZERO,
        },
    }
}

fn prepared(connection: ConnectionAttempt) -> (LaneRequest, oneshot::Receiver<LaneResult>) {
    let (settled, receiver) = oneshot::channel();
    let id: CorrelationId = "01890f2e-7b4c-7cc0-98c4-000000000001".parse().unwrap();
    (
        LaneRequest {
            connection,
            send_before: ClockInstant::ZERO.after(std::time::Duration::from_secs(3)),
            wire: "test".to_owned(),
            expected: ExpectedReply::RequestStatus(id),
            settled,
        },
        receiver,
    )
}

#[test]
fn submitter_paused_after_admission_cannot_enqueue_after_final_withdrawal() {
    let (handle, mut owner) = link_lane_channel();
    let admitted = admitted_connection();
    let write_fence = WireWriteFence::new(Arc::new(ManualClock::new(
        admitted.clock_anchor.central_time,
    )));
    let mut socket = AdmittedSocketLane::new(&owner, &write_fence);
    assert!(socket.admit(admitted.clone()));

    // A submitter has already taken the fast-path observation of A. The
    // actual handoff is held until the owner has retired and drained A.
    assert!(handle.is_admitted(&admitted.connection));
    let (late, _) = prepared(admitted.connection.clone());
    let (accepted, mut disposition) = prepared(admitted.connection);
    assert_eq!(
        handle.intake.enqueue(&handle.commands, accepted, false),
        Ok(())
    );
    drop(socket);
    owner.reject_queued();
    assert_eq!(
        disposition.try_recv(),
        Ok(LaneResult::Untouched(
            UntouchedCommand::ConnectionUnavailable
        ))
    );

    assert_eq!(
        handle.intake.enqueue(&handle.commands, late, false),
        Err(UntouchedCommand::ConnectionUnavailable)
    );
    assert!(owner.commands.try_recv().is_err());
    assert!(handle.watch_admitted().borrow().is_none());
}

#[derive(Clone, Copy, Debug)]
enum IntakeStep {
    AdmitFirst,
    AdmitSecond,
    SubmitFirst,
    SubmitSecond,
    Withdraw,
}

#[hegel::test(test_cases = 125)]
fn generated_handoffs_settle_every_accepted_command_after_withdrawal(case: hegel::TestCase) {
    let generated = case.draw(
        gs::vecs(gs::sampled_from(vec![
            IntakeStep::AdmitFirst,
            IntakeStep::AdmitSecond,
            IntakeStep::SubmitFirst,
            IntakeStep::SubmitSecond,
            IntakeStep::Withdraw,
        ]))
        .max_size(40),
    );
    let (handle, mut owner) = link_lane_channel();
    let first = admitted_connection();
    let second = admitted_connection();
    let write_fence =
        WireWriteFence::new(Arc::new(ManualClock::new(first.clock_anchor.central_time)));
    let mut current: Option<&ConnectionAttempt> = None;
    let mut accepted = Vec::new();
    let mut owner_socket = None;

    for step in generated.into_iter().chain([IntakeStep::Withdraw]) {
        match step {
            IntakeStep::AdmitFirst | IntakeStep::AdmitSecond if owner_socket.is_none() => {
                let view = if matches!(step, IntakeStep::AdmitFirst) {
                    &first
                } else {
                    &second
                };
                let mut socket = AdmittedSocketLane::new(&owner, &write_fence);
                assert!(socket.admit(view.clone()));
                owner_socket = Some(socket);
                current = Some(&view.connection);
            }
            IntakeStep::SubmitFirst | IntakeStep::SubmitSecond => {
                let connection = if matches!(step, IntakeStep::SubmitFirst) {
                    &first.connection
                } else {
                    &second.connection
                };
                let (request, disposition) = prepared(connection.clone());
                let expected = if current != Some(connection) {
                    Err(UntouchedCommand::ConnectionUnavailable)
                } else if accepted.len() >= OPERATION_CAPACITY {
                    Err(UntouchedCommand::WriterBusy)
                } else {
                    Ok(())
                };
                assert_eq!(
                    handle.intake.enqueue(&handle.commands, request, false),
                    expected,
                    "{step:?}"
                );
                if expected.is_ok() {
                    accepted.push(disposition);
                }
            }
            IntakeStep::Withdraw => {
                drop(owner_socket.take());
                owner.reject_queued();
                current = None;
                for mut disposition in accepted.drain(..) {
                    assert_eq!(
                        disposition.try_recv(),
                        Ok(LaneResult::Untouched(
                            UntouchedCommand::ConnectionUnavailable
                        )),
                        "accepted commands must settle at retirement"
                    );
                }
            }
            IntakeStep::AdmitFirst | IntakeStep::AdmitSecond => {}
        }
    }
}
