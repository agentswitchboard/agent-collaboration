use std::sync::Arc;

use central_link_client::{ClientAction, ConnectionAttempt, LinkClient};
use central_link_protocol::{
    LinkCallRefusal, MacWorkReply, MacWorkRpc, session_lifecycle_rpc::EndFleetSessionRpc,
};
use router_clock::ManualClock;
use serde_json::json;
use state_machine_core::{StateMachine, TransitionError, check_state_machine_transitions};

use super::{
    CallEnding, CallEvent, CallGate, CallGateEvent, CallGatePolicy, CallNotReached, CallPhase,
    CallPhaseRefusal, CallPolicy, CallSettlement, FrameExposure,
};

fn checked_reply() -> MacWorkReply {
    let target = serde_json::from_value(json!("019f0000-0000-7000-8000-000000000001")).unwrap();
    let request = serde_json::from_value(json!({
        "target": {"kind":"centralId", "sessionId":"019f0000-0000-7000-8000-000000000002"},
        "cause": {"kind":"givenUp"}
    }))
    .unwrap();
    let (_, check) = EndFleetSessionRpc::work_request(target, request)
        .prepare()
        .unwrap();
    check.check(serde_json::from_value(json!({
        "call":"endFleetSession",
        "envelope": {"target":"019f0000-0000-7000-8000-000000000001", "body":{"result":"noSuchStart"}}
    })).unwrap()).unwrap()
}

#[test]
fn accepted_call_reducer_matches_every_declared_transition() {
    let phases = [
        CallPhase::Queued,
        CallPhase::Writing,
        CallPhase::Written,
        CallPhase::Settled,
    ];
    let events = [
        CallEvent::BeginWrite,
        CallEvent::FlushCompleted,
        CallEvent::End(CallEnding::Deadline),
        CallEvent::WriteEnded {
            exposure: FrameExposure::Unwritten,
            cause: CallEnding::Shutdown,
        },
        CallEvent::WriteEnded {
            exposure: FrameExposure::PossiblyWritten,
            cause: CallEnding::Shutdown,
        },
        CallEvent::Answered(Box::new(checked_reply())),
        CallEvent::RefusedByCentral(LinkCallRefusal::WrongTarget),
    ];
    let cases = phases
        .into_iter()
        .flat_map(|phase| events.iter().cloned().map(move |event| (phase, event)));
    check_state_machine_transitions::<CallPolicy>(cases).unwrap();
}

#[test]
fn writing_cannot_end_without_evidence_from_its_own_writer() {
    let cause = CallEnding::DriverDropped;
    assert!(matches!(
        CallPolicy::transition(&CallPhase::Writing, CallEvent::End(cause)),
        Err(TransitionError::Domain(CallPhaseRefusal::WrongPhase))
    ));

    let end_write = |exposure| {
        CallPolicy::transition(
            &CallPhase::Writing,
            CallEvent::WriteEnded { exposure, cause },
        )
        .unwrap()
    };
    let (unwritten, untouched) = end_write(FrameExposure::Unwritten);
    let (written, possible) = end_write(FrameExposure::PossiblyWritten);
    assert_eq!(unwritten, CallPhase::Settled);
    assert_eq!(written, CallPhase::Settled);
    assert!(matches!(
        untouched.as_slice(),
        [CallSettlement::NotReached(CallNotReached::Ended(
            CallEnding::DriverDropped
        ))]
    ));
    assert!(matches!(
        possible.as_slice(),
        [CallSettlement::AnswerLost(CallEnding::DriverDropped)]
    ));
    assert!(matches!(
        CallPolicy::transition(&written, CallEvent::End(CallEnding::Deadline)),
        Err(TransitionError::Stale)
    ));
}

#[expect(
    clippy::panic,
    reason = "A new link client must offer an attempt in this fixture."
)]
fn attempt(clock: Arc<ManualClock>) -> ConnectionAttempt {
    let machine = "019f0000-0000-7000-8000-000000000002".parse().unwrap();
    let mut client = LinkClient::new(machine, clock);
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("a new client begins with a due attempt");
    };
    attempt
}

#[test]
fn the_call_gate_table_covers_open_withdrawal_and_new_bindings() {
    let clock = Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap()));
    let current = attempt(clock.clone());
    let stale = attempt(clock);
    let states = [
        CallGate::Unavailable,
        CallGate::Open(current.clone()),
        CallGate::EnrollmentEnded,
    ];
    let events = [
        CallGateEvent::Registered(current.clone()),
        CallGateEvent::Withdraw(current.clone()),
        CallGateEvent::Withdraw(stale.clone()),
        CallGateEvent::EndEnrollment,
        CallGateEvent::NewBinding,
    ];
    let cases = states.into_iter().flat_map(|state| {
        events
            .iter()
            .cloned()
            .map(move |event| (state.clone(), event))
    });
    check_state_machine_transitions::<CallGatePolicy>(cases).unwrap();
    assert_eq!(
        CallGatePolicy::transition(&CallGate::Open(current), CallGateEvent::Withdraw(stale)),
        Err(TransitionError::Stale)
    );
}
