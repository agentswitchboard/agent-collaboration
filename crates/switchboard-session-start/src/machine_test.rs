use fleet_identity_protocol::{CreateSessionKind, HarnessKind};
use state_machine_core::{EventTag, Transition};

use super::{
    CentralStanding, DirectiveResult, StartEnvelope, StartInput, StartMachine, StartRefusal,
    StartRevision, StartStateKind, StateMachine, TransitionError, WaiterSlot,
};
use crate::{
    CandidateAsker, LaunchSpecRef, LinkNotTaken, LinkResult, LocalStartDigest, SaveResult,
    StartCandidate, StartInstanceId, StartScope,
};

fn candidate() -> StartEnvelope {
    StartEnvelope::new(StartCandidate {
        scope: StartScope {
            enrollment: String::from("019c0000-0000-7000-8000-000000000001")
                .try_into()
                .unwrap(),
            operation_id: "019c0000-0000-7000-8000-000000000002".parse().unwrap(),
        },
        machine: "019c0000-0000-7000-8000-000000000003".parse().unwrap(),
        asker: CandidateAsker::Operator,
        agent_program: HarnessKind::Cursor,
        kind: CreateSessionKind::Ordinary {},
        local_digest: LocalStartDigest::new([0; 32]),
        launch: LaunchSpecRef::new(1),
    })
    .unwrap()
}

fn ask() -> StartInput {
    StartInput::Ask {
        slot: WaiterSlot::new(1),
        candidate: Ok(candidate()),
        central: CentralStanding::Ready,
    }
}

fn create_unknown() -> StartMachine {
    let machine = StartMachine::new(StartInstanceId::new(
        "019c0000-0000-7000-8000-000000000004".parse().unwrap(),
    ));
    let (machine, _) = StartMachine::transition(&machine, ask()).unwrap();
    let (machine, _) = StartMachine::transition(
        &machine,
        StartInput::DirectiveResult {
            id: machine.outstanding_id().unwrap(),
            result: DirectiveResult::Save(SaveResult::Saved(StartRevision::new(1))),
        },
    )
    .unwrap();
    let (machine, _) = StartMachine::transition(
        &machine,
        StartInput::DirectiveResult {
            id: machine.outstanding_id().unwrap(),
            result: DirectiveResult::SendCreate(LinkResult::NotTaken(LinkNotTaken::LinkNotReady)),
        },
    )
    .unwrap();
    assert_eq!(machine.kind(), StartStateKind::CreateUnknown);
    machine
}

#[test]
fn protocol_attempt_ceiling_has_the_declared_edge() {
    let mut machine = create_unknown();
    machine.accepted.as_deref_mut().unwrap().attempt = 9_007_199_254_740_991.try_into().unwrap();
    let input = ask();
    let (next, _) = StartMachine::transition(&machine, input.clone()).unwrap();
    assert_eq!(next.kind(), StartStateKind::SavingOutcome);
    assert_eq!(next.attempt(), machine.attempt());
    let edge = Transition {
        from: machine.kind().tag(),
        event: input.event_tag(),
        to: next.kind().tag(),
        terminal: false,
    };
    assert!(StartMachine::transitions().contains(&edge));
}

#[test]
fn exhausted_directive_sequence_refuses_without_a_new_id_or_output() {
    let mut machine = create_unknown();
    machine.sequence = u64::MAX;
    let prior = machine.clone();
    assert_eq!(
        StartMachine::transition(&machine, ask()),
        Err(TransitionError::Domain(
            StartRefusal::DirectiveSequenceExhausted
        ))
    );
    assert_eq!(machine, prior);
}
