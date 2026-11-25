use central_link_protocol::{
    CentralCallReply, LinkCallRefusal,
    person_link_rpc::{OrganizationWorkspacePage, PersonWorkspacePageLimit},
};
use hegel::generators as gs;
use state_machine_core::{
    TransitionCheckError, check_declared_transitions, check_state_machine_transitions,
};

use super::{
    ConnectionEffect as Effect, ConnectionEvent as Event, ConnectionMachine as Machine,
    ConnectionState as State, CorrelationId, Envelope, ListOrganizationWorkspaces,
    ListOrganizationWorkspacesRpc, OrganizationId, PersonLinkError, StateMachine, TransitionError,
    TypedCallResult, WorkspaceRead,
};
use central_link_protocol::CentralLinkRpc;

fn organization() -> OrganizationId {
    "019f0000-0000-7000-8000-00000000c801"
        .to_owned()
        .try_into()
        .unwrap()
}

fn correlation(serial: u16) -> CorrelationId {
    format!("019f0000-0000-7000-8000-{serial:012x}")
        .parse()
        .unwrap()
}

fn read() -> WorkspaceRead {
    Envelope {
        target: organization(),
        body: ListOrganizationWorkspaces {
            operation_id: "019f0000-0000-7000-8000-00000000c811".parse().unwrap(),
            after: Some("019f0000-0000-7000-8000-00000000c812".parse().unwrap()),
            limit: PersonWorkspacePageLimit::try_from(1).unwrap(),
        },
    }
}

fn reply(serial: u16, target: OrganizationId) -> Event {
    let reply: CentralCallReply = ListOrganizationWorkspacesRpc::reply(Envelope {
        target,
        body: OrganizationWorkspacePage {
            workspaces: Vec::new(),
            next: None,
        },
    });
    Event::Reply {
        correlation: correlation(serial),
        result: Box::new(TypedCallResult::Answered { reply }),
    }
}

#[test]
fn table_and_reducer_cover_each_other() {
    let read = read();
    let attempt = super::ReadAttempt {
        read: read.clone(),
        correlation: correlation(1),
    };
    let states = [
        State::Connecting(read.clone()),
        State::Connected(None),
        State::Connected(Some(attempt.clone())),
        State::ClosedByLifetime(read.clone()),
        State::Reconnecting(read),
        State::Refused(PersonLinkError::Socket),
    ];
    let events = [
        Event::Read(attempt),
        Event::Upgraded(correlation(2)),
        Event::LifetimeClosed,
        Event::SocketRetired,
        reply(1, organization()),
        reply(2, organization()),
        reply(
            1,
            "019f0000-0000-7000-8000-00000000c899"
                .to_owned()
                .try_into()
                .unwrap(),
        ),
        Event::Failed(PersonLinkError::Upgrade),
    ];
    let cases = || {
        states.iter().flat_map(|state| {
            events
                .iter()
                .map(move |event| (state.clone(), event.clone()))
        })
    };
    check_state_machine_transitions::<Machine>(cases()).unwrap();

    let mut missing = Machine::transitions().to_vec();
    missing.pop().unwrap();
    let apply = |state: &State, event| Machine::transition(state, event).ok().map(|(next, _)| next);
    assert!(matches!(
        check_declared_transitions(cases(), &missing, apply),
        Err(TransitionCheckError::Undeclared(_))
    ));

    let mut extra = Machine::transitions().to_vec();
    let unwitnessed = state_machine_core::Transition {
        from: "refused",
        event: "lifetimeClosed",
        to: "reconnecting",
        terminal: false,
    };
    extra.push(unwitnessed);
    assert_eq!(
        check_declared_transitions(cases(), &extra, apply),
        Err(TransitionCheckError::Unreachable(unwitnessed))
    );
}

#[hegel::test(test_cases = 48)]
fn generated_histories_keep_one_read_and_ignore_old_replies(case: hegel::TestCase) {
    let lifetimes = case.draw(gs::integers::<u16>().min_value(1).max_value(8));
    let repetitions = case.draw(gs::integers::<u8>().min_value(1).max_value(5));
    let refuse_at_end = case.draw(gs::booleans());
    let read = read();
    let (mut state, initial) = Machine::begin(read.clone());
    assert!(matches!(initial, Effect::Upgrade));
    for serial in 1..=lifetimes {
        let (connected, effects) =
            Machine::transition(&state, Event::Upgraded(correlation(serial))).unwrap();
        assert!(matches!(effects.as_slice(), [Effect::Send(attempt)]
            if attempt.correlation == correlation(serial) && attempt.read == read));
        for _ in 0..repetitions {
            assert!(matches!(
                Machine::transition(&connected, reply(serial - 1, organization())),
                Err(TransitionError::Stale)
            ));
        }
        let (closed, effects) = Machine::transition(&connected, Event::LifetimeClosed).unwrap();
        assert!(matches!(effects.as_slice(), [Effect::RetireSocket]));
        let (reconnecting, effects) = Machine::transition(&closed, Event::SocketRetired).unwrap();
        assert!(matches!(effects.as_slice(), [Effect::Upgrade]));
        state = reconnecting;
    }

    let serial = lifetimes + 1;
    let (connected, effects) =
        Machine::transition(&state, Event::Upgraded(correlation(serial))).unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Send(attempt)] if attempt.read == read));
    let answer = if refuse_at_end {
        Event::Reply {
            correlation: correlation(serial),
            result: Box::new(TypedCallResult::Refused {
                reason: LinkCallRefusal::WrongTarget,
            }),
        }
    } else {
        reply(serial, organization())
    };
    let (idle, effects) = Machine::transition(&connected, answer.clone()).unwrap();
    if refuse_at_end {
        assert!(matches!(
            effects.as_slice(),
            [Effect::Finish(Err(PersonLinkError::WorkspaceRefused(
                LinkCallRefusal::WrongTarget
            )))]
        ));
    } else {
        assert!(matches!(effects.as_slice(), [Effect::Finish(Ok(_))]));
    }
    assert!(matches!(idle, State::Connected(None)));
    assert!(matches!(
        Machine::transition(&idle, answer),
        Err(TransitionError::NotAllowed { .. })
    ));
}
