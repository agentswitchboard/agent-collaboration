use approval_request_protocol::{
    Answer, ApprovalDecideReply, ApprovalListFilter, ApprovalListReply, ApprovalReadReply,
    ApprovalRequestId, SessionCallDecision,
};
use central_link_protocol::{
    CentralCallReply, LinkCallRefusal,
    person_link_rpc::{OrganizationWorkspacePage, PersonWorkspacePageLimit},
};
use hegel::generators as gs;
use state_machine_core::{
    TransitionCheckError, check_declared_transitions, check_state_machine_transitions,
};

use super::{
    AddressedPersonCall, CallAttempt, ConnectionEffect as Effect, ConnectionEvent as Event,
    ConnectionMachine as Machine, ConnectionState as State, CorrelationId, DecidePersonApproval,
    DecidePersonApprovalRpc, Envelope, ListOrganizationWorkspaces, ListOrganizationWorkspacesRpc,
    ListPersonApprovals, ListPersonApprovalsRpc, OrganizationId, PersonCall, PersonLinkError,
    ReadPersonApproval, ReadPersonApprovalRpc, StateMachine, TransitionError, TypedCallResult,
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

fn person_calls() -> [PersonCall; 4] {
    let request_id: ApprovalRequestId = "019f0000-0000-7000-8000-00000000c813".parse().unwrap();
    [
        PersonCall::ListWorkspaces(ListOrganizationWorkspaces {
            operation_id: "019f0000-0000-7000-8000-00000000c811".parse().unwrap(),
            after: Some("019f0000-0000-7000-8000-00000000c812".parse().unwrap()),
            limit: PersonWorkspacePageLimit::try_from(1).unwrap(),
        }),
        PersonCall::ListApprovals(ListPersonApprovals {
            filter: ApprovalListFilter::Waiting,
            cursor: None,
        }),
        PersonCall::ReadApproval(ReadPersonApproval {
            request_id: request_id.clone(),
        }),
        PersonCall::DecideApproval(DecidePersonApproval {
            request_id,
            operation_id: "019f0000-0000-7000-8000-00000000c814".parse().unwrap(),
            answer: Answer::UnprovenCall {
                decision: SessionCallDecision::Allow,
            },
        }),
    ]
}

fn person_call(body: PersonCall) -> AddressedPersonCall {
    Envelope {
        target: organization(),
        body,
    }
}

fn reply(call: &PersonCall, serial: u16, target: OrganizationId) -> Event {
    let reply: CentralCallReply = match call {
        PersonCall::ListWorkspaces(_) => ListOrganizationWorkspacesRpc::reply(Envelope {
            target,
            body: OrganizationWorkspacePage {
                workspaces: Vec::new(),
                next: None,
            },
        }),
        PersonCall::ListApprovals(_) => ListPersonApprovalsRpc::reply(Envelope {
            target,
            body: ApprovalListReply::Page {
                items: Vec::new(),
                next: None,
            },
        }),
        PersonCall::ReadApproval(_) => ReadPersonApprovalRpc::reply(Envelope {
            target,
            body: Box::new(ApprovalReadReply::NotVisible {}),
        }),
        PersonCall::DecideApproval(_) => DecidePersonApprovalRpc::reply(Envelope {
            target,
            body: ApprovalDecideReply::TooLate {},
        }),
    };
    Event::Reply {
        correlation: correlation(serial),
        result: Box::new(TypedCallResult::Answered { reply }),
    }
}

#[test]
fn table_and_reducer_cover_each_other() {
    for call in person_calls().map(person_call) {
        check_call_transitions(call);
    }
}

fn check_call_transitions(call: AddressedPersonCall) {
    let attempt = CallAttempt {
        call: call.clone(),
        correlation: correlation(1),
    };
    let states = [
        State::Connecting(call.clone()),
        State::Connected(None),
        State::Connected(Some(attempt.clone())),
        State::ClosedByLifetime(call.clone()),
        State::Reconnecting(call.clone()),
        State::Refused(PersonLinkError::Socket),
    ];
    let events = [
        Event::Call(attempt),
        Event::Upgraded(correlation(2)),
        Event::LifetimeClosed,
        Event::SocketRetired,
        reply(&call.body, 1, organization()),
        reply(&call.body, 2, organization()),
        reply(
            &call.body,
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
fn generated_histories_keep_one_call_and_ignore_old_replies(case: hegel::TestCase) {
    let lifetimes = case.draw(gs::integers::<u16>().min_value(1).max_value(8));
    let repetitions = case.draw(gs::integers::<u8>().min_value(1).max_value(5));
    let refuse_at_end = case.draw(gs::booleans());
    for call in person_calls().map(person_call) {
        check_call_history(call, lifetimes, repetitions, refuse_at_end);
    }
}

fn check_call_history(
    call: AddressedPersonCall,
    lifetimes: u16,
    repetitions: u8,
    refuse_at_end: bool,
) {
    let (mut state, initial) = Machine::begin(call.clone());
    assert!(matches!(initial, Effect::Upgrade));
    for serial in 1..=lifetimes {
        let (connected, effects) =
            Machine::transition(&state, Event::Upgraded(correlation(serial))).unwrap();
        assert!(matches!(effects.as_slice(), [Effect::Send(attempt)]
            if attempt.correlation == correlation(serial) && attempt.call == call));
        for _ in 0..repetitions {
            assert!(matches!(
                Machine::transition(&connected, reply(&call.body, serial - 1, organization())),
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
    assert!(matches!(effects.as_slice(), [Effect::Send(attempt)] if attempt.call == call));
    let answer = if refuse_at_end {
        Event::Reply {
            correlation: correlation(serial),
            result: Box::new(TypedCallResult::Refused {
                reason: LinkCallRefusal::WrongTarget,
            }),
        }
    } else {
        reply(&call.body, serial, organization())
    };
    let (idle, effects) = Machine::transition(&connected, answer.clone()).unwrap();
    if refuse_at_end {
        assert!(matches!(
            effects.as_slice(),
            [Effect::Finish(Err(PersonLinkError::CallRefused(
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

#[test]
fn decide_rejects_a_correlated_reply_of_another_call_type() {
    let [workspace_list, approval_list, approval_read, decide] = person_calls();
    let state = State::Connected(Some(CallAttempt {
        call: person_call(decide),
        correlation: correlation(1),
    }));

    for call in [workspace_list, approval_list, approval_read] {
        let (next, effects) = Machine::transition(&state, reply(&call, 1, organization())).unwrap();

        assert!(matches!(next, State::Refused(PersonLinkError::Protocol)));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Finish(Err(PersonLinkError::Protocol))]
        ));
    }
}
