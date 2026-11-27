use super::*;
use state_machine_core::check_state_machine_transitions;

fn attempts() -> [CloseAttempt; 2] {
    [
        CloseAttempt("019ac19f-aa00-7000-8000-000000000001".parse().unwrap()),
        CloseAttempt("019ac19f-aa00-7000-8000-000000000002".parse().unwrap()),
    ]
}

#[test]
fn close_marks_take_exactly_the_declared_edges() {
    let [first, second] = attempts();
    let states = [
        RouteCloseState::Open,
        RouteCloseState::Closing { attempt: first },
        RouteCloseState::Closing { attempt: second },
    ];
    let events = [
        RouteCloseEvent::CloseSent { attempt: first },
        RouteCloseEvent::CloseSent { attempt: second },
        RouteCloseEvent::Readmit { attempt: first },
        RouteCloseEvent::Readmit { attempt: second },
    ];
    let cases = states
        .into_iter()
        .flat_map(|state| events.map(|event| (state, event)));

    assert_eq!(
        check_state_machine_transitions::<RouteCloseMachine>(cases),
        Ok(())
    );
    for attempt in [first, second] {
        let mark = RouteCloseState::Closing { attempt };
        assert_eq!(
            RouteCloseMachine::transition(
                &RouteCloseState::Open,
                RouteCloseEvent::CloseSent { attempt }
            ),
            Ok((mark, Vec::new())),
        );
        assert_eq!(
            RouteCloseMachine::transition(&mark, RouteCloseEvent::Readmit { attempt }),
            Ok((RouteCloseState::Open, Vec::new())),
        );
    }
}

#[test]
fn a_close_is_refused_when_marked_and_readmit_requires_the_exact_attempt() {
    let [first, second] = attempts();
    let close = RouteCloseState::Closing { attempt: first };
    let open = RouteCloseState::Open;
    let cases = [
        (
            close,
            RouteCloseEvent::CloseSent { attempt: first },
            RouteCloseRefusal::AlreadyClosing,
            "already_closing",
        ),
        (
            close,
            RouteCloseEvent::CloseSent { attempt: second },
            RouteCloseRefusal::AlreadyClosing,
            "already_closing",
        ),
        (
            close,
            RouteCloseEvent::Readmit { attempt: second },
            RouteCloseRefusal::DifferentMarker,
            "different_marker",
        ),
        (
            open,
            RouteCloseEvent::Readmit { attempt: first },
            RouteCloseRefusal::MarkerAbsent,
            "marker_absent",
        ),
        (
            open,
            RouteCloseEvent::Readmit { attempt: second },
            RouteCloseRefusal::MarkerAbsent,
            "marker_absent",
        ),
    ];

    for (state, event, refusal, tag) in cases {
        let mut mark = state;
        let result = RouteCloseMachine::transition(&mark, event).map(|(next, _)| mark = next);

        assert_eq!(
            result,
            Err(TransitionError::Domain(refusal)),
            "{state:?} {event:?}"
        );
        assert_eq!(mark, state);
        assert_eq!(refusal.refusal_tag(), tag);
    }
}
