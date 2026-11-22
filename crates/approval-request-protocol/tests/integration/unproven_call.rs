use approval_request_protocol::{
    AdmittedDestination, Answer, ApprovalWindow, CallStopCause, DecidedBy, DecisionFactor,
    DeliverEndingRequest, EndingView, HolderStopCause, InvalidApprovalEnding, InvocationDigest,
    Raise, RequestView, SessionCallDecision, StopRequest, WaitingStatus,
};
use serde_json::{Value, json};

use super::map_only::assert_objects_require_named_maps;

const PERSON_ID: &str = "01934a50-1000-7000-8000-000000000004";
const OTHER_PERSON_ID: &str = "01934a50-1000-7000-8000-000000000005";
const SESSION_ID: &str = "01934a50-1000-7000-8000-000000000021";
const RUN_ID: &str = "01934a50-1000-7000-8000-000000000002";
const REQUEST_ID: &str = "01934a50-1000-7000-8000-000000000011";

fn start_summary() -> Value {
    json!({"kind":"startSession","agentProgram":"claude","workingFolder":"/project","mode":"plan"})
}

fn end_summary() -> Value {
    json!({"kind":"endSessionForGood","sessionId":SESSION_ID})
}

fn raise_specimen(operation: Value) -> Value {
    json!({
        "kind":"unprovenCall",
        "requestId":REQUEST_ID,
        "holder":{"routerRun":RUN_ID},
        "invocation":InvocationDigest::from_bound_bytes(b"opaque test fingerprint"),
        "offer":{"operation":operation}
    })
}

#[test]
fn only_session_operations_and_bounded_summaries_cross_the_wire() {
    for operation in [start_summary(), end_summary()] {
        let specimen = raise_specimen(operation);
        assert_objects_require_named_maps::<Raise>(&specimen).unwrap();
        let raise: Raise = serde_json::from_value(specimen.clone()).unwrap();
        assert_eq!(serde_json::to_value(&raise).unwrap(), specimen);
        assert_eq!(raise.requesting_session(), None);
        for field in ["arguments", "operationId", "fingerprint", "model", "scope"] {
            let mut with_execution_parameter = specimen.clone();
            with_execution_parameter["offer"]["operation"][field] = json!("foreign");
            assert!(
                serde_json::from_value::<Raise>(with_execution_parameter).is_err(),
                "{field}"
            );
        }
        let mut with_session = specimen.clone();
        with_session["holder"]["sessionId"] = json!(SESSION_ID);
        assert!(serde_json::from_value::<Raise>(with_session).is_err());
    }
    for (field, value) in [
        ("kind", json!("runCommand")),
        ("agentProgram", json!("codex")),
        ("workingFolder", json!("")),
        ("workingFolder", json!("x".repeat(2_001))),
        ("mode", json!("x".repeat(201))),
    ] {
        let mut summary = start_summary();
        summary[field] = value;
        assert!(
            serde_json::from_value::<Raise>(raise_specimen(summary)).is_err(),
            "{field}"
        );
    }
    assert!(serde_json::from_value::<InvocationDigest>(json!("a".repeat(64))).is_err());
}

#[test]
fn parked_choices_require_a_fresh_person_and_the_ending_contains_no_invocation() {
    let raise: Raise = serde_json::from_value(raise_specimen(start_summary())).unwrap();
    let actor: DecidedBy =
        serde_json::from_value(json!({"kind":"person","personId":PERSON_ID})).unwrap();
    let other_person: DecidedBy =
        serde_json::from_value(json!({"kind":"person","personId":OTHER_PERSON_ID})).unwrap();
    let session: DecidedBy =
        serde_json::from_value(json!({"kind":"session","session":SESSION_ID})).unwrap();
    let members = [actor.clone(), other_person.clone(), session.clone()];
    assert!(AdmittedDestination::for_request(&raise, &session, &members).is_err());
    let admitted = AdmittedDestination::for_request(&raise, &actor, &members).unwrap();
    let bystander = AdmittedDestination::for_request(&raise, &other_person, &members).unwrap();
    let window = ApprovalWindow::from_first_save("2026-01-01T00:00:00Z".parse().unwrap()).unwrap();
    assert!(RequestView::waiting(&admitted, WaitingStatus::PassedOn, window).is_err());
    let waiting = RequestView::waiting(&admitted, WaitingStatus::Pending, window)
        .unwrap()
        .for_recipient(&actor)
        .unwrap();
    let waiting = serde_json::to_value(waiting).unwrap();
    assert_eq!(
        waiting["invocation"],
        raise_specimen(start_summary())["invocation"]
    );
    assert_objects_require_named_maps::<RequestView>(&waiting).unwrap();
    let mut forwarded = waiting;
    forwarded["state"]["status"] = json!("passedOn");
    assert!(serde_json::from_value::<RequestView>(forwarded).is_err());

    for decision in [SessionCallDecision::Allow, SessionCallDecision::Decline] {
        let answer = Answer::UnprovenCall { decision };
        assert!(matches!(
            admitted.decide(answer.clone(), DecisionFactor::NoFreshPersonFactor),
            Err(InvalidApprovalEnding::FreshFactorRequired)
        ));
        let ending = admitted
            .decide(answer, DecisionFactor::FreshPersonFactor)
            .unwrap();
        let frame = DeliverEndingRequest::from_raise(&raise, ending).unwrap();
        let wire = serde_json::to_value(&frame).unwrap();
        assert_eq!(
            wire,
            json!({
                "requestId":REQUEST_ID,"contentsDigest":raise.contents_digest().unwrap(),
                "ending":{"kind":"unprovenCall","ending":{"kind":"decided","decision":decision,"personId":PERSON_ID}}
            })
        );
        assert_objects_require_named_maps::<DeliverEndingRequest>(&wire).unwrap();
        let mut injected = wire;
        injected["ending"]["ending"]["operation"] = end_summary();
        assert!(serde_json::from_value::<DeliverEndingRequest>(injected).is_err());
        let different: Raise = serde_json::from_value(raise_specimen(end_summary())).unwrap();
        assert!(frame.check_bound_raise(&different).is_err());
        let giver_view = EndingView::for_destination(&admitted, &frame)
            .unwrap()
            .for_recipient(&actor)
            .unwrap();
        let other_view = EndingView::for_destination(&bystander, &frame)
            .unwrap()
            .for_recipient(&other_person)
            .unwrap();
        assert_eq!(
            serde_json::to_value(giver_view).unwrap()["ending"]["decision"],
            json!(decision)
        );
        assert_eq!(
            serde_json::to_value(other_view).unwrap()["ending"],
            json!({"kind":"decided","personId":PERSON_ID})
        );
    }

    let stop = json!({
        "kind":"unprovenCall","requestId":REQUEST_ID,"routerRun":RUN_ID,
        "contentsDigest":raise.contents_digest().unwrap(),"cause":"callWithdrawn"
    });
    assert_objects_require_named_maps::<StopRequest>(&stop).unwrap();
    assert_eq!(
        serde_json::from_value::<StopRequest>(stop).unwrap().cause(),
        HolderStopCause::CallWithdrawn
    );
    assert_eq!(
        serde_json::to_value(CallStopCause::CallWithdrawn).unwrap(),
        json!("callWithdrawn")
    );
}
