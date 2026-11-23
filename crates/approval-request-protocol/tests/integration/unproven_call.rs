use approval_request_protocol::{
    AdmittedDestination, Answer, ApprovalWindow, CallStopCause, DecideReply, DecideRequest,
    DecidedBy, DeliverEndingRequest, Delivery, Ending, EndingView, HolderStopCause,
    InvalidApprovalEnding, InvocationDigest, PrivateEnding, Raise, RequestView,
    SessionCallDecision, StopRequest, WaitingStatus,
};
use serde_json::{Value, json};

use super::map_only::assert_objects_require_named_maps;

const PERSON_ID: &str = "01934a50-1000-7000-8000-000000000004";
const OTHER_PERSON_ID: &str = "01934a50-1000-7000-8000-000000000005";
const SESSION_ID: &str = "01934a50-1000-7000-8000-000000000021";
const RUN_ID: &str = "01934a50-1000-7000-8000-000000000002";
const REQUEST_ID: &str = "01934a50-1000-7000-8000-000000000011";

fn start_summary() -> Value {
    json!({"kind":"startSession","agentProgram":"claude","workingFolder":"/project","mode":"plan","workFolders":["/project/docs","/project/src"]})
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

fn provider_raise_specimen() -> Value {
    json!({
        "kind":"providerApproval","requestId":REQUEST_ID,
        "holder":{"routerRun":RUN_ID,"sessionId":SESSION_ID},
        "offer":{"title":"Review","description":null,"subject":null,"options":[
            {"id":1,"label":"Allow","effect":"allow","lasts":{"kind":"thisRequest"}}
        ]}
    })
}

#[test]
fn a_projected_local_refusal_cannot_author_an_ending_for_either_family() {
    let provider: Raise = serde_json::from_value(provider_raise_specimen()).unwrap();
    let parked: Raise = serde_json::from_value(raise_specimen(end_summary())).unwrap();
    for (original, opposite, kind) in [
        (&provider, &parked, "providerApproval"),
        (&parked, &provider, "unprovenCall"),
    ] {
        let typed: PrivateEnding = serde_json::from_value(json!({
            "kind":kind,"ending":{"kind":"cancelled","reason":{
                "kind":"stopped","cause":"holderRefusedLocally"
            }}
        }))
        .unwrap();
        assert_eq!(typed.check_bound_raise(original), Ok(()));
        assert_eq!(
            typed.check_bound_raise(opposite),
            Err(InvalidApprovalEnding::BindingMismatch)
        );
        let projected = typed.into_ending();
        for target in [opposite, original] {
            assert_eq!(
                DeliverEndingRequest::from_raise(target, projected.clone()),
                Err(InvalidApprovalEnding::BindingMismatch),
                "{kind} against {:?}",
                target.kind()
            );
        }
    }
}

#[test]
fn an_exact_typed_stop_authors_a_local_refusal_only_for_its_bound_raise() {
    let provider: Raise = serde_json::from_value(provider_raise_specimen()).unwrap();
    let parked: Raise = serde_json::from_value(raise_specimen(end_summary())).unwrap();
    for (raise, opposite, stop_kind, other_stop_kind, ending_kind) in [
        (
            &provider,
            &parked,
            "provider",
            "unprovenCall",
            "providerApproval",
        ),
        (
            &parked,
            &provider,
            "unprovenCall",
            "provider",
            "unprovenCall",
        ),
    ] {
        let specimen = json!({
            "kind":stop_kind,"requestId":raise.request_id(),"routerRun":raise.router_run(),
            "contentsDigest":raise.contents_digest().unwrap(),"cause":"holderRefusedLocally"
        });
        let exact = serde_json::from_value::<StopRequest>(specimen.clone()).unwrap();
        let frame = DeliverEndingRequest::from_stop(raise, &exact).unwrap();
        assert_eq!(frame.check_bound_raise(raise), Ok(()));
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({
                "requestId":raise.request_id(),"contentsDigest":raise.contents_digest().unwrap(),
                "ending":{"kind":ending_kind,"ending":{"kind":"cancelled","reason":{
                    "kind":"stopped","cause":"holderRefusedLocally"
                }}}
            })
        );

        for (field, wrong_value) in [
            ("requestId", json!("01934a50-1000-7000-8000-000000000012")),
            ("routerRun", json!("01934a50-1000-7000-8000-000000000003")),
            ("contentsDigest", json!(opposite.contents_digest().unwrap())),
            ("kind", json!(other_stop_kind)),
        ] {
            let mut mismatched = specimen.clone();
            mismatched[field] = wrong_value;
            let mismatched = serde_json::from_value::<StopRequest>(mismatched).unwrap();
            assert_eq!(
                DeliverEndingRequest::from_stop(raise, &mismatched),
                Err(InvalidApprovalEnding::BindingMismatch),
                "{stop_kind} with mismatched {field}"
            );
        }
    }
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
    for folders in [json!(vec!["/project"; 17]), json!(["x".repeat(501)])] {
        let mut summary = start_summary();
        summary["workFolders"] = folders;
        assert!(serde_json::from_value::<Raise>(raise_specimen(summary)).is_err());
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
        waiting,
        json!({
            "kind":"unprovenCall","requestId":REQUEST_ID,"offer":{"operation":start_summary()},
            "state":{"kind":"waiting"},"window":window
        })
    );
    assert!(
        serde_json::to_value(raise.offer_view())
            .unwrap()
            .get("invocation")
            .is_none()
    );
    let mut disclosed = waiting.clone();
    disclosed["invocation"] = json!(InvocationDigest::from_bound_bytes(b"holder-only"));
    assert!(serde_json::from_value::<RequestView>(disclosed).is_err());
    assert_objects_require_named_maps::<RequestView>(&waiting).unwrap();
    let mut forwarded = waiting;
    forwarded["state"]["status"] = json!("passedOn");
    assert!(serde_json::from_value::<RequestView>(forwarded).is_err());

    for decision in [SessionCallDecision::Allow, SessionCallDecision::Decline] {
        let answer = Answer::UnprovenCall { decision };
        let ending = admitted.decide(answer).unwrap();
        let frame = DeliverEndingRequest::from_decision(&raise, ending).unwrap();
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
        let accepted = admitted.decide(Answer::UnprovenCall { decision }).unwrap();
        assert!(DeliverEndingRequest::from_decision(&different, accepted).is_err());
        let raw = Ending::UnprovenCallDecided {
            decision,
            person_id: PERSON_ID.parse().unwrap(),
        };
        assert!(DeliverEndingRequest::from_raise(&raise, raw).is_err());
        for (reader, identity, expected_ending) in [
            (
                &admitted,
                &actor,
                json!({"kind":"giver","personId":PERSON_ID,"decision":decision}),
            ),
            (
                &bystander,
                &other_person,
                json!({"kind":"decided","personId":PERSON_ID}),
            ),
        ] {
            let ended = RequestView::ended(
                reader,
                window,
                &frame,
                window.raised_at(),
                Delivery::Owed {},
            )
            .unwrap()
            .for_recipient(identity)
            .unwrap();
            let expected = json!({
                "kind":"unprovenCall","requestId":REQUEST_ID,"offer":{"operation":start_summary()},
                "state":{"kind":"ended","ending":expected_ending,"endedAt":window.raised_at(),"delivery":{"kind":"owed"}},
                "window":window
            });
            let wire = serde_json::to_value(ended).unwrap();
            assert_eq!(wire, expected);
            assert_objects_require_named_maps::<RequestView>(&wire).unwrap();
        }
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

#[test]
fn the_parent_session_wire_cannot_carry_parked_answers_or_standing() {
    let parked = json!({"kind":"unprovenCall","decision":"allow"});
    let request = |value| json!({"requestId":REQUEST_ID,"operationId":REQUEST_ID,"value":value});
    assert!(serde_json::from_value::<DecideRequest>(request(parked.clone())).is_err());
    assert!(
        serde_json::from_value::<DecideRequest>(request(json!({"kind":"approval","optionId":1})))
            .is_ok()
    );
    let saved = |answer| json!({"kind":"savedDecision","decision":{"answer":answer,"by":{"kind":"session","session":SESSION_ID}}});
    assert!(serde_json::from_value::<DecideReply>(saved(parked.clone())).is_err());
    assert!(
        serde_json::from_value::<DecideReply>(saved(
            json!({"kind":"question","answer":{"kind":"declined"}})
        ))
        .is_ok()
    );
    let standing = json!({"kind":"standing","ending":{"kind":"unprovenCall","ending":{"kind":"decided","personId":PERSON_ID}}});
    assert!(serde_json::from_value::<DecideReply>(standing).is_err());
    assert!(
        serde_json::from_value::<Ending>(
            json!({"kind":"decided","answer":parked,"by":{"kind":"person","personId":PERSON_ID}})
        )
        .is_err()
    );
}

#[test]
fn parked_cancelled_and_timed_out_frames_and_views_round_trip_with_named_maps() {
    let raise: Raise = serde_json::from_value(raise_specimen(end_summary())).unwrap();
    let actor: DecidedBy =
        serde_json::from_value(json!({"kind":"person","personId":PERSON_ID})).unwrap();
    let admitted =
        AdmittedDestination::for_request(&raise, &actor, std::slice::from_ref(&actor)).unwrap();
    let window = ApprovalWindow::from_first_save("2026-01-01T00:00:00Z".parse().unwrap()).unwrap();
    for (ending, ended_at) in [
        (
            json!({"kind":"cancelled","reason":{"kind":"stopped","cause":"callWithdrawn"}}),
            window.raised_at(),
        ),
        (json!({"kind":"timedOut"}), window.deadline()),
    ] {
        let wire = json!({"requestId":REQUEST_ID,"contentsDigest":raise.contents_digest().unwrap(),
            "ending":{"kind":"unprovenCall","ending":ending}});
        let frame: DeliverEndingRequest = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(&frame).unwrap(), wire);
        assert_objects_require_named_maps::<DeliverEndingRequest>(&wire).unwrap();
        let expected_view = json!({
            "kind":"unprovenCall","requestId":REQUEST_ID,"offer":{"operation":end_summary()},
            "state":{"kind":"ended","ending":ending,"endedAt":ended_at,"delivery":{"kind":"closedWithoutReceipt"}},
            "window":window
        });
        let view = RequestView::ended(
            &admitted,
            window,
            &frame,
            ended_at,
            Delivery::ClosedWithoutReceipt {},
        )
        .unwrap()
        .for_recipient(&actor)
        .unwrap();
        assert_eq!(serde_json::to_value(view).unwrap(), expected_view);
        assert_objects_require_named_maps::<RequestView>(&expected_view).unwrap();
    }
    let invalid = json!({"kind":"unprovenCall","requestId":REQUEST_ID,"routerRun":RUN_ID,
        "contentsDigest":raise.contents_digest().unwrap(),"cause":"askWithdrawn"});
    assert!(serde_json::from_value::<StopRequest>(invalid).is_err());
}
