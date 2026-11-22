use approval_request_protocol::{
    AdmittedDestination, ApprovalWindow, DecideRequest, DecidedBy, DecisionFactor,
    DeliverEndingReply, DeliverEndingRequest, Delivery, Ending, EndingView, InvalidApprovalEnding,
    PrivateEnding, Raise, RequestView, RetainedSessionDecision, StopReply, WaitingStatus,
};
use serde_json::{Value, json};

const REQUEST_ID: &str = "01934a50-1000-7000-8000-000000000001";
const RUN_ID: &str = "01934a50-1000-7000-8000-000000000002";
const OPERATION_ID: &str = "01934a50-1000-7000-8000-000000000003";
const GIVER_SESSION: &str = "01934a50-1000-7000-8000-000000000004";
const OTHER_SESSION: &str = "01934a50-1000-7000-8000-000000000005";
const REQUESTER_SESSION: &str = "01934a50-1000-7000-8000-000000000006";
const PERSON_ID: &str = "01934a50-1000-7000-8000-000000000007";
const OTHER_REQUEST: &str = "01934a50-1000-7000-8000-000000000008";
const RAISED: &str = "2026-11-22T10:00:00Z";
const DEADLINE: &str = "2026-11-22T11:00:00Z";

fn person() -> serde_json::Result<DecidedBy> {
    serde_json::from_value(json!({"kind":"person","personId":PERSON_ID}))
}

fn session(id: &str) -> serde_json::Result<DecidedBy> {
    serde_json::from_value(json!({"kind":"session","session":id}))
}

fn window() -> serde_json::Result<ApprovalWindow> {
    serde_json::from_value(json!({"raisedAt":RAISED,"deadline":DEADLINE}))
}

fn provider_question() -> Value {
    json!({"kind":"providerQuestion","requestId":REQUEST_ID,
    "holder":{"sessionId":REQUESTER_SESSION,"routerRun":RUN_ID},
    "offer":{"prompt":"Name a region","fields":[
        {"kind":"text","fieldId":1,"label":"Region","description":null,"required":true}
    ]}})
}

fn provider_approval(lasts: Value) -> Value {
    json!({"kind":"providerApproval","requestId":REQUEST_ID,
    "holder":{"sessionId":REQUESTER_SESSION,"routerRun":RUN_ID},
    "offer":{"title":"Read config","description":null,"subject":null,"options":[
        {"id":1,"label":"Allow","effect":"allow","lasts":lasts}
    ]}})
}

fn decide_question() -> serde_json::Result<DecideRequest> {
    serde_json::from_value(json!({"requestId":REQUEST_ID,"operationId":OPERATION_ID,
    "value":{"kind":"question","answer":{"kind":"answered","fields":[
        {"fieldId":1,"value":{"kind":"text","value":"giver-only"}}
    ]}}}))
}

#[test]
fn admitted_destination_controls_saved_and_shared_standing_projections() {
    let raise: Raise = serde_json::from_value(provider_question()).unwrap();
    let approval_window = window().unwrap();
    let giver = session(GIVER_SESSION).unwrap();
    let other = session(OTHER_SESSION).unwrap();
    let requester = session(REQUESTER_SESSION).unwrap();
    let nonmember = session("01934a50-1000-7000-8000-000000000009").unwrap();
    let owner = person().unwrap();
    let members = [
        giver.clone(),
        other.clone(),
        requester.clone(),
        owner.clone(),
    ];
    let admitted_giver = AdmittedDestination::for_request(&raise, &giver, &members).unwrap();
    let admitted_other = AdmittedDestination::for_request(&raise, &other, &members).unwrap();
    assert!(matches!(
        AdmittedDestination::for_request(&raise, &requester, &members),
        Err(InvalidApprovalEnding::RequesterIsReader)
    ));
    assert!(matches!(
        AdmittedDestination::for_request(&raise, &nonmember, &members),
        Err(InvalidApprovalEnding::DestinationNotAdmitted)
    ));

    let decide = decide_question().unwrap();
    let ending = admitted_giver
        .decide(decide.value.clone(), DecisionFactor::NoFreshPersonFactor)
        .unwrap();
    let ending = DeliverEndingRequest::from_raise(&raise, ending).unwrap();
    let retained = RetainedSessionDecision {
        request_id: decide.request_id.clone(),
        session: GIVER_SESSION.parse().unwrap(),
        operation_id: decide.operation_id.clone(),
        answer: decide.value.clone(),
    };
    let success = decide.saved_reply(&admitted_giver, &retained).unwrap();
    assert!(
        serde_json::to_string(&success)
            .unwrap()
            .contains("giver-only")
    );
    assert!(decide.saved_reply(&admitted_other, &retained).is_err());
    let mut altered = decide.clone();
    altered.operation_id = OTHER_REQUEST.parse().unwrap();
    assert!(altered.saved_reply(&admitted_giver, &retained).is_err());
    altered = decide.clone();
    altered.request_id = OTHER_REQUEST.parse().unwrap();
    assert!(altered.standing_reply(&admitted_giver, &ending).is_err());

    let mut mismatched_ending = ending.clone();
    mismatched_ending.request_id = OTHER_REQUEST.parse().unwrap();
    assert!(
        decide
            .standing_reply(&admitted_giver, &mismatched_ending)
            .is_err()
    );
    assert!(
        RequestView::ended(
            &admitted_giver,
            approval_window,
            &mismatched_ending,
            approval_window.raised_at(),
            Delivery::Owed {}
        )
        .is_err()
    );

    let admitted_owner = AdmittedDestination::for_request(&raise, &owner, &members).unwrap();
    let giver_projection = EndingView::for_destination(&admitted_giver, &ending).unwrap();
    assert!(giver_projection.for_recipient(&owner).is_err());
    let owner_projection = EndingView::for_destination(&admitted_owner, &ending).unwrap();
    let other_projection = EndingView::for_destination(&admitted_other, &ending).unwrap();
    let owner_reply = owner_projection.for_recipient(&owner).unwrap();
    let other_reply = other_projection.for_recipient(&other).unwrap();
    for visible in [&owner_reply, &other_reply] {
        let wire = serde_json::to_string(visible).unwrap();
        assert!(wire.contains(GIVER_SESSION));
        assert!(!wire.contains("giver-only"));
    }
    let person_winner = DeliverEndingRequest::from_raise(
        &raise,
        admitted_owner
            .decide(decide.value, DecisionFactor::NoFreshPersonFactor)
            .unwrap(),
    )
    .unwrap();
    let owner_projection = EndingView::for_destination(&admitted_owner, &person_winner).unwrap();
    let owner_reply = owner_projection.for_recipient(&owner).unwrap();
    assert!(
        serde_json::to_string(&owner_reply)
            .unwrap()
            .contains("giver-only")
    );
    let other_projection = EndingView::for_destination(&admitted_other, &person_winner).unwrap();
    assert!(
        !serde_json::to_string(&other_projection.for_recipient(&other).unwrap())
            .unwrap()
            .contains("giver-only")
    );
    let read = RequestView::ended(
        &admitted_giver,
        approval_window,
        &ending,
        approval_window.raised_at(),
        Delivery::Owed {},
    )
    .unwrap();
    assert!(read.for_recipient(&other).is_err());
}

#[test]
fn handover_checks_exact_session_and_fresh_factor_preconditions() {
    for bad_incarnation in ["host/session", "01934a50-1000-4000-8000-000000000006"] {
        let mut bad_raise = provider_question();
        bad_raise["holder"]["sessionId"] = json!(bad_incarnation);
        assert!(serde_json::from_value::<Raise>(bad_raise).is_err());
    }

    for (lasts, ineligible_session) in [
        (json!({"kind":"thisRequest"}), REQUESTER_SESSION),
        (
            json!({"kind":"keptUntilChanged","kept":"agentSettingsFile"}),
            GIVER_SESSION,
        ),
        (json!({"kind":"notVerified"}), GIVER_SESSION),
    ] {
        let provider: Raise = serde_json::from_value(provider_approval(lasts)).unwrap();
        let forged = Ending::Decided {
            answer: serde_json::from_value(json!({"kind":"approval","optionId":1})).unwrap(),
            by: session(ineligible_session).unwrap(),
        };
        assert!(matches!(
            DeliverEndingRequest::from_raise(&provider, forged),
            Err(InvalidApprovalEnding::IneligibleDecider)
        ));
        let delivered = json!({
            "requestId":REQUEST_ID,"contentsDigest":provider.contents_digest().unwrap(),
            "ending":{"kind":"providerApproval","ending":{"kind":"decided","answer":1,"by":session(ineligible_session).unwrap()}}
        });
        let received: DeliverEndingRequest = serde_json::from_value(delivered).unwrap();
        assert!(matches!(
            received.check_bound_raise(&provider),
            Err(InvalidApprovalEnding::IneligibleDecider)
        ));
    }
    let person = person().unwrap();
    let retained: Raise =
        serde_json::from_value(provider_approval(json!({"kind":"notVerified"}))).unwrap();
    let owner = AdmittedDestination::for_request(&retained, &person, std::slice::from_ref(&person))
        .unwrap();
    let answer: approval_request_protocol::Answer =
        serde_json::from_value(json!({"kind":"approval","optionId":1})).unwrap();
    assert!(matches!(
        owner.decide(answer.clone(), DecisionFactor::NoFreshPersonFactor),
        Err(InvalidApprovalEnding::FreshFactorRequired)
    ));
    assert!(
        owner
            .decide(answer, DecisionFactor::FreshPersonFactor)
            .is_ok()
    );
    let forged_parked_ending = json!({"kind":"unprovenCall","ending":{"kind":"timedOut"}});
    assert!(serde_json::from_value::<PrivateEnding>(forged_parked_ending).is_err());
}

#[test]
fn views_check_ending_chronology_at_decode_and_build() {
    let approval_window = window().unwrap();
    let provider: Raise = serde_json::from_value(provider_question()).unwrap();
    let person = person().unwrap();
    let provider_dest =
        AdmittedDestination::for_request(&provider, &person, std::slice::from_ref(&person))
            .unwrap();
    let provider_waiting =
        RequestView::waiting(&provider_dest, WaitingStatus::PassedOn, approval_window);
    assert_eq!(
        serde_json::to_value(provider_waiting.view()).unwrap()["state"]["status"],
        "passedOn"
    );

    for (timed_out, ending, ended_at, accepted) in [
        (
            false,
            json!({"kind":"cancelled","reason":{"kind":"requesterGone"}}),
            "2026-11-22T09:59:59.999999999Z",
            false,
        ),
        (
            false,
            json!({"kind":"cancelled","reason":{"kind":"requesterGone"}}),
            RAISED,
            true,
        ),
        (
            false,
            json!({"kind":"cancelled","reason":{"kind":"requesterGone"}}),
            "2026-11-22T10:59:59.999999999Z",
            true,
        ),
        (
            false,
            json!({"kind":"cancelled","reason":{"kind":"requesterGone"}}),
            DEADLINE,
            false,
        ),
        (
            true,
            json!({"kind":"timedOut"}),
            "2026-11-22T10:59:59.999999999Z",
            false,
        ),
        (true, json!({"kind":"timedOut"}), DEADLINE, true),
        (
            true,
            json!({"kind":"timedOut"}),
            "2026-11-22T11:00:01Z",
            true,
        ),
    ] {
        let request_view = json!({"kind":"providerQuestion","requestId":REQUEST_ID,
            "offer":provider_question()["offer"],"window":approval_window,
            "state":{"kind":"ended","ending":{"kind":"standing","ending":ending},
                     "endedAt":ended_at,"delivery":{"kind":"owed"}}});
        assert_eq!(
            serde_json::from_value::<RequestView>(request_view).is_ok(),
            accepted,
            "{timed_out} {ended_at}"
        );
        assert_eq!(
            RequestView::ended(
                &provider_dest,
                approval_window,
                &DeliverEndingRequest::from_raise(
                    &provider,
                    serde_json::from_value(ending).unwrap()
                )
                .unwrap(),
                ended_at.parse().unwrap(),
                Delivery::Owed {},
            )
            .is_ok(),
            accepted,
            "{timed_out} {ended_at}"
        );
    }
}

#[test]
fn small_delivery_applies_only_exact_received_and_authoritative_closure() {
    let owed = Delivery::Owed {};
    let received = Delivery::ReceivedByHolder {};
    let closed = Delivery::ClosedWithoutReceipt {};
    assert_eq!(
        DeliverEndingReply::Received {}.delivery_after_reply(owed),
        received
    );
    assert_eq!(
        DeliverEndingReply::NotHeld {}.delivery_after_reply(owed),
        closed
    );
    assert_eq!(
        DeliverEndingReply::Received {}.delivery_after_reply(closed),
        received
    );
    assert_eq!(
        DeliverEndingReply::NotHeld {}.delivery_after_reply(received),
        received
    );
    for reply in [
        DeliverEndingReply::StopOwed {},
        DeliverEndingReply::HolderNotReady {},
        DeliverEndingReply::EndingDiffers {},
    ] {
        assert_eq!(reply.delivery_after_reply(owed), owed);
    }
    let standing: StopReply =
        serde_json::from_value(json!({"kind":"stands","ending":{"kind":"decidedEarlier"}}))
            .unwrap();
    assert_eq!(standing.delivery_after_stop(owed), closed);
    assert_eq!(standing.delivery_after_stop(received), received);
    assert_eq!(StopReply::NotVisible {}.delivery_after_stop(owed), owed);
    for retired_shape in [
        "noHandoverOwed",
        "receiptUnknown",
        "reported",
        "requesterGone",
    ] {
        assert!(serde_json::from_value::<Delivery>(json!({"kind":retired_shape})).is_err());
    }
}
