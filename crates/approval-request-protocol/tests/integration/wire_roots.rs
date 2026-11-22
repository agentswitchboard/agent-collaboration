use std::fmt::Debug;

use approval_request_protocol::{
    ApprovalOffer, Description, Ending, Label, QuestionOffer, Raise, Subject, Title,
    WaitingRevision,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const REQUEST_ID: &str = "01934a50-1000-7000-8000-000000000001";
const RUN_ID: &str = "01934a50-1000-7000-8000-000000000002";
const OPERATION_ID: &str = "01934a50-1000-7000-8000-000000000003";
const PERSON_ID: &str = "01934a50-1000-7000-8000-000000000004";
const MACHINE_ID: &str = "01934a50-1000-7000-8000-000000000005";
const RAISED_AT: &str = "2026-11-22T10:00:00Z";
const DEADLINE: &str = "2026-11-22T11:00:00Z";

fn session_ref() -> Value {
    json!({ "machineId": MACHINE_ID, "sessionId": "host/session" })
}

fn holder() -> Value {
    json!({ "session": session_ref(), "routerRun": RUN_ID })
}

fn offered_options() -> Value {
    json!([{
        "id": "once", "label": "Continue", "effect": "allow", "lasts": "thisRequest", "kept": "nowhere"
    }])
}

fn approval_offer() -> Value {
    json!({
        "title": "Use the tool",
        "description": null,
        "subject": "Read config",
        "options": offered_options()
    })
}

fn approval_raise() -> Value {
    json!({ "kind": "providerApproval", "requestId": REQUEST_ID, "holder": holder(), "offer": approval_offer() })
}

fn question_offer() -> Value {
    json!({
        "prompt": "Choose a region",
        "fields": [{"kind": "singleChoice", "fieldId": "region", "label": "Region",
                    "description": null, "required": true, "options": [{"optionId": "ca", "label": "Canada"}]}]
    })
}

fn call_offer() -> Value {
    json!({"operation": "writeConfig", "summary": "Write config",
        "invocation": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "options": offered_options() })
}

fn offered_approval() -> Value {
    json!({"kind": "providerApproval", "offer": approval_offer()})
}

fn approval_answer() -> Value {
    json!({"kind": "approval", "optionId": "once"})
}

fn deciding_person() -> Value {
    json!({"kind": "person", "personId": PERSON_ID})
}

fn ending() -> Value {
    json!({"kind": "decided", "answer": approval_answer(), "by": deciding_person()})
}

fn ending_with_answer() -> Value {
    json!({"kind": "decidedWithAnswer", "answer": approval_answer(), "by": deciding_person()})
}

fn request_view() -> Value {
    json!({
        "requestId": REQUEST_ID, "offer": offered_approval(),
        "state": {
            "kind": "ended", "ending": {"kind": "decided", "by": deciding_person()},
            "endedAt": RAISED_AT, "delivery": {"kind": "reported", "report": "deliveryUnknown"}
        },
        "raisedAt": RAISED_AT, "deadline": DEADLINE
    })
}

fn decide_request() -> Value {
    json!({"requestId": REQUEST_ID, "operationId": OPERATION_ID, "answer": approval_answer()})
}

fn waiting_list() -> Value {
    json!({"kind": "snapshot", "revision": 6,
           "requests": [{"requestId": REQUEST_ID, "offer": offered_approval()}]})
}

// The expected roots come from the operations and the approval design.
// The generator has its own registration list. These declarations drive neither its input nor its output.
macro_rules! expected_wire_roots {
    ($root:ident) => {{
        $root!(RegisterRunRequest, json!({"routerRun": RUN_ID}));
        $root!(RegisterRunReply, json!({"kind":"runRegistered"}), json!({"kind":"runRetired"}));
        $root!(Raise, approval_raise(),
            json!({"kind":"providerQuestion","requestId":REQUEST_ID,"holder":holder(),"offer":question_offer()}),
            json!({"kind":"unprovenCall","requestId":REQUEST_ID,"holder":{"routerRun":RUN_ID},"offer":call_offer()}));
        $root!(RaiseReply,
            json!({"kind":"raiseReceipt","receipt":{"requestId":REQUEST_ID,"raisedAt":RAISED_AT,"deadline":DEADLINE}}),
            json!({"kind":"alreadyStopped"}), json!({"kind":"alreadyEnded"}), json!({"kind":"contentsDiffer"}), json!({"kind":"tooManyOutstanding"}));
        $root!(StopRequest,
            json!({"kind":"provider","requestId":REQUEST_ID,"routerRun":RUN_ID,"cause":"turnStopped"}),
            json!({"kind":"provider","requestId":REQUEST_ID,"routerRun":RUN_ID,"cause":"sessionClosed"}),
            json!({"kind":"provider","requestId":REQUEST_ID,"routerRun":RUN_ID,"cause":"providerConnectionLost"}),
            json!({"kind":"unprovenCall","requestId":REQUEST_ID,"routerRun":RUN_ID,"cause":"callWithdrawn"}));
        $root!(HolderStopCause, json!("turnStopped"), json!("sessionClosed"),
            json!("providerConnectionLost"), json!("callWithdrawn"));
        $root!(StopReply,
            json!({"kind":"stopReceipt","ending":{"kind":"cancelled","reason":{"kind":"stopped","cause":"turnStopped"}}}),
            json!({"kind":"stopReceipt","ending":{"kind":"cancelled","reason":{"kind":"sessionEnded"}}}),
            json!({"kind":"stopReceipt","ending":{"kind":"cancelled","reason":{"kind":"enrollmentEnded"}}}),
            json!({"kind":"stopReceipt","ending":{"kind":"timedOut"}}),
            json!({"kind":"stopReceipt","ending":{"kind":"decidedEarlier"}}));
        $root!(StopReceiptEnding, json!({"kind":"timedOut"}), json!({"kind":"decidedEarlier"}));
        $root!(OfferEnding, json!({"kind":"provider","requestId":REQUEST_ID,"holder":holder(),"ending":ending()}),
            json!({"kind":"unprovenCall","requestId":REQUEST_ID,"holder":{"routerRun":RUN_ID},"ending":{"kind":"timedOut"}}));
        $root!(ReportDeliveryRequest,
            json!({"requestId":REQUEST_ID,"routerRun":RUN_ID,"report":"applied"}));
        $root!(ReportDeliveryReply,
            json!({"kind":"deliveryRecorded"}), json!({"kind":"reportDiffers"}), json!({"kind":"noHandoverOwed"}),
            json!({"kind":"refused","refusal":{"kind":"run","refusal":"runRetired"}}));
        $root!(ListPersonRequests, json!({}));
        $root!(ListPersonRequestsReply, json!({"kind":"listed","requests":[request_view()]}));
        $root!(ReadPersonRequest, json!({"requestId":REQUEST_ID}));
        $root!(ReadPersonRequestReply, json!({"kind":"read","request":request_view()}),
            json!({"kind":"refused","refusal":{"kind":"notVisible"}}));
        $root!(DecidePersonRequest, decide_request());
        $root!(DecidePersonReply, json!({"kind":"decided","ending":ending_with_answer()}));
        $root!(ListSessionRequests, json!({}));
        $root!(ListSessionRequestsReply, json!({"kind":"listed","requests":[request_view()]}));
        $root!(ReadSessionRequest, json!({"requestId":REQUEST_ID}));
        $root!(ReadSessionRequestReply, json!({"kind":"read","request":request_view()}),
            json!({"kind":"refused","refusal":{"kind":"notVisible"}}));
        $root!(DecideSessionRequest, decide_request());
        $root!(DecideSessionReply, json!({"kind":"decided","ending":ending_with_answer()}));
        $root!(PassOnRequest, json!({"requestId":REQUEST_ID,"operationId":OPERATION_ID}));
        $root!(PassOnReply, json!({"kind":"passedOn"}));
        $root!(WaitingListAtRequest, json!({"afterRevision":4}), json!({"afterRevision":null}));
        $root!(WaitingListAtReply, waiting_list(), json!({"kind":"unchanged","revision":6}),
            json!({"kind":"unavailable"}));
        $root!(OfferView, offered_approval());
        $root!(Answer, approval_answer(), json!({"kind":"call","optionId":"once"}),
            json!({"kind":"question","answer":{"kind":"declined"}}),
            json!({"kind":"question","answer":{"kind":"answered","fields":[
                {"fieldId":"text","value":"ca"}, {"fieldId":"check","value":true},
                {"fieldId":"number","value":2.5}, {"fieldId":"region","value":{"selectedOptionIds":["ca"]}}
            ]}}));
        $root!(DecidedBy, deciding_person(), json!({"kind":"session","session":session_ref()}));
        $root!(Ending, ending(), json!({"kind":"cancelled","reason":{"kind":"stopped","cause":"turnStopped"}}), json!({"kind":"timedOut"}));
        $root!(EndingView, ending_with_answer(), json!({"kind":"decided","by":deciding_person()}));
        $root!(HolderReport, json!("goneBeforeHandover"), json!("replyWrittenEffectNotConfirmed"),
            json!("deliveryUnknown"), json!("applied"));
        $root!(Delivery, json!({"kind":"noHandoverOwed"}), json!({"kind":"awaitingReport"}),
            json!({"kind":"reported","report":"deliveryUnknown"}), json!({"kind":"receiptUnknown"}));
        $root!(RequestState, json!({"kind":"waiting","status":"passedOn"}),
            json!({"kind":"ended","ending":ending()}));
        $root!(RequestView, request_view());
        $root!(Refusal, json!({"kind":"notVisible"}), json!({"kind":"invalidAnswer","reason":{"kind":"wrongKind"}}));
    }};
}

fn round_trip_root<WireRoot: Debug + DeserializeOwned + Serialize>(
    specimen: Value,
) -> Result<(), String> {
    let wire_text = specimen.to_string();
    let root_name = std::any::type_name::<WireRoot>();
    let decoded = serde_json::from_str::<WireRoot>(&wire_text)
        .map_err(|error| format!("{root_name} rejected {specimen}: {error}"))?;
    let serialized = serde_json::to_string(&decoded).map_err(|error| error.to_string())?;
    let regenerated: Value =
        serde_json::from_str(&serialized).map_err(|error| error.to_string())?;
    if regenerated != specimen {
        return Err(format!(
            "{root_name}: expected {specimen}, got {regenerated}"
        ));
    }

    let mut unknown_kind = specimen.clone();
    if let Some(kind) = unknown_kind.get_mut("kind") {
        *kind = json!("unrecognisedKind");
    } else if unknown_kind.is_string() {
        unknown_kind = json!("unrecognisedKind");
    }
    if unknown_kind != specimen
        && serde_json::from_str::<WireRoot>(&unknown_kind.to_string()).is_ok()
    {
        return Err(format!("{root_name} accepted an unknown variant"));
    }

    if let Some(mut extra_field) = specimen.as_object().cloned() {
        extra_field.insert("unrecognisedField".to_owned(), json!(true));
        if serde_json::from_str::<WireRoot>(&Value::Object(extra_field).to_string()).is_ok() {
            return Err(format!("{root_name} accepted an extra field"));
        }
    }
    Ok(())
}

#[test]
fn expected_rust_roots_round_trip_through_serde() -> Result<(), String> {
    macro_rules! check_shape {
        ($wire_type:ident, $($specimen:expr),+ $(,)?) => {
            $(round_trip_root::<approval_request_protocol::$wire_type>($specimen)?;)+
        };
    }
    expected_wire_roots!(check_shape);
    Ok(())
}

#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
pub(crate) fn check_expected_typescript_roots(typescript: &str) {
    macro_rules! check_export {
        ($wire_type:ident, $($specimen:expr),+ $(,)?) => {
            let declaration = concat!("export type ", stringify!($wire_type), " =");
            assert!(
                typescript.lines().any(|line| line.starts_with(declaration)),
                "missing TypeScript wire root {}",
                stringify!($wire_type)
            );
        };
    }
    expected_wire_roots!(check_export);
}

#[test]
fn raise_rejects_wrong_kind_holder_payload_and_extra_fields() {
    let mut wrong_kind = approval_raise();
    wrong_kind["kind"] = json!("providerQuestion");
    let mut unknown_kind = approval_raise();
    unknown_kind["kind"] = json!("externalCall");
    let mut approval_with_callback = approval_raise();
    approval_with_callback["holder"]["callback"] = json!("native-callback");
    let mut revised_offer = approval_raise();
    revised_offer["offer"]["revision"] = json!(2);
    let mut question_with_options = json!({
        "kind":"providerQuestion", "requestId":REQUEST_ID, "holder":holder(), "offer":question_offer()
    });
    question_with_options["offer"]["options"] = approval_offer()["options"].clone();
    let call_with_session = json!({
        "kind":"unprovenCall", "requestId":REQUEST_ID, "holder":holder(), "offer":call_offer()
    });
    let mut call_with_arguments = json!({
        "kind":"unprovenCall", "requestId":REQUEST_ID, "holder":{"routerRun":RUN_ID}, "offer":call_offer()
    });
    call_with_arguments["offer"]["arguments"] = json!({});
    for invalid in [
        wrong_kind,
        unknown_kind,
        approval_with_callback,
        revised_offer,
        question_with_options,
        call_with_session,
        call_with_arguments,
    ] {
        assert!(
            serde_json::from_str::<Raise>(&invalid.to_string()).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn text_and_collection_bounds_apply_at_construction_and_decode() {
    let at_limit = "é".repeat(200);
    let too_long = format!("{at_limit}é");
    assert!(Title::new(&at_limit).is_ok());
    assert!(Title::new(&too_long).is_err());
    assert!(serde_json::from_str::<Title>(&json!(&too_long).to_string()).is_err());
    assert!(serde_json::from_value::<Label>(json!(too_long)).is_err());
    assert!(serde_json::from_value::<Description>(json!("é".repeat(2_000))).is_ok());
    assert!(serde_json::from_value::<Description>(json!("é".repeat(2_001))).is_err());
    assert!(serde_json::from_value::<Subject>(json!("é".repeat(500))).is_ok());
    assert!(serde_json::from_value::<Subject>(json!("é".repeat(501))).is_err());

    let mut offer_at_limit = approval_offer();
    let base_option = offer_at_limit["options"][0].clone();
    let options: Vec<_> = (0..16)
        .map(|index| {
            let mut option = base_option.clone();
            option["id"] = json!(format!("choice-{index}"));
            option
        })
        .collect();
    offer_at_limit["options"] = json!(options);
    assert!(serde_json::from_value::<ApprovalOffer>(offer_at_limit.clone()).is_ok());
    let options = offer_at_limit["options"].as_array_mut().unwrap();
    options.push(base_option);
    assert!(serde_json::from_value::<ApprovalOffer>(offer_at_limit).is_err());

    let mut question_at_limit = question_offer();
    let base_field = question_at_limit["fields"][0].clone();
    let fields: Vec<_> = (0..32)
        .map(|index| {
            let mut field = base_field.clone();
            field["fieldId"] = json!(format!("field-{index}"));
            field
        })
        .collect();
    question_at_limit["fields"] = json!(fields);
    assert!(serde_json::from_value::<QuestionOffer>(question_at_limit.clone()).is_ok());
    let fields = question_at_limit["fields"].as_array_mut().unwrap();
    fields.push(base_field);
    assert!(serde_json::from_value::<QuestionOffer>(question_at_limit).is_err());

    let mut duplicate_option = approval_offer();
    duplicate_option["options"] = json!([
        duplicate_option["options"][0],
        duplicate_option["options"][0]
    ]);
    assert!(serde_json::from_value::<ApprovalOffer>(duplicate_option).is_err());
    let mut duplicate_field = question_offer();
    duplicate_field["fields"] = json!([duplicate_field["fields"][0], duplicate_field["fields"][0]]);
    assert!(serde_json::from_value::<QuestionOffer>(duplicate_field).is_err());

    let mut invalid_selection_range = question_offer();
    invalid_selection_range["fields"][0]["kind"] = json!("multiChoice");
    invalid_selection_range["fields"][0]["min"] = json!(2);
    assert!(serde_json::from_value::<QuestionOffer>(invalid_selection_range).is_err());
}

#[test]
fn answer_disclosure_names_the_winner_and_scopes_values_to_the_giver() {
    let ending: Ending = serde_json::from_value(ending()).unwrap();
    let giver = serde_json::from_value(deciding_person()).unwrap();
    let other = serde_json::from_value(json!({"kind":"session","session":session_ref()})).unwrap();
    assert_eq!(
        serde_json::to_value(ending.for_destination(&giver)).unwrap(),
        ending_with_answer()
    );
    assert_eq!(
        serde_json::to_value(ending.for_destination(&other)).unwrap(),
        json!({"kind":"decided","by":deciding_person()})
    );
}

#[test]
fn waiting_revision_round_trips_exactly_within_the_safe_integer_range() {
    assert!(serde_json::from_str::<WaitingRevision>("9007199254740991").is_ok());
    for invalid in ["9007199254740992", "-1", "0.5", "\"6\""] {
        assert!(
            serde_json::from_str::<WaitingRevision>(invalid).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn protocol_uses_the_checked_workspace_ids() {
    for invalid_id in [
        "01934A50-1000-7000-8000-000000000001",
        "01934a50-1000-4000-8000-000000000001",
        "01934a50-1000-7000-0000-000000000001",
    ] {
        let mut invalid_raise = approval_raise();
        invalid_raise["requestId"] = json!(invalid_id);
        let invalid_run = json!({"routerRun":invalid_id});
        assert!(
            serde_json::from_value::<Raise>(invalid_raise).is_err(),
            "{invalid_id}"
        );
        assert!(
            serde_json::from_value::<approval_request_protocol::RegisterRunRequest>(invalid_run)
                .is_err(),
            "{invalid_id}"
        );
    }
}

#[test]
fn stop_requires_a_cause_for_the_kind_of_wait_and_the_ending_keeps_that_cause() {
    use approval_request_protocol::{CancelReason, HolderStopCause, StopRequest};

    let stopped: StopRequest = serde_json::from_value(json!({
        "kind":"provider","requestId":REQUEST_ID,"routerRun":RUN_ID,"cause":"providerConnectionLost"
    }))
    .unwrap();
    assert_eq!(stopped.cause(), HolderStopCause::ProviderConnectionLost);
    assert_eq!(
        serde_json::to_value(Ending::Cancelled {
            reason: CancelReason::Stopped {
                cause: stopped.cause()
            }
        })
        .unwrap(),
        json!({"kind":"cancelled","reason":{"kind":"stopped","cause":"providerConnectionLost"}})
    );

    for (kind, cause) in [
        ("provider", "callWithdrawn"),
        ("unprovenCall", "turnStopped"),
        ("unprovenCall", "sessionClosed"),
        ("unprovenCall", "providerConnectionLost"),
    ] {
        let invalid = json!({"kind":kind,"requestId":REQUEST_ID,"routerRun":RUN_ID,"cause":cause});
        assert!(
            serde_json::from_str::<StopRequest>(&invalid.to_string()).is_err(),
            "{invalid}"
        );
    }
    let missing_cause = json!({"kind":"provider","requestId":REQUEST_ID,"routerRun":RUN_ID});
    assert!(serde_json::from_str::<StopRequest>(&missing_cause.to_string()).is_err());
}
