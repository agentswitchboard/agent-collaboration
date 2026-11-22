use std::fmt::Debug;

use approval_request_protocol::{
    AdmittedDestination, ApprovalOffer, DecideRequest, DeliverEndingRequest, Description, Ending,
    Label, QuestionOffer, Raise, Subject, Title,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

const REQUEST_ID: &str = "01934a50-1000-7000-8000-000000000001";
const RUN_ID: &str = "01934a50-1000-7000-8000-000000000002";
const OPERATION_ID: &str = "01934a50-1000-7000-8000-000000000003";
const PERSON_ID: &str = "01934a50-1000-7000-8000-000000000004";
const CENTRAL_SESSION_ID: &str = "01934a50-1000-7000-8000-000000000005";
const RAISED_AT: &str = "2026-11-22T10:00:00Z";
const DEADLINE: &str = "2026-11-22T11:00:00Z";
const DIGEST: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

const DESTINATION_SESSION_ID: &str = "01934a50-1000-7000-8000-000000000006";

fn deciding_session() -> Value {
    json!({"kind":"session","session":DESTINATION_SESSION_ID})
}

fn holder() -> Value {
    json!({ "sessionId": CENTRAL_SESSION_ID, "routerRun": RUN_ID })
}

fn offered_options() -> Value {
    json!([{
        "id": 1, "label": "Continue", "effect": "allow", "lasts":{"kind":"thisRequest"}
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
        "fields": [{"kind": "singleChoice", "fieldId": 1, "label": "Region",
                    "description": null, "required": true, "options": [{"optionId": 1, "label": "Canada"}]}]
    })
}

fn question_with_all_fields() -> Value {
    let choices = json!([{"optionId":1,"label":"Canada"}]);
    json!({"prompt":"Complete the form","fields":[
        {"kind":"text","fieldId":1,"label":"Name","description":null,"required":true},
        {"kind":"number","fieldId":2,"label":"Distance","description":null,"required":true},
        {"kind":"boolean","fieldId":3,"label":"Verified","description":null,"required":true},
        {"kind":"singleChoice","fieldId":4,"label":"Country","description":null,"required":true,"options":choices},
        {"kind":"multiChoice","fieldId":5,"label":"Region","description":null,"required":true,
            "selection":{"options":choices,"min":1,"max":1}}
    ]})
}

fn offered_approval() -> Value {
    json!({"kind": "providerApproval", "offer": approval_offer()})
}

fn approval_answer() -> Value {
    json!({"kind": "approval", "optionId": 1})
}

fn deciding_person() -> Value {
    json!({"kind": "person", "personId": PERSON_ID})
}

fn ending() -> Value {
    json!({"kind": "decided", "answer": approval_answer(), "by": deciding_person()})
}

fn ending_with_answer() -> Value {
    json!({"kind":"providerApproval","ending":{"kind":"giver","decision":{"answer":1,"by":deciding_person()}}})
}

fn approval_window() -> Value {
    json!({"raisedAt": RAISED_AT, "deadline": DEADLINE})
}

fn decision() -> Value {
    json!({"answer": approval_answer(), "by": deciding_session()})
}

fn request_view() -> Value {
    json!({
        "kind":"providerApproval", "requestId": REQUEST_ID, "offer": approval_offer(),
        "state": {
            "kind":"ended","ending":{"kind":"standing","ending":{"kind":"decided","by":deciding_person()}},
            "endedAt": RAISED_AT, "delivery": {"kind": "receivedByHolder"}
        },
        "window": approval_window()
    })
}

fn decide_request() -> Value {
    json!({"requestId": REQUEST_ID, "operationId": OPERATION_ID, "value": approval_answer()})
}

// The expected roots come from the operations and the approval design.
// The generator has its own registration list. These declarations drive neither its input nor its output.

fn authority_refused() -> Value {
    json!({"kind":"authorityRefused","refusal":{"kind":"runRetired"}})
}

fn private_approval_ending() -> Value {
    json!({"kind":"providerApproval","ending":{"kind":"decided","answer":1,"by":deciding_person()}})
}

fn standing_approval_ending() -> Value {
    json!({"kind":"providerApproval","ending":{"kind":"standing","ending":{"kind":"decided","by":deciding_person()}}})
}

macro_rules! expected_wire_roots {
    ($root:ident) => {{
        $root!(RegisterRunRequest, json!({"routerRun":RUN_ID}));
        $root!(RegisterRunReply, json!({"kind":"registered"}), json!({"kind":"runRetired"}),
            json!({"kind":"runNotRegistered"}), json!({"kind":"enrollmentEnded"}));
        $root!(RunRefusal, json!({"kind":"runRetired"}), json!({"kind":"runNotRegistered"}), json!({"kind":"enrollmentEnded"}));
        $root!(Raise, approval_raise(),
            json!({"kind":"providerQuestion","requestId":REQUEST_ID,"holder":holder(),"offer":question_offer()}));
        $root!(RaiseReceipt, json!({"requestId":REQUEST_ID,"window":approval_window()}));
        $root!(RaiseReply, json!({"kind":"receipt","receipt":{"requestId":REQUEST_ID,"window":approval_window()}}),
            json!({"kind":"alreadyStopped"}), json!({"kind":"sessionEnded"}),
            json!({"kind":"contentsDiffer"}), json!({"kind":"tooManyOutstanding"}), authority_refused());
        $root!(StopRequest,
            json!({"kind":"provider","requestId":REQUEST_ID,"routerRun":RUN_ID,"contentsDigest":DIGEST,"cause":"turnStopped"}),
            json!({"kind":"provider","requestId":REQUEST_ID,"routerRun":RUN_ID,"contentsDigest":DIGEST,"cause":"askReplaced"}));
        $root!(HolderStopCause, json!("turnStopped"), json!("sessionClosed"),
            json!("providerConnectionLost"), json!("askReplaced"));
        $root!(StopReply,
            json!({"kind":"stands","ending":{"kind":"cancelled","reason":{"kind":"stopped","cause":"turnStopped"}}}),
            json!({"kind":"stands","ending":{"kind":"cancelled","reason":{"kind":"sessionEnded"}}}),
            json!({"kind":"stands","ending":{"kind":"cancelled","reason":{"kind":"enrollmentEnded"}}}),
            json!({"kind":"stands","ending":{"kind":"timedOut"}}),
            json!({"kind":"stands","ending":{"kind":"decidedEarlier"}}),
            json!({"kind":"notVisible"}), authority_refused());
        $root!(StopReceiptEnding, json!({"kind":"timedOut"}), json!({"kind":"decidedEarlier"}));
        $root!(PrivateEnding, private_approval_ending(),
            json!({"kind":"providerQuestion","ending":{"kind":"decided","answer":{"kind":"declined"},"by":deciding_person()}}));
        $root!(DeliverEndingRequest, json!({"requestId":REQUEST_ID,"contentsDigest":DIGEST,"ending":private_approval_ending()}));
        $root!(DeliverEndingReply, json!({"kind":"received"}), json!({"kind":"stopOwed"}),
            json!({"kind":"notHeld"}), json!({"kind":"holderNotReady"}), json!({"kind":"endingDiffers"}));
        $root!(RequestContentsDigest, json!(DIGEST));
        $root!(DecideRequest, decide_request());
        $root!(DecideReply, json!({"kind":"savedDecision","decision":decision()}),json!({"kind":"standing","ending":standing_approval_ending()}),
            json!({"kind":"notVisible"}),json!({"kind":"invalid"}),json!({"kind":"tooLate"}),json!({"kind":"operationConflict"}));
        $root!(PassOnRequest, json!({"requestId":REQUEST_ID}));
        $root!(PassOnReply, json!({"kind":"forwarded"}),json!({"kind":"notVisible"}),json!({"kind":"tooLate"}),json!({"kind":"ended"}));
        $root!(OfferView, offered_approval(),
            json!({"kind":"providerQuestion","offer":question_offer()}));
        $root!(Answer, approval_answer(),
            json!({"kind":"question","answer":{"kind":"declined"}}),
            json!({"kind":"question","answer":{"kind":"answered","fields":[
                {"fieldId":1,"value":{"kind":"text","value":"ca"}}, {"fieldId":2,"value":{"kind":"boolean","value":true}},
                {"fieldId":3,"value":{"kind":"number","value":"2.5"}}, {"fieldId":4,"value":{"kind":"selectedOptions","selectedOptionIds":[1]}}
            ]}}));
        $root!(DecidedBy, deciding_person(), deciding_session());
        $root!(Ending, ending(), json!({"kind":"cancelled","reason":{"kind":"stopped","cause":"turnStopped"}}), json!({"kind":"timedOut"}));
        $root!(EndingView, ending_with_answer(), standing_approval_ending());
        $root!(Delivery, json!({"kind":"owed"}), json!({"kind":"receivedByHolder"}), json!({"kind":"closedWithoutReceipt"}));
        $root!(RequestState, json!({"kind":"waiting","status":"passedOn"}), json!({"kind":"ended","ending":ending()}));
        $root!(RequestView, request_view(),
            json!({"kind":"providerQuestion","requestId":REQUEST_ID,"offer":question_offer(),"window":approval_window(),
                "state":{"kind":"ended","ending":{"kind":"giver","decision":{"answer":{"kind":"declined"},"by":deciding_person()}},
                         "endedAt":RAISED_AT,"delivery":{"kind":"owed"}}}));
        $root!(ApprovalWindow, approval_window());
        $root!(ApprovalDecision, decision());
        $root!(OptionLifetime, json!({"kind":"thisRequest"}), json!({"kind":"thisTurn","kept":"agentMemory"}),
            json!({"kind":"thisSession","kept":"nowhere"}), json!({"kind":"keptUntilChanged","kept":"agentRulesFile"}), json!({"kind":"notVerified"}));
        $root!(CentralSessionId, json!(CENTRAL_SESSION_ID));
        $root!(SessionDecider, deciding_session());
        $root!(ProviderHolder, holder());
        $root!(QuestionOffer, question_with_all_fields());
        $root!(QuestionFieldAnswer, json!({"fieldId":1,"value":{"kind":"number","value":"2.50"}}));
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
    // These exported dependencies are exercised inside the standalone roots and
    // concrete generic instantiations. This inventory also detects added exports.
    let dependencies = [
        "ApprovalOffer",
        "ApprovalRequestId",
        "ApprovalWindowFields",
        "CancelReason",
        "ChoiceOption",
        "ChoiceOptions",
        "DecideReplyWire",
        "Description",
        "DestinationEnding",
        "EndingViewWire",
        "GiverDecision",
        "HolderEnding",
        "Label",
        "MultiChoiceSelection",
        "MultiChoiceSelectionFields",
        "OfferedOption",
        "OfferedOptionId",
        "OfferedOptions",
        "OptionEffect",
        "ProviderCancellation",
        "ProviderStopCause",
        "QuestionAnswer",
        "QuestionAnswerValue",
        "QuestionField",
        "QuestionFieldId",
        "QuestionFields",
        "QuestionNumber",
        "RequestStateView",
        "RequestViewFields",
        "RetainedOptionStorage",
        "RouterRunId",
        "StandingEnding",
        "Subject",
        "Title",
        "TransientOptionStorage",
        "WaitingStatus",
    ];
    let mut expected: std::collections::BTreeSet<_> = dependencies.into_iter().collect();
    macro_rules! check_export {
        ($wire_type:ident, $($specimen:expr),+ $(,)?) => {
            expected.insert(stringify!($wire_type));
            let declaration = concat!("export type ", stringify!($wire_type), " =");
            assert!(
                typescript.lines().any(|line| line.starts_with(declaration)),
                "missing TypeScript wire root {}",
                stringify!($wire_type)
            );
        };
    }
    expected_wire_roots!(check_export);
    let observed: std::collections::BTreeSet<_> = typescript
        .lines()
        .filter_map(|line| line.strip_prefix("export type "))
        .filter_map(|line| line.split(['<', ' ', '=']).next())
        .map(|name| {
            name.strip_suffix("_Serialize")
                .or_else(|| name.strip_suffix("_Deserialize"))
                .unwrap_or(name)
        })
        .collect();
    assert_eq!(
        observed, expected,
        "exported approval wire type inventory changed"
    );
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
    let mut later_kind = approval_raise();
    later_kind["kind"] = json!("unprovenCall");
    for invalid in [
        wrong_kind,
        unknown_kind,
        approval_with_callback,
        revised_offer,
        question_with_options,
        later_kind,
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
    let options: Vec<_> = (1..=16)
        .map(|index| {
            let mut option = base_option.clone();
            option["id"] = json!(index);
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
    let fields: Vec<_> = (1..=32)
        .map(|index| {
            let mut field = base_field.clone();
            field["fieldId"] = json!(index);
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
    invalid_selection_range["fields"][0]
        .as_object_mut()
        .unwrap()
        .remove("options");
    invalid_selection_range["fields"][0]["selection"] = json!({
        "options":[{"optionId":1,"label":"Canada"}],"min":2,"max":1
    });
    assert!(serde_json::from_value::<QuestionOffer>(invalid_selection_range).is_err());
}

#[test]
fn answer_disclosure_names_the_winner_and_scopes_values_to_the_giver() {
    let raise: Raise = serde_json::from_value(approval_raise()).unwrap();
    let giver = serde_json::from_value(deciding_session()).unwrap();
    let other = serde_json::from_value(json!({
        "kind":"session","session":"01934a50-1000-7000-8000-000000000007"
    }))
    .unwrap();
    let mut ending_wire = ending();
    ending_wire["by"] = deciding_session();
    let ending =
        DeliverEndingRequest::from_raise(&raise, serde_json::from_value(ending_wire).unwrap())
            .unwrap();
    let members = [giver, other];
    let sender = AdmittedDestination::for_request(&raise, &members[0], &members).unwrap();
    let reader = AdmittedDestination::for_request(&raise, &members[1], &members).unwrap();
    let decide: DecideRequest = serde_json::from_value(decide_request()).unwrap();
    let mut expected_giver = ending_with_answer();
    expected_giver["ending"]["decision"]["by"] = deciding_session();
    let mut expected_standing = standing_approval_ending();
    expected_standing["ending"]["ending"]["by"] = deciding_session();
    assert_eq!(
        serde_json::to_value(decide.standing_reply(&sender, &ending).unwrap()).unwrap()["ending"],
        expected_giver
    );
    assert_eq!(
        serde_json::to_value(decide.standing_reply(&reader, &ending).unwrap()).unwrap()["ending"],
        expected_standing
    );
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
        "kind":"provider","requestId":REQUEST_ID,"routerRun":RUN_ID,"contentsDigest":DIGEST,"cause":"providerConnectionLost"
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
    ] {
        let invalid = json!({"kind":kind,"requestId":REQUEST_ID,"routerRun":RUN_ID,"contentsDigest":DIGEST,"cause":cause});
        assert!(
            serde_json::from_str::<StopRequest>(&invalid.to_string()).is_err(),
            "{invalid}"
        );
    }
    let missing_cause = json!({"kind":"provider","requestId":REQUEST_ID,"routerRun":RUN_ID,"contentsDigest":DIGEST});
    assert!(serde_json::from_str::<StopRequest>(&missing_cause.to_string()).is_err());
}

#[test]
fn every_wire_root_checks_named_records_and_tagged_unions_at_each_depth() -> Result<(), String> {
    macro_rules! check_named {
        ($wire_type:ident, $($specimen:expr),+ $(,)?) => {
            $(crate::map_only::assert_objects_require_named_maps::<approval_request_protocol::$wire_type>(&$specimen)?;)+
        };
    }
    expected_wire_roots!(check_named);
    Ok(())
}

#[test]
fn each_concrete_ending_and_request_state_uses_the_named_reader() -> Result<(), String> {
    use super::map_only::assert_objects_require_named_maps;
    use approval_request_protocol::{EndingView, PrivateEnding, RequestView};

    for (kind, offer, answer, reasons) in [
        (
            "providerApproval",
            approval_offer(),
            json!(1),
            &[
                "requesterGone",
                "enrollmentEnded",
                "sessionEnded",
                "turnStopped",
            ][..],
        ),
        (
            "providerQuestion",
            question_offer(),
            json!({"kind":"declined"}),
            &[
                "requesterGone",
                "enrollmentEnded",
                "sessionEnded",
                "askReplaced",
            ][..],
        ),
    ] {
        let person = deciding_person();
        let mut endings = vec![
            json!({"kind":"timedOut"}),
            json!({"kind":"decided","answer":answer,"by":person}),
        ];
        endings.extend(reasons.iter().map(|reason| {
            let cancellation = match *reason {
                "turnStopped" | "askReplaced" | "callWithdrawn" => {
                    json!({"kind":"stopped","cause":reason})
                }
                _ => json!({"kind":reason}),
            };
            json!({"kind":"cancelled","reason":cancellation})
        }));
        for private in endings {
            let mut public = private.clone();
            assert_objects_require_named_maps::<PrivateEnding>(
                &json!({"kind":kind,"ending":private}),
            )?;
            let visible = if private["kind"] == "decided" {
                public.as_object_mut().unwrap().remove("answer");
                json!({"kind":"giver","decision":{"answer":answer,"by":person}})
            } else {
                json!({"kind":"standing","ending":public})
            };
            for projection in [visible, json!({"kind":"standing","ending":public})] {
                assert_objects_require_named_maps::<EndingView>(
                    &json!({"kind":kind,"ending":projection}),
                )?;
                let end_time = if private["kind"] == "timedOut" {
                    DEADLINE
                } else {
                    RAISED_AT
                };
                assert_objects_require_named_maps::<RequestView>(&json!({
                    "kind":kind,"requestId":REQUEST_ID,"offer":offer,"window":approval_window(),
                    "state":{"kind":"ended","ending":projection,"endedAt":end_time,"delivery":{"kind":"owed"}}
                }))?;
            }
        }
        assert_objects_require_named_maps::<RequestView>(&json!({
            "kind":kind,"requestId":REQUEST_ID,"offer":offer,"window":approval_window(),
            "state":{"kind":"waiting","status":"pending"}
        }))?;
    }
    Ok(())
}
