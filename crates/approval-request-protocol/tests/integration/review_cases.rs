use approval_request_protocol::{
    AdmittedDestination, Answer, ApprovalWindow, ChoiceOption, ChoiceOptions, DecideReply,
    DecidedBy, DeliverEndingRequest, Ending, Label, MultiChoiceSelection, OfferedOption,
    OfferedOptionId, OfferedOptions, OptionEffect, OptionLifetime, PrivateEnding, QuestionField,
    QuestionFieldId, QuestionFields, QuestionOffer, Raise, RaiseReply, RegisterRunRequest,
    RequestView, RetainedOptionStorage, TransientOptionStorage, WaitingStatus,
};
use serde_json::{Value, json};

const REQUEST: &str = "01934a50-1000-7000-8000-000000000001";
const RUN: &str = "01934a50-1000-7000-8000-000000000002";
const MACHINE: &str = "01934a50-1000-7000-8000-000000000003";
const PERSON: &str = "01934a50-1000-7000-8000-000000000004";
const RAISED: &str = "2026-11-22T10:00:00Z";
const DEADLINE: &str = "2026-11-22T11:00:00Z";

fn session() -> Value {
    json!(MACHINE)
}

fn checked_window() -> Value {
    json!({"raisedAt":RAISED,"deadline":DEADLINE})
}

fn provider_raise() -> Value {
    json!({
        "kind":"providerApproval","requestId":REQUEST,"holder":{"routerRun":RUN,"sessionId":MACHINE},
        "offer":{"title":"Use the tool","description":null,"subject":null,"options":[offered_option()]}
    })
}

fn offered_option() -> Value {
    json!({"id":1,"label":"Allow once","effect":"allow","lasts":{"kind":"thisRequest"}})
}

fn single_choice_question(choice: Value) -> Value {
    json!({"prompt":"Choose a region","fields":[
        {"kind":"singleChoice","fieldId":1,"label":"Region","required":true,"options":[choice]}
    ]})
}

fn multi_choice_field(minimum: u8, maximum: u8) -> Value {
    json!({
        "kind":"multiChoice","fieldId":1,"label":"Regions","description":null,"required":true,
        "selection":{"options":[{"optionId":1,"label":"Canada"}],"min":minimum,"max":maximum}
    })
}

#[test]
fn provider_projections_accept_only_bounded_local_ordinals() {
    assert!(OfferedOptionId::new(1).is_ok());
    assert!(OfferedOptionId::new(16).is_ok());
    assert!(OfferedOptionId::new(0).is_err());
    assert!(OfferedOptionId::new(17).is_err());
    assert!(QuestionFieldId::new(1).is_ok());
    assert!(QuestionFieldId::new(32).is_ok());
    assert!(QuestionFieldId::new(0).is_err());
    assert!(QuestionFieldId::new(33).is_err());

    for invalid in [
        json!("native-option"),
        json!("x".repeat(100_000)),
        json!(0),
        json!(17),
    ] {
        let mut specimen = provider_raise();
        specimen["offer"]["options"][0]["id"] = invalid.clone();
        assert!(serde_json::from_str::<Raise>(&specimen.to_string()).is_err());
    }
    for invalid in [
        json!("native-field"),
        json!("x".repeat(100_000)),
        json!(0),
        json!(33),
    ] {
        let mut specimen = single_choice_question(json!({"optionId":1,"label":"Canada"}));
        specimen["fields"][0]["fieldId"] = invalid;
        assert!(serde_json::from_str::<QuestionOffer>(&specimen.to_string()).is_err());
    }
}

#[test]
fn a_verified_short_duration_cannot_be_backed_by_a_file() {
    for effect in [OptionEffect::Allow, OptionEffect::Decline] {
        for duration in ["thisRequest", "thisTurn", "thisSession"] {
            for storage in ["agentSettingsFile", "agentRulesFile"] {
                let invalid = json!({"id":1,"label":"This time only","effect":effect,"lasts":{"kind":duration,"kept":storage}});
                assert!(serde_json::from_str::<OfferedOption>(&invalid.to_string()).is_err());
            }
        }
        for lifetime in [
            OptionLifetime::KeptUntilChanged {
                kept: RetainedOptionStorage::AgentSettingsFile,
            },
            OptionLifetime::KeptUntilChanged {
                kept: RetainedOptionStorage::AgentRulesFile,
            },
            OptionLifetime::NotVerified {},
        ] {
            let retained = OfferedOption {
                id: OfferedOptionId::new(1).unwrap(),
                label: Label::new("This time only").unwrap(),
                effect,
                lasts: lifetime,
            };
            assert!(retained.needs_person_with_fresh_factor());
            assert!(
                serde_json::from_str::<OfferedOption>(&serde_json::to_string(&retained).unwrap())
                    .is_ok()
            );
        }
        let temporary = OfferedOption {
            id: OfferedOptionId::new(1).unwrap(),
            label: Label::new("Always allow").unwrap(),
            effect,
            lasts: OptionLifetime::ThisSession {
                kept: TransientOptionStorage::AgentMemory,
            },
        };
        assert!(!temporary.needs_person_with_fresh_factor());
    }
}

#[test]
fn holder_and_destination_endings_reject_the_wrong_request_payload() {
    let mut other_raise = provider_raise();
    other_raise["requestId"] = json!(PERSON);
    let other_raise: Raise = serde_json::from_value(other_raise).unwrap();
    let provider: Raise = serde_json::from_value(provider_raise()).unwrap();
    let by: DecidedBy = serde_json::from_value(json!({"kind":"person","personId":PERSON})).unwrap();
    let approval = approval_request_protocol::ProviderAnswer::Approval {
        option_id: OfferedOptionId::new(1).unwrap(),
    };
    let offered = DeliverEndingRequest::from_raise(
        &provider,
        Ending::Decided {
            answer: approval,
            by,
        },
    )
    .unwrap();
    assert!(offered.check_bound_raise(&other_raise).is_err());
    assert!(offered.check_bound_raise(&provider).is_ok());

    let wrong_call_answer = json!({
        "kind":"unprovenCall",
        "ending":{"kind":"decided","answer":{"kind":"approval","optionId":1},"by":{"kind":"person","personId":PERSON}}
    });
    assert!(serde_json::from_value::<PrivateEnding>(wrong_call_answer).is_err());
    let mut holder_wire = serde_json::to_value(offered).unwrap();
    holder_wire["ending"]["ending"]["answer"] = json!(2);
    let unoffered: DeliverEndingRequest = serde_json::from_value(holder_wire).unwrap();
    assert!(unoffered.check_bound_raise(&provider).is_err());

    let wrong_provider_stop = json!({
        "kind":"providerQuestion",
        "ending":{"kind":"cancelled","reason":{"kind":"stopped","cause":"callWithdrawn"}}
    });
    assert!(serde_json::from_value::<PrivateEnding>(wrong_provider_stop).is_err());

    let mismatched_view = json!({
        "kind":"providerApproval","requestId":REQUEST,"offer":provider_raise()["offer"],"window":checked_window(),
        "state":{"kind":"ended","ending":{"kind":"giver","decision":{"answer":{"kind":"call","optionId":1},"by":{"kind":"person","personId":PERSON}}},"endedAt":RAISED,"delivery":{"kind":"owed"}}
    });
    assert!(serde_json::from_value::<RequestView>(mismatched_view.clone()).is_err());
    let mut wrong_answer_id = mismatched_view.clone();
    wrong_answer_id["state"]["ending"]["decision"]["answer"] = json!(2);
    assert!(serde_json::from_value::<RequestView>(wrong_answer_id).is_err());
    let mut wrong_stop_view = mismatched_view;
    wrong_stop_view["state"]["ending"] = json!({"kind":"standing","ending":{"kind":"cancelled","reason":{"kind":"stopped","cause":"callWithdrawn"}}});
    assert!(serde_json::from_value::<RequestView>(wrong_stop_view).is_err());
}

#[test]
fn a_decide_success_requires_answer_and_attribution() {
    for impossible in [
        json!({"kind":"savedDecision","decision":{"kind":"timedOut"}}),
        json!({"kind":"savedDecision","decision":{"answer":{"kind":"approval","optionId":1}}}),
        json!({"kind":"savedDecision","decision":{"by":{"kind":"person","personId":PERSON}}}),
    ] {
        assert!(serde_json::from_value::<DecideReply>(impossible).is_err());
    }
    let mut saved = json!({
        "kind":"savedDecision", "decision":{
            "answer":{"kind":"approval","optionId":1},
            "by":{"kind":"person","personId":PERSON}
        }
    });
    assert!(serde_json::from_value::<DecideReply>(saved.clone()).is_err());
    saved["decision"]["by"] = json!({"kind":"session","session":MACHINE});
    assert!(serde_json::from_value::<DecideReply>(saved).is_ok());
}

#[test]
fn empty_approval_options_are_refused_at_construction_and_decode() {
    assert!(
        OfferedOptions::new(Vec::new()).is_err(),
        "accepted zero options at construction"
    );
    let mut specimen = provider_raise();
    specimen["offer"]["options"] = json!([]);
    assert!(
        serde_json::from_str::<Raise>(&specimen.to_string()).is_err(),
        "accepted zero options on the wire"
    );
    assert!(serde_json::from_value::<Raise>(provider_raise()).is_ok());
}

#[test]
fn empty_question_fields_are_refused_at_construction_and_decode() {
    assert!(
        QuestionFields::new(Vec::new()).is_err(),
        "accepted zero fields at construction"
    );
    let mut specimen = single_choice_question(json!({"optionId":1,"label":"Canada"}));
    specimen["fields"] = json!([]);
    assert!(
        serde_json::from_str::<QuestionOffer>(&specimen.to_string()).is_err(),
        "accepted zero fields on the wire"
    );
    assert!(
        serde_json::from_value::<QuestionOffer>(single_choice_question(
            json!({"optionId":1,"label":"Canada"})
        ))
        .is_ok()
    );
}

#[test]
fn standalone_choices_reject_empty_duplicate_and_excessive_ranges() {
    let choice = ChoiceOption {
        option_id: OfferedOptionId::new(1).unwrap(),
        label: Label::new("Canada").unwrap(),
    };
    assert!(ChoiceOptions::new(Vec::new()).is_err());
    assert!(ChoiceOptions::new(vec![choice.clone(), choice.clone()]).is_err());
    let choices = ChoiceOptions::new(vec![choice]).unwrap();
    assert!(
        MultiChoiceSelection::new(choices.clone(), Some(1), Some(2)).is_err(),
        "accepted max above offered choices at construction"
    );
    assert!(MultiChoiceSelection::new(choices.clone(), Some(2), Some(1)).is_err());
    assert!(MultiChoiceSelection::new(choices, Some(1), Some(1)).is_ok());

    let invalid = multi_choice_field(1, 2);
    assert!(
        serde_json::from_str::<QuestionField>(&invalid.to_string()).is_err(),
        "accepted max above offered choices on the wire"
    );
    assert!(
        serde_json::from_value::<QuestionOffer>(json!({"prompt":"Choose","fields":[invalid]}))
            .is_err()
    );
    assert!(serde_json::from_value::<QuestionField>(multi_choice_field(1, 1)).is_ok());
    for options in [
        json!([]),
        json!([{"optionId":1,"label":"A"},{"optionId":1,"label":"B"}]),
    ] {
        let mut invalid = single_choice_question(json!({"optionId":1,"label":"Canada"}));
        invalid["fields"][0]["options"] = options;
        assert!(serde_json::from_value::<QuestionField>(invalid["fields"][0].clone()).is_err());
    }
}

#[test]
fn raise_receipt_rejects_a_nested_unknown_field() {
    let valid = json!({"kind":"receipt","receipt":{"requestId":REQUEST,"window":checked_window()}});
    assert!(serde_json::from_value::<RaiseReply>(valid.clone()).is_ok());
    let mut invalid = valid;
    invalid["receipt"]["extra"] = json!(true);
    assert!(
        serde_json::from_str::<RaiseReply>(&invalid.to_string()).is_err(),
        "nested RaiseReceipt accepted extra field"
    );
}

#[test]
fn question_offer_rejects_a_nested_unknown_choice_field() {
    let valid = json!({"optionId":1,"label":"Canada"});
    assert!(serde_json::from_value::<QuestionOffer>(single_choice_question(valid.clone())).is_ok());
    let mut invalid = valid;
    invalid["extra"] = json!(true);
    assert!(
        serde_json::from_str::<QuestionOffer>(&single_choice_question(invalid).to_string())
            .is_err(),
        "nested ChoiceOption accepted extra field"
    );
}

#[test]
fn first_save_and_deadline_are_one_checked_hour() {
    let valid: ApprovalWindow = serde_json::from_value(checked_window()).unwrap();
    assert_eq!(
        ApprovalWindow::from_first_save(valid.raised_at()).unwrap(),
        valid
    );
    assert_eq!(
        ApprovalWindow::new(valid.raised_at(), valid.deadline()).unwrap(),
        valid
    );
    for deadline in ["2026-11-22T10:59:59.999Z", RAISED, "2026-11-22T09:59:59Z"] {
        let mut invalid = checked_window();
        invalid["deadline"] = json!(deadline);
        assert!(serde_json::from_str::<ApprovalWindow>(&invalid.to_string()).is_err());
    }
    assert!(ApprovalWindow::from_first_save(chrono::DateTime::<chrono::Utc>::MAX_UTC).is_err());
}

#[test]
fn approval_named_objects_reject_positional_arrays_at_each_depth() {
    assert!(serde_json::from_str::<RaiseReply>(r#"["alreadyEnded"]"#).is_err());
    assert!(serde_json::from_str::<Answer>(r#"["approval",1]"#).is_err());
    assert!(
        serde_json::from_str::<approval_request_protocol::OptionLifetime>(
            r#"["thisTurn","agentMemory"]"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<approval_request_protocol::QuestionAnswerValue>("true").is_err()
    );

    assert!(serde_json::from_str::<RegisterRunRequest>(&json!([RUN]).to_string()).is_err());
    assert!(
        serde_json::from_value::<RaiseReply>(
            json!({"kind":"receipt","receipt":[REQUEST,checked_window()]})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<RaiseReply>(
            json!({"kind":"receipt","receipt":{"requestId":REQUEST,"window":[RAISED,DEADLINE]}})
        )
        .is_err()
    );

    let mut invalid_provider = provider_raise();
    invalid_provider["holder"]["sessionId"] = json!([MACHINE, "host/session"]);
    assert!(serde_json::from_str::<Raise>(&invalid_provider.to_string()).is_err());
    assert!(
        serde_json::from_value::<QuestionOffer>(single_choice_question(json!([1, "Canada"])))
            .is_err()
    );
}

#[test]
fn the_requester_cannot_construct_its_own_read_projection() {
    let raise: Raise = serde_json::from_value(provider_raise()).unwrap();
    let requester: DecidedBy =
        serde_json::from_value(json!({"kind":"session","session":session()})).unwrap();
    // Central resolved two addresses with the same native name to distinct incarnations.
    let native_requester: workspace_ids::SessionRef = serde_json::from_value(json!({
        "machineId":MACHINE,"sessionId":"host/session"
    }))
    .unwrap();
    let native_destination: workspace_ids::SessionRef = serde_json::from_value(json!({
        "machineId":"01934a50-1000-7000-8000-000000000006","sessionId":"host/session"
    }))
    .unwrap();
    assert_eq!(native_requester.session_id, native_destination.session_id);
    assert_ne!(native_requester.machine_id, native_destination.machine_id);
    let different: DecidedBy = serde_json::from_value(json!({
        "kind":"session","session":"01934a50-1000-7000-8000-000000000005"
    }))
    .unwrap();
    let window = serde_json::from_value(checked_window()).unwrap();
    let membership = [requester.clone(), different.clone()];
    assert!(AdmittedDestination::for_request(&raise, &requester, &membership).is_err());
    let admitted = AdmittedDestination::for_request(&raise, &different, &membership).unwrap();
    let view = RequestView::waiting(&admitted, WaitingStatus::Pending, window).unwrap();
    assert_eq!(view.view().request_id(), raise.request_id());
}
