use approval_request_protocol::{
    Answer, InvalidAnswer, OfferView, OfferedOption, QuestionOffer, check_answer,
};
use hegel::{TestCase, generators as gs};
use serde_json::{Value, json};

fn decoded_offer(offer: Value) -> serde_json::Result<OfferView> {
    serde_json::from_str(&offer.to_string())
}

fn decoded_answer(answer: Value) -> serde_json::Result<Answer> {
    serde_json::from_str(&answer.to_string())
}

fn question_with_choices(
    required: bool,
    minimum: u8,
    maximum: u8,
) -> serde_json::Result<OfferView> {
    decoded_offer(json!({
        "kind": "providerQuestion",
        "offer": {
            "prompt": "Answer both fields",
            "fields": [
                {"kind":"boolean", "fieldId":"confirmed","label":"Confirm","required":required},
                {"kind":"multiChoice", "fieldId":"regions","label":"Regions","required":true,
                 "min":minimum,"max":maximum,
                 "options":[{"optionId":"ca","label":"Canada"},{"optionId":"au","label":"Australia"},
                            {"optionId":"us","label":"United States"}]}
            ]
        }
    }))
}

fn question_answer(fields: Vec<Value>) -> serde_json::Result<Answer> {
    decoded_answer(json!({"kind":"question","answer":{"kind":"answered","fields":fields}}))
}

fn choice_answer(selected: &[&str]) -> Value {
    json!({"fieldId":"regions","value":{"selectedOptionIds":selected}})
}

#[test]
fn first_invalid_relationship_is_reported_for_shipped_answers() {
    let offer = question_with_choices(true, 1, 2).unwrap();
    let confirmed = json!({"fieldId":"confirmed","value":true});
    for (fields, expected_refusal) in [
        (
            vec![confirmed.clone(), choice_answer(&["ca", "xx"])],
            json!({"kind":"unofferedOption","optionId":"xx"}),
        ),
        (
            vec![choice_answer(&["ca"])],
            json!({"kind":"missingRequiredField","fieldId":"confirmed"}),
        ),
        (
            vec![
                json!({"fieldId":"confirmed","value":"yes"}),
                choice_answer(&["ca"]),
            ],
            json!({"kind":"wrongValueType","fieldId":"confirmed"}),
        ),
        (
            vec![confirmed.clone(), choice_answer(&["ca", "ca"])],
            json!({"kind":"repeatedSelection","fieldId":"regions","optionId":"ca"}),
        ),
        (
            vec![confirmed.clone(), confirmed.clone(), choice_answer(&["ca"])],
            json!({"kind":"repeatedField","fieldId":"confirmed"}),
        ),
        (
            vec![confirmed.clone(), choice_answer(&[])],
            json!({"kind":"wrongSelectionCount","fieldId":"regions"}),
        ),
        (
            vec![confirmed.clone(), choice_answer(&["ca", "au", "us"])],
            json!({"kind":"wrongSelectionCount","fieldId":"regions"}),
        ),
        (
            vec![
                confirmed.clone(),
                choice_answer(&["ca"]),
                json!({"fieldId":"extra","value":true}),
            ],
            json!({"kind":"unofferedField","fieldId":"extra"}),
        ),
    ] {
        let actual = check_answer(&offer, &question_answer(fields).unwrap()).unwrap_err();
        assert_eq!(serde_json::to_value(actual).unwrap(), expected_refusal);
    }
    assert_eq!(
        check_answer(
            &offer,
            &question_answer(vec![confirmed, choice_answer(&["ca"])]).unwrap()
        ),
        Ok(())
    );
    let declined = decoded_answer(json!({"kind":"question","answer":{"kind":"declined"}})).unwrap();
    assert_eq!(check_answer(&offer, &declined), Ok(()));
}

#[hegel::test]
fn filled_question_matches_the_independent_choice_set_and_presence_oracle(case: TestCase) {
    let confirmation_required = case.draw(gs::booleans());
    let confirmation = case.draw(gs::integers::<u8>().min_value(0).max_value(2));
    let lowest = case.draw(gs::integers::<u8>().min_value(0).max_value(3));
    let highest = case.draw(gs::integers::<u8>().min_value(lowest).max_value(3));
    let selection_count = case.draw(gs::integers::<u8>().min_value(0).max_value(5));
    let mut selected = Vec::new();
    for _selection in 0..selection_count {
        let index = case.draw(gs::integers::<usize>().min_value(0).max_value(3));
        selected.push(["ca", "au", "us", "unoffered"][index]);
    }
    let include_selection = case.draw(gs::booleans());
    let mut fields = Vec::new();
    match confirmation {
        0 => {}
        1 => fields.push(json!({"fieldId":"confirmed","value":false})),
        _ => fields.push(json!({"fieldId":"confirmed","value":"false"})),
    }
    if include_selection {
        fields.push(choice_answer(&selected));
    }
    if case.draw(gs::booleans()) {
        fields.reverse();
    }

    // Oracle: the regions field is one of the eight subsets of the three offered IDs,
    // with each member written once and with cardinality in the declared interval.
    let allowed_subsets = [
        &[][..],
        &["ca"][..],
        &["au"][..],
        &["us"][..],
        &["au", "ca"][..],
        &["ca", "us"][..],
        &["au", "us"][..],
        &["au", "ca", "us"][..],
    ];
    let mut sorted_selection = selected;
    sorted_selection.sort_unstable();
    let expected = matches!((confirmation_required, confirmation), (false, 0) | (_, 1))
        && include_selection
        && allowed_subsets.contains(&sorted_selection.as_slice())
        && (lowest..=highest).contains(&selection_count);

    let offer = question_with_choices(confirmation_required, lowest, highest).unwrap();
    assert_eq!(
        check_answer(&offer, &question_answer(fields).unwrap()).is_ok(),
        expected
    );
}

#[hegel::test]
fn request_kind_and_option_membership_are_both_required(case: TestCase) {
    let parked = case.draw(gs::booleans());
    let kind = case.draw(gs::integers::<usize>().min_value(0).max_value(2));
    let selected = case.draw(gs::integers::<usize>().min_value(0).max_value(3));
    let offered = ["allow-once", "decline"];
    let options = json!([
        {"id":offered[0],"label":"Allow","effect":"allow","lasts":"thisRequest","kept":"nowhere"},
        {"id":offered[1],"label":"Decline","effect":"decline","lasts":"thisRequest","kept":"nowhere"}
    ]);
    let offer = if parked {
        json!({"kind":"unprovenCall", "offer":{
            "operation":"readConfig","summary":"Read config","invocation":"a".repeat(64),"options":options
        }})
    } else {
        json!({"kind":"providerApproval","offer":{"title":"Allow a tool","options":options}})
    };
    let offer = decoded_offer(offer).unwrap();
    let selected_id = ["allow-once", "decline", "allow-all", "ALLOW-ONCE"][selected];
    let answer = match kind {
        0 => decoded_answer(json!({"kind":"approval","optionId":selected_id})),
        1 => decoded_answer(json!({"kind":"call","optionId":selected_id})),
        _ => decoded_answer(json!({"kind":"question","answer":{"kind":"declined"}})),
    }
    .unwrap();

    // Oracle: approval and call have disjoint answer alphabets. Each alphabet has these two entries.
    let expected = match (parked, kind, selected) {
        (false, 0, 0 | 1) | (true, 1, 0 | 1) => Ok(()),
        (false, 0, _) | (true, 1, _) => Err(InvalidAnswer::UnofferedOption {
            option_id: serde_json::from_value(json!(selected_id)).unwrap(),
        }),
        _ => Err(InvalidAnswer::WrongKind {}),
    };
    assert_eq!(check_answer(&offer, &answer), expected);
}

#[test]
fn question_shapes_reject_extra_fields_and_values_without_a_wire_type() {
    for invalid in [
        json!({"kind":"declined","fields":[]}),
        json!({"kind":"answered","fields":[{"fieldId":"regions","value":null}]}),
        json!({"kind":"answered","fields":[{"fieldId":"regions","value":["ca"]}]}),
        json!({"kind":"answered","fields":[{"fieldId":"regions","value":{"selectedOptionIds":["ca"],"extra":true}}]}),
        json!({"kind":"answered","fields":[{"fieldId":"regions","value":true,"extra":true}]}),
    ] {
        let invalid = json!({"kind":"question","answer":invalid});
        assert!(
            serde_json::from_str::<Answer>(&invalid.to_string()).is_err(),
            "{invalid}"
        );
    }
    let question_offer = json!({"prompt":"Confirm?", "fields":[
        {"kind":"boolean","fieldId":"confirm","label":"Confirm","required":true,"options":[]}
    ]});
    assert!(serde_json::from_value::<QuestionOffer>(question_offer).is_err());
}

#[test]
fn verified_storage_uses_the_declared_duration_for_allow_decline_and_abort() {
    for effect in ["allow", "decline", "abort"] {
        for (duration, needs_factor) in [
            ("thisRequest", false),
            ("thisTurn", false),
            ("thisSession", false),
            ("keptUntilChanged", true),
            ("notVerified", true),
        ] {
            let offered_option: OfferedOption = serde_json::from_value(json!({
                "id":"choice","label":"Always allow","effect":effect,"lasts":duration,"kept":"nowhere"
            })).unwrap();
            assert_eq!(
                offered_option.needs_person_with_fresh_factor(),
                needs_factor,
                "{effect}: {duration}"
            );
        }
    }
}

#[test]
fn either_unverified_option_field_requires_a_fresh_factor_for_allow_and_decline() {
    for effect in ["allow", "decline"] {
        for (duration, storage) in [("notVerified", "nowhere"), ("thisRequest", "notVerified")] {
            let offered_option: OfferedOption = serde_json::from_value(json!({
                "id": "choice", "label": "This time only", "effect": effect,
                "lasts": duration, "kept": storage
            }))
            .unwrap();
            assert!(
                offered_option.needs_person_with_fresh_factor(),
                "{effect} with duration {duration} and storage {storage} requires a fresh factor"
            );
        }
    }
}

#[test]
fn question_numbers_refuse_the_first_unsafe_integer_on_both_sides() {
    use approval_request_protocol::{InvalidQuestionNumber, QuestionNumber};
    use serde_json::Number;

    for boundary in [-9_007_199_254_740_991i64, 9_007_199_254_740_991] {
        let number = Number::from(boundary);
        assert_eq!(
            QuestionNumber::try_from(number.clone())
                .unwrap()
                .as_json_number(),
            &number
        );
        let wire_answer = format!(
            r#"{{"kind":"question","answer":{{"kind":"answered","fields":[{{"fieldId":"count","value":{boundary}}}]}}}}"#
        );
        let decoded: Answer = serde_json::from_str(&wire_answer).unwrap();
        let value = serde_json::to_value(decoded).unwrap();
        assert_eq!(value["answer"]["fields"][0]["value"], json!(boundary));
    }

    for boundary in [-9_007_199_254_740_992i64, 9_007_199_254_740_992] {
        assert_eq!(
            QuestionNumber::try_from(Number::from(boundary)),
            Err(InvalidQuestionNumber::UnsafeInteger)
        );
        for spelling in [
            boundary.to_string(),
            format!("{boundary}.0"),
            format!("{boundary}e0"),
        ] {
            let wire_answer = format!(
                r#"{{"kind":"question","answer":{{"kind":"answered","fields":[{{"fieldId":"count","value":{spelling}}}]}}}}"#
            );
            let error = serde_json::from_str::<Answer>(&wire_answer).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&InvalidQuestionNumber::UnsafeInteger.to_string()),
                "{error}"
            );
        }
    }

    for nonfinite in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            QuestionNumber::try_from(nonfinite),
            Err(InvalidQuestionNumber::NonFinite)
        );
    }
}
