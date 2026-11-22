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
                {"kind":"boolean", "fieldId":1,"label":"Confirm","required":required},
                {"kind":"multiChoice", "fieldId":2,"label":"Regions","required":true,
                 "selection":{"min":minimum,"max":maximum,
                 "options":[{"optionId":1,"label":"Canada"},{"optionId":2,"label":"Australia"},
                            {"optionId":3,"label":"United States"}]}}
            ]
        }
    }))
}

fn question_answer(fields: Vec<Value>) -> serde_json::Result<Answer> {
    decoded_answer(json!({"kind":"question","answer":{"kind":"answered","fields":fields}}))
}

fn choice_answer(selected: &[u8]) -> Value {
    json!({"fieldId":2,"value":{"kind":"selectedOptions","selectedOptionIds":selected}})
}

#[test]
fn first_invalid_relationship_is_reported_for_shipped_answers() {
    let offer = question_with_choices(true, 1, 2).unwrap();
    let confirmed = json!({"fieldId":1,"value":{"kind":"boolean","value":true}});
    for (fields, expected_refusal) in [
        (
            vec![confirmed.clone(), choice_answer(&[1, 4])],
            json!({"kind":"unofferedOption","optionId":4}),
        ),
        (
            vec![choice_answer(&[1])],
            json!({"kind":"missingRequiredField","fieldId":1}),
        ),
        (
            vec![
                json!({"fieldId":1,"value":{"kind":"text","value":"yes"}}),
                choice_answer(&[1]),
            ],
            json!({"kind":"wrongValueType","fieldId":1}),
        ),
        (
            vec![confirmed.clone(), choice_answer(&[1, 1])],
            json!({"kind":"repeatedSelection","fieldId":2,"optionId":1}),
        ),
        (
            vec![confirmed.clone(), confirmed.clone(), choice_answer(&[1])],
            json!({"kind":"repeatedField","fieldId":1}),
        ),
        (
            vec![confirmed.clone(), choice_answer(&[])],
            json!({"kind":"wrongSelectionCount","fieldId":2}),
        ),
        (
            vec![confirmed.clone(), choice_answer(&[1, 2, 3])],
            json!({"kind":"wrongSelectionCount","fieldId":2}),
        ),
        (
            vec![
                confirmed.clone(),
                choice_answer(&[1]),
                json!({"fieldId":3,"value":{"kind":"boolean","value":true}}),
            ],
            json!({"kind":"unofferedField","fieldId":3}),
        ),
    ] {
        let actual = check_answer(&offer, &question_answer(fields).unwrap()).unwrap_err();
        assert_eq!(serde_json::to_value(actual).unwrap(), expected_refusal);
    }
    assert_eq!(
        check_answer(
            &offer,
            &question_answer(vec![confirmed, choice_answer(&[1])]).unwrap()
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
        selected.push([1, 2, 3, 4][index]);
    }
    let include_selection = case.draw(gs::booleans());
    let mut fields = Vec::new();
    match confirmation {
        0 => {}
        1 => fields.push(json!({"fieldId":1,"value":{"kind":"boolean","value":false}})),
        _ => fields.push(json!({"fieldId":1,"value":{"kind":"text","value":"false"}})),
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
        &[1][..],
        &[2][..],
        &[3][..],
        &[1, 2][..],
        &[1, 3][..],
        &[2, 3][..],
        &[1, 2, 3][..],
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
    let offered = [1, 2];
    let options = json!([
        {"id":offered[0],"label":"Allow","effect":"allow","lasts":{"kind":"thisRequest"}},
        {"id":offered[1],"label":"Decline","effect":"decline","lasts":{"kind":"thisRequest"}}
    ]);
    let offer = if parked {
        json!({"kind":"unprovenCall", "offer":{
            "operation":"readConfig","summary":"Read config","options":options
        }})
    } else {
        json!({"kind":"providerApproval","offer":{"title":"Allow a tool","options":options}})
    };
    let offer = decoded_offer(offer).unwrap();
    let selected_id = [1, 2, 3, 4][selected];
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
        json!({"kind":"answered","fields":[{"fieldId":2,"value":null}]}),
        json!({"kind":"answered","fields":[{"fieldId":2,"value":[1]}]}),
        json!({"kind":"answered","fields":[{"fieldId":2,"value":{"kind":"selectedOptions","selectedOptionIds":[1],"extra":true}}]}),
        json!({"kind":"answered","fields":[{"fieldId":2,"value":{"kind":"boolean","value":true},"extra":true}]}),
    ] {
        let invalid = json!({"kind":"question","answer":invalid});
        assert!(
            serde_json::from_str::<Answer>(&invalid.to_string()).is_err(),
            "{invalid}"
        );
    }
    let question_offer = json!({"prompt":"Confirm?", "fields":[
        {"kind":"boolean","fieldId":1,"label":"Confirm","required":true,"options":[]}
    ]});
    assert!(serde_json::from_value::<QuestionOffer>(question_offer).is_err());
}

#[test]
fn only_retained_or_unverified_lifetimes_require_a_fresh_factor() {
    for effect in ["allow", "decline", "abort"] {
        for (lifetime, needs_factor) in [
            (json!({"kind":"thisRequest"}), false),
            (json!({"kind":"thisTurn","kept":"agentMemory"}), false),
            (json!({"kind":"thisSession","kept":"agentMemory"}), false),
            (
                json!({"kind":"keptUntilChanged","kept":"agentRulesFile"}),
                true,
            ),
            (json!({"kind":"notVerified"}), true),
        ] {
            let offered_option: OfferedOption = serde_json::from_value(json!({
                "id":1,"label":"Always allow","effect":effect,"lasts":lifetime
            }))
            .unwrap();
            assert_eq!(
                offered_option.needs_person_with_fresh_factor(),
                needs_factor,
                "{effect}: {lifetime}"
            );
        }
    }
}

#[test]
fn question_numbers_preserve_exact_decimal_strings_and_refuse_other_spellings() {
    use approval_request_protocol::{InvalidQuestionNumber, QuestionNumber};

    for spelling in [
        "0",
        "1",
        "-123",
        "-0.125",
        "10.050",
        "9007199254740992000000000000000000000000",
    ] {
        let number = QuestionNumber::new(spelling).unwrap();
        assert_eq!(number.as_str(), spelling);
        let wire = json!({"kind":"question","answer":{"kind":"answered","fields":[
            {"fieldId":1,"value":{"kind":"number","value":spelling}}
        ]}});
        let answer: Answer = serde_json::from_str(&wire.to_string()).unwrap();
        assert_eq!(serde_json::to_value(answer).unwrap(), wire);
    }

    for spelling in [
        "", "-0", "-01", "00", "01.2", "+1", "1e2", "2.", ".2", "--1", "1.2.3", "NaN", " 1", "١",
    ] {
        assert_eq!(
            QuestionNumber::new(spelling),
            Err(InvalidQuestionNumber::InvalidSpelling)
        );
        assert!(serde_json::from_value::<QuestionNumber>(json!(spelling)).is_err());
    }

    assert_eq!(
        QuestionNumber::new("9".repeat(41)),
        Err(InvalidQuestionNumber::TooLong)
    );
    for other in [json!(7), json!(2.5), json!(true), json!(null)] {
        assert!(serde_json::from_value::<QuestionNumber>(other).is_err());
    }
}
