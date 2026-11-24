use super::{DuplicateMacCallKind, HandledMacCalls, MacCallKind, MacCapabilities};
use crate::{DataFrame, FrameDirection, LinkFrame, ProtocolError, decode_text, encode_text};
use serde_json::{Value, json};

#[test]
fn capabilities_round_trip_to_central_with_a_required_set() {
    for handled_calls in [
        vec![],
        vec![MacCallKind::DeliverEnding],
        vec![MacCallKind::ApprovalNotice],
        vec![MacCallKind::DeliverEnding, MacCallKind::ApprovalNotice],
    ] {
        let expected_wire = json!({"kind":"macCapabilities","handledCalls":handled_calls});
        let frame = LinkFrame::Data(DataFrame::MacCapabilities(MacCapabilities {
            handled_calls: HandledMacCalls::try_from(handled_calls).unwrap(),
        }));

        let encoded = encode_text(&frame, FrameDirection::ToCentral).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&encoded).unwrap(),
            expected_wire
        );
        assert_eq!(
            decode_text(&encoded, FrameDirection::ToCentral).unwrap(),
            frame
        );
        assert!(matches!(
            decode_text(&encoded, FrameDirection::ToRouter),
            Err(ProtocolError::WrongDirection),
        ));
        assert!(matches!(
            encode_text(&frame, FrameDirection::ToRouter),
            Err(ProtocolError::WrongDirection),
        ));
    }
}

#[test]
fn duplicate_kinds_cannot_enter_the_capability_set() {
    let repeated_kind = MacCallKind::ApprovalNotice;
    assert_eq!(
        HandledMacCalls::try_from(vec![repeated_kind, repeated_kind]),
        Err(DuplicateMacCallKind {
            call_kind: repeated_kind
        })
    );
    let handled_calls = HandledMacCalls::try_from(vec![
        MacCallKind::ApprovalNotice,
        MacCallKind::DeliverEnding,
    ])
    .unwrap();
    assert!(handled_calls.contains(MacCallKind::DeliverEnding));
    assert!(handled_calls.contains(MacCallKind::ApprovalNotice));
    assert!(!HandledMacCalls::default().contains(MacCallKind::ApprovalNotice));
}

#[test]
fn capability_codec_and_schema_reject_invalid_announcements() {
    let schema = serde_json::to_value(schemars::schema_for!(DataFrame)).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let valid = json!({"kind":"macCapabilities","handledCalls":[]});
    assert!(validator.is_valid(&valid));
    for invalid in [
        json!({"kind":"macCapabilities"}),
        json!({"kind":"macCapabilities","handledCalls":null}),
        json!({"kind":"macCapabilities","handledCalls":"deliverEnding"}),
        json!({"kind":"macCapabilities","handledCalls":["macProbe"]}),
        json!({"kind":"macCapabilities","handledCalls":["futureCall"]}),
        json!({"kind":"macCapabilities","handledCalls":["deliverEnding","approvalNotice","deliverEnding"]}),
        json!({"kind":"macCapabilities","handledCalls":[],"extra":true}),
        json!(["macCapabilities", []]),
    ] {
        assert!(
            decode_text(&invalid.to_string(), FrameDirection::ToCentral).is_err(),
            "{invalid}"
        );
        assert!(!validator.is_valid(&invalid), "{invalid}");
    }
}
