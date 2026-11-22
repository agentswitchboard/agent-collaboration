use super::*;
use crate::{DataFrame, LinkFrame, ProtocolError, decode_text, encode_text};
use serde_json::{Value, json};
use test_calls::{CentralProbe, MacProbe, ProbeReply, ProbeRequest};

const CORRELATION: &str = "019f0000-0000-7000-8000-000000000001";
const MACHINE: &str = "019f0000-0000-7000-8000-000000000002";

#[test]
fn typed_calls_and_answers_round_trip_only_in_their_own_direction() {
    let correlation_id: CorrelationId = CORRELATION.parse().unwrap();
    let target: workspace_ids::MachineId = MACHINE.parse().unwrap();
    let request = Envelope {
        target: target.clone(),
        body: ProbeRequest { sequence: 7 },
    };
    let reply = Envelope {
        target,
        body: ProbeReply { echoed: 7 },
    };
    let frames = [
        (
            FrameDirection::ToCentral,
            DataFrame::CallCentral {
                correlation_id: correlation_id.clone(),
                request: Box::new(CentralProbe::request(request.clone())),
            },
            json!({
                "kind":"callCentral","correlationId":CORRELATION,
                "request":{"call":"centralProbe","envelope":{"target":MACHINE,"body":{"sequence":7}}}
            }),
        ),
        (
            FrameDirection::ToRouter,
            DataFrame::CentralCallReply {
                correlation_id: correlation_id.clone(),
                result: Box::new(TypedCallResult::Answered {
                    reply: CentralProbe::reply(reply.clone()),
                }),
            },
            json!({
                "kind":"centralCallReply","correlationId":CORRELATION,
                "result":{"kind":"answered","reply":{"call":"centralProbe","envelope":{"target":MACHINE,"body":{"echoed":7}}}}
            }),
        ),
        (
            FrameDirection::ToRouter,
            DataFrame::CallMac {
                correlation_id: correlation_id.clone(),
                request: Box::new(MacProbe::request(request)),
            },
            json!({
                "kind":"callMac","correlationId":CORRELATION,
                "request":{"call":"macProbe","envelope":{"target":MACHINE,"body":{"sequence":7}}}
            }),
        ),
        (
            FrameDirection::ToCentral,
            DataFrame::MacCallReply {
                correlation_id,
                result: Box::new(TypedCallResult::Answered {
                    reply: MacProbe::reply(reply),
                }),
            },
            json!({
                "kind":"macCallReply","correlationId":CORRELATION,
                "result":{"kind":"answered","reply":{"call":"macProbe","envelope":{"target":MACHINE,"body":{"echoed":7}}}}
            }),
        ),
    ];
    for (direction, data, expected) in frames {
        let frame = LinkFrame::Data(data);
        let encoded = encode_text(&frame, direction).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&encoded).unwrap(), expected);
        assert_eq!(decode_text(&encoded, direction).unwrap(), frame);
        let opposite = match direction {
            FrameDirection::ToCentral => FrameDirection::ToRouter,
            FrameDirection::ToRouter => FrameDirection::ToCentral,
        };
        assert!(matches!(
            decode_text(&encoded, opposite),
            Err(ProtocolError::WrongDirection)
        ));
    }
}

#[test]
fn unregistered_call_has_a_correlation_only_if_its_wrapper_is_valid() {
    let expected: CorrelationId = CORRELATION.parse().unwrap();
    for (kind, direction) in [
        ("callCentral", FrameDirection::ToCentral),
        ("callMac", FrameDirection::ToRouter),
    ] {
        let unknown = json!({
            "kind":kind,"correlationId":CORRELATION,
            "request":{"call":"notRegistered","envelope":{"target":MACHINE,"body":{"sequence":7}}}
        });
        assert!(matches!(
            decode_text(&unknown.to_string(), direction),
            Err(ProtocolError::UnknownTypedCall { correlation_id }) if correlation_id == expected
        ));
        let mut invalid = unknown;
        invalid["request"]["envelope"]["surprise"] = json!(true);
        assert!(matches!(
            decode_text(&invalid.to_string(), direction),
            Err(ProtocolError::Json(_))
        ));
    }
}

#[test]
fn call_wrappers_require_tags_and_envelope_fields_by_name() {
    let call = json!({
        "kind":"callCentral","correlationId":CORRELATION,
        "request":{"call":"centralProbe","envelope":{"target":MACHINE,"body":{"sequence":7}}}
    });
    let mut no_tag = call.clone();
    no_tag.as_object_mut().unwrap().remove("kind");
    let mut no_call = call.clone();
    no_call["request"].as_object_mut().unwrap().remove("call");
    let mut extra = call.clone();
    extra["extra"] = json!(1);
    let mut extra_request = call.clone();
    extra_request["request"]["extra"] = json!(1);
    let mut bad_envelope = call.clone();
    bad_envelope["request"]["envelope"] = json!([MACHINE, {"sequence":7}]);
    let mut bad_request = call.clone();
    bad_request["request"] = json!(["centralProbe", {"target":MACHINE,"body":{"sequence":7}}]);
    for invalid in [
        no_tag,
        no_call,
        extra,
        extra_request,
        bad_envelope,
        bad_request,
        json!([call]),
    ] {
        assert!(
            matches!(
                decode_text(&invalid.to_string(), FrameDirection::ToCentral),
                Err(ProtocolError::Json(_))
            ),
            "accepted malformed wrapper {invalid}"
        );
    }
}

#[test]
fn refusal_is_separate_from_a_registered_service_answer() {
    for (cause, expected) in [
        (MachineLinkCloseCode::Expired, "expired"),
        (MachineLinkCloseCode::Revoked, "revoked"),
        (MachineLinkCloseCode::Inactive, "inactive"),
    ] {
        let wire = json!({"kind":"macCallReply","correlationId":CORRELATION,
            "result":{"kind":"refused","reason":{"kind":"authorityLapsed","cause":expected}}
        });
        let expected = LinkFrame::Data(DataFrame::MacCallReply {
            correlation_id: CORRELATION.parse().unwrap(),
            result: Box::new(TypedCallResult::Refused {
                reason: LinkCallRefusal::AuthorityLapsed { cause },
            }),
        });
        assert_eq!(
            decode_text(&wire.to_string(), FrameDirection::ToCentral).unwrap(),
            expected
        );
    }
}
