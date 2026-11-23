use super::*;
use crate::{DataFrame, LinkFrame, ProtocolError, decode_text, encode_text};
use serde_json::{Value, json};
use test_calls::{MacProbe, ProbeReply, ProbeRequest};

const CORRELATION: &str = "019f0000-0000-7000-8000-000000000001";
const MACHINE: &str = "019f0000-0000-7000-8000-000000000002";

#[test]
fn typed_mac_calls_and_answers_round_trip_only_in_their_own_direction() {
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
        "request":{"call":"registerRun","envelope":{"target":{"organizationId":MACHINE},"body":{"routerRun":CORRELATION}}}
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
    bad_request["request"] = json!(["registerRun", {"target":MACHINE,"body":{"sequence":7}}]);
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

#[test]
fn typed_reply_wrappers_reject_unknown_fields_tags_and_positional_arrays() {
    let reply = json!({
        "kind":"centralCallReply","correlationId":CORRELATION,
        "result":{"kind":"answered","reply":{"call":"registerRun",
            "envelope":{"target":{"organizationId":MACHINE},"body":{"kind":"registered"}}}},
    });
    let mut missing_kind = reply.clone();
    missing_kind["result"]
        .as_object_mut()
        .unwrap()
        .remove("kind");
    let mut wrong_name = reply.clone();
    wrong_name["result"]["reply"]["call"] = json!("unregistered");
    let mut extra_result = reply.clone();
    extra_result["result"]["extra"] = json!(1);
    let mut positional_reply = reply.clone();
    positional_reply["result"]["reply"] =
        json!(["registerRun", {"target":MACHINE,"body":{"echoed":7}}]);
    let mut positional_envelope = reply;
    positional_envelope["result"]["reply"]["envelope"] = json!([MACHINE, {"echoed":7}]);
    for invalid in [
        missing_kind,
        wrong_name,
        extra_result,
        positional_reply,
        positional_envelope,
    ] {
        assert!(
            matches!(
                decode_text(&invalid.to_string(), FrameDirection::ToRouter),
                Err(ProtocolError::Json(_))
            ),
            "accepted malformed reply {invalid}"
        );
    }
}

#[test]
fn registered_service_call_and_reply_frames_keep_closed_concrete_schemas() {
    let schema = serde_json::to_value(schemars::schema_for!(DataFrame)).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let service = json!({"organizationId": CORRELATION});
    let holder = json!({
        "organizationId": CORRELATION, "machineId": MACHINE,
        "enrollmentId":"019f0000-0000-7000-8000-000000000003",
        "routerRun": "019f0000-0000-7000-8000-000000000004"
    });
    let run = &holder["routerRun"];
    let session_service = json!(CORRELATION);
    let digest = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let specimens = [
        (
            FrameDirection::ToCentral,
            "callCentral",
            "registerRun",
            &service,
            json!({"routerRun": run}),
            json!({"kind":"registered"}),
        ),
        (
            FrameDirection::ToCentral,
            "callCentral",
            "raise",
            &service,
            json!({"kind":"providerQuestion","requestId":CORRELATION,
                   "holder":{"sessionId":MACHINE,"routerRun":run},
                   "offer":{"prompt":"Choose a region","fields":[{"kind":"text","fieldId":1,
                     "label":"Region","description":null,"required":true}]}}),
            json!({"kind":"receipt","receipt":{"requestId":CORRELATION,
                   "window":{"raisedAt":"2030-03-17T17:46:40Z","deadline":"2030-03-17T18:46:40Z"}}}),
        ),
        (
            FrameDirection::ToCentral,
            "callCentral",
            "stop",
            &service,
            json!({"kind":"provider","requestId":CORRELATION,"routerRun":run,
                   "contentsDigest":digest,"cause":"turnStopped"}),
            json!({"kind":"stands","ending":{"kind":"timedOut"}}),
        ),
        (
            FrameDirection::ToCentral,
            "callCentral",
            "createFleetSession",
            &session_service,
            json!({"operationId":CORRELATION,"attempt":1,"creator":{"kind":"routerOperator"},
                   "agentProgram":"codex","sessionKind":{"kind":"ordinary"}, "project":{"kind":"named","projectRef":{"workspace":"019a3dce-2000-7000-8000-000000000004","project":"019a3dce-2000-7000-8000-000000000005"}}}),
            json!({"result":"superseded"}),
        ),
        (
            FrameDirection::ToCentral,
            "callCentral",
            "bindFleetSession",
            &session_service,
            json!({"sessionId":MACHINE,"harnessSessionId":"harness-session","secret":digest}),
            json!({"result":"noSuchStart"}),
        ),
        (
            FrameDirection::ToCentral,
            "callCentral",
            "endFleetSession",
            &session_service,
            json!({"target":{"kind":"centralId","sessionId":MACHINE},"cause":{"kind":"givenUp"}}),
            json!({"result":"noSuchStart"}),
        ),
        (
            FrameDirection::ToRouter,
            "callMac",
            "deliverEnding",
            &holder,
            json!({"requestId":CORRELATION,"contentsDigest":digest,
                   "ending":{"kind":"providerApproval","ending":{"kind":"timedOut"}}}),
            json!({"kind":"received"}),
        ),
    ];
    for (direction, frame_kind, call, target, request, reply) in specimens {
        let (reply_direction, reply_kind) = match direction {
            FrameDirection::ToCentral => (FrameDirection::ToRouter, "centralCallReply"),
            FrameDirection::ToRouter => (FrameDirection::ToCentral, "macCallReply"),
        };
        let request_wire = json!({
            "kind":frame_kind, "correlationId":CORRELATION,
            "request":{"call":call,"envelope":{"target":target,"body":request}}
        });
        let reply_wire = json!({
            "kind":reply_kind, "correlationId":CORRELATION,
            "result":{"kind":"answered","reply":{"call":call,"envelope":{"target":target,"body":reply}}}
        });
        for (wire, frame_direction, envelope) in [
            (request_wire, direction, "/request/envelope"),
            (reply_wire, reply_direction, "/result/reply/envelope"),
        ] {
            assert_round_trip(&validator, &wire, frame_direction);
            assert_rejects_open_envelope(&validator, &wire, frame_direction, envelope);
        }
    }

    for call in ["decide", "passOn"] {
        let unregistered = json!({
            "kind":"callCentral","correlationId":CORRELATION,
            "request":{"call":call,"envelope":{"target":service,"body":{}}}
        });
        assert!(!validator.is_valid(&unregistered));
        assert!(matches!(
            decode_text(&unregistered.to_string(), FrameDirection::ToCentral),
            Err(ProtocolError::UnknownTypedCall { .. })
        ));
    }
}

fn assert_round_trip(validator: &jsonschema::Validator, wire: &Value, direction: FrameDirection) {
    assert!(validator.is_valid(wire), "schema rejected {wire}");
    let frame = decode_text(&wire.to_string(), direction).unwrap();
    let encoded = encode_text(&frame, direction).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&encoded).unwrap(), *wire);
}

fn assert_rejects_open_envelope(
    validator: &jsonschema::Validator,
    wire: &Value,
    direction: FrameDirection,
    envelope: &str,
) {
    for invalid_field in ["target", "body"] {
        let mut invalid = wire.clone();
        invalid.pointer_mut(envelope).unwrap()[invalid_field] = json!(["not a named record"]);
        assert!(!validator.is_valid(&invalid), "schema admitted {invalid}");
        assert!(decode_text(&invalid.to_string(), direction).is_err());
    }
}

#[test]
fn the_link_registers_session_and_approval_calls_in_their_respective_directions() {
    use crate::{
        CentralLinkRpc, MacLinkRpc,
        session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc},
    };
    fn to_central<Call: CentralLinkRpc>() {}
    fn to_mac<Call: MacLinkRpc>() {}

    to_central::<CreateFleetSessionRpc>();
    to_central::<BindFleetSessionRpc>();
    to_central::<EndFleetSessionRpc>();
    to_central::<RegisterRunRpc>();
    to_central::<RaiseApprovalRpc>();
    to_central::<StopApprovalRpc>();
    to_mac::<DeliverEndingRpc>();
}

#[test]
fn typed_frame_layers_require_closed_named_records() {
    let schema = serde_json::to_value(schemars::schema_for!(DataFrame)).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    for (kind, direction, call, is_reply) in [
        (
            "callCentral",
            FrameDirection::ToCentral,
            "registerRun",
            false,
        ),
        ("callMac", FrameDirection::ToRouter, "macProbe", false),
        (
            "centralCallReply",
            FrameDirection::ToRouter,
            "registerRun",
            true,
        ),
        ("macCallReply", FrameDirection::ToCentral, "macProbe", true),
    ] {
        let body = if call == "registerRun" {
            if is_reply {
                json!({"kind":"registered"})
            } else {
                json!({"routerRun":CORRELATION})
            }
        } else if is_reply {
            json!({"echoed":7})
        } else {
            json!({"sequence":7})
        };
        let target = if call == "registerRun" {
            json!({"organizationId":MACHINE})
        } else {
            json!(MACHINE)
        };
        let named_call = json!({"call":call,"envelope":{"target":target,"body":body}});
        let content = if is_reply {
            json!({"kind":"answered","reply":named_call})
        } else {
            named_call
        };
        let field = if is_reply { "result" } else { "request" };
        let mut valid = json!({"kind":kind,"correlationId":CORRELATION});
        valid[field] = content.clone();
        assert_round_trip(&validator, &valid, direction);
        let call_path = if is_reply {
            "/result/reply"
        } else {
            "/request"
        };
        let envelope_path = format!("{call_path}/envelope");
        let body_path = format!("{envelope_path}/body");
        let mut invalids = Vec::new();
        invalids.push(json!([kind, CORRELATION, content]));
        let mut bad = valid.clone();
        bad.as_object_mut().unwrap().remove("kind");
        invalids.push(bad);
        let mut bad = valid.clone();
        bad["kind"] = json!("unregisteredFrame");
        invalids.push(bad);
        let mut bad = valid.clone();
        bad["extra"] = json!(true);
        invalids.push(bad);
        for pointer in [call_path, envelope_path.as_str(), body_path.as_str()] {
            let mut bad = valid.clone();
            bad.pointer_mut(pointer).unwrap()["extra"] = json!(true);
            invalids.push(bad);
        }
        let mut bad = valid.clone();
        bad.pointer_mut(call_path)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("call");
        invalids.push(bad);
        let mut bad = valid.clone();
        bad.pointer_mut(call_path).unwrap()["call"] = json!("unregistered");
        invalids.push(bad);
        let mut bad = valid.clone();
        *bad.pointer_mut(call_path).unwrap() = json!([call,{"target":MACHINE,"body":body}]);
        invalids.push(bad);
        let mut bad = valid.clone();
        *bad.pointer_mut(&envelope_path).unwrap() = json!([MACHINE, body]);
        invalids.push(bad);
        let mut bad = valid.clone();
        *bad.pointer_mut(&body_path).unwrap() = json!([7]);
        invalids.push(bad);
        if is_reply {
            let mut bad = valid.clone();
            bad["result"].as_object_mut().unwrap().remove("kind");
            invalids.push(bad);
            let mut bad = valid.clone();
            bad["result"]["kind"] = json!("unregistered");
            invalids.push(bad);
            let mut bad = valid.clone();
            bad["result"]["extra"] = json!(true);
            invalids.push(bad);
            let mut bad = valid.clone();
            bad["result"] = json!(["answered", valid.pointer(call_path).unwrap()]);
            invalids.push(bad);
        }
        for invalid in invalids {
            assert!(!validator.is_valid(&invalid), "schema accepted {invalid}");
            assert!(
                serde_json::from_value::<DataFrame>(invalid.clone()).is_err(),
                "direct reader accepted {invalid}"
            );
            assert!(
                decode_text(&invalid.to_string(), direction).is_err(),
                "codec accepted {invalid}"
            );
        }
    }
}

#[test]
fn direct_typed_readers_and_unit_refusals_are_closed_named_maps() {
    let schema = serde_json::to_value(schemars::schema_for!(DataFrame)).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let call = json!({"call":"registerRun","envelope":{"target":{"organizationId":MACHINE},"body":{"routerRun":CORRELATION}}});
    assert!(serde_json::from_value::<CentralCall>(call.clone()).is_ok());
    assert!(
        serde_json::from_value::<CentralCall>(json!(["registerRun", call["envelope"]])).is_err()
    );
    assert!(serde_json::from_value::<MacCall>(json!(["macProbe", call["envelope"]])).is_err());
    assert!(
        serde_json::from_value::<DataFrame>(json!([
            "renewMachineToken",
            CORRELATION,
            "header.payload.signature"
        ]))
        .is_err()
    );
    for kind in ["unknownCall", "wrongTarget", "centralFault"] {
        let valid = json!({"kind":kind});
        let refusal: LinkCallRefusal = serde_json::from_value(valid.clone()).unwrap();
        assert_eq!(serde_json::to_value(refusal).unwrap(), valid);
        for invalid in [
            json!({"kind":kind,"unused":true}),
            json!([kind]),
            json!({}),
            json!({"kind":"unknown"}),
        ] {
            assert!(serde_json::from_value::<LinkCallRefusal>(invalid).is_err());
        }
    }
    for root in ["macCallReply", "centralCallReply"] {
        let direction = if root == "macCallReply" {
            FrameDirection::ToCentral
        } else {
            FrameDirection::ToRouter
        };
        for kind in ["unknownCall", "wrongTarget", "authorityLapsed"] {
            let reason = if kind == "authorityLapsed" {
                json!({"kind":kind,"cause":"expired"})
            } else {
                json!({"kind":kind})
            };
            let wire = json!({"kind":root,"correlationId":CORRELATION,"result":{"kind":"refused","reason":reason}});
            assert!(validator.is_valid(&wire));
            assert!(decode_text(&wire.to_string(), direction).is_ok());
            let mut bad = wire.clone();
            bad["result"]["reason"]["extra"] = json!(true);
            assert!(!validator.is_valid(&bad));
            assert!(decode_text(&bad.to_string(), direction).is_err());
        }
    }
}

#[test]
fn central_probe_keeps_its_fixture_wire_and_is_unregistered_in_the_production_codec() {
    use crate::fixture_central_probe::{
        decode_probe_call, decode_probe_reply, encode_probe_call, encode_probe_reply,
    };
    let correlation: CorrelationId = CORRELATION.parse().unwrap();
    let request = Envelope {
        target: MACHINE.parse().unwrap(),
        body: ProbeRequest { sequence: 7 },
    };
    let text = encode_probe_call(correlation.clone(), request.clone()).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap(),
        json!({"kind":"callCentral","correlationId":CORRELATION,
        "request":{"call":"centralProbe","envelope":{"target":MACHINE,"body":{"sequence":7}}}})
    );
    assert_eq!(
        decode_probe_call(&text).unwrap(),
        (correlation.clone(), request)
    );
    assert!(matches!(
        decode_text(&text, FrameDirection::ToCentral),
        Err(ProtocolError::UnknownTypedCall { .. })
    ));
    let answer = TypedCallResult::Answered {
        reply: Envelope {
            target: MACHINE.parse().unwrap(),
            body: ProbeReply { echoed: 7 },
        },
    };
    let text = encode_probe_reply(correlation.clone(), answer.clone()).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap(),
        json!({"kind":"centralCallReply","correlationId":CORRELATION,
        "result":{"kind":"answered","reply":{"call":"centralProbe","envelope":{"target":MACHINE,"body":{"echoed":7}}}}})
    );
    assert_eq!(decode_probe_reply(&text).unwrap(), (correlation, answer));
}
