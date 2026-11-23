use crate::{
    CentralCall, CentralCallReply, DataFrame, FrameDirection, LinkFrame, MacCall, MacCallAddress,
    MacCallReply, TypedCallResult, decode_text, encode_text,
};
use approval_request_protocol::{ApprovalNoticeReply, ListApprovalsForSessionReply};
use serde_json::{Value, json};

const RECORD: &str = "01934a50-1000-7000-8000-000000000001";
const SESSION: &str = "01934a50-1000-7000-8000-000000000002";
const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

fn inbound(call: &str, input: Value) -> Value {
    json!({
        "kind": "callCentral", "correlationId": RECORD,
        "request": {
            "call": call,
            "envelope": {
                "target": {"organizationId": RECORD},
                "body": {"sessionId": SESSION, "secret": SECRET, "input": input},
            },
        },
    })
}

#[test]
fn parent_session_calls_require_a_named_session_and_canonical_secret() {
    let cases = [
        inbound("listApprovalsForSession", json!({"cursor": null})),
        inbound("readApprovalForSession", json!({"requestId": RECORD})),
        inbound(
            "decideApprovalAsSession",
            json!({
                "requestId": RECORD, "operationId": SESSION,
                "value": {"kind": "unprovenCall", "decision": "allow"},
            }),
        ),
        inbound(
            "passOnApproval",
            json!({"requestId": RECORD, "operationId": SESSION}),
        ),
    ];
    for frame in cases {
        let result = decode_text(&frame.to_string(), FrameDirection::ToCentral).unwrap();
        let LinkFrame::Data(DataFrame::CallCentral { request, .. }) = &result else {
            panic!("expected a typed request");
        };
        assert!(matches!(
            &**request,
            CentralCall::ListApprovalsForSession(_)
                | CentralCall::ReadApprovalForSession(_)
                | CentralCall::DecideApprovalAsSession(_)
                | CentralCall::PassOnApproval(_)
        ));
        let rendered = encode_text(&result, FrameDirection::ToCentral).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&rendered).unwrap(), frame);
        let mut invalid = frame.clone();
        invalid["request"]["envelope"]["body"]["secret"] = json!("missing-proof");
        assert!(decode_text(&invalid.to_string(), FrameDirection::ToCentral).is_err());
        invalid["request"]["envelope"]["body"] = json!([SESSION, SECRET, {}]);
        assert!(decode_text(&invalid.to_string(), FrameDirection::ToCentral).is_err());
    }
}

fn page(next: Value) -> Value {
    json!({
        "kind":"centralCallReply","correlationId":RECORD,
        "result":{"kind":"answered","reply":{
            "call":"listApprovalsForSession","envelope":{
                "target":{"organizationId":RECORD},
                "body":{"kind":"page","items":[],"next":next}
            }
        }}
    })
}

#[test]
fn list_page_next_is_required_and_nullable_in_serde_and_the_schema() {
    let schema = serde_json::to_value(schemars::schema_for!(DataFrame)).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let terminal = page(Value::Null);
    let continuation = page(json!(RECORD));
    for valid in [&terminal, &continuation] {
        let decoded = decode_text(&valid.to_string(), FrameDirection::ToRouter).unwrap();
        let LinkFrame::Data(DataFrame::CentralCallReply { result, .. }) = decoded else {
            panic!("expected a typed page reply");
        };
        let TypedCallResult::Answered {
            reply: CentralCallReply::ListApprovalsForSession(reply),
        } = *result
        else {
            panic!("expected a page");
        };
        let ListApprovalsForSessionReply::Page { next, .. } = reply.body;
        assert_eq!(
            next.map(|cursor| cursor.to_string())
                .map_or(Value::Null, Value::String),
            valid["result"]["reply"]["envelope"]["body"]["next"]
        );
        assert!(
            validator.is_valid(valid),
            "valid page failed schema: {valid}"
        );
    }
    let mut missing = terminal;
    missing["result"]["reply"]["envelope"]["body"]
        .as_object_mut()
        .unwrap()
        .remove("next");
    assert!(decode_text(&missing.to_string(), FrameDirection::ToRouter).is_err());
    assert!(!validator.is_valid(&missing));
}

#[test]
fn the_notice_targets_only_its_session_and_has_a_closed_acknowledgement() {
    let target = json!({
        "organizationId": RECORD, "machineId": RECORD, "enrollmentId": RECORD, "sessionId": SESSION
    });
    let sent = json!({"kind":"callMac","correlationId":RECORD,"request":{
        "call":"approvalNotice","envelope":{"target":target,"body":{"requestId":RECORD}}
    }});
    let LinkFrame::Data(DataFrame::CallMac { request, .. }) =
        decode_text(&sent.to_string(), FrameDirection::ToRouter).unwrap()
    else {
        panic!("expected a notice");
    };
    assert!(matches!(&*request, MacCall::ApprovalNotice(_)));
    let address = MacCallAddress::from(&*request);
    for (kind, expected) in [
        ("received", ApprovalNoticeReply::Received {}),
        ("notHere", ApprovalNoticeReply::NotHere {}),
    ] {
        let reply = json!({"kind":"macCallReply","correlationId":RECORD,"result":{"kind":"answered","reply":{
            "call":"approvalNotice","envelope":{"target":target,"body":{"kind":kind}}
        }}});
        let LinkFrame::Data(DataFrame::MacCallReply { result, .. }) =
            decode_text(&reply.to_string(), FrameDirection::ToCentral).unwrap()
        else {
            panic!("expected a notice acknowledgement");
        };
        assert!(address.matches_reply(&result));
        assert!(
            matches!(*result, TypedCallResult::Answered { reply: MacCallReply::ApprovalNotice(envelope) }
            if envelope.body == expected)
        );
        let mut mismatched = reply.clone();
        mismatched["result"]["reply"]["envelope"]["target"]["sessionId"] = json!(RECORD);
        let LinkFrame::Data(DataFrame::MacCallReply { result, .. }) =
            decode_text(&mismatched.to_string(), FrameDirection::ToCentral).unwrap()
        else {
            panic!("expected a differently addressed acknowledgement");
        };
        assert!(!address.matches_reply(&result));
        mismatched["result"]["reply"]["envelope"]["body"]["kind"] = json!("read");
        assert!(decode_text(&mismatched.to_string(), FrameDirection::ToCentral).is_err());
    }
}
