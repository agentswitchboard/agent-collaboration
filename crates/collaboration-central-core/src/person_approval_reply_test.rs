//! Fixed wire bytes for person approval replies on the WebSocket.

use approval_request_protocol::{ApprovalDecideReply, ApprovalListReply, ApprovalReadReply};
use serde_json::{Value, json};

#[test]
fn person_approval_replies_match_their_fixed_bytes() {
    let replies = reply_vectors();
    check_replies::<ApprovalListReply>(&replies["ApprovalListReply"]);
    check_replies::<ApprovalReadReply>(&replies["ApprovalReadReply"]);
    check_replies::<ApprovalDecideReply>(&replies["ApprovalDecideReply"]);
    // Match whole-second and fractional fixtures at the same positions.
    for (read, list) in replies["ApprovalReadReply"]
        .as_array()
        .unwrap()
        .iter()
        .take(2)
        .zip(
            replies["ApprovalListReply"]
                .as_array()
                .unwrap()
                .iter()
                .take(2),
        )
    {
        let ApprovalReadReply::Request { view } =
            serde_json::from_str::<ApprovalReadReply>(read.as_str().unwrap()).unwrap()
        else {
            panic!("expected request view")
        };
        let list: Value = serde_json::from_str(list.as_str().unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(approval_request_protocol::RequestSummary::from(&view)).unwrap(),
            list["items"][0]
        );
    }
}

fn check_replies<Reply: serde::de::DeserializeOwned + serde::Serialize>(vectors: &Value) {
    for canonical in vectors.as_array().unwrap() {
        let bytes = canonical.as_str().unwrap();
        let decoded: Reply = serde_json::from_str(bytes).unwrap();
        let encoded = serde_json::to_value(decoded).unwrap();
        assert_eq!(
            approval_request_protocol::canonical_json(&encoded).unwrap(),
            bytes
        );
    }
}

#[test]
fn approval_people_list_requires_its_nullable_continuation() {
    let last: ApprovalListReply =
        serde_json::from_str(r#"{"kind":"page","items":[],"next":null}"#).unwrap();
    assert_eq!(
        serde_json::to_value(last).unwrap(),
        json!({"kind":"page","items":[],"next":null})
    );
    for malformed in [
        r#"{"kind":"page","items":[]}"#,
        r#"["page",[],null]"#,
        r#"{"kind":"page","items":[],"next":null,"extra":false}"#,
    ] {
        assert!(
            serde_json::from_str::<ApprovalListReply>(malformed).is_err(),
            "{malformed}"
        );
    }
}

fn reply_vectors() -> Value {
    serde_json::from_str(include_str!(
        "../contracts/approval-person-reply-vectors.json"
    ))
    .unwrap()
}
