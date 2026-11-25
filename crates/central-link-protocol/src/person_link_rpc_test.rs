use crate::{FrameDirection, ProtocolError, decode_text};
use serde_json::{Value, json};

const ORGANIZATION: &str = "019f0000-0000-7000-8000-00000000d201";
const WORKSPACE: &str = "019f0000-0000-7000-8000-00000000d202";
const CORRELATION: &str = "019f0000-0000-7000-8000-00000000d203";

#[test]
fn workspace_lookup_codec_rejects_positional_body_decoder() {
    assert_named_body(
        "lookupOrganizationWorkspace",
        json!({"workspaceId":WORKSPACE}),
        json!([WORKSPACE]),
    );
}

#[test]
fn change_records_codec_rejects_positional_body_decoder() {
    assert_named_body(
        "readOrganizationChangeRecords",
        json!({"after":null,"limit":1}),
        json!([null, 1]),
    );
}

fn assert_named_body(call: &str, named: Value, positional: Value) {
    let frame = |body: Value| {
        json!({
            "kind":"callCentral", "correlationId":CORRELATION,
            "request":{"call":call,"envelope":{"target":ORGANIZATION,"body":body}},
        })
        .to_string()
    };
    assert!(decode_text(&frame(named), FrameDirection::ToCentral).is_ok());
    let decoded = decode_text(&frame(positional), FrameDirection::ToCentral);
    assert!(
        matches!(decoded, Err(ProtocolError::Json(_))),
        "{call}: positional body must be rejected as invalid JSON, got {decoded:?}",
    );
}
