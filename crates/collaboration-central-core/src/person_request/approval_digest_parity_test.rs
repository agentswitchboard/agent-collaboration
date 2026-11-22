use super::{admin_operations::canonical_json, compact_jwt::sha256_url};
use serde_json::Value;

#[test]
fn approval_digests_use_the_existing_sign_in_canonical_bytes_and_encoding() {
    let fixtures: Vec<Value> = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../approval-request-protocol/tests/fixtures/approval-digest-vectors.json"
    )))
    .unwrap();

    for fixture in fixtures {
        let vector = &fixture["raise"];
        let current = canonical_json(&vector["body"]).unwrap();
        let shared = approval_request_protocol::canonical_json(&vector["body"]).unwrap();
        assert_eq!(current, shared);
        assert_eq!(shared, vector["canonical"].as_str().unwrap());
        assert_eq!(
            sha256_url(current.as_bytes()),
            approval_request_protocol::sha256_url(shared.as_bytes())
        );
        assert_eq!(
            approval_request_protocol::sha256_url(shared.as_bytes()),
            vector["digest"].as_str().unwrap()
        );
    }

    let fractional = serde_json::json!({"value": 1.5});
    assert!(canonical_json(&fractional).is_err());
    assert!(approval_request_protocol::canonical_json(&fractional).is_err());
}
