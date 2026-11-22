use super::OrganizationStatementDigestReply;
use serde_json::json;

const DIGEST_REPLY: &[u8] = include_bytes!("../tests/fixtures/organization-statement-digest.json");

#[test]
fn digest_reply_retains_its_exact_wire_bytes() {
    let reply: OrganizationStatementDigestReply = serde_json::from_slice(DIGEST_REPLY).unwrap();
    assert_eq!(reply.signing_key_id.as_str(), "stamp-a");
    assert_eq!(reply.expires_at.unix_seconds().0, 1_900_000_060);
    assert_eq!(serde_json::to_vec(&reply).unwrap(), DIGEST_REPLY);
}

#[test]
fn digest_reply_rejects_empty_key_malformed_digest_and_nonportable_seconds() {
    let valid: serde_json::Value = serde_json::from_slice(DIGEST_REPLY).unwrap();
    for (field, invalid) in [
        ("signingKeyId", json!("")),
        ("digest", json!("not-a-digest")),
        ("digest", json!("F".repeat(64))),
        ("expiresAt", json!(-1)),
        ("expiresAt", json!(9_007_199_254_740_992_i64)),
    ] {
        let mut body = valid.clone();
        *body.get_mut(field).unwrap() = invalid;
        assert!(
            serde_json::from_value::<OrganizationStatementDigestReply>(body).is_err(),
            "{field}"
        );
    }
}
