use approval_request_protocol::{InvocationDigest, Raise, RequestContentsDigest, canonical_json};
use serde_json::{Value, json};

#[test]
fn immutable_raise_bytes_are_fixed_for_each_request_kind() {
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/approval-digest-vectors.json")).unwrap();
    assert_eq!(fixtures.len(), 3);
    for fixture in fixtures {
        let request = &fixture["raise"];
        let raise: Raise = serde_json::from_value(request["body"].clone()).unwrap();
        if let Raise::UnprovenCall { invocation, .. } = &raise {
            assert_eq!(
                invocation,
                &InvocationDigest::from_bound_bytes(b"immutable parked invocation specimen")
            );
        }

        assert_eq!(
            canonical_json(&serde_json::to_value(&raise).unwrap()).unwrap(),
            request["canonical"].as_str().unwrap()
        );
        assert_eq!(
            raise.contents_digest().unwrap().as_str(),
            request["digest"].as_str().unwrap()
        );
        let mut different = request["body"].clone();
        different["holder"]["routerRun"] = json!("01934a50-1000-7000-8000-000000009999");
        let other_raise: Raise = serde_json::from_value(different).unwrap();
        assert_ne!(
            other_raise.contents_digest().unwrap(),
            raise.contents_digest().unwrap()
        );
    }
}

#[test]
fn digest_wire_encodings_are_canonical_and_do_not_accept_hex_or_padding() {
    let valid = "A".repeat(43);
    assert!(RequestContentsDigest::try_from(valid.clone()).is_ok());

    for malformed in [
        format!("{valid}="),
        "f".repeat(64),
        "A".repeat(42),
        format!("{}+", "A".repeat(42)),
        format!("{}B", "A".repeat(42)),
    ] {
        assert!(serde_json::from_value::<RequestContentsDigest>(json!(&malformed)).is_err());
    }
    assert!(canonical_json(&json!({"value": 2.5})).is_err());
}
