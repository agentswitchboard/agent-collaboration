use approval_request_protocol::{
    DeliverEndingRequest, EndingDigest, InvocationDigest, PrivateEnding, Raise,
    RequestContentsDigest, canonical_json,
};
use serde_json::{Value, json};

#[test]
fn immutable_raise_and_private_ending_bytes_are_fixed_for_each_request_kind() {
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("../fixtures/approval-digest-vectors.json")).unwrap();
    assert_eq!(fixtures.len(), 3);
    for fixture in fixtures {
        let request = &fixture["raise"];
        let ending = &fixture["ending"];
        let raise: Raise = serde_json::from_value(request["body"].clone()).unwrap();
        let private_ending: PrivateEnding = serde_json::from_value(ending["body"].clone()).unwrap();
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
        assert_eq!(
            canonical_json(&serde_json::to_value(&private_ending).unwrap()).unwrap(),
            ending["canonical"].as_str().unwrap()
        );
        assert_eq!(
            private_ending.ending_digest().unwrap().as_str(),
            ending["digest"].as_str().unwrap()
        );

        let delivered =
            DeliverEndingRequest::from_raise(&raise, private_ending.into_ending()).unwrap();
        assert_eq!(
            serde_json::to_value(&delivered.ending).unwrap(),
            ending["body"]
        );
        assert!(delivered.check_bound_raise(&raise).is_ok());

        let mut different = request["body"].clone();
        different["holder"]["routerRun"] = json!("01934a50-1000-7000-8000-000000009999");
        let other_raise: Raise = serde_json::from_value(different).unwrap();
        assert_ne!(
            other_raise.contents_digest().unwrap(),
            delivered.contents_digest
        );
        assert!(delivered.check_bound_raise(&other_raise).is_err());
    }
}

#[test]
fn digest_wire_encodings_are_canonical_and_do_not_accept_hex_or_padding() {
    let valid = "A".repeat(43);
    assert!(RequestContentsDigest::try_from(valid.clone()).is_ok());
    assert!(EndingDigest::try_from(valid.clone()).is_ok());

    for malformed in [
        format!("{valid}="),
        "f".repeat(64),
        "A".repeat(42),
        format!("{}+", "A".repeat(42)),
        format!("{}B", "A".repeat(42)),
    ] {
        assert!(serde_json::from_value::<RequestContentsDigest>(json!(&malformed)).is_err());
        assert!(serde_json::from_value::<EndingDigest>(json!(malformed)).is_err());
    }
    assert!(canonical_json(&json!({"value": 2.5})).is_err());
}
