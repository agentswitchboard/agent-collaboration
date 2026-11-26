use serde::Deserialize;

use super::{ApprovalDecideRequest, ApprovalListRequest};

#[derive(Deserialize)]
struct DecideIntentVector {
    body: ApprovalDecideRequest,
    canonical: String,
    digest: String,
}

#[test]
fn decide_intent_matches_the_retained_canonical_bytes() {
    let vectors: [DecideIntentVector; 3] =
        serde_json::from_str(include_str!("approval-decide-intent-vectors.json")).unwrap();
    for vector in vectors {
        assert_eq!(vector.body.canonical_intent().unwrap(), vector.canonical);
        assert_eq!(vector.body.intent_digest().unwrap(), vector.digest);
    }
}

#[test]
fn approval_list_input_requires_a_named_map() {
    assert!(
        serde_json::from_str::<ApprovalListRequest>(
            r#"["019a3dce-2000-7000-8000-000000000001","waiting",null]"#,
        )
        .is_err()
    );
}
