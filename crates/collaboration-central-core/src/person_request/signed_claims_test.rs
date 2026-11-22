use super::{PersonClaims, PersonTokenHeader, ProofClaims, ProofHeader};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

pub(crate) fn signed_claims_specimens() -> [(&'static str, Value, Value, Value); 4] {
    let person_header = json!({"alg": "EdDSA", "kid": "stamp-a", "typ": "at+jwt"});
    let accepted_person_header = json!({"alg": "EdDSA", "kid": "stamp-a"});
    let mut wrong_person_header = accepted_person_header.clone();
    wrong_person_header["kid"] = json!(7);

    let person_claims = person_claims_json();
    let mut accepted_person_claims = person_claims.clone();
    let claims_fields = accepted_person_claims
        .as_object_mut()
        .expect("claims object");
    claims_fields.remove("second_factor_at");
    claims_fields.remove("second_factor_method");
    claims_fields.insert("aud".into(), json!(["central-admin"]));
    let mut wrong_person_claims = accepted_person_claims.clone();
    wrong_person_claims["person_revision"] = json!("4");

    let proof_header = json!({
        "typ": "dpop+jwt",
        "alg": "ES256",
        "jwk": {
            "kty": "EC",
            "crv": "P-256",
            "x": "axfR8uEsQkf4vOblY6RA8ncDfYEt6zOg9KE5RdiYwpY",
            "y": "T-NC4v4af5uO5-tKfA-eFivOM1drMV7Oy7ZAaDe_UfU"
        }
    });
    let mut wrong_proof_header = proof_header.clone();
    wrong_proof_header["jwk"]["d"] = json!("unexpected-private-member");

    let proof_claims = proof_claims_json();
    let mut wrong_proof_claims = proof_claims.clone();
    wrong_proof_claims["intent"] = json!(false);

    [
        specimen::<PersonTokenHeader>(
            "PersonTokenHeader",
            person_header,
            accepted_person_header,
            wrong_person_header,
        ),
        specimen::<PersonClaims>(
            "PersonClaims",
            person_claims,
            accepted_person_claims,
            wrong_person_claims,
        ),
        specimen::<ProofHeader>(
            "ProofHeader",
            proof_header.clone(),
            proof_header,
            wrong_proof_header,
        ),
        specimen::<ProofClaims>(
            "ProofClaims",
            proof_claims.clone(),
            proof_claims,
            wrong_proof_claims,
        ),
    ]
}

fn specimen<Wire: Serialize + DeserializeOwned>(
    name: &'static str,
    expected_output: Value,
    accepted_input: Value,
    wrong_input: Value,
) -> (&'static str, Value, Value, Value) {
    let wire = serde_json::from_value::<Wire>(expected_output.clone())
        .unwrap_or_else(|error| panic!("{name} output could not be constructed: {error}"));
    let output = serde_json::to_value(wire).expect("serialize signed wire specimen");
    assert_eq!(output, expected_output, "{name} changed its write shape");
    assert!(
        serde_json::from_value::<Wire>(accepted_input.clone()).is_ok(),
        "{name} rejected its accepted input"
    );
    assert!(
        serde_json::from_value::<Wire>(wrong_input.clone()).is_err(),
        "{name} accepted the deliberate wrong input"
    );
    (name, output, accepted_input, wrong_input)
}

fn person_claims_json() -> Value {
    json!({
        "iss": "https://signin.example",
        "aud": "central-admin",
        "sub": "019f0000-0000-7000-8000-00000000d202",
        "sid": "019f0000-0000-7000-8000-00000000d206",
        "iat": 1_900_000_000,
        "exp": 1_900_000_290,
        "cnf": {"jkt": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
        "token_profile": "person-fleet",
        "organization_id": "019f0000-0000-7000-8000-00000000d201",
        "authorization_subject_id": "019f0000-0000-7000-8000-00000000d203",
        "person_revision": 4,
        "organization_revision": 3,
        "permission_policy_digest": "af40ec7fb999c4ef7f9611f322bab013e05666b56cd1d3101fac1e9e001d074a",
        "permissions": ["organization:view"],
        "second_factor_at": null,
        "second_factor_method": null,
        "session_expires_at_ms": 1_900_086_400_000_i64,
        "authorization_snapshot_at_ms": 1_899_999_990_000_i64,
        "authorization_snapshot_valid_until_ms": 1_900_000_200_000_i64
    })
}

fn proof_claims_json() -> Value {
    json!({
        "jti": "019f0000-0000-7000-8000-00000000d207",
        "htm": "POST",
        "htu": "https://central.example/organization-admin/v1/workspace-lookup",
        "iat": 1_900_000_000,
        "ath": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "intent": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    })
}

#[test]
fn signed_claims_decode_only_exact_nonnegative_timestamps_and_positive_revisions() {
    const LAST_EXACT: i64 = 9_007_199_254_740_991;
    const FIRST_UNSAFE: i64 = 9_007_199_254_740_992;

    for field in [
        "iat",
        "exp",
        "second_factor_at",
        "session_expires_at_ms",
        "authorization_snapshot_at_ms",
        "authorization_snapshot_valid_until_ms",
        "person_revision",
        "organization_revision",
    ] {
        let mut claims = person_claims_json();
        claims[field] = json!(LAST_EXACT);
        assert!(
            serde_json::from_value::<PersonClaims>(claims.clone()).is_ok(),
            "{field} rejected the largest safe integer"
        );
        for rejected in [-1, FIRST_UNSAFE, i64::MAX] {
            claims[field] = json!(rejected);
            assert!(
                serde_json::from_value::<PersonClaims>(claims.clone()).is_err(),
                "{field} accepted {rejected}"
            );
        }
    }

    for field in ["person_revision", "organization_revision"] {
        let mut claims = person_claims_json();
        claims[field] = json!(0);
        assert!(serde_json::from_value::<PersonClaims>(claims).is_err());
    }

    let mut proof = proof_claims_json();
    proof["iat"] = json!(LAST_EXACT);
    assert!(serde_json::from_value::<ProofClaims>(proof.clone()).is_ok());
    for rejected in [-1, FIRST_UNSAFE, i64::MAX] {
        proof["iat"] = json!(rejected);
        assert!(serde_json::from_value::<ProofClaims>(proof.clone()).is_err());
    }
}
