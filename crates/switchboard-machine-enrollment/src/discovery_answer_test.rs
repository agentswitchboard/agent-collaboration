use super::{DecodeAnswerError, decode_discovery};

#[test]
fn discovery_requires_a_secure_organization_path() {
    let body = br#"{
        "client_id":"019a0000-0000-7000-8000-000000000001",
        "link_url":"https://central.example/v1/organizations/{organization_id}/link"
    }"#;
    let discovery = decode_discovery(body).expect("scripted sign-in discovery");
    assert_eq!(
        discovery.client_id.to_string(),
        "019a0000-0000-7000-8000-000000000001"
    );
    let org = "019a0000-0000-7000-8000-000000000006"
        .parse()
        .expect("organization");
    assert_eq!(
        discovery
            .link_url
            .for_organization(org)
            .expect("central route")
            .as_str(),
        "https://central.example/v1/organizations/019a0000-0000-7000-8000-000000000006/link"
    );

    for incorrect in [
        r#"{"client_id":"019a0000-0000-7000-8000-000000000001"}"#,
        r#"{"client_id":"019a0000-0000-7000-8000-000000000001","link_url":"http://central.example/{organization_id}/link"}"#,
        r#"{"client_id":"019a0000-0000-7000-8000-000000000001","link_url":"https://central.example/static/link"}"#,
        r#"{"client_id":"019a0000-0000-7000-8000-000000000001","link_url":"https://central.example/{organization_id}/link","extra":true}"#,
    ] {
        assert_eq!(
            decode_discovery(incorrect.as_bytes()),
            Err(DecodeAnswerError::Malformed)
        );
    }
}
