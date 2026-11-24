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
        r#"{"client_id":"019a0000-0000-7000-8000-000000000001","link_url":"https://central.example/static/link/"}"#,
        r#"{"client_id":"019a0000-0000-7000-8000-000000000001","link_url":"https://central.example/{organization_id}/link/{organization_id}"}"#,
        r#"{"client_id":"019a0000-0000-7000-8000-000000000001","link_url":"https://central.example/{organization_id}/link","extra":true}"#,
    ] {
        assert_eq!(
            decode_discovery(incorrect.as_bytes()),
            Err(DecodeAnswerError::Malformed)
        );
    }
}

#[test]
fn accepts_the_pinned_static_query_and_case_insensitive_https_scheme() {
    let body = br#"{
        "client_id":"019a0000-0000-7000-8000-000000000001",
        "link_url":"HTTPS://central.example/v1/organizations/{organization_id}?route=public&mode=direct"
    }"#;
    let discovery = decode_discovery(body).expect("the Sign-in schema accepts a static query");
    let organization = "019a0000-0000-7000-8000-000000000006"
        .parse()
        .expect("organization");
    assert_eq!(
        discovery
            .link_url
            .for_organization(organization)
            .expect("central route")
            .as_str(),
        "https://central.example/v1/organizations/019a0000-0000-7000-8000-000000000006?route=public&mode=direct"
    );
}

#[test]
fn malformed_discovery_templates_are_refused_without_echoing_the_input() {
    for template in [
        "/v1/opaque-input/{organization_id}/link",
        "http://central.example/opaque-input/{organization_id}",
        "https:///central.example/opaque-input/{organization_id}",
        "https://central.example/opaque-input/no-placeholder/link",
        "https://central.example/opaque-input/{organization_id}/{organization_id}",
        "https://central.example/opaque-input?org={organization_id}",
        "https://central.example/opaque-input/{organization_id}?org={organization_id}",
        "https://opaque-input@central.example/{organization_id}",
        "https://central.example/opaque-input/{organization_id}/../link",
        "https://central.example/opaque-input\\{organization_id}/link",
        "https://central.example/opaque-input/{organization_id}/link with spaces",
        "https://central.example/opaque-input/{organization_id}?mode=with spaces",
        "https://central.example/opaque-input/{organization_id}#fragment",
    ] {
        let body = serde_json::json!({
            "client_id": "019a0000-0000-7000-8000-000000000001",
            "link_url": template,
        })
        .to_string();
        let error = decode_discovery(body.as_bytes()).expect_err(template);
        assert_eq!(error, DecodeAnswerError::Malformed);
        assert!(!error.to_string().contains("opaque-input"));
    }
}
