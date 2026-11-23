//! Conflicting preflight failures through each real private HTTP entry.

use crate::ConformanceResult;
use reqwest::{Client, Method, StatusCode, header::ALLOW};

pub async fn check_approval_preflight(
    client: &Client,
    origin: &str,
    expected_entry: Option<&str>,
) -> ConformanceResult<()> {
    let cases = [
        (
            Method::PUT,
            &[][..],
            StatusCode::METHOD_NOT_ALLOWED,
            Some("POST"),
        ),
        (Method::POST, &[][..], StatusCode::UNAUTHORIZED, None),
        (
            Method::POST,
            &[("authorization", "DPoP fixture-token")],
            StatusCode::UNAUTHORIZED,
            None,
        ),
        (
            Method::POST,
            &[("dpop", "fixture-proof")],
            StatusCode::UNAUTHORIZED,
            None,
        ),
        (
            Method::POST,
            &[
                ("authorization", "DPoP fixture-token"),
                ("dpop", "fixture-proof"),
            ],
            StatusCode::BAD_REQUEST,
            None,
        ),
    ];
    let mut differences = Vec::new();
    for (method, headers, expected_status, expected_allow) in cases {
        let mut request = client
            .request(
                method.clone(),
                format!("{origin}/organization-admin/v1/approval-list"),
            )
            .header("content-type", "application/json")
            .body("{}");
        for &(name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await?;
        let status = response.status();
        let allow = response
            .headers()
            .get(ALLOW)
            .map(|value| value.to_str())
            .transpose()?
            .map(str::to_owned);
        let entry = response
            .headers()
            .get("x-collaboration-central-entry")
            .map(|value| value.to_str())
            .transpose()?
            .map(str::to_owned);
        let body_is_empty = response.bytes().await?.is_empty();
        if status != expected_status
            || allow.as_deref() != expected_allow
            || entry.as_deref() != expected_entry
            || !body_is_empty
        {
            let sent = headers.iter().map(|(name, _)| *name).collect::<Vec<_>>();
            differences.push(format!(
                "{method} with headers {sent:?} and missing address: expected {expected_status} with Allow {expected_allow:?}, empty body and entry {expected_entry:?}, got {status} with Allow {allow:?}, empty body {body_is_empty} and entry {entry:?}"
            ));
        }
    }
    if differences.is_empty() {
        Ok(())
    } else {
        Err(differences.join("\n").into())
    }
}
