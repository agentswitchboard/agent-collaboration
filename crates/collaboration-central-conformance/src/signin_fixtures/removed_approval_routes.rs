//! Probe removed approval paths through the actual private entry on both hosts.

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

use super::{
    LIST_WORKSPACES, NOW_SECONDS, ORGANIZATION, Person, fresh_request_id, list_workspaces_body,
    list_workspaces_intent,
};
use crate::ConformanceResult;

pub async fn check_removed_approval_routes(
    client: &Client,
    origin: &str,
    expected_entry: Option<&str>,
) -> ConformanceResult<()> {
    let person = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    let request = "019f0000-0000-7000-8000-00000000000a";
    let operation = "019f0000-0000-7000-8000-000000000064";
    let answer = json!({"kind":"approval","optionId":1});
    let paths = [
        (
            "/organization-admin/v1/approval-list",
            json!({"organization":ORGANIZATION,"filter":"waiting"}),
            json!({"v":1,"operation":"ApprovalList","organization":ORGANIZATION,
                "target":ORGANIZATION,"operationId":null,"params":{"filter":"waiting","cursor":null}}),
        ),
        (
            "/organization-admin/v1/approval-read",
            json!({"organization":ORGANIZATION,"requestId":request}),
            json!({"v":1,"operation":"ApprovalRead","organization":ORGANIZATION,
                "target":request,"operationId":null,"params":{}}),
        ),
        (
            "/organization-admin/v1/approval-decide",
            json!({"organization":ORGANIZATION,"requestId":request,"operationId":operation,"answer":answer}),
            json!({"v":1,"operation":"ApprovalDecide","organization":ORGANIZATION,
                "target":request,"operationId":operation,"params":{"answer":answer}}),
        ),
    ];

    let control = "/organization-admin/v1/unknown-approval-route";
    let response = client
        .post(format!("{origin}{control}"))
        .json(&json!({}))
        .send()
        .await?;
    assert_eq!(entry(&response)?, expected_entry);
    let unknown = (response.status(), response.bytes().await?);
    assert_eq!(unknown.0, StatusCode::NOT_FOUND);
    assert!(unknown.1.is_empty());

    for (path, body, intent) in paths {
        for (label, body) in [("missing address", json!({})), ("valid address", body)] {
            let response = signed_post(client, origin, &person, path, &body, &intent).await?;
            assert_eq!(entry(&response)?, expected_entry, "{path} {label}");
            assert_eq!(
                (response.status(), response.bytes().await?),
                unknown,
                "{path} {label}"
            );
        }
    }

    let response = signed_post(
        client,
        origin,
        &person,
        LIST_WORKSPACES,
        &list_workspaces_body(ORGANIZATION, None, Some(100)),
        &list_workspaces_intent(ORGANIZATION, None, 100),
    )
    .await?;
    assert_eq!(entry(&response)?, expected_entry);
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(&response.bytes().await?)?;
    assert!(
        body.get("workspaces").is_some_and(Value::is_array),
        "remaining private operation: {body}"
    );
    Ok(())
}

async fn signed_post(
    client: &Client,
    origin: &str,
    person: &Person,
    path: &str,
    body: &Value,
    intent: &Value,
) -> ConformanceResult<reqwest::Response> {
    let headers = person.headers_with(
        path,
        intent,
        &fresh_request_id(NOW_SECONDS)?,
        NOW_SECONDS,
        &|_| {},
    )?;
    let mut request = client.post(format!("{origin}{path}")).json(body);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    Ok(request.send().await?)
}

fn entry(response: &reqwest::Response) -> ConformanceResult<Option<&str>> {
    Ok(response
        .headers()
        .get("x-collaboration-central-entry")
        .map(reqwest::header::HeaderValue::to_str)
        .transpose()?)
}
