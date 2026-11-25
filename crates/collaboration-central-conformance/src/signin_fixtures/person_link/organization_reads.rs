//! Compare the real person websocket reads with the still-served signed reads.

use super::super::{
    CREATE, LOOKUP, NOW_SECONDS, OWNER, Person, RECORDS, create_body, create_intent,
    fresh_request_id, lookup_intent, records_intent,
};
use super::{PersonLinkFixture, correlation, dial, install_floors, open_person, read};
use crate::ConformanceResult as Result;
use central_link_protocol::{FrameDirection, decode_text};
use futures_util::SinkExt;
use reqwest_websocket::{Message, WebSocket};
use serde_json::{Value, json};

const LOOKUP_CALL: &str = "lookupOrganizationWorkspace";
const RECORDS_CALL: &str = "readOrganizationChangeRecords";
const FOREIGN_ORGANIZATION: &str = "019f0000-0000-7000-8000-00000000fe01";
const OTHER_MACHINE: &str = "019f0000-0000-7000-8000-00000000fe02";

#[derive(Clone, Copy)]
pub enum OrganizationReadCase {
    Lookup,
    AuditPages,
    Permissions,
    PrincipalBoundaries,
}

pub async fn check_organization_read(
    fixture: PersonLinkFixture<'_>,
    post: impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> Result<(u16, Vec<u8>)>,
    case: OrganizationReadCase,
) -> Result<()> {
    let organization = fixture.organization;
    install_floors(organization, &post).await?;
    let owner = Person::owner(organization, NOW_SECONDS)?;
    let workspace = fresh_request_id(NOW_SECONDS)?;
    if matches!(
        case,
        OrganizationReadCase::Lookup | OrganizationReadCase::AuditPages
    ) {
        let name = "Person websocket reads";
        let created = signed_read(
            &post,
            &owner,
            CREATE,
            create_body(organization, &workspace, name),
            create_intent(organization, &workspace, name),
        )
        .await?;
        assert_eq!(text_field(&created, "result"), Some("applied"));
    }
    match case {
        OrganizationReadCase::Lookup => {
            let mut person = owner;
            person.permissions = vec!["organization:view".into()];
            person.second_factor_at = None;
            let mut socket = open_person(&fixture, &person, NOW_SECONDS, 100, 140).await?;
            let absent = fresh_request_id(NOW_SECONDS)?;
            for (serial, workspace_id, exists) in [(1, &workspace, true), (2, &absent, false)] {
                let signed = signed_read(
                    &post,
                    &person,
                    LOOKUP,
                    json!({"organizationId":organization,"workspaceId":workspace_id}),
                    lookup_intent(organization, workspace_id),
                )
                .await?;
                assert_eq!(text_field(&signed, "result"), Some("found"));
                let workspace = signed
                    .get("workspace")
                    .ok_or("signed lookup omitted workspace")?;
                assert_eq!(workspace.is_object(), exists);
                let answer = if exists {
                    assert_eq!(
                        text_field(workspace, "workspaceId"),
                        Some(workspace_id.as_str())
                    );
                    json!({"result":"found","workspace":workspace})
                } else {
                    assert_eq!(workspace, &Value::Null);
                    json!({"result":"absent"})
                };
                expect_call(
                    &mut socket,
                    serial,
                    organization,
                    LOOKUP_CALL,
                    json!({"workspaceId":workspace_id}),
                    answered(organization, LOOKUP_CALL, answer),
                )
                .await?;
            }
        }
        OrganizationReadCase::AuditPages => {
            check_audit_pages(&fixture, &post, &workspace).await?;
        }
        OrganizationReadCase::Permissions => {
            for (serial, permission, route, call, body, intent, request) in [
                (
                    1,
                    "organization:readAudit",
                    LOOKUP,
                    LOOKUP_CALL,
                    json!({"organizationId":organization,"workspaceId":workspace}),
                    lookup_intent(organization, &workspace),
                    json!({"workspaceId":workspace}),
                ),
                (
                    2,
                    "organization:view",
                    RECORDS,
                    RECORDS_CALL,
                    json!({"organizationId":organization,"limit":1}),
                    records_intent(organization, None, 1),
                    json!({"limit":1}),
                ),
            ] {
                let mut person = Person::owner(organization, NOW_SECONDS)?;
                person.permissions = vec![permission.into()];
                person.second_factor_at = None;
                let signed = signed_read(&post, &person, route, body, intent).await?;
                assert_eq!(signed, json!({"result":"refused","reason":"notPermitted"}));
                let mut socket = open_person(&fixture, &person, NOW_SECONDS, 100, 140).await?;
                expect_call(
                    &mut socket,
                    serial,
                    organization,
                    call,
                    request,
                    answered(
                        organization,
                        call,
                        json!({"result":"refused","reason":{"kind":"notPermitted"}}),
                    ),
                )
                .await?;
            }
        }
        OrganizationReadCase::PrincipalBoundaries => {
            let mut person = open_person(&fixture, &owner, NOW_SECONDS, 100, 140).await?;
            let mut machine =
                dial(fixture.client, fixture.machine_url, fixture.machine_token).await?;
            let Message::Text(welcome) = read(&mut machine, "machine Welcome").await? else {
                return Err("machine did not receive a text Welcome".into());
            };
            let welcome: Value = serde_json::from_str(&welcome)?;
            assert_eq!(text_field(&welcome, "kind"), Some("welcome"));
            let wrong_target = json!({"kind":"refused","reason":{"kind":"wrongTarget"}});
            for (serial, call, request) in [
                (1, LOOKUP_CALL, json!({"workspaceId":workspace})),
                (2, RECORDS_CALL, json!({"limit":1})),
            ] {
                expect_call(
                    &mut person,
                    serial,
                    FOREIGN_ORGANIZATION,
                    call,
                    request.clone(),
                    wrong_target.clone(),
                )
                .await?;
                expect_call(
                    &mut machine,
                    serial,
                    organization,
                    call,
                    request,
                    wrong_target.clone(),
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn check_audit_pages(
    fixture: &PersonLinkFixture<'_>,
    post: &impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> Result<(u16, Vec<u8>)>,
    workspace: &str,
) -> Result<()> {
    let organization = fixture.organization;
    let enrollment = json!({
        "organizationId":organization,"approver":OWNER,"permissionChecked":"machine:enroll",
        "requestId":fresh_request_id(NOW_SECONDS)?,"machineId":OTHER_MACHINE,"label":"Person audit machine",
        "allowedWorkspaces":[workspace],"approverSecondFactorAt":NOW_SECONDS-60,"codeExpiresAt":NOW_SECONDS+600,
    });
    let (status, body) = post("/internal/device-login-decisions", &enrollment, vec![]).await?;
    assert_eq!(status, 200);
    let decision: Value = serde_json::from_slice(&body)?;
    assert_eq!(text_field(&decision, "result"), Some("approved"));

    let mut auditor = Person::owner(organization, NOW_SECONDS)?;
    auditor.permissions = vec!["organization:readAudit".into()];
    auditor.second_factor_at = None;
    let signed = signed_read(
        post,
        &auditor,
        RECORDS,
        json!({"organizationId":organization,"limit":100}),
        records_intent(organization, None, 100),
    )
    .await?;
    assert_eq!(text_field(&signed, "result"), Some("records"));
    let records = signed
        .get("records")
        .and_then(Value::as_array)
        .ok_or("expected signed audit rows")?;
    let record_ids = records
        .iter()
        .map(|record| text_field(record, "id").ok_or("expected record id"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(
        record_ids
            .iter()
            .zip(record_ids.iter().skip(1))
            .all(|(first, second)| first < second)
    );
    assert!(records.iter().any(|record| text_field(record, "kind")
        == Some("organizationProvisioned")
        && record.get("origin") == Some(&json!({"kind":"signInProvisioning"}))));
    assert!(records.iter().any(|record| text_field(record, "kind") == Some("workspaceCreated")
        && record.get("origin") == Some(&json!({"kind":"person","personId":OWNER,"permissionChecked":"workspace:create"}))
        && text_field(record, "targetId") == Some(workspace)));
    assert!(records.iter().any(|record| text_field(record, "kind")
        == Some("machineAllowanceChanged")
        && text_field(record, "machineId") == Some(OTHER_MACHINE)
        && text_field(record, "workspaceId") == Some(workspace)
        && text_field(record, "state") == Some("active")));

    let mut socket = open_person(fixture, &auditor, NOW_SECONDS, 100, 140).await?;
    let mut after = None;
    for (index, (record, cursor)) in records.iter().zip(record_ids).enumerate() {
        let next = (index + 1 < records.len()).then_some(cursor);
        expect_call(
            &mut socket,
            u16::try_from(index + 1)?,
            organization,
            RECORDS_CALL,
            json!({"after":after,"limit":1}),
            answered(
                organization,
                RECORDS_CALL,
                json!({"result":"records","records":[record],"next":next}),
            ),
        )
        .await?;
        after = Some(cursor);
    }
    // An exact-size terminal page and a read after its last ID both have no next cursor.
    expect_call(
        &mut socket,
        100,
        organization,
        RECORDS_CALL,
        json!({"after":after,"limit":1}),
        answered(
            organization,
            RECORDS_CALL,
            json!({"result":"records","records":[],"next":null}),
        ),
    )
    .await
}

fn text_field<'value>(value: &'value Value, field: &str) -> Option<&'value str> {
    value.get(field).and_then(Value::as_str)
}

async fn signed_read(
    post: &impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> Result<(u16, Vec<u8>)>,
    person: &Person,
    path: &str,
    body: Value,
    intent: Value,
) -> Result<Value> {
    let headers = person.headers(path, &intent, &fresh_request_id(NOW_SECONDS)?, NOW_SECONDS)?;
    let (status, bytes) = post(path, &body, headers).await?;
    assert_eq!(status, 200, "signed {path}");
    Ok(serde_json::from_slice(&bytes)?)
}

fn answered(organization: &str, call: &str, body: Value) -> Value {
    json!({"kind":"answered","reply":{"call":call,"envelope":{
        "target":organization,"body":body
    }}})
}

async fn expect_call(
    socket: &mut WebSocket,
    serial: u16,
    organization: &str,
    call: &'static str,
    request: Value,
    expected: Value,
) -> Result<()> {
    // Spell out the wire so these tests compile and fail at the actual reply on the old build.
    let correlation_id = correlation(serial)?;
    let frame = json!({"kind":"callCentral","correlationId":correlation_id,"request":{
        "call":call,"envelope":{"target":organization,"body":request}
    }});
    socket.send(Message::Text(frame.to_string())).await?;
    let message = read(socket, call).await?;
    let Message::Text(reply) = message else {
        return Err(format!("{call}: expected a text reply, received {message:?}").into());
    };
    let received = serde_json::from_str::<Value>(&reply)?;
    let expected =
        json!({"kind":"centralCallReply","correlationId":correlation_id,"result":expected});
    if received != expected {
        return Err(format!("{call}: received {received}, expected {expected}").into());
    }
    decode_text(&reply, FrameDirection::ToRouter)?;
    Ok(())
}
