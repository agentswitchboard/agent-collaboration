//! The production person upgrade and lifecycle on either Central host.

use super::{
    ADMIN_AUDIENCE, ISSUER, LIST_WORKSPACES, NOW_SECONDS, OWNER, OWNER_SUBJECT, POLICY, Person,
    fresh_request_id, list_workspaces_body, list_workspaces_intent, sign,
};
use crate::{ConformanceResult as Result, fixture_link_reader::read_fixture_frame};
use central_link_protocol::PersonLinkCloseCode;
use futures_util::SinkExt;
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
use serde_json::{Value, json};

const REVISIONS: &str = "/internal/organization-authorization-revisions";

pub struct PersonLinkFixture<'a> {
    pub client: &'a reqwest::Client,
    pub person_url: &'a str,
    pub machine_url: &'a str,
    pub organization: &'a str,
    pub machine_token: &'a str,
}

pub async fn check_person_link_admission(
    fixture: PersonLinkFixture<'_>,
    post: impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> Result<(u16, Vec<u8>)>,
    advance: impl AsyncFn(u64) -> Result<()>,
) -> Result<()> {
    let mut person = Person::owner(fixture.organization, NOW_SECONDS)?;
    let normal = claims(&person, NOW_SECONDS, 100, 140)?;
    let mut wrong_audience = normal.clone();
    wrong_audience["aud"] = json!(ADMIN_AUDIENCE);
    let mut wrong_organization = normal.clone();
    wrong_organization["organization_id"] = json!("019f0000-0000-7000-8000-00000000c800");
    let mut expired = normal.clone();
    expired["exp"] = json!(NOW_SECONDS);
    let admin_token = person.token(NOW_SECONDS, &|_| {})?;
    let person_token = sign_bearer(&normal)?;

    install_floors(fixture.organization, &post).await?;
    for (url, token, label) in [
        (
            fixture.person_url,
            sign_bearer(&wrong_audience)?,
            "wrong audience",
        ),
        (
            fixture.person_url,
            sign_bearer(&wrong_organization)?,
            "wrong organization",
        ),
        (fixture.person_url, sign_bearer(&expired)?, "expired bearer"),
        (
            fixture.person_url,
            admin_token,
            "admin token on the person route",
        ),
        (
            fixture.person_url,
            fixture.machine_token.to_owned(),
            "machine token on the person route",
        ),
        (
            fixture.machine_url,
            person_token.clone(),
            "person token on the machine route",
        ),
    ] {
        let refused = fixture
            .client
            .get(url)
            .bearer_auth(token)
            .header("connection", "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .send()
            .await?;
        if refused.status().as_u16() != 401 {
            return Err(format!("{label}: expected HTTP 401, got {}", refused.status()).into());
        }
        if url == fixture.person_url && !refused.bytes().await?.is_empty() {
            return Err(format!("{label}: refused person upgrade must have an empty body").into());
        }
    }

    let headers = person.headers_with(
        LIST_WORKSPACES,
        &list_workspaces_intent(fixture.organization, None, 100),
        &fresh_request_id(NOW_SECONDS)?,
        NOW_SECONDS,
        &|claims| *claims = normal.clone(),
    )?;
    let refused = post(
        LIST_WORKSPACES,
        &list_workspaces_body(fixture.organization, None, None),
        headers,
    )
    .await?;
    if refused.0 != 401 || !refused.1.is_empty() {
        return Err("person-link token entered the private admin route".into());
    }

    let mut snapshot = dial(fixture.client, fixture.person_url, &person_token).await?;
    let mut session = open_person(&fixture, &person, NOW_SECONDS, 140, 120).await?;
    let mut superseded = open_person(&fixture, &person, NOW_SECONDS, 170, 180).await?;
    expect_pong(&mut snapshot).await?;
    let mut machine = dial(fixture.client, fixture.machine_url, fixture.machine_token).await?;
    let machine_welcome = read(&mut machine, "machine Welcome").await?;
    if !matches!(machine_welcome, Message::Text(text) if text.starts_with("{\"kind\":\"welcome\""))
    {
        return Err("machine bearer lost its existing machine Welcome".into());
    }
    expect_pong(&mut machine).await?;

    advance(60).await?;
    expect_pong(&mut snapshot).await?;
    advance(40).await?;
    send_handled_call(&mut snapshot).await?;
    expect_reconnect(&mut snapshot, "snapshot expiry").await?;
    advance(20).await?;
    send_handled_call(&mut session).await?;
    expect_reconnect(&mut session, "session expiry").await?;

    expect_http(
        post(REVISIONS, &person_revision(fixture.organization, 5), vec![]).await?,
        200,
        "person revision",
    )?;
    expect_reconnect(&mut superseded, "pushed person revision").await?;
    person.revision = 5;
    let mut organization_socket =
        open_person(&fixture, &person, NOW_SECONDS + 120, 100, 140).await?;
    expect_http(
        post(
            REVISIONS,
            &organization_revision(fixture.organization, 4),
            vec![],
        )
        .await?,
        200,
        "organization revision",
    )?;
    expect_reconnect(&mut organization_socket, "pushed organization revision").await
}

pub async fn install_floors(
    organization: &str,
    post: &impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> Result<(u16, Vec<u8>)>,
) -> Result<()> {
    expect_http(
        post(REVISIONS, &organization_revision(organization, 3), vec![]).await?,
        200,
        "organization floor",
    )?;
    expect_http(
        post(REVISIONS, &person_revision(organization, 4), vec![]).await?,
        200,
        "person floor",
    )
}

fn organization_revision(organization: &str, revision: u8) -> Value {
    json!({"kind":"organization", "organizationId":organization, "revision":revision,
        "state":"active","permissionPolicyDigest":POLICY})
}

fn person_revision(organization: &str, revision: u8) -> Value {
    json!({"kind":"person","organizationId":organization,"personId":OWNER,
        "subjectId":OWNER_SUBJECT,"revision":revision,"state":"active"})
}

fn claims(person: &Person, now: i64, snapshot: i64, session: i64) -> Result<Value> {
    Ok(json!({
        "iss":ISSUER, "aud":format!("{ISSUER}/audiences/central-person-link"), "sub":person.id,
        "iat":now-1, "exp":now+59, "jti":fresh_request_id(now)?, "token_profile":"person-link",
        "organization_id":person.organization, "authorization_subject_id":person.subject,
        "person_revision":person.revision, "organization_revision":person.organization_revision,
        "permission_policy_digest":person.policy_digest, "permissions":person.permissions,
        "session_expires_at_ms":(now+session)*1000, "authorization_snapshot_at_ms":now*1000,
        "authorization_snapshot_valid_until_ms":(now+snapshot)*1000,
        "second_factor_at":person.second_factor_at, "second_factor_method":"totp",
    }))
}

fn sign_bearer(claims: &Value) -> Result<String> {
    sign(
        &json!({"typ":"at+jwt","alg":"EdDSA","kid":"stamp-a"}),
        claims,
        7,
    )
}

pub async fn open_person(
    fixture: &PersonLinkFixture<'_>,
    person: &Person,
    now: i64,
    snapshot: i64,
    session: i64,
) -> Result<WebSocket> {
    dial(
        fixture.client,
        fixture.person_url,
        &sign_bearer(&claims(person, now, snapshot, session)?)?,
    )
    .await
}

pub async fn dial(client: &reqwest::Client, url: &str, token: &str) -> Result<WebSocket> {
    Ok(client
        .get(url)
        .bearer_auth(token)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?)
}

pub fn expect_http(
    (status, _): (u16, Vec<u8>),
    expected: u16,
    operation: &'static str,
) -> Result<()> {
    if status != expected {
        return Err(format!("{operation}: expected HTTP {expected}, got {status}").into());
    }
    Ok(())
}

pub async fn read(socket: &mut WebSocket, expected: &'static str) -> Result<Message> {
    Ok(read_fixture_frame(socket, expected)
        .await?
        .ok_or(expected)??)
}

async fn send_ping(socket: &mut WebSocket) -> Result<()> {
    socket.send(Message::Text("ping".to_owned())).await?;
    Ok(())
}

async fn expect_pong(socket: &mut WebSocket) -> Result<()> {
    send_ping(socket).await?;
    if !matches!(read(socket, "Pong").await?, Message::Text(text) if text == "pong") {
        return Err("current connection did not answer Ping".into());
    }
    Ok(())
}

async fn expect_reconnect(socket: &mut WebSocket, expected: &'static str) -> Result<()> {
    let frame = read(socket, expected).await?;
    if !matches!(frame, Message::Close { code, .. } if u16::from(code) == PersonLinkCloseCode::Reconnect.code())
    {
        return Err(format!("{expected}: expected reconnect close, got {frame:?}").into());
    }
    Ok(())
}

async fn send_handled_call(socket: &mut WebSocket) -> Result<()> {
    let frame = json!({
        "kind": "callCentral",
        "correlationId": "019f0000-0000-7000-8000-00000000c807",
        "request": {"call":"fixtureUnavailable","envelope":{"target":{},"body":{}}},
    });
    socket.send(Message::Text(frame.to_string())).await?;
    Ok(())
}

pub async fn check_workspace_list(
    fixture: PersonLinkFixture<'_>,
    post: impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> Result<(u16, Vec<u8>)>,
) -> Result<()> {
    use central_link_protocol::{
        CentralLinkRpc, DataFrame, LinkFrame, LinkRequest, ReplyPayload, RequestPayload,
        TypedCallResult,
        person_link_rpc::{ListOrganizationWorkspaces, ListOrganizationWorkspacesRpc},
    };
    use fleet_rpc::Envelope;
    use workspace_model::{
        Caller, RefusalReason,
        central_mail::{CentralMailRefusal, CentralMailResult},
    };
    let organization = fixture.organization;
    let person = Person::owner(organization, NOW_SECONDS)?;
    install_floors(organization, &post).await?;
    let create_id = fresh_request_id(NOW_SECONDS)?;
    let name = "Shared directory person-link workspace";
    let headers = person.headers(
        super::CREATE,
        &super::create_intent(organization, &create_id, name),
        &fresh_request_id(NOW_SECONDS)?,
        NOW_SECONDS,
    )?;
    expect_http(
        post(
            super::CREATE,
            &super::create_body(organization, &create_id, name),
            headers,
        )
        .await?,
        200,
        "workspace creation",
    )?;

    let mut socket = open_person(&fixture, &person, NOW_SECONDS, 100, 140).await?;
    let operation: workspace_ids::RequestId = fresh_request_id(NOW_SECONDS)?.parse()?;
    let list = |serial| -> Result<LinkFrame> {
        Ok(LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: correlation(serial)?,
            request: Box::new(ListOrganizationWorkspacesRpc::request(Envelope {
                target: organization.to_owned().try_into()?,
                body: ListOrganizationWorkspaces {
                    operation_id: operation.clone(),
                    after: None,
                    limit: 5u16.try_into()?,
                },
            })),
        }))
    };
    for serial in [1, 2] {
        send_frame(&mut socket, list(serial)?).await?;
        let LinkFrame::Data(DataFrame::CentralCallReply {
            correlation_id,
            result,
        }) = read_wire(&mut socket, "person workspace list").await?
        else {
            return Err("expected a correlated workspace-list answer".into());
        };
        let TypedCallResult::Answered { reply } = *result else {
            return Err("authorized workspace list was refused".into());
        };
        let answer = ListOrganizationWorkspacesRpc::take_reply(reply).ok_or("wrong list reply")?;
        if correlation_id != correlation(serial)?
            || answer.target.as_str() != organization
            || !matches!(answer.body.workspaces.as_slice(), [one] if one.name.as_str() == name)
            || answer.body.next.is_some()
        {
            return Err(
                "list did not return the workspace created by the existing admin service".into(),
            );
        }
    }

    let machine_id = "019f0000-0000-7000-8000-00000000c808".parse()?;
    let session = workspace_ids::SessionRef {
        machine_id,
        session_id: "person-link-fenced-machine-call".parse()?,
    };
    let message = LinkRequest::new(
        correlation(3)?,
        Caller::Session(session.clone()),
        RequestPayload::Mail(serde_json::from_value(json!({
            "requestId":fresh_request_id(NOW_SECONDS)?,
            "operation":{"kind":"sendMessage","to":session,"body":"must not send","delivery":"queue","replyTo":null,"sentAt":chrono::DateTime::from_timestamp(NOW_SECONDS, 0)},
        }))?),
    )?;
    send_frame(
        &mut socket,
        LinkFrame::Data(DataFrame::Request {
            request: Box::new(message),
        }),
    )
    .await?;
    if read_wire(&mut socket, "person message refusal").await?
        != (LinkFrame::Data(DataFrame::Reply {
            correlation_id: correlation(3)?,
            result: Box::new(ReplyPayload::Mail(CentralMailResult::Refused(
                CentralMailRefusal::Workspace(RefusalReason::WorkspaceNotAllowed),
            ))),
        }))
    {
        return Err("person credential changed the machine-message refusal".into());
    }
    send_frame(&mut socket, LinkFrame::Data(DataFrame::CallCentral {
        correlation_id: correlation(4)?,
        request: Box::new(serde_json::from_value(json!({
            "call":"registerRun","envelope":{"target":{"organizationId":organization},"body":{"routerRun":"019f0000-0000-7000-8000-00000000c804"}}
        }))?),
    })).await?;
    check_wrong_target(&mut socket, 4).await?;
    let mut machine = dial(fixture.client, fixture.machine_url, fixture.machine_token).await?;
    if !matches!(
        read_wire(&mut machine, "machine Welcome").await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ) {
        return Err("machine link did not welcome its own credential".into());
    }
    send_frame(&mut machine, list(5)?).await?;
    check_wrong_target(&mut machine, 5).await?;
    expect_pong(&mut machine).await
}

fn correlation(serial: u16) -> Result<central_link_protocol::CorrelationId> {
    Ok(format!("019f0000-0000-7000-8000-{serial:012x}").parse()?)
}

async fn send_frame(socket: &mut WebSocket, frame: central_link_protocol::LinkFrame) -> Result<()> {
    let text = central_link_protocol::encode_text(
        &frame,
        central_link_protocol::FrameDirection::ToCentral,
    )?;
    socket.send(Message::Text(text)).await?;
    Ok(())
}

async fn read_wire(
    socket: &mut WebSocket,
    expected: &'static str,
) -> Result<central_link_protocol::LinkFrame> {
    let Message::Text(text) = read(socket, expected).await? else {
        return Err(format!("expected WSS text for {expected}").into());
    };
    Ok(central_link_protocol::decode_text(
        &text,
        central_link_protocol::FrameDirection::ToRouter,
    )?)
}

async fn check_wrong_target(socket: &mut WebSocket, serial: u16) -> Result<()> {
    use central_link_protocol::{DataFrame, LinkCallRefusal, LinkFrame, TypedCallResult};
    if read_wire(socket, "wrong-principal typed reply").await?
        != (LinkFrame::Data(DataFrame::CentralCallReply {
            correlation_id: correlation(serial)?,
            result: Box::new(TypedCallResult::Refused {
                reason: LinkCallRefusal::WrongTarget,
            }),
        }))
    {
        return Err("wrong principal entered a typed operation".into());
    }
    Ok(())
}
