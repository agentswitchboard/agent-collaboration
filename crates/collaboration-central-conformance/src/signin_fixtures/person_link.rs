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
