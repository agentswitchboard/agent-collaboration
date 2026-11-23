//! Real public native links: signed admission, ordinary client identity and live frames.
mod alarm_rearm;
mod approval_calls;
mod authority_races;
mod batch_contract;
mod batch_restore;
mod caller_authority;
mod delivery;
mod fixture;
mod idle_expiry;
mod link_support;
mod private_enrollment_invalidation;
mod receipt_enrollment_clock;
mod receipt_frames;
mod renewal_contract;
mod reply_due_work;
mod request_authority;
mod revocation;
mod revocation_backpressure;
mod session_calls;
mod transport_contract;
mod typed_call_holder;
mod typed_call_write_state;
mod typed_calls;
use central_link_client::{ClientAction, ClientEvent, LinkClient};
use central_link_protocol::{DataFrame, LinkFrame, ReplyPayload, RequestPayload};
use collaboration_central_conformance::{ConformanceResult as Result, signin_fixtures as signed};
use fixture::{Fixture, INSTALLATION, OTHER_MACHINE, OTHER_ORG, receive, send, start_fixture};
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{Message, RequestBuilderExt};
use serde_json::json;
use std::time::Duration;
use workspace_model::{
    Caller, RefusalReason, WorkspaceResult,
    central_mail::{CentralMailRefusal, CentralMailResult},
};

#[expect(
    clippy::panic_in_result_fn,
    reason = "Socket and owning-store assertions must fail while fixture errors propagate."
)]
#[tokio::test]
async fn ordinary_client_uses_installation_and_receives_correlated_workspace_and_mail_replies()
-> Result<()> {
    // Arrange: the client knows only its machine_id and issued bearer token.
    let fixture = start_fixture().await?;
    let mut client = LinkClient::new(INSTALLATION.parse()?, fixture.clock.clone());
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        return Err("client did not connect".into());
    };
    client.dial_started(&attempt)?;
    let mut socket = fixture.connect().await?;

    // Act / Assert: Welcome and storage must use the same Router machine id.
    let welcome = receive(&mut socket).await?;
    assert_eq!(
        welcome,
        LinkFrame::Data(DataFrame::Welcome {
            machine_id: INSTALLATION.parse()?,
            central_time: chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0)
                .ok_or("fixture UTC")?,
            effective_expires_at: chrono::DateTime::from_timestamp(signed::NOW_SECONDS + 600, 0)
                .ok_or("fixture expiry")?,
        })
    );
    assert!(matches!(
        client.receive(&attempt, welcome)?,
        ClientEvent::Welcomed { .. }
    ));
    client.ready(&attempt)?;
    let runtime = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    let rows = runtime
        .runtime()
        .statement_host()
        .read(central_storage::Statement {
            sql: "SELECT machine_id FROM login_machines WHERE machine_id=?",
            args: vec![central_storage::SqlValue::Text(INSTALLATION.into())],
        })
        .await?;
    assert!(
        matches!(rows.rows.as_slice(), [row] if matches!(row.as_slice(), [central_storage::SqlValue::Text(id)] if id == INSTALLATION)),
        "owning row must use the Router machine id"
    );
    send(&mut socket, &LinkFrame::Ping).await?;
    assert_eq!(receive(&mut socket).await?, LinkFrame::Pong);
    let caller = link_support::caller()?;
    let create = RequestPayload::Workspace(serde_json::from_value(json!({
        "requestId":"019f0000-0000-7000-8000-00000000ce10","workspaceId":null,
        "operation":{"kind":"createWorkspace","name":"Real link workspace"},
    }))?);
    let request = client.begin_request(caller, create)?;
    let LinkFrame::Data(DataFrame::Request { request: expected }) = &request else {
        return Err("client request shape".into());
    };
    send(&mut socket, &request).await?;
    let reply = receive(&mut socket).await?;
    let ClientEvent::Reply {
        correlation_id,
        result: ReplyPayload::Workspace(reply),
    } = client.receive(&attempt, reply)?
    else {
        return Err("workspace reply missing".into());
    };
    assert_eq!(&correlation_id, expected.correlation_id());
    assert!(
        matches!(reply.result, WorkspaceResult::Done(workspace_model::WorkspaceAnswer::WorkspaceCreated { ref workspace }) if workspace.name.as_str() == "Real link workspace")
    );
    let mail = RequestPayload::Mail(serde_json::from_value(
        json!({"requestId":"019f0000-0000-7000-8000-00000000ce11","operation":{"kind":"setNoticeCadence","cadence":"m25"}}),
    )?);
    let caller = Caller::Person {
        machine_id: INSTALLATION.parse()?,
        person_id: signed::OWNER.parse()?,
    };
    let request = client.begin_request(caller, mail)?;
    send(&mut socket, &request).await?;
    let reply = receive(&mut socket).await?;
    assert!(matches!(
        client.receive(&attempt, reply)?,
        ClientEvent::Reply {
            result: ReplyPayload::Mail(CentralMailResult::Refused(CentralMailRefusal::Workspace(RefusalReason::InvalidInput { field }))),
            ..
        } if field == "caller"
    ));
    socket
        .close(reqwest_websocket::CloseCode::Normal, None)
        .await?;
    fixture.close().await
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "Live refusal and heartbeat assertions observe the real public socket."
)]
#[tokio::test]
async fn semantic_requests_recheck_caller_and_original_grant_expiry() -> Result<()> {
    let fixture = start_fixture().await?;
    let mut client = LinkClient::new(INSTALLATION.parse()?, fixture.clock.clone());
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        return Err("client did not connect".into());
    };
    client.dial_started(&attempt)?;
    let mut socket = fixture.connect().await?;
    client.receive(&attempt, receive(&mut socket).await?)?;
    client.ready(&attempt)?;
    let payload = |request_id| -> Result<RequestPayload> {
        Ok(RequestPayload::Workspace(serde_json::from_value(json!({
            "requestId":request_id,"workspaceId":null,"operation":{"kind":"createWorkspace","name":"Must not create"},
        }))?))
    };
    let Caller::Session(mut session) = link_support::caller()? else {
        return Err("expected a session caller".into());
    };
    session.machine_id = OTHER_MACHINE.parse()?;
    let mismatched = Caller::Session(session);
    send(
        &mut socket,
        &client.begin_request(mismatched, payload("019f0000-0000-7000-8000-00000000ce20")?)?,
    )
    .await?;
    let reply = receive(&mut socket).await?;
    assert!(matches!(
        client.receive(&attempt, reply)?,
        ClientEvent::Reply {
            result: ReplyPayload::Workspace(workspace_model::WorkspaceReply {
                result: WorkspaceResult::Refused(RefusalReason::MachineRevoked),
                ..
            }),
            ..
        }
    ));
    // Business time advances only through the host's injected clock.
    fixture.clock.advance(Duration::from_secs(600))?;
    send(&mut socket, &LinkFrame::Ping).await?;
    assert_eq!(receive(&mut socket).await?, LinkFrame::Pong);
    let caller = link_support::caller()?;
    send(
        &mut socket,
        &client.begin_request(caller, payload("019f0000-0000-7000-8000-00000000ce21")?)?,
    )
    .await?;
    let reply = receive(&mut socket).await?;
    assert!(matches!(
        client.receive(&attempt, reply)?,
        ClientEvent::Reply {
            result: ReplyPayload::Workspace(workspace_model::WorkspaceReply {
                result: WorkspaceResult::CentralUnavailable,
                ..
            }),
            ..
        }
    ));
    let runtime = fixture
        .host
        .central(&signed::ORGANIZATION.to_owned().try_into()?)
        .await?;
    assert!(
        runtime
            .runtime()
            .statement_host()
            .read(central_storage::Statement {
                sql: "SELECT workspace_id FROM directory_workspaces",
                args: vec![]
            })
            .await?
            .rows
            .is_empty()
    );
    socket
        .close(reqwest_websocket::CloseCode::Normal, None)
        .await?;
    fixture.close().await
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "HTTP upgrade refusals must reject before any Welcome."
)]
#[tokio::test]
async fn public_route_requires_upgrade_and_header_token_and_matching_organization() -> Result<()> {
    let fixture = start_fixture().await?;
    let client = fixture.client()?;
    assert!(
        !client
            .get(fixture.url(signed::ORGANIZATION))
            .bearer_auth(&fixture.token)
            .send()
            .await?
            .status()
            .is_success()
    );
    for request in [
        client.get(fixture.url(signed::ORGANIZATION)),
        client.get(format!(
            "{}?token={}",
            fixture.url(signed::ORGANIZATION),
            fixture.token
        )),
        client
            .get(fixture.url(signed::ORGANIZATION))
            .bearer_auth(signed::tamper_signature(&fixture.token)?),
        client
            .get(fixture.url(signed::ORGANIZATION))
            .bearer_auth("bad-token"),
        client
            .get(fixture.url(signed::ORGANIZATION))
            .bearer_auth(signed::resign_machine_token(
                &fixture.token,
                &[("exp", json!(signed::NOW_SECONDS + 601))],
            )?),
        client
            .get(fixture.url(signed::ORGANIZATION))
            .bearer_auth(signed::resign_machine_token(
                &fixture.token,
                &[
                    ("client_id", json!(OTHER_MACHINE)),
                    ("azp", json!(OTHER_MACHINE)),
                ],
            )?),
        client
            .get(fixture.url(signed::ORGANIZATION))
            // A well-formed machine_id with no machine row is refused.
            .bearer_auth(fixture.token_for_installation("019f0000-0000-7000-8000-00000000ce0f")?),
        client
            .get(fixture.url(OTHER_ORG))
            .bearer_auth(&fixture.token),
    ] {
        let response = request.upgrade().send().await?;
        assert_eq!(response.status().as_u16(), 401);
    }
    fixture.close().await
}

#[tokio::test]
async fn signed_machine_bearer_with_retired_installation_id_is_refused() -> Result<()> {
    let fixture = start_fixture().await?;
    let mut control = fixture.connect().await?;
    if !matches!(
        receive(&mut control).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ) {
        return Err("the enrolled machine must receive Welcome with the current principal".into());
    }
    control
        .close(reqwest_websocket::CloseCode::Normal, None)
        .await?;

    let signed_bearer = signed::resign_machine_token(
        &fixture.token,
        &[(
            "principal",
            json!({
                "kind": "machine",
                "machine_id": INSTALLATION,
                "enrollment_id": fixture.enrollment_id,
                "installation_id": INSTALLATION,
            }),
        )],
    )?;
    let status = fixture
        .client()?
        .get(fixture.url(signed::ORGANIZATION))
        .bearer_auth(signed_bearer)
        .upgrade()
        .send()
        .await?
        .status();
    fixture.close().await?;
    if status != reqwest::StatusCode::UNAUTHORIZED {
        return Err(format!("retired installation_id must return HTTP 401, got {status}").into());
    }
    Ok(())
}

async fn receipt_result(
    socket: &mut reqwest_websocket::WebSocket,
    message_id: workspace_ids::MessageId,
    expected: central_link_protocol::ReceiptResult,
) -> Result<()> {
    let correlation_id = "019f0000-0000-7000-8000-00000000c201".parse()?;
    let frame = DataFrame::Receipts {
        correlation_id,
        receipts: central_link_protocol::ReceiptBatch::new(vec![central_link_protocol::Receipt {
            message_id,
            kind: central_link_protocol::ReceiptKind::Delivered {
                arrived_at: chrono::DateTime::from_timestamp(signed::NOW_SECONDS, 0)
                    .ok_or("fixture UTC")?,
            },
        }])?,
    };
    let DataFrame::Receipts { correlation_id, .. } = &frame else {
        return Err("receipt fixture changed".into());
    };
    let expected_correlation = correlation_id.clone();
    send(socket, &LinkFrame::Data(frame)).await?;
    if receive(socket).await?
        != LinkFrame::Data(DataFrame::ReceiptResults {
            correlation_id: expected_correlation,
            results: vec![expected],
        })
    {
        return Err("receipt result or correlation changed".into());
    }
    Ok(())
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "The real socket must stay open after an unknown receipt and drain on shutdown."
)]
#[tokio::test]
async fn unknown_receipt_keeps_the_link_open_and_shutdown_drains_it() -> Result<()> {
    let fixture = start_fixture().await?;
    let mut socket = fixture.connect().await?;
    receive(&mut socket).await?;
    receipt_result(
        &mut socket,
        "019f0000-0000-7000-8000-00000000ce30".parse()?,
        central_link_protocol::ReceiptResult::NoSuchMessage,
    )
    .await?;
    socket.send(Message::Text("ping".into())).await?;
    assert!(matches!(socket.next().await, Some(Ok(Message::Text(text))) if text == "pong"));
    fixture.close().await?;
    assert!(!matches!(socket.next().await, Some(Ok(Message::Text(_)))));
    Ok(())
}

#[tokio::test]
async fn real_public_tcp_excludes_all_private_paths_before_and_after_owner_credentials()
-> Result<()> {
    let fixture = start_fixture().await?;
    let result = private_route_boundary(&fixture).await;
    fixture.close().await?;
    result
}

async fn private_route_boundary(fixture: &Fixture) -> Result<()> {
    let organization = signed::ORGANIZATION;
    let private = reqwest::Client::builder()
        .no_proxy()
        .unix_socket(fixture.signin_socket.clone())
        .build()?;
    let digest_path = "/internal/organization-statement-digest";
    let response = private
        .post(format!("http://localhost{digest_path}"))
        .json(&json!({"organizationId":organization}))
        .send()
        .await?;
    if response.status().as_u16() != 200 {
        return Err("private sign-in digest must be mounted".into());
    }
    let mut public_url = reqwest::Url::parse(&fixture.url(organization))?;
    let signed_owner = signed::Person::owner(organization, signed::NOW_SECONDS)?;
    let public = fixture.client()?;
    let runtime = fixture
        .host
        .central(&organization.to_owned().try_into()?)
        .await?;
    let rows = || central_storage::Statement {
        sql: "SELECT (SELECT COUNT(*) FROM directory_workspaces) AS workspaces, (SELECT COUNT(*) FROM runtime_request_log) AS requests, (SELECT COUNT(*) FROM admin_used_requests) AS proofs, (SELECT COUNT(*) FROM access_actor_identities) AS actors",
        args: vec![],
    };
    let before = runtime.runtime().statement_host().read(rows()).await?;
    let operation = "019f0000-0000-7000-8000-00000000d7e1";
    for path in [
        "/internal/organization-provisioning",
        digest_path,
        "/internal/organization-authorization-revisions",
        "/internal/device-login-decisions",
        "/internal/machine-token-decisions",
        signed::CREATE,
        signed::LOOKUP,
        signed::RECORDS,
    ] {
        public_url.set_path(path);
        for authorized in [false, true] {
            let mut request = public.post(public_url.clone()).json(&signed::create_body(
                organization,
                operation,
                "Public refused",
            ));
            if authorized {
                for (name, value) in signed_owner.headers(
                    path,
                    &signed::create_intent(organization, operation, "Public refused"),
                    &signed::fresh_request_id(signed::NOW_SECONDS)?,
                    signed::NOW_SECONDS,
                )? {
                    request = request.header(name, value);
                }
            }
            if request.send().await?.status().as_u16() != 404 {
                return Err(format!(
                    "public listener exposed {path} with credentials={authorized}"
                )
                .into());
            }
        }
    }
    if runtime.runtime().statement_host().read(rows()).await? != before {
        return Err("private route refusals changed owning rows".into());
    }
    Ok(())
}

#[tokio::test]
async fn public_machine_link_fixture_uses_the_supplied_clock_and_mints_fresh_tokens() -> Result<()>
{
    use collaboration_central_tokio::test_support::start_machine_link_fixture;
    use router_clock::ManualClock;
    use runtime_primitives::SyncTime;
    use std::sync::Arc;

    let initial_time = signed::NOW_SECONDS + 1200;
    let clock = Arc::new(ManualClock::starting_at(SyncTime::from_unix_millis(
        initial_time * 1000,
    ))?);
    let fixture = start_machine_link_fixture(clock.clone()).await?;
    let origin = fixture.origin();
    let observed = async {
        let mut first = fixture.connect().await?;
        require_fixture_welcome(&mut first, initial_time).await?;

        clock.advance(Duration::from_secs(30))?;
        let renewed_token = fixture.mint_machine_token().await?;
        let mut current = fixture
            .client()?
            .get(format!(
                "{origin}/v1/organizations/{}/link",
                fixture.organization_id.as_str()
            ))
            .bearer_auth(renewed_token)
            .upgrade()
            .send()
            .await?
            .into_websocket()
            .await?;
        require_fixture_welcome(&mut current, initial_time + 30).await
    }
    .await;
    fixture.close().await?;
    observed?;

    let address = origin
        .strip_prefix("http://")
        .ok_or("HTTP fixture origin")?;
    if tokio::net::TcpStream::connect(address).await.is_ok() {
        return Err("the fixture listener survived its shutdown".into());
    }
    Ok(())
}

async fn require_fixture_welcome(
    socket: &mut reqwest_websocket::WebSocket,
    now: i64,
) -> Result<()> {
    let welcome = receive(socket).await?;
    if !matches!(welcome,
        LinkFrame::Data(DataFrame::Welcome { machine_id, central_time, effective_expires_at })
            if machine_id.as_str() == INSTALLATION
            && central_time.timestamp() == now
            && effective_expires_at.timestamp() == now + 600)
    {
        return Err("fixture Welcome did not carry the enrolled machine and injected clock".into());
    }
    Ok(())
}
