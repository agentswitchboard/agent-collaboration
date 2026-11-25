//! Create through the real person socket and the existing signed HTTP route.

use super::{
    NOW_SECONDS, Person, PersonLinkFixture, correlation, fresh_request_id, install_floors,
    open_person, read_wire, send_frame,
};
use crate::ConformanceResult as Result;
use central_link_protocol::{
    CentralLinkRpc, DataFrame, LinkFrame, TypedCallResult,
    person_link_rpc::{
        CreateOrganizationWorkspace, CreateOrganizationWorkspaceRefusal as Refusal,
        CreateOrganizationWorkspaceResult as Answer, CreateOrganizationWorkspaceRpc,
    },
};
use fleet_rpc::Envelope;
use reqwest_websocket::WebSocket;
use serde_json::{Value, json};
use std::{num::NonZeroU32, time::Duration};

#[derive(Clone, Copy)]
pub enum CreateCallCase {
    AtFactorTime,
    AtFactorLimit,
    PastFactorLimit,
    MissingFactor,
    FutureFactor,
    NoPermission,
    DuplicateName,
    ChangedOperation,
    RetainedAcrossTransports,
}

pub async fn check_workspace_create(
    fixture: PersonLinkFixture<'_>,
    case: CreateCallCase,
    post: impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> Result<(u16, Vec<u8>)>,
    advance: impl AsyncFn(Duration) -> Result<()>,
) -> Result<()> {
    let organization = fixture.organization;
    install_floors(organization, &post).await?;
    let mut person = Person::owner(organization, NOW_SECONDS)?;
    person.second_factor_at = match case {
        CreateCallCase::AtFactorTime => Some(NOW_SECONDS),
        CreateCallCase::AtFactorLimit | CreateCallCase::PastFactorLimit => Some(NOW_SECONDS - 900),
        CreateCallCase::MissingFactor => None,
        CreateCallCase::FutureFactor => Some(NOW_SECONDS + 1),
        CreateCallCase::NoPermission
        | CreateCallCase::DuplicateName
        | CreateCallCase::ChangedOperation
        | CreateCallCase::RetainedAcrossTransports => person.second_factor_at,
    };
    if matches!(case, CreateCallCase::NoPermission) {
        person
            .permissions
            .retain(|permission| permission != "workspace:create");
    }
    let mut socket = open_person(&fixture, &person, NOW_SECONDS, 100, 140).await?;
    let operation = fresh_request_id(NOW_SECONDS)?;
    let name = "Workspace from the person link";
    match case {
        CreateCallCase::AtFactorTime | CreateCallCase::AtFactorLimit => {
            let socket_reply = create(&mut socket, organization, &operation, name, 1).await?;
            expect_created(&socket_reply, &operation, name)?;
            if matches!(case, CreateCallCase::AtFactorTime) {
                let legacy = create_http(&person, &operation, name, &post).await?;
                assert_same_workspace(&socket_reply, &legacy)?;
                let repeated_http = create_http(&person, &operation, name, &post).await?;
                if legacy != repeated_http {
                    return Err("HTTP after socket create did not repeat its receipt bytes".into());
                }
            }
        }
        CreateCallCase::PastFactorLimit
        | CreateCallCase::MissingFactor
        | CreateCallCase::FutureFactor => {
            if matches!(case, CreateCallCase::PastFactorLimit) {
                advance(Duration::from_millis(1)).await?;
            }
            expect_refusal(
                create(&mut socket, organization, &operation, name, 1).await?,
                Refusal::SecondFactorRequired {
                    within_seconds: NonZeroU32::new(900).ok_or("factor window")?,
                },
            )?;
        }
        CreateCallCase::NoPermission => expect_refusal(
            create(&mut socket, organization, &operation, name, 1).await?,
            Refusal::NotPermitted {},
        )?,
        CreateCallCase::DuplicateName => {
            let existing = fresh_request_id(NOW_SECONDS)?;
            assert_http_result(
                &create_http(&person, &existing, name, &post).await?,
                "applied",
            )?;
            expect_refusal(
                create(&mut socket, organization, &operation, name, 1).await?,
                Refusal::NameInUse {},
            )?;
            let retained = create_http(&person, &operation, name, &post).await?;
            if serde_json::from_slice::<Value>(&retained)?
                != json!({"result":"refused","reason":"nameInUse"})
            {
                return Err(
                    "the HTTP path did not serve the retained duplicate-name refusal".into(),
                );
            }
        }
        CreateCallCase::ChangedOperation => {
            assert_http_result(
                &create_http(&person, &operation, name, &post).await?,
                "applied",
            )?;
            expect_refusal(
                create(
                    &mut socket,
                    organization,
                    &operation,
                    "A different workspace name",
                    1,
                )
                .await?,
                Refusal::Conflict {},
            )?;
        }
        CreateCallCase::RetainedAcrossTransports => {
            let legacy = create_http(&person, &operation, name, &post).await?;
            let first = create(&mut socket, organization, &operation, name, 1).await?;
            expect_created(&first, &operation, name)?;
            assert_same_workspace(&first, &legacy)?;
            let repeat = create(&mut socket, organization, &operation, name, 2).await?;
            if first != repeat {
                return Err(
                    format!("socket replay changed its retained answer: {repeat:?}").into(),
                );
            }
            if create_http(&person, &operation, name, &post).await? != legacy {
                return Err("HTTP replay did not retain its original receipt bytes".into());
            }
        }
    }
    Ok(())
}

async fn create_http(
    person: &Person,
    operation: &str,
    name: &str,
    post: &impl AsyncFn(&str, &Value, Vec<(&'static str, String)>) -> Result<(u16, Vec<u8>)>,
) -> Result<Vec<u8>> {
    use super::super::{CREATE, create_body, create_intent};
    let proof = fresh_request_id(NOW_SECONDS)?;
    let headers = person.headers(
        CREATE,
        &create_intent(&person.organization, operation, name),
        &proof,
        NOW_SECONDS,
    )?;
    let (status, body) = post(
        CREATE,
        &create_body(&person.organization, operation, name),
        headers,
    )
    .await?;
    if status != 200 {
        return Err(format!("HTTP create returned {status}").into());
    }
    Ok(body)
}

async fn create(
    socket: &mut WebSocket,
    organization: &str,
    operation: &str,
    name: &str,
    serial: u16,
) -> Result<Answer> {
    let id = correlation(serial)?;
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: id.clone(),
            request: Box::new(CreateOrganizationWorkspaceRpc::request(Envelope {
                target: organization.to_owned().try_into()?,
                body: CreateOrganizationWorkspace {
                    operation_id: operation.parse()?,
                    name: name.to_owned().try_into()?,
                    description: String::new().try_into()?,
                },
            })),
        }),
    )
    .await?;
    let LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result,
    }) = read_wire(socket, "typed create response").await?
    else {
        return Err("create did not return a typed response".into());
    };
    if id != correlation_id {
        return Err("create correlation changed".into());
    }
    let TypedCallResult::Answered { reply } = *result else {
        return Err(format!("create failed at the link boundary: {result:?}").into());
    };
    let answer = CreateOrganizationWorkspaceRpc::take_reply(reply).ok_or("wrong typed reply")?;
    if answer.target.as_str() != organization {
        return Err("create organization changed in the reply".into());
    }
    Ok(answer.body)
}

fn expect_created(reply: &Answer, operation: &str, name: &str) -> Result<()> {
    let expected = json!({"result":"created","workspace":{
        "workspaceId":operation,"name":name,"description":"","state":"active",
    }});
    if serde_json::to_value(reply)? != expected {
        return Err(format!("expected the created workspace {operation}, got {reply:?}").into());
    }
    Ok(())
}

fn expect_refusal(reply: Answer, reason: Refusal) -> Result<()> {
    if reply != (Answer::Refused { reason }) {
        return Err(format!("expected create refusal {reason:?}, got {reply:?}").into());
    }
    Ok(())
}

fn assert_http_result(reply: &[u8], expected: &str) -> Result<Value> {
    let reply: Value = serde_json::from_slice(reply)?;
    if reply.get("result").and_then(Value::as_str) != Some(expected) {
        return Err(format!("expected HTTP {expected}, got {reply}").into());
    }
    Ok(reply)
}

fn assert_same_workspace(socket: &Answer, legacy: &[u8]) -> Result<()> {
    let Answer::Created { workspace } = socket else {
        return Err("the person call did not return the retained workspace".into());
    };
    let legacy = assert_http_result(legacy, "applied")?;
    if legacy.get("workspace") != Some(&serde_json::to_value(workspace)?) {
        return Err("the socket and HTTP paths returned different retained workspaces".into());
    }
    Ok(())
}
