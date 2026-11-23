//! Shared socket scenarios for Central's production session-call dispatcher.
use crate::ConformanceResult as Result;
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, CorrelationId, DataFrame, FrameDirection, LinkCallRefusal,
    LinkFrame, MachineLinkCloseCode, TypedCallResult, decode_text, encode_text,
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc},
};
use central_storage::Statement;
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    CreateSessionKind, EndFleetSession, EndFleetSessionReply, EndSessionTarget, NewEndSettlement,
    OrganizationId, RouterEndCause, SessionCreator, SessionSecret,
};
use fleet_rpc::Envelope;
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{Message, WebSocket};
use workspace_ids::CentralSessionId;

const CORRELATION: &str = "019f0000-0000-7000-8000-000000005301";
const OPERATION: &str = "019f0000-0000-7000-8000-000000005302";
const HARNESS: &str = "typed-session-test";

pub fn session_records() -> Statement {
    Statement {
        sql: "SELECT actor_id, session_state, harness_session_id FROM actor_sessions ORDER BY actor_id",
        args: vec![],
    }
}

/// Full persisted fields let refusal tests detect writes that leave the state tag alone.
pub fn session_mutation_records() -> Statement {
    Statement {
        sql: "SELECT * FROM actor_sessions ORDER BY actor_id",
        args: vec![],
    }
}

/// Every stage uses the value returned by the preceding call from the owning store.
pub async fn create_bind_end(
    socket: &mut WebSocket,
    organization: &OrganizationId,
) -> Result<String> {
    let (session_id, secret) = reserve(socket, organization).await?;
    bind(socket, organization, session_id.clone(), secret).await?;
    let actual = call::<EndFleetSessionRpc>(socket, organization, end(session_id.clone())).await?;
    if !matches!(
        answer::<EndFleetSessionRpc>(actual, organization)?,
        EndFleetSessionReply::Settled {
            settlement: NewEndSettlement::Ended {
                cause: RouterEndCause::EndedForGood {},
                ..
            }
        }
    ) {
        return Err("End did not settle the active session with its requested cause".into());
    }
    Ok(session_id.as_str().to_owned())
}

async fn reserve(
    socket: &mut WebSocket,
    organization: &OrganizationId,
) -> Result<(CentralSessionId, SessionSecret)> {
    let created = call::<CreateFleetSessionRpc>(socket, organization, create()?).await?;
    let CreateFleetSessionReply::Reserved {
        session_id, secret, ..
    } = answer::<CreateFleetSessionRpc>(created, organization)?
    else {
        return Err("Create did not reserve a session".into());
    };
    Ok((session_id, secret))
}

async fn bind(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    session_id: CentralSessionId,
    secret: SessionSecret,
) -> Result<()> {
    let actual = call::<BindFleetSessionRpc>(
        socket,
        organization,
        BindFleetSession {
            session_id,
            harness_session_id: HARNESS.parse()?,
            secret,
        },
    )
    .await?;
    let BindFleetSessionReply::Active { address } =
        answer::<BindFleetSessionRpc>(actual, organization)?
    else {
        return Err("Bind did not activate the reserved session".into());
    };
    if address.session_id.as_str() != HARNESS {
        return Err("Bind returned a different harness address".into());
    }
    Ok(())
}

pub async fn refuse_wrong_session_target(
    socket: &mut WebSocket,
    wrong_target: &OrganizationId,
) -> Result<()> {
    let wrong = call::<CreateFleetSessionRpc>(socket, wrong_target, create()?).await?;
    expect_refusal(wrong, LinkCallRefusal::WrongTarget)
}

pub enum SessionLinkCall {
    Create,
    Bind(BindFleetSession),
    End(EndFleetSession),
}

/// Prepare calls that could change the organization store if a revoked socket
/// were allowed to reach the service.
pub async fn prepare_session_call(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    operation: SessionLifecycleStage,
) -> Result<SessionLinkCall> {
    match operation {
        SessionLifecycleStage::Create => Ok(SessionLinkCall::Create),
        SessionLifecycleStage::Bind => {
            let (session_id, secret) = reserve(socket, organization).await?;
            Ok(SessionLinkCall::Bind(BindFleetSession {
                session_id,
                harness_session_id: HARNESS.parse()?,
                secret,
            }))
        }
        SessionLifecycleStage::End => {
            let (session_id, secret) = reserve(socket, organization).await?;
            bind(socket, organization, session_id.clone(), secret).await?;
            Ok(SessionLinkCall::End(end(session_id)))
        }
    }
}

#[derive(Clone, Copy)]
pub enum SessionLifecycleStage {
    Create,
    Bind,
    End,
}

pub async fn refuse_lapsed_session_call(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    lifecycle_call: SessionLinkCall,
) -> Result<()> {
    let correlation = send_session_call(socket, organization, lifecycle_call).await?;
    expect_lapsed_session_call(socket, &correlation, MachineLinkCloseCode::Revoked).await
}

pub async fn send_session_call(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    lifecycle_call: SessionLinkCall,
) -> Result<CorrelationId> {
    match lifecycle_call {
        SessionLinkCall::Create => {
            send_call::<CreateFleetSessionRpc>(socket, organization, create()?).await
        }
        SessionLinkCall::Bind(body) => {
            send_call::<BindFleetSessionRpc>(socket, organization, body).await
        }
        SessionLinkCall::End(body) => {
            send_call::<EndFleetSessionRpc>(socket, organization, body).await
        }
    }
}

pub async fn expect_lapsed_session_call(
    socket: &mut WebSocket,
    correlation: &CorrelationId,
    cause: MachineLinkCloseCode,
) -> Result<()> {
    expect_refusal(
        read_reply(socket, correlation).await?,
        LinkCallRefusal::AuthorityLapsed { cause },
    )?;
    if !matches!(socket.next().await, Some(Ok(Message::Close { code, .. }))
        if MachineLinkCloseCode::from_code(code.into()) == Some(cause))
    {
        return Err(format!("expected the original session socket to close with {cause:?}").into());
    }
    Ok(())
}

pub async fn expect_create_refusal(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    reason: LinkCallRefusal,
) -> Result<()> {
    expect_refusal(
        call::<CreateFleetSessionRpc>(socket, organization, create()?).await?,
        reason,
    )
}

pub fn session_identity_records() -> Statement {
    Statement {
        sql: "SELECT * FROM actor_identities ORDER BY actor_id",
        args: vec![],
    }
}

fn create() -> Result<CreateFleetSession> {
    Ok(CreateFleetSession {
        operation_id: OPERATION.parse()?,
        attempt: 1_u64.try_into()?,
        creator: SessionCreator::RouterOperator {},
        agent_program: fleet_identity_protocol::HarnessKind::Codex,
        session_kind: CreateSessionKind::Ordinary {},
        project: fleet_identity_protocol::StartProject::Named {
            project_ref: fleet_identity_protocol::ProjectRef {
                workspace: "019a3dce-2000-7000-8000-000000000004".parse()?,
                project: "019a3dce-2000-7000-8000-000000000005"
                    .to_owned()
                    .try_into()?,
            },
        },
    })
}

pub(crate) fn end(session_id: CentralSessionId) -> EndFleetSession {
    EndFleetSession {
        target: EndSessionTarget::CentralId { session_id },
        cause: RouterEndCause::EndedForGood {},
    }
}

pub(crate) async fn call<R: CentralLinkRpc<Target = OrganizationId>>(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    body: R::Request,
) -> Result<TypedCallResult<CentralCallReply>> {
    let correlation = send_call::<R>(socket, organization, body).await?;
    read_reply(socket, &correlation).await
}

async fn send_call<R: CentralLinkRpc<Target = OrganizationId>>(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    body: R::Request,
) -> Result<CorrelationId> {
    let correlation: CorrelationId = CORRELATION.parse()?;
    let request = LinkFrame::Data(DataFrame::CallCentral {
        correlation_id: correlation.clone(),
        request: Box::new(R::request(Envelope {
            target: organization.clone(),
            body,
        })),
    });
    socket
        .send(Message::Text(encode_text(
            &request,
            FrameDirection::ToCentral,
        )?))
        .await?;
    Ok(correlation)
}

async fn read_reply(
    socket: &mut WebSocket,
    expected: &CorrelationId,
) -> Result<TypedCallResult<CentralCallReply>> {
    let Some(Ok(Message::Text(text))) = socket.next().await else {
        return Err("expected a typed reply on the original socket".into());
    };
    let LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result,
    }) = decode_text(&text, FrameDirection::ToRouter)?
    else {
        return Err("expected a CentralCallReply frame".into());
    };
    if &correlation_id != expected {
        return Err("CentralCallReply did not match the call's correlation".into());
    }
    Ok(*result)
}

pub(crate) fn answer<R: CentralLinkRpc<Target = OrganizationId>>(
    result: TypedCallResult<CentralCallReply>,
    organization: &OrganizationId,
) -> Result<R::Reply> {
    match result {
        TypedCallResult::Refused { reason } => {
            Err(format!("expected session answer, got {reason:?}").into())
        }
        TypedCallResult::Answered { reply } => R::take_reply(reply)
            .filter(|envelope| &envelope.target == organization)
            .map(|envelope| envelope.body)
            .ok_or_else(|| "wrong session reply marker or organization".into()),
    }
}

fn expect_refusal(
    observed: TypedCallResult<CentralCallReply>,
    reason: LinkCallRefusal,
) -> Result<()> {
    if observed != (TypedCallResult::Refused { reason }) {
        return Err(format!("expected refusal {reason:?}, got {observed:?}").into());
    }
    Ok(())
}
