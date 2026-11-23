//! C2 socket fixtures with real Central-created, bound sessions.
use approval_request_protocol::{
    Answer, ApprovalNoticeReply, ApprovalRequestId, ApprovalSessionCall,
    DecideApprovalAsSessionReply, DecideApprovalAsSessionRequest, ListApprovalsForSessionReply,
    ListApprovalsForSessionRequest, PassOnApprovalReply, PassOnApprovalRequest, Raise,
    ReadApprovalForSessionReply, ReadApprovalForSessionRequest, RegisterRunReply,
};
use central_link_protocol::{
    CentralLinkRpc, CorrelationId, DataFrame, LinkCallRefusal, LinkFrame, MacCall, MacLinkRpc,
    TypedCallResult,
    approval_rpc::{
        ApprovalHolderTarget, ApprovalNoticeRpc, ApprovalServiceTarget, DecideApprovalAsSessionRpc,
        ListApprovalsForSessionRpc, PassOnApprovalForSessionRpc, ReadApprovalForSessionRpc,
    },
};
use central_storage::{SqlValue, Statement};
use fleet_identity_protocol::{OrganizationId, SessionSecret};
use fleet_rpc::Envelope;
use reqwest_websocket::WebSocket;
use workspace_ids::{CentralSessionId, RequestId};

use crate::{
    ConformanceResult as Result,
    approval_link_scenarios::{
        ApprovalLinkFixture, heartbeat, raise, receive_frame, register_run, require_receipt,
        send_frame,
    },
    approval_provider_scenarios::{ProviderAsk, create_bound, provider_raise},
};

#[path = "parent_approval_cases.rs"]
mod cases;
pub use cases::{Case, observe_case, pass_crosses_deadline};
pub use cases::{
    CompetingApprovalFixture, DecisionCommitOrder, DecisionCompetitor, decide_against,
};
#[path = "parent_approval_delivery_cases.rs"]
mod delivery_cases;
#[path = "parent_approval_proof_cases.rs"]
mod proof_cases;
pub use delivery_cases::{
    ParentApprovalPause, PauseControl, SettlesRecoveredNotice,
    hidden_approval_matches_an_absent_one_on_wire, pass_notice_reaches_a_separate_destination,
    pass_notice_survives_origin_loss, queued_notice_stops_at_deadline, read_survives_lost_reply,
    stale_recovery_page,
};
pub use proof_cases::{ParentApprovalProof, observe_parent_proof};
#[path = "parent_approval_rollback.rs"]
mod rollback;
pub use rollback::{enrollment_and_approval_rollback, session_end_and_approval_rollback};

const CORRELATION: &str = "019f0000-0000-7000-8000-000000007101";
const ABSENT_REQUEST: &str = "019f0000-0000-7000-8000-000000007102";

pub(crate) struct SessionProof {
    pub(crate) session_id: CentralSessionId,
    pub(crate) secret: SessionSecret,
}

impl SessionProof {
    pub(crate) fn request<Input>(&self, input: Input) -> ApprovalSessionCall<SessionSecret, Input> {
        ApprovalSessionCall {
            session_id: self.session_id.clone(),
            secret: self.secret.clone(),
            input,
        }
    }
}

struct ParentSetup {
    target: ApprovalHolderTarget,
    socket: WebSocket,
    parent: SessionProof,
    child: SessionProof,
    approval: Raise,
    grandparent: Option<SessionProof>,
}

impl ParentSetup {
    async fn open(fixture: &impl ApprovalLinkFixture, chain: Chain) -> Result<Self> {
        let target = fixture.target()?;
        let mut socket = fixture.connect().await?;
        let organization = &target.organization_id;
        assert_eq!(
            register_run(&mut socket, organization, &target.router_run).await?,
            RegisterRunReply::Registered {}
        );
        let grandparent = match chain {
            Chain::Direct | Chain::KeptOption => None,
            Chain::ThreeLevels | Chain::EndedParent => {
                let (session_id, secret) = create_bound(&mut socket, organization, 4, None).await?;
                Some(SessionProof { session_id, secret })
            }
        };
        let (session_id, secret) = create_bound(&mut socket, organization, 1, None).await?;
        let parent = SessionProof { session_id, secret };
        let (session_id, secret) = create_bound(
            &mut socket,
            organization,
            2,
            Some((&parent.session_id, parent.secret.clone())),
        )
        .await?;
        let child = SessionProof { session_id, secret };
        if let Some(ancestor) = &grandparent {
            seed_delegate_edge(fixture, &parent.session_id, &ancestor.session_id).await?;
        }
        if matches!(chain, Chain::EndedParent) {
            use crate::session_link_scenarios::{
                answer as session_answer, call as session_call, end,
            };
            use central_link_protocol::session_lifecycle_rpc::EndFleetSessionRpc;
            let ended = session_call::<EndFleetSessionRpc>(
                &mut socket,
                organization,
                end(parent.session_id.clone()),
            )
            .await?;
            assert!(matches!(
                session_answer::<EndFleetSessionRpc>(ended, organization)?,
                fleet_identity_protocol::EndFleetSessionReply::Settled { .. }
            ));
        }
        let mut approval = provider_raise(&target, &child.session_id, 3, ProviderAsk::Approval)?;
        if matches!(chain, Chain::KeptOption)
            && let Raise::ProviderApproval { offer, .. } = &mut approval
        {
            let mut options = offer.options.as_slice().to_vec();
            let offered = options
                .first_mut()
                .ok_or("provider approval had no options")?;
            offered.lasts = approval_request_protocol::OptionLifetime::NotVerified {};
            offer.options = approval_request_protocol::OfferedOptions::new(options)?;
        }
        require_receipt(raise(&mut socket, organization, approval.clone()).await?)?;
        acknowledge_notice(
            &mut socket,
            &target,
            &parent.session_id,
            approval.request_id(),
            ApprovalNoticeReply::Received {},
        )
        .await?;
        Ok(Self {
            target,
            socket,
            parent,
            child,
            approval,
            grandparent,
        })
    }

    async fn list(&mut self) -> Result<ListApprovalsForSessionReply> {
        answered(
            call::<ListApprovalsForSessionRpc>(
                &mut self.socket,
                &self.target.organization_id,
                self.parent
                    .request(ListApprovalsForSessionRequest { cursor: None }),
            )
            .await?,
        )
    }

    async fn read(&mut self, request_id: ApprovalRequestId) -> Result<ReadApprovalForSessionReply> {
        answered(
            call::<ReadApprovalForSessionRpc>(
                &mut self.socket,
                &self.target.organization_id,
                self.parent
                    .request(ReadApprovalForSessionRequest { request_id }),
            )
            .await?,
        )
    }

    async fn decide(
        &mut self,
        request_id: ApprovalRequestId,
        operation_id: RequestId,
        value: Answer,
    ) -> Result<DecideApprovalAsSessionReply> {
        answered(
            call::<DecideApprovalAsSessionRpc>(
                &mut self.socket,
                &self.target.organization_id,
                self.parent.request(DecideApprovalAsSessionRequest {
                    request_id,
                    operation_id,
                    value,
                }),
            )
            .await?,
        )
    }

    async fn pass(
        &mut self,
        request_id: ApprovalRequestId,
        operation_id: RequestId,
    ) -> Result<PassOnApprovalReply> {
        answered(
            call::<PassOnApprovalForSessionRpc>(
                &mut self.socket,
                &self.target.organization_id,
                self.parent.request(PassOnApprovalRequest {
                    request_id,
                    operation_id,
                }),
            )
            .await?,
        )
    }
}

pub(crate) async fn call<R: CentralLinkRpc<Target = ApprovalServiceTarget>>(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    body: R::Request,
) -> Result<TypedCallResult<R::Reply>> {
    let target = ApprovalServiceTarget {
        organization_id: organization.clone(),
    };
    let correlation: CorrelationId = CORRELATION.parse()?;
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: correlation.clone(),
            request: Box::new(R::request(Envelope {
                target: target.clone(),
                body,
            })),
        }),
    )
    .await?;
    let LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result,
    }) = receive_frame(socket).await?
    else {
        return Err("expected a correlated C2 reply".into());
    };
    if correlation_id != correlation {
        return Err("C2 changed the correlation".into());
    }
    match *result {
        TypedCallResult::Refused { reason } => Ok(TypedCallResult::Refused { reason }),
        TypedCallResult::Answered { reply } => {
            let reply = R::take_reply(reply).ok_or("C2 changed the reply marker")?;
            if reply.target != target {
                return Err("C2 changed the organization".into());
            }
            Ok(TypedCallResult::Answered { reply: reply.body })
        }
    }
}

pub(crate) fn answered<T>(reply: TypedCallResult<T>) -> Result<T> {
    match reply {
        TypedCallResult::Answered { reply } => Ok(reply),
        TypedCallResult::Refused { reason } => {
            Err(format!("unexpected C2 refusal {reason:?}").into())
        }
    }
}

pub(crate) async fn acknowledge_notice(
    socket: &mut WebSocket,
    machine: &ApprovalHolderTarget,
    session: &CentralSessionId,
    request_id: &ApprovalRequestId,
    reply: ApprovalNoticeReply,
) -> Result<()> {
    let LinkFrame::Data(DataFrame::CallMac {
        correlation_id,
        request,
    }) = receive_frame(socket).await?
    else {
        return Err("expected a C2 notice on the machine socket".into());
    };
    let MacCall::ApprovalNotice(notice) = *request else {
        return Err("expected an approvalNotice marker".into());
    };
    let target = notice.target;
    if target.organization_id != machine.organization_id
        || target.machine_id != machine.machine_id
        || target.enrollment_id != machine.enrollment_id
        || &target.session_id != session
        || &notice.body.request_id != request_id
    {
        return Err(format!("C2 wrong notice address or request: {target:?}").into());
    }
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::MacCallReply {
            correlation_id,
            result: Box::new(TypedCallResult::Answered {
                reply: ApprovalNoticeRpc::reply(Envelope {
                    target,
                    body: reply,
                }),
            }),
        }),
    )
    .await
}

pub(crate) fn typed_id(suffix: u16) -> Result<RequestId> {
    Ok(format!("019f0000-0000-7000-8000-00000000{suffix:04x}").parse()?)
}

pub(crate) fn option(ordinal: u8) -> Result<Answer> {
    Ok(Answer::Approval {
        option_id: approval_request_protocol::OfferedOptionId::new(ordinal)?,
    })
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

async fn first_read(
    fixture: &impl ApprovalLinkFixture,
    parent: &CentralSessionId,
    approval: &ApprovalRequestId,
) -> Result<Vec<Vec<SqlValue>>> {
    Ok(fixture.read(Statement {
        sql: "SELECT d.first_read_at FROM approval_request_destinations d JOIN approval_requests r ON r.approval_row_id=d.approval_row_id WHERE r.request_id=? AND d.session_id=?",
        args: vec![text(approval.as_str()), text(parent.as_str())],
    }).await?.rows)
}

#[derive(Clone, Copy)]
enum Chain {
    Direct,
    ThreeLevels,
    EndedParent,
    KeptOption,
}

async fn seed_delegate_edge(
    fixture: &impl ApprovalLinkFixture,
    child: &CentralSessionId,
    parent: &CentralSessionId,
) -> Result<()> {
    fixture.fixture_write(Statement {
        sql: "UPDATE actor_sessions SET session_kind='delegate',creator_kind='session',creator_actor_id=? WHERE actor_id=?",
        args: vec![text(parent.as_str()), text(child.as_str())],
    }).await?;
    fixture.fixture_write(Statement {
        sql: "INSERT INTO actor_delegates(actor_id,parent_actor_id,created_at,updated_at) SELECT actor_id,?,created_at,updated_at FROM actor_sessions WHERE actor_id=?",
        args: vec![text(parent.as_str()), text(child.as_str())],
    }).await
}
