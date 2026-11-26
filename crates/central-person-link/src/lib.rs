//! The authority and two-state lifetime of one admitted person connection.
#![forbid(unsafe_code)]

mod connection_registry;
pub use connection_registry::{PersonConnectionRegistry, RegisteredPersonConnection};

use fleet_identity_protocol::OrganizationId;
use fleet_signin::FleetPermission;
pub use fleet_signin::person_proof::PersonSocketAttachment;
use workspace_ids::PersonId;

/// The only transition is Admitted to Ended. The transport drops Ended sockets.
#[derive(Clone)]
pub enum PersonConnectionState {
    Admitted(PersonSocketAttachment),
    Ended,
}

impl PersonConnectionState {
    pub fn check_time(&mut self, now_ms: i64) -> Option<&PersonSocketAttachment> {
        if matches!(self, Self::Admitted(connection) if !connection.is_current(now_ms)) {
            *self = Self::Ended;
        }
        match self {
            Self::Admitted(connection) => Some(connection),
            Self::Ended => None,
        }
    }

    pub fn observe_revision(&mut self, revision: &PersonRevision) -> bool {
        if matches!(self, Self::Admitted(connection) if revision.supersedes(connection)) {
            *self = Self::Ended;
            return true;
        }
        false
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersonRevision {
    Organization {
        organization: OrganizationId,
        revision: i64,
    },
    Person {
        organization: OrganizationId,
        person: PersonId,
        revision: i64,
    },
}

impl PersonRevision {
    pub fn supersedes(&self, facts: &PersonSocketAttachment) -> bool {
        match self {
            Self::Organization {
                organization,
                revision,
            } => organization == facts.organization() && *revision > facts.organization_revision(),
            Self::Person {
                organization,
                person,
                revision,
            } => {
                organization == facts.organization()
                    && person == facts.person()
                    && *revision > facts.person_revision()
            }
        }
    }
}

use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, FrameDirection, LinkCallRefusal, LinkFrame,
    ProtocolError, ReplyPayload, RequestRefusal, RequestStatusAnswer, TokenRenewalRefusal,
    TokenRenewalResult, TypedCallResult, decode_text,
    person_link_rpc::{
        CreateOrganizationWorkspace, ListOrganizationWorkspaces, LookupOrganizationWorkspace,
        ReadOrganizationChangeRecords,
    },
};
use fleet_rpc::Envelope;
use fleet_signin::approval_person_commands::{
    ApprovalDecideRequest, ApprovalListRequest, ApprovalPersonOperation, ApprovalReadRequest,
};

pub enum PersonFrame {
    Reply(Vec<LinkFrame>),
    ListWorkspaces {
        correlation: CorrelationId,
        request: Envelope<OrganizationId, ListOrganizationWorkspaces>,
    },
    CreateWorkspace {
        correlation: CorrelationId,
        request: Envelope<OrganizationId, CreateOrganizationWorkspace>,
    },
    LookupWorkspace {
        correlation: CorrelationId,
        request: Envelope<OrganizationId, LookupOrganizationWorkspace>,
    },
    ReadChangeRecords {
        correlation: CorrelationId,
        request: Envelope<OrganizationId, ReadOrganizationChangeRecords>,
    },
    Approval {
        correlation: CorrelationId,
        operation: ApprovalPersonOperation,
    },
    ProtocolViolation,
}

fn approval(
    connection: &PersonSocketAttachment,
    correlation: CorrelationId,
    operation: ApprovalPersonOperation,
) -> PersonFrame {
    if operation.organization() != connection.organization() {
        return refusal(correlation, LinkCallRefusal::WrongTarget);
    }
    PersonFrame::Approval {
        correlation,
        operation,
    }
}

pub fn dispatch(connection: &PersonSocketAttachment, text: &str) -> PersonFrame {
    let frame = match decode_text(text, FrameDirection::ToCentral) {
        Ok(frame) => frame,
        Err(ProtocolError::UnknownTypedCall { correlation_id }) => {
            return refusal(correlation_id, LinkCallRefusal::UnknownCall);
        }
        Err(_) => return PersonFrame::ProtocolViolation,
    };
    match frame {
        LinkFrame::Ping => PersonFrame::Reply(vec![LinkFrame::Pong]),
        LinkFrame::Data(DataFrame::CallCentral {
            correlation_id,
            request,
        }) => match *request {
            CentralCall::ListOrganizationWorkspaces(request)
                if &request.target == connection.organization()
                    && connection.permits(FleetPermission::ViewOrganization) =>
            {
                PersonFrame::ListWorkspaces {
                    correlation: correlation_id,
                    request,
                }
            }
            CentralCall::CreateOrganizationWorkspace(request)
                if &request.target == connection.organization() =>
            {
                PersonFrame::CreateWorkspace {
                    correlation: correlation_id,
                    request,
                }
            }
            CentralCall::LookupOrganizationWorkspace(request)
                if &request.target == connection.organization() =>
            {
                PersonFrame::LookupWorkspace {
                    correlation: correlation_id,
                    request,
                }
            }
            CentralCall::ReadOrganizationChangeRecords(request)
                if &request.target == connection.organization() =>
            {
                PersonFrame::ReadChangeRecords {
                    correlation: correlation_id,
                    request,
                }
            }
            CentralCall::ListPersonApprovals(request) => approval(
                connection,
                correlation_id,
                ApprovalPersonOperation::List(ApprovalListRequest {
                    organization: request.target,
                    filter: request.body.filter,
                    cursor: request.body.cursor,
                }),
            ),
            CentralCall::ReadPersonApproval(request) => approval(
                connection,
                correlation_id,
                ApprovalPersonOperation::Read(ApprovalReadRequest {
                    organization: request.target,
                    request_id: request.body.request_id,
                }),
            ),
            CentralCall::DecidePersonApproval(request) => approval(
                connection,
                correlation_id,
                ApprovalPersonOperation::Decide(ApprovalDecideRequest {
                    organization: request.target,
                    request_id: request.body.request_id,
                    operation_id: request.body.operation_id,
                    answer: request.body.answer,
                }),
            ),
            CentralCall::CreateOrganizationWorkspace(_)
            | CentralCall::ListOrganizationWorkspaces(_)
            | CentralCall::LookupOrganizationWorkspace(_)
            | CentralCall::ReadOrganizationChangeRecords(_)
            | CentralCall::RegisterRun(_)
            | CentralCall::Raise(_)
            | CentralCall::Stop(_)
            | CentralCall::ListApprovalsForSession(_)
            | CentralCall::ReadApprovalForSession(_)
            | CentralCall::DecideApprovalAsSession(_)
            | CentralCall::PassOnApproval(_)
            | CentralCall::CreateFleetSession(_)
            | CentralCall::BindFleetSession(_)
            | CentralCall::EndFleetSession(_) => {
                refusal(correlation_id, LinkCallRefusal::WrongTarget)
            }
        },
        LinkFrame::Data(DataFrame::Request { request }) => {
            PersonFrame::Reply(vec![refuse_request(&request)])
        }
        LinkFrame::Data(DataFrame::Batch { requests }) => {
            PersonFrame::Reply(requests.requests().iter().map(refuse_request).collect())
        }
        LinkFrame::Data(DataFrame::RequestStatus { correlation_id, .. }) => {
            reply(DataFrame::RequestStatusReply {
                correlation_id,
                result: Box::new(RequestStatusAnswer::Refused(RequestRefusal::Workspace(
                    workspace_model::RefusalReason::WorkspaceNotAllowed,
                ))),
            })
        }
        LinkFrame::Data(DataFrame::Receipts {
            correlation_id,
            receipts,
        }) => reply(DataFrame::ReceiptResults {
            correlation_id,
            results: vec![
                central_link_protocol::ReceiptResult::NotApplicable;
                receipts.receipts().len()
            ],
        }),
        LinkFrame::Data(DataFrame::RenewMachineToken { correlation_id, .. }) => {
            reply(DataFrame::MachineTokenRenewal {
                correlation_id,
                result: TokenRenewalResult::Refused {
                    reason: TokenRenewalRefusal::TokenRejected,
                },
            })
        }
        LinkFrame::Pong
        | LinkFrame::Data(
            DataFrame::MacCallReply { .. }
            | DataFrame::MacCapabilities(_)
            | DataFrame::Welcome { .. }
            | DataFrame::Reply { .. }
            | DataFrame::RequestStatusReply { .. }
            | DataFrame::Deliver { .. }
            | DataFrame::ReceiptResults { .. }
            | DataFrame::CentralCallReply { .. }
            | DataFrame::CallMac { .. }
            | DataFrame::MachineTokenRenewal { .. },
        ) => PersonFrame::ProtocolViolation,
    }
}

fn refuse_request(request: &central_link_protocol::LinkRequest) -> LinkFrame {
    LinkFrame::Data(DataFrame::Reply {
        correlation_id: request.correlation_id().clone(),
        result: Box::new(ReplyPayload::workspace_refused(
            request.payload(),
            workspace_model::RefusalReason::WorkspaceNotAllowed,
        )),
    })
}

fn refusal(correlation_id: CorrelationId, reason: LinkCallRefusal) -> PersonFrame {
    reply(DataFrame::CentralCallReply {
        correlation_id,
        result: Box::new(TypedCallResult::Refused { reason }),
    })
}

fn reply(frame: DataFrame) -> PersonFrame {
    PersonFrame::Reply(vec![LinkFrame::Data(frame)])
}
