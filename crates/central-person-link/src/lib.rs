//! The authority and two-state lifetime of one admitted person connection.
#![forbid(unsafe_code)]

mod connection_registry;
pub use connection_registry::{PersonConnectionRegistry, RegisteredPersonConnection};

use fleet_identity_protocol::{OrganizationId, UnixMillis};
use fleet_signin::person_proof::{
    CheckedPersonLink, OrganizationFloor, OrganizationFloorState, PersonFloor, PersonFloorState,
    PersonLinkFacts, PersonLinkSecondFactor,
};
use fleet_signin::{FenceRevision, FleetPermission};
use workspace_ids::PersonId;

/// The private connection state accepted only from signed admission or the owning socket.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PersonSocketAttachment {
    person_id: PersonId,
    organization_id: OrganizationId,
    permissions: Vec<FleetPermission>,
    authorization_snapshot_at_ms: UnixMillis,
    authorization_snapshot_valid_until_ms: UnixMillis,
    person_revision: FenceRevision,
    organization_revision: FenceRevision,
    session_expires_at_ms: UnixMillis,
    #[serde(deserialize_with = "Option::deserialize")]
    second_factor: Option<PersonLinkSecondFactor>,
}

impl PersonSocketAttachment {
    pub fn admit(
        checked: &CheckedPersonLink,
        organization: &OrganizationId,
        now_ms: i64,
        organization_floor: &OrganizationFloor,
        person_floor: &PersonFloor,
    ) -> Option<Self> {
        let facts = checked.facts();
        (now_ms < checked.upgrade_expires_at_ms()
            && &facts.organization_id == organization
            && accepts_floors(facts, organization_floor, person_floor))
        .then(|| Self {
            person_id: facts.person_id.clone(),
            organization_id: facts.organization_id.clone(),
            permissions: facts.permissions.clone(),
            authorization_snapshot_at_ms: facts.authorization_snapshot_at_ms,
            authorization_snapshot_valid_until_ms: facts.authorization_snapshot_valid_until_ms,
            person_revision: facts.person_revision,
            organization_revision: facts.organization_revision,
            session_expires_at_ms: facts.session_expires_at_ms,
            second_factor: facts.second_factor.clone(),
        })
    }

    pub fn organization(&self) -> &OrganizationId {
        &self.organization_id
    }

    pub fn person(&self) -> &PersonId {
        &self.person_id
    }

    pub fn permits(&self, permission: FleetPermission) -> bool {
        self.permissions.contains(&permission)
    }

    pub fn is_current(&self, now_ms: i64) -> bool {
        now_ms
            < self
                .authorization_snapshot_valid_until_ms
                .0
                .min(self.session_expires_at_ms.0)
    }

    pub fn still_matches(&self, organization: &OrganizationFloor, person: &PersonFloor) -> bool {
        organization.state == OrganizationFloorState::Active
            && organization.revision.get() == self.organization_revision.get()
            && person.state == PersonFloorState::Active
            && person.revision.get() == self.person_revision.get()
    }

    pub fn is_well_formed(&self) -> bool {
        let snapshot = self.authorization_snapshot_at_ms.0;
        let snapshot_expiry = self.authorization_snapshot_valid_until_ms.0;
        snapshot >= 0
            && self.session_expires_at_ms.0 <= 9_007_199_254_740_991
            && snapshot_expiry <= 9_007_199_254_740_991
            && snapshot_expiry
                .checked_sub(snapshot)
                .is_some_and(|life| (1..=300_000).contains(&life))
            && self.session_expires_at_ms.0 > snapshot
            && self.second_factor.as_ref().is_none_or(|factor| {
                !factor.method.is_empty() && (0..=9_007_199_254_740).contains(&factor.at.0)
            })
    }
}

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

pub fn accepts_floors(
    facts: &PersonLinkFacts,
    organization_floor: &OrganizationFloor,
    person_floor: &PersonFloor,
) -> bool {
    organization_floor.state == OrganizationFloorState::Active
        && organization_floor.revision.get() == facts.organization_revision.get()
        && organization_floor.policy_digest == facts.permission_policy_digest
        && person_floor.state == PersonFloorState::Active
        && person_floor.revision.get() == facts.person_revision.get()
        && person_floor.subject_id == facts.authorization_subject_id
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
            } => {
                organization == &facts.organization_id
                    && *revision > facts.organization_revision.get()
            }
            Self::Person {
                organization,
                person,
                revision,
            } => {
                organization == &facts.organization_id
                    && person == &facts.person_id
                    && *revision > facts.person_revision.get()
            }
        }
    }
}

use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, FrameDirection, LinkCallRefusal, LinkFrame,
    ProtocolError, ReplyPayload, RequestRefusal, RequestStatusAnswer, TokenRenewalRefusal,
    TokenRenewalResult, TypedCallResult, decode_text, person_link_rpc::ListOrganizationWorkspaces,
};
use fleet_rpc::Envelope;

pub enum PersonFrame {
    Reply(Vec<LinkFrame>),
    ListWorkspaces {
        correlation: CorrelationId,
        request: Envelope<OrganizationId, ListOrganizationWorkspaces>,
    },
    ProtocolViolation,
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
            CentralCall::ListOrganizationWorkspaces(_)
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
