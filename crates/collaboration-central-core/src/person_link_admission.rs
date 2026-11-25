//! Apply signed person-link authority to the owning organization's live floors.

mod change_record_projection;

use crate::{
    PersonApprovalAnswer, WorkspaceRuntime, organization_authority_cache::StampStanding,
    person_approval_application::PersonApprovalApplication,
};
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, CorrelationId, DataFrame, LinkCallRefusal, LinkFrame,
    TypedCallResult,
    person_link_rpc::{
        DecidePersonApprovalRpc, ListOrganizationWorkspaces, ListOrganizationWorkspacesRpc,
        ListPersonApprovalsRpc, LookupOrganizationWorkspace, LookupOrganizationWorkspaceResult,
        LookupOrganizationWorkspaceRpc, OrganizationReadRefusal, OrganizationWorkspace,
        OrganizationWorkspacePage, OrganizationWorkspaceState, ReadOrganizationChangeRecordsRpc,
        ReadPersonApprovalRpc,
    },
};
use central_person_link::{PersonFrame, PersonSocketAttachment, dispatch};
use central_storage::StoreFailure;
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::Envelope;
use fleet_runtime_ports::{CentralStore, WorkspaceStores, workspace_decode_error::corrupt};
use fleet_signin::{
    FleetPermission,
    authorization_floor_store::{read_organization_floor, read_person_floor},
    person_proof::{AdmissionRefusal, OrganizationFloor, PersonFloor, PersonRequestRefusal},
};

pub enum PersonLinkAdmission {
    Admitted(PersonSocketAttachment),
    Refused,
}

pub enum PersonSocketRegistration<T> {
    Registered(T),
    Stale,
}

pub enum PersonLinkReply {
    Frames(Vec<LinkFrame>),
    Reconnect,
    ProtocolViolation,
}

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub async fn admit_person_link(
        &self,
        token: &str,
    ) -> Result<PersonLinkAdmission, StoreFailure> {
        let check = self
            .signin_authority
            .check()
            .ok_or(StoreFailure::Unavailable)?;
        let checked = match check.verify_person_link(token).await {
            Ok(checked) => checked,
            Err(PersonRequestRefusal::VerificationUnavailable | PersonRequestRefusal::Policy) => {
                return Err(StoreFailure::Unavailable);
            }
            Err(_) => return Ok(PersonLinkAdmission::Refused),
        };
        let _turn = self.execution.take_central_turn().await;
        let Some((organization, person)) = self
            .person_link_floors(&checked.facts().organization_id, &checked.facts().person_id)
            .await?
        else {
            return Ok(PersonLinkAdmission::Refused);
        };
        let now = self.execution.clock().now().unix_millis();
        Ok(
            match PersonSocketAttachment::admit(
                &checked,
                self.execution.organization(),
                now,
                &organization,
                &person,
            ) {
                Some(connection) => PersonLinkAdmission::Admitted(connection),
                None => PersonLinkAdmission::Refused,
            },
        )
    }

    /// Register the physical socket under the same turn that advances a revision.
    pub async fn register_person_socket<T>(
        &self,
        connection: &PersonSocketAttachment,
        register: impl FnOnce() -> T,
    ) -> Result<PersonSocketRegistration<T>, StoreFailure> {
        let _turn = self.execution.take_central_turn().await;
        if !self.person_link_is_current(connection).await? {
            return Ok(PersonSocketRegistration::Stale);
        }
        Ok(PersonSocketRegistration::Registered(register()))
    }

    async fn person_link_floors(
        &self,
        organization: &fleet_identity_protocol::OrganizationId,
        person_id: &workspace_ids::PersonId,
    ) -> Result<Option<(OrganizationFloor, PersonFloor)>, StoreFailure> {
        if organization != self.execution.organization() {
            return Ok(None);
        }
        self.prepare_signin_authority().await?;
        let store = self.execution.statement_host();
        let organization_floor = read_organization_floor(store, organization.as_str()).await?;
        let person_floor =
            read_person_floor(store, organization.as_str(), person_id.as_str()).await?;
        if self.signin_authority.standing(self.execution.clock().now()) != StampStanding::Valid {
            return Ok(None);
        }
        Ok(organization_floor.zip(person_floor))
    }

    async fn person_link_is_current(
        &self,
        attachment: &PersonSocketAttachment,
    ) -> Result<bool, StoreFailure> {
        if !attachment.is_current(self.execution.clock().now().unix_millis()) {
            return Ok(false);
        }
        Ok(self
            .person_link_floors(attachment.organization(), attachment.person())
            .await?
            .is_some_and(|(organization, person)| {
                attachment.still_matches(&organization, &person)
                    && attachment.is_current(self.execution.clock().now().unix_millis())
            }))
    }

    pub async fn handle_person_link_text(
        &self,
        connection: &PersonSocketAttachment,
        text: &str,
    ) -> Result<PersonLinkReply, StoreFailure> {
        let request = dispatch(connection, text);
        #[cfg(feature = "test-support")]
        if matches!(&request, PersonFrame::Approval { .. }) {
            self.execution
                .pause_points()
                .reached_central(fleet_runtime::PausePoint::BeforePersonApprovalTurn)
                .await;
        }
        let turn = self.execution.take_central_turn().await;
        if !self.person_link_is_current(connection).await? {
            return Ok(PersonLinkReply::Reconnect);
        }
        Ok(match request {
            PersonFrame::Reply(frames) => PersonLinkReply::Frames(frames),
            PersonFrame::ProtocolViolation => PersonLinkReply::ProtocolViolation,
            PersonFrame::ListWorkspaces {
                correlation,
                request,
            } => organization_read_reply::<ListOrganizationWorkspacesRpc>(
                correlation,
                request.target,
                self.read_person_workspace_page(request.body).await,
            ),
            PersonFrame::LookupWorkspace {
                correlation,
                request,
            } => organization_read_reply::<LookupOrganizationWorkspaceRpc>(
                correlation,
                request.target,
                self.lookup_workspace_on_person_link(connection, request.body)
                    .await,
            ),
            PersonFrame::ReadChangeRecords {
                correlation,
                request,
            } => organization_read_reply::<ReadOrganizationChangeRecordsRpc>(
                correlation,
                request.target,
                self.read_change_records_on_person_link(connection, request.body)
                    .await,
            ),
            PersonFrame::Approval {
                correlation,
                operation,
            } => {
                let result = self
                    .commit_person_approval(
                        turn,
                        PersonApprovalApplication::from_person_link(connection, operation),
                    )
                    .await;
                let _ = self.after_event().await;
                match result {
                    Ok(answer) => approval_reply(correlation, connection.organization(), answer),
                    // The caller has no definitive answer and can reconcile with the
                    // same operation ID.
                    Err(
                        error
                        @ (StoreFailure::AnswerLost | StoreFailure::AlarmUnpublishable { .. }),
                    ) => {
                        return Err(error);
                    }
                    Err(_) => person_call_reply(
                        correlation,
                        TypedCallResult::Refused {
                            reason: LinkCallRefusal::CentralFault,
                        },
                    ),
                }
            }
        })
    }

    async fn lookup_workspace_on_person_link(
        &self,
        connection: &PersonSocketAttachment,
        request: LookupOrganizationWorkspace,
    ) -> Result<LookupOrganizationWorkspaceResult, StoreFailure> {
        use LookupOrganizationWorkspaceResult::{Absent, Found, Refused};
        if !connection.permits(FleetPermission::ViewOrganization) {
            return Ok(Refused {
                reason: OrganizationReadRefusal::NotPermitted {},
            });
        }
        Ok(
            match self
                .find_organization_workspace(&request.workspace_id)
                .await?
            {
                Some(workspace) => Found {
                    workspace: project_workspace(workspace)?,
                },
                None => Absent {},
            },
        )
    }

    async fn read_person_workspace_page(
        &self,
        request: ListOrganizationWorkspaces,
    ) -> Result<OrganizationWorkspacePage, StoreFailure> {
        let after = request
            .after
            .map(|cursor| message_board::WorkspaceId::try_from(String::from(cursor)))
            .transpose()
            .map_err(|_| corrupt("directory_workspaces.workspace_id"))?;
        let page = self
            .list_organization_workspaces(after.as_ref(), request.limit.into())
            .await?;
        Ok(OrganizationWorkspacePage {
            next: page.next.map(project_workspace_id).transpose()?,
            workspaces: page
                .workspaces
                .into_iter()
                .map(project_workspace)
                .collect::<Result<_, StoreFailure>>()?,
        })
    }
}

fn organization_read_reply<Rpc: CentralLinkRpc>(
    correlation_id: CorrelationId,
    target: Rpc::Target,
    answer: Result<Rpc::Reply, StoreFailure>,
) -> PersonLinkReply {
    let result = match answer {
        Ok(body) => TypedCallResult::Answered {
            reply: Rpc::reply(Envelope { target, body }),
        },
        Err(_) => TypedCallResult::Refused {
            reason: LinkCallRefusal::CentralFault,
        },
    };
    person_call_reply(correlation_id, result)
}

fn person_call_reply(
    correlation_id: CorrelationId,
    result: TypedCallResult<CentralCallReply>,
) -> PersonLinkReply {
    PersonLinkReply::Frames(vec![LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result: Box::new(result),
    })])
}

fn project_workspace_id(
    id: message_board::WorkspaceId,
) -> Result<workspace_ids::WorkspaceId, StoreFailure> {
    workspace_ids::WorkspaceId::try_from(String::from(id))
        .map_err(|_| corrupt("directory_workspaces.workspace_id"))
}

fn project_workspace(
    workspace: fleet_contracts::WorkspaceView,
) -> Result<OrganizationWorkspace, StoreFailure> {
    Ok(OrganizationWorkspace {
        workspace_id: project_workspace_id(workspace.workspace_id)?,
        name: workspace_model::ResourceName::try_from(String::from(workspace.name))
            .map_err(|_| corrupt("directory_workspaces.name"))?,
        description: workspace_model::Description::try_from(String::from(workspace.description))
            .map_err(|_| corrupt("directory_workspaces.description"))?,
        state: match workspace.state {
            message_board::WorkspaceState::Active => OrganizationWorkspaceState::Active,
            message_board::WorkspaceState::Archiving => OrganizationWorkspaceState::Archiving,
            message_board::WorkspaceState::Archived => OrganizationWorkspaceState::Archived,
        },
    })
}

fn approval_reply(
    correlation: CorrelationId,
    organization: &OrganizationId,
    answer: PersonApprovalAnswer,
) -> PersonLinkReply {
    let target = organization.clone();
    let reply = match answer {
        PersonApprovalAnswer::List(body) => {
            ListPersonApprovalsRpc::reply(Envelope { target, body })
        }
        PersonApprovalAnswer::Read(body) => ReadPersonApprovalRpc::reply(Envelope {
            target,
            body: Box::new(body),
        }),
        PersonApprovalAnswer::Decide(body) => {
            DecidePersonApprovalRpc::reply(Envelope { target, body })
        }
        PersonApprovalAnswer::Refused(AdmissionRefusal::NotPermitted) => {
            return person_call_reply(
                correlation,
                TypedCallResult::Refused {
                    reason: LinkCallRefusal::NotPermitted,
                },
            );
        }
        PersonApprovalAnswer::Unauthenticated
        | PersonApprovalAnswer::Refused(
            AdmissionRefusal::Expired | AdmissionRefusal::RefreshRequired,
        ) => {
            return PersonLinkReply::Reconnect;
        }
        PersonApprovalAnswer::Refused(
            AdmissionRefusal::AuthorizationSyncRequired
            | AdmissionRefusal::SecondFactorRequired
            | AdmissionRefusal::RequestTime
            | AdmissionRefusal::RequestReused,
        ) => {
            return person_call_reply(
                correlation,
                TypedCallResult::Refused {
                    reason: LinkCallRefusal::CentralFault,
                },
            );
        }
    };
    person_call_reply(correlation, TypedCallResult::Answered { reply })
}
