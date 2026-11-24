//! Apply signed person-link authority to the owning organization's live floors.

use crate::{WorkspaceRuntime, organization_authority_cache::StampStanding};
use central_link_protocol::{
    CentralLinkRpc, DataFrame, LinkCallRefusal, LinkFrame, TypedCallResult,
    person_link_rpc::{
        ListOrganizationWorkspaces, ListOrganizationWorkspacesRpc, OrganizationWorkspace,
        OrganizationWorkspacePage, OrganizationWorkspaceState,
    },
};
use central_person_link::{PersonFrame, PersonSocketAttachment, dispatch};
use central_storage::StoreFailure;
use fleet_runtime_ports::{CentralStore, WorkspaceStores, workspace_decode_error::corrupt};
use fleet_signin::{
    authorization_floor_store::{read_organization_floor, read_person_floor},
    person_proof::{OrganizationFloor, PersonFloor, PersonRequestRefusal},
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
        let _turn = self.execution.take_central_turn().await;
        if !self.person_link_is_current(connection).await? {
            return Ok(PersonLinkReply::Reconnect);
        }
        Ok(match dispatch(connection, text) {
            PersonFrame::Reply(frames) => PersonLinkReply::Frames(frames),
            PersonFrame::ProtocolViolation => PersonLinkReply::ProtocolViolation,
            PersonFrame::ListWorkspaces {
                correlation,
                request,
            } => {
                let result = match self.read_person_workspace_page(request.body).await {
                    Ok(page) => TypedCallResult::Answered {
                        reply: ListOrganizationWorkspacesRpc::reply(fleet_rpc::Envelope {
                            target: request.target,
                            body: page,
                        }),
                    },
                    Err(_) => TypedCallResult::Refused {
                        reason: LinkCallRefusal::CentralFault,
                    },
                };
                PersonLinkReply::Frames(vec![LinkFrame::Data(DataFrame::CentralCallReply {
                    correlation_id: correlation,
                    result: Box::new(result),
                })])
            }
        })
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
        let project_id = |id: message_board::WorkspaceId| {
            workspace_ids::WorkspaceId::try_from(String::from(id))
                .map_err(|_| corrupt("directory_workspaces.workspace_id"))
        };
        Ok(OrganizationWorkspacePage {
            next: page.next.map(project_id).transpose()?,
            workspaces: page
                .workspaces
                .into_iter()
                .map(|workspace| {
                    Ok(OrganizationWorkspace {
                        workspace_id: project_id(workspace.workspace_id)?,
                        name: workspace_model::ResourceName::try_from(String::from(workspace.name))
                            .map_err(|_| corrupt("directory_workspaces.name"))?,
                        description: workspace_model::Description::try_from(String::from(
                            workspace.description,
                        ))
                        .map_err(|_| corrupt("directory_workspaces.description"))?,
                        state: match workspace.state {
                            message_board::WorkspaceState::Active => {
                                OrganizationWorkspaceState::Active
                            }
                            message_board::WorkspaceState::Archiving => {
                                OrganizationWorkspaceState::Archiving
                            }
                            message_board::WorkspaceState::Archived => {
                                OrganizationWorkspaceState::Archived
                            }
                        },
                    })
                })
                .collect::<Result<_, StoreFailure>>()?,
        })
    }
}
