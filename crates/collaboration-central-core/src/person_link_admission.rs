//! Apply signed person-link authority to the owning organization's live floors.

use crate::{
    AdminRefusal, OrganizationAdminResult, WorkspaceRuntime,
    organization_authority_cache::StampStanding,
    organization_workspace_plans::apply_workspace_create, person_request::CreateWorkspaceRequest,
};
use central_link_protocol::{
    CentralLinkRpc, DataFrame, LinkCallRefusal, LinkFrame, TypedCallResult,
    person_link_rpc::{
        CreateOrganizationWorkspace, CreateOrganizationWorkspaceRefusal,
        CreateOrganizationWorkspaceResult, CreateOrganizationWorkspaceRpc,
        ListOrganizationWorkspaces, ListOrganizationWorkspacesRpc, OrganizationWorkspace,
        OrganizationWorkspacePage, OrganizationWorkspaceState,
    },
};
use central_person_link::{PersonFrame, PersonSocketAttachment, dispatch};
use central_storage::StoreFailure;
use fleet_runtime_ports::{CentralStore, WorkspaceStores, workspace_decode_error::corrupt};
use fleet_signin::{
    FleetPermission,
    authorization_floor_store::{
        organization_guard, person_guard, read_organization_floor, read_person_floor,
    },
    person_proof::{
        OrganizationFloor, PersonFloor, PersonRequestRefusal, SECOND_FACTOR_WINDOW_SECONDS,
    },
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
        if self.current_person_link_floors(connection).await?.is_none() {
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

    async fn current_person_link_floors(
        &self,
        attachment: &PersonSocketAttachment,
    ) -> Result<Option<(OrganizationFloor, PersonFloor)>, StoreFailure> {
        if !attachment.is_current(self.execution.clock().now().unix_millis()) {
            return Ok(None);
        }
        Ok(self
            .person_link_floors(attachment.organization(), attachment.person())
            .await?
            .filter(|(organization, person)| {
                attachment.still_matches(organization, person)
                    && attachment.is_current(self.execution.clock().now().unix_millis())
            }))
    }

    pub async fn handle_person_link_text(
        &self,
        connection: &PersonSocketAttachment,
        text: &str,
    ) -> Result<PersonLinkReply, StoreFailure> {
        let turn = self.execution.take_central_turn().await;
        let Some((organization, person)) = self.current_person_link_floors(connection).await?
        else {
            return Ok(PersonLinkReply::Reconnect);
        };
        Ok(match dispatch(connection, text) {
            PersonFrame::Reply(frames) => PersonLinkReply::Frames(frames),
            PersonFrame::ProtocolViolation => PersonLinkReply::ProtocolViolation,
            PersonFrame::CreateWorkspace {
                correlation,
                request,
            } => {
                let now = self.execution.now()?;
                if !connection.is_current(now.timestamp_millis()) {
                    return Ok(PersonLinkReply::Reconnect);
                }
                let answer = self
                    .create_workspace_on_person_link(
                        connection,
                        request.body,
                        now,
                        &organization,
                        &person,
                    )
                    .await;
                drop(turn);
                let _ = self.after_event().await;
                let result = match answer {
                    Ok(answer) => TypedCallResult::Answered {
                        reply: CreateOrganizationWorkspaceRpc::reply(fleet_rpc::Envelope {
                            target: request.target,
                            body: answer,
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

    async fn create_workspace_on_person_link(
        &self,
        connection: &PersonSocketAttachment,
        request: CreateOrganizationWorkspace,
        now: workspace_model::Timestamp,
        organization: &OrganizationFloor,
        person: &PersonFloor,
    ) -> Result<CreateOrganizationWorkspaceResult, StoreFailure> {
        use CreateOrganizationWorkspaceRefusal as Refusal;
        use CreateOrganizationWorkspaceResult::{Created, Refused};
        if !connection.permits(FleetPermission::CreateWorkspace) {
            return Ok(Refused {
                reason: Refusal::NotPermitted {},
            });
        }
        if !connection.has_fresh_second_factor(now.timestamp_millis()) {
            let within_seconds = u32::try_from(SECOND_FACTOR_WINDOW_SECONDS)
                .ok()
                .and_then(std::num::NonZeroU32::new)
                .ok_or(StoreFailure::Unavailable)?;
            return Ok(Refused {
                reason: Refusal::SecondFactorRequired { within_seconds },
            });
        }
        let organization_id = connection.organization().as_str();
        let statements = vec![
            organization_guard(organization_id, organization)?,
            person_guard(organization_id, connection.person().as_str(), person)?,
        ];
        let request = CreateWorkspaceRequest {
            organization_id: connection.organization().clone(),
            operation_id: String::from(request.operation_id)
                .try_into()
                .map_err(|_| corrupt("workspace.create.operation_id"))?,
            name: String::from(request.name)
                .try_into()
                .map_err(|_| corrupt("workspace.create.name"))?,
            description: request.description.into(),
        };
        let answer =
            apply_workspace_create(self, connection.person(), &request, now, statements).await?;
        match answer.result {
            OrganizationAdminResult::Applied { workspace } => Ok(Created {
                workspace: project_workspace(workspace)?,
            }),
            OrganizationAdminResult::Refused {
                reason: AdminRefusal::NameInUse,
            } => Ok(Refused {
                reason: Refusal::NameInUse {},
            }),
            OrganizationAdminResult::Refused {
                reason: AdminRefusal::Conflict,
            } => Ok(Refused {
                reason: Refusal::Conflict {},
            }),
            OrganizationAdminResult::Found { .. }
            | OrganizationAdminResult::Records { .. }
            | OrganizationAdminResult::Refused { .. } => Err(corrupt("workspace.create.result")),
        }
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
                .collect::<Result<_, _>>()?,
        })
    }
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
