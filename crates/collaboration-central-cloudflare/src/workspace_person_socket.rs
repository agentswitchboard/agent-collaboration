//! Workerd socket wiring and strictly decoded private person attachments.

use super::{CentralObject, workspace_link_text::write_central_text};
use central_link_protocol::{PersonLinkCloseCode, encode_central_egress};
use collaboration_central_core::{
    PersonConnectionState, PersonLinkAdmission, PersonLinkReply, PersonRevision,
    PersonSocketAttachment, PersonSocketRegistration,
};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::Clock;
use worker::{
    Request, Response, Result, State, WebSocket, WebSocketIncomingMessage, WebSocketPair,
};

const PERSON_SOCKET_TAG: &str = "person-link";

pub(super) fn is_person_socket(state: &State, socket: &WebSocket) -> Result<bool> {
    Ok(state
        .as_raw()
        .get_tags(socket.as_ref())?
        .iter()
        .any(|tag| tag == PERSON_SOCKET_TAG))
}

impl CentralObject {
    pub(super) async fn upgrade_person_socket(
        &self,
        organization: OrganizationId,
        request: &Request,
    ) -> Result<Response> {
        if !crate::workspace_link_entry::is_upgrade(request)? {
            return Response::error("websocket upgrade required", 426);
        }
        let Some(token) = crate::workspace_link_entry::bearer(request)? else {
            return Response::empty().map(|response| response.with_status(401));
        };
        let runtime = self.runtime(organization).await?;
        let attachment = match runtime.admit_person_link(&token).await {
            Ok(PersonLinkAdmission::Admitted(attachment)) => attachment,
            Ok(PersonLinkAdmission::Refused) => {
                return Response::empty().map(|response| response.with_status(401));
            }
            Err(_) => return Response::empty().map(|response| response.with_status(503)),
        };
        let pair = WebSocketPair::new()?;
        pair.server.serialize_attachment(&attachment)?;
        match runtime
            .register_person_socket(&attachment, || {
                self.state
                    .accept_websocket_with_tags(&pair.server, &[PERSON_SOCKET_TAG]);
            })
            .await
        {
            Ok(PersonSocketRegistration::Registered(())) => Response::from_websocket(pair.client),
            Ok(PersonSocketRegistration::Stale) => {
                Response::empty().map(|response| response.with_status(401))
            }
            Err(_) => Response::empty().map(|response| response.with_status(503)),
        }
    }

    pub(super) async fn handle_person_socket(
        &self,
        socket: WebSocket,
        incoming: WebSocketIncomingMessage,
    ) -> Result<()> {
        let Some(attachment) = read_attachment(&socket) else {
            return reconnect(&socket);
        };
        self.check_physical_organization(attachment.organization())?;
        #[cfg(feature = "test-support")]
        super::signin_launcher_controls::sample_clock(self, attachment.organization()).await?;
        let mut connection = PersonConnectionState::Admitted(attachment);
        let Some(attachment) = connection.check_time(self.clock.now().unix_millis()) else {
            return reconnect(&socket);
        };
        let runtime = self.runtime(attachment.organization().clone()).await?;
        let Some(text) = self.machine_text(&socket, incoming)? else {
            return Ok(());
        };
        match runtime.handle_person_link_text(attachment, &text).await {
            Ok(PersonLinkReply::Frame(frame)) => write_central_text(
                &socket,
                encode_central_egress(&frame)
                    .map_err(|error| worker::Error::RustError(error.to_string()))?,
            ),
            Ok(PersonLinkReply::Reconnect) => reconnect(&socket),
            Ok(PersonLinkReply::ProtocolViolation) => {
                socket.close(Some(1008), Some("unsupported person frame"))
            }
            Err(_) => socket.close(Some(1011), Some("central unavailable")),
        }
    }
}

pub(super) fn close_affected(state: &State, revision: &PersonRevision) {
    for socket in state.get_websockets_with_tag(PERSON_SOCKET_TAG) {
        match read_attachment(&socket) {
            Some(attachment) => {
                if PersonConnectionState::Admitted(attachment).observe_revision(revision) {
                    let _ = reconnect(&socket);
                }
            }
            None => {
                let _ = reconnect(&socket);
            }
        }
    }
}

fn read_attachment(socket: &WebSocket) -> Option<PersonSocketAttachment> {
    socket
        .deserialize_attachment::<PersonSocketAttachment>()
        .ok()
        .flatten()
        .filter(PersonSocketAttachment::is_well_formed)
}

fn reconnect(socket: &WebSocket) -> Result<()> {
    socket.close(
        Some(PersonLinkCloseCode::Reconnect.code()),
        Some("reconnect"),
    )
}
