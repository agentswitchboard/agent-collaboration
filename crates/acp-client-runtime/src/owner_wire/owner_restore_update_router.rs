//! Read public ACP notifications at the wire boundary.

use super::OwnerAcpWire;
use crate::acp_transport::RestoreSelection;
use agent_client_protocol::{Agent, ConnectionTo, Dispatch, Error, HandleDispatchFrom, Handled};
use session_event_model::session_profile_codec::STATE_METHOD;
use std::sync::Arc;

pub(crate) struct OwnerRestoreUpdateRouter(pub(crate) Arc<OwnerAcpWire>);

impl HandleDispatchFrom<Agent> for OwnerRestoreUpdateRouter {
    async fn handle_dispatch_from(
        &mut self,
        message: Dispatch,
        _connection: ConnectionTo<Agent>,
    ) -> Result<Handled<Dispatch>, Error> {
        let session = match &message {
            Dispatch::Notification(notification)
                if matches!(notification.method(), "session/update" | STATE_METHOD) =>
            {
                notification
                    .params()
                    .get("sessionId")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            }
            _ => None,
        };
        let Some(session) = session else {
            return Ok(Handled::No {
                message,
                retry: false,
            });
        };
        #[cfg(any(test, feature = "test-observation"))]
        self.0
            .at_opening_effect_for_test(super::OpeningPausePoint::CreatedUpdateIngress);
        Ok(match self.0.forward_restored_update(&session, message) {
            None => Handled::Yes,
            Some(message) => Handled::No {
                message,
                retry: false,
            },
        })
    }

    fn describe_chain(&self) -> impl std::fmt::Debug {
        "OwnerRestoreUpdateRouter"
    }
}

impl OwnerAcpWire {
    pub(crate) fn forward_restored_update(
        &self,
        session: &str,
        message: Dispatch,
    ) -> Option<Dispatch> {
        let selected = self.select_restore_update(session);
        self.forward_selected_restore_update(session, selected, message)
    }

    fn select_restore_update(&self, session: &str) -> RestoreSelection {
        let _edit = self
            .route_edits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.transport.select_restore_registration(session)
    }

    fn forward_selected_restore_update(
        &self,
        session: &str,
        selected: RestoreSelection,
        message: Dispatch,
    ) -> Option<Dispatch> {
        self.restore_updates.forward(session, selected, message)
    }
}

#[cfg(test)]
#[path = "owner_restore_selection_test.rs"]
mod tests;
