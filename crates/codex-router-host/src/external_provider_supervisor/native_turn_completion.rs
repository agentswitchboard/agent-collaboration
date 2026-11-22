//! Keep the native completion notification behind the prompt's admission release.

use super::ExternalProviderSupervisor;
use collaboration_protocol::{OperationId, ProviderIdentity};
use collaboration_runtime::NativeTurnCompletion;
use provider_session_machines::session_turn_machine::{SessionTurn, TurnId};
use session_event_model::TurnOutcome;
use std::{future::Future, pin::Pin};

impl NativeTurnCompletion for ExternalProviderSupervisor {
    fn await_admission_release<'a>(
        &'a self,
        session: &'a message_board::SessionRef,
        turn_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let Ok(turn_id) = uuid::Uuid::parse_str(turn_id).map(TurnId) else {
                return;
            };
            let Ok(ProviderIdentity::Session(target)) =
                ProviderIdentity::from_board_identity(&message_board::Identity::Session {
                    session: session.clone(),
                })
            else {
                return;
            };
            let Some(owner) = self
                .owner_door(&target.endpoint)
                .and_then(|door| door.existing(&target))
            else {
                return;
            };
            let mut snapshots = owner.snapshots();
            // The Hub can offer TurnEnded before the owner task has published
            // the matching snapshot. Running/Cancelling for this turn is old.
            let operation = snapshots
                .wait_for(|snapshot| !matches!(snapshot.turn, SessionTurn::Running { turn } | SessionTurn::Cancelling { turn } if turn == turn_id))
                .await
                .ok()
                .and_then(|snapshot| {
                    snapshot.last_live_turn.as_ref()
                        .filter(|terminal| terminal.turn == turn_id && matches!(terminal.outcome, TurnOutcome::Ended { .. }))
                        .and_then(|terminal| OperationId::try_from(terminal.operation.to_string()).ok())
                });
            let Some(operation) = operation else {
                return;
            };
            let cell = self.inner.uncertainty.slot(&target.endpoint);
            let mut eligibility = cell.subscribe_eligibility();
            let mut reported = false;
            loop {
                eligibility.borrow_and_update();
                let blocked = self
                    .inner
                    .live_operations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .holds_custody_of(&operation);
                if !blocked {
                    return;
                }
                if !reported {
                    #[cfg(any(test, feature = "test-support"))]
                    cell.fixture.notice_native_completion_wait();
                    reported = true;
                }
                if eligibility.changed().await.is_err() {
                    return;
                }
            }
        })
    }
}
