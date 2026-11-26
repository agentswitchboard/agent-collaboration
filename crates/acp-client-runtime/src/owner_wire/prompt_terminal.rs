//! Complete the exact prompt reply through the reader or its fallback.

use crate::provider_prompt_result_codec::decode_prompt_result;
use agent_client_protocol::Error;
use provider_session_machines::{
    session_owner::{DispatchAuthorization, DispatchResult, PromptCompletion, WireOutcome},
    session_turn_machine::TurnId,
};
use session_event_model::{LocalCause, StopReason};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use switchboard_harness_contracts::{ReservedWorkReply, WorkReply};
use tokio::sync::mpsc;

pub(crate) struct PromptTerminal {
    pub(crate) turn: TurnId,
    pub(crate) compact: bool,
    owned_reply: Option<PromptReply>,
}

struct PromptReply {
    authorization: DispatchAuthorization,
    wire_result: Option<Result<serde_json::Value, Error>>,
    completion: ReservedWorkReply,
}

impl PromptTerminal {
    pub(super) fn new(
        turn: TurnId,
        compact: bool,
        authorization: DispatchAuthorization,
        wire_result: Option<Result<serde_json::Value, Error>>,
        completion: ReservedWorkReply,
    ) -> Self {
        Self {
            turn,
            compact,
            owned_reply: Some(PromptReply {
                authorization,
                wire_result,
                completion,
            }),
        }
    }

    pub(super) fn route(self, reader: Option<PromptTerminalSender>) {
        if let Some(reader) = reader {
            reader.send_or_finish(self);
        } else {
            self.finish(String::new(), None);
        }
    }

    pub(crate) fn stop_reason(&self) -> Option<StopReason> {
        decode_prompt_result(
            self.owned_reply
                .as_ref()?
                .wire_result
                .as_ref()?
                .as_ref()
                .ok()?,
        )
        .ok()
    }

    pub(crate) fn finish(mut self, response: String, local_cause: Option<LocalCause>) {
        if let Some(reply) = self.owned_reply.take() {
            reply.finish(response, local_cause);
        }
    }
}

impl Drop for PromptTerminal {
    fn drop(&mut self) {
        if let Some(reply) = self.owned_reply.take() {
            reply.finish(String::new(), None);
        }
    }
}

impl PromptReply {
    fn finish(self, response: String, local_cause: Option<LocalCause>) {
        if let Some(Err(error)) = &self.wire_result
            && let Some(failure) =
                crate::provider_acp_error_mapping::acp_operation_error(error.clone())
                    .coded_wire_failure()
        {
            self.completion
                .send(WorkReply::Dispatch(DispatchResult::PromptFailed {
                    authorization: self.authorization,
                    failure,
                }));
            return;
        }
        let outcome = match self.wire_result {
            Some(Ok(reply)) => {
                decode_prompt_result(&reply).map_or(WireOutcome::Unknown, |stop_reason| {
                    WireOutcome::Confirmed(PromptCompletion {
                        stop_reason,
                        local_cause,
                        response,
                    })
                })
            }
            _ => WireOutcome::Unknown,
        };
        self.completion
            .send(WorkReply::Dispatch(DispatchResult::PromptFinished {
                authorization: self.authorization,
                outcome,
            }));
    }
}

#[derive(Clone)]
pub(crate) struct PromptTerminalSender {
    sender: mpsc::Sender<PromptTerminal>,
    publication: Arc<Mutex<()>>,
    #[cfg(feature = "test-observation")]
    observer: Option<TerminalAdmissionObserver>,
}

pub(crate) struct PromptTerminalReceiver {
    receiver: mpsc::Receiver<PromptTerminal>,
    publication: Arc<Mutex<()>>,
    #[cfg(test)]
    close_waiting: Option<tokio::sync::oneshot::Sender<()>>,
}

pub(super) fn prompt_terminal_channel() -> (PromptTerminalSender, PromptTerminalReceiver) {
    let (sender, receiver) = mpsc::channel(1);
    let publication = Arc::new(Mutex::new(()));
    (
        PromptTerminalSender {
            sender,
            publication: Arc::clone(&publication),
            #[cfg(feature = "test-observation")]
            observer: None,
        },
        PromptTerminalReceiver {
            receiver,
            publication,
            #[cfg(test)]
            close_waiting: None,
        },
    )
}

impl PromptTerminalSender {
    fn send_or_finish(&self, terminal: PromptTerminal) {
        #[cfg(feature = "test-observation")]
        let turn = terminal.turn;
        let result = {
            let _publication = self
                .publication
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.sender.try_send(terminal)
        };
        #[cfg(feature = "test-observation")]
        if let Some(observer) = &self.observer {
            let outcome = match &result {
                Ok(()) => TerminalAdmission::Queued,
                Err(mpsc::error::TrySendError::Closed(_)) => TerminalAdmission::Closed,
                Err(mpsc::error::TrySendError::Full(_)) => TerminalAdmission::Full,
            };
            observer.report(turn, outcome);
        }
        if let Err(undelivered) = result {
            undelivered.into_inner().finish(String::new(), None);
        }
    }

    #[cfg(feature = "test-observation")]
    pub(super) fn with_observer(mut self, observer: TerminalAdmissionObserver) -> Self {
        self.observer = Some(observer);
        self
    }
}

impl PromptTerminalReceiver {
    pub(crate) async fn recv(&mut self) -> Option<PromptTerminal> {
        self.receiver.recv().await
    }
}

impl Drop for PromptTerminalReceiver {
    fn drop(&mut self) {
        let pending = {
            // Tokio's try_send reserves capacity and publishes in separate
            // steps. Complete that span before closing and taking leftovers.
            let _publication = lock_for_close(
                &self.publication,
                #[cfg(test)]
                self.close_waiting.take(),
            );
            self.receiver.close();
            let mut pending = Vec::new();
            while let Ok(terminal) = self.receiver.try_recv() {
                pending.push(terminal);
            }
            pending
        };
        drop(pending);
    }
}

fn lock_for_close(
    gate: &Mutex<()>,
    #[cfg(test)] waiting: Option<tokio::sync::oneshot::Sender<()>>,
) -> MutexGuard<'_, ()> {
    #[cfg(test)]
    if let Some(waiting) = waiting {
        match gate.try_lock() {
            Ok(locked) => return locked,
            Err(std::sync::TryLockError::Poisoned(error)) => return error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                let _test_gone = waiting.send(());
            }
        }
    }
    gate.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(feature = "test-observation")]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TerminalAdmission {
    Queued,
    Closed,
    Full,
}

#[cfg(feature = "test-observation")]
type TerminalAdmissionSignal = tokio::sync::oneshot::Sender<(TurnId, TerminalAdmission)>;

#[cfg(feature = "test-observation")]
#[derive(Clone, Default)]
pub(super) struct TerminalAdmissionObserver(Arc<Mutex<Option<TerminalAdmissionSignal>>>);

#[cfg(feature = "test-observation")]
impl TerminalAdmissionObserver {
    pub(super) fn observe(&self) -> tokio::sync::oneshot::Receiver<(TurnId, TerminalAdmission)> {
        let (signal, observed) = tokio::sync::oneshot::channel();
        let previous = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(signal);
        assert!(
            previous.is_none(),
            "one pending prompt terminal observation"
        );
        observed
    }

    fn report(&self, turn: TurnId, outcome: TerminalAdmission) {
        let signal = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(signal) = signal {
            let _test_gone = signal.send((turn, outcome));
        }
    }
}

#[cfg(test)]
#[path = "prompt_terminal_test.rs"]
mod tests;
