//! Custody's command queue and its synchronous admission boundary.

use central_link_protocol::RequestStatusQuery;
use switchboard_machine_core_store::PendingWrite;
use tokio::sync::{mpsc, oneshot};
use workspace_ids::{MessageId, SessionRef};
use workspace_model::{
    Caller,
    central_mail::{MessageRecord, MessageStatus},
};

use crate::{
    custody_link::CustodyLinkError,
    custody_run_group::{CustodyStop, StopOnExit},
    custody_status::{
        CustodyRequestStatus, MessageStatusError, message_query, message_status_reply,
    },
    custody_submission::{
        Submission, SubmissionPayload, SubmissionRoute, SubmitRefusal, choose_submission_route,
        wire_scope,
    },
};

/// Custody's command queue has stopped accepting work.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CustodyQueueClosed {
    #[error("custody command queue is closed")]
    Closed,
}

/// Tool access to the running custody worker.
#[derive(Clone)]
pub struct CustodyTaskHandle {
    commands: mpsc::Sender<CustodyCommand>,
    stop: CustodyStop,
}

/// A slot in the queue. The command is admitted when the slot is committed.
pub struct CustodyAdmission {
    permit: mpsc::OwnedPermit<CustodyCommand>,
    stop: CustodyStop,
}

/// A tool outcome that may require a reply from the worker.
pub struct PendingCustodyReply<T>(CustodyReply<T>);

enum CustodyReply<T> {
    Ready(T),
    Admitted {
        reply: oneshot::Receiver<T>,
        lost_reply: T,
    },
}

impl<T> PendingCustodyReply<T> {
    pub async fn resolve(self) -> T {
        match self.0 {
            CustodyReply::Ready(outcome) => outcome,
            CustodyReply::Admitted { reply, lost_reply } => reply.await.unwrap_or(lost_reply),
        }
    }
}

pub(super) enum CustodyCommand {
    Submit {
        caller: Caller,
        payload: SubmissionPayload,
        unavailable: SubmissionUnavailable,
        reply: oneshot::Sender<Submission>,
    },
    Status {
        caller: Caller,
        query: RequestStatusQuery,
        reply: oneshot::Sender<CustodyRequestStatus>,
    },
    Pending {
        caller: Caller,
        reply: oneshot::Sender<Result<Vec<PendingWrite>, CustodyLinkError>>,
    },
    Waiting {
        session: SessionRef,
        reply: oneshot::Sender<Result<Vec<MessageRecord>, CustodyLinkError>>,
    },
}

impl CustodyCommand {
    pub(super) fn refuse_unstarted(self) {
        match self {
            Self::Submit {
                reply, unavailable, ..
            } => {
                let _ = reply.send(unavailable.reply());
            }
            Self::Status { reply, .. } => {
                let _ = reply.send(CustodyRequestStatus::StatusUnavailable);
            }
            Self::Pending { reply, .. } => {
                let _ = reply.send(Err(CustodyLinkError::TaskClosed));
            }
            Self::Waiting { reply, .. } => {
                let _ = reply.send(Err(CustodyLinkError::TaskClosed));
            }
        }
    }
}

pub(super) struct CustodyCommandReceiver {
    // Field drop order revokes admission before the receiver is destroyed,
    // including when the worker unwinds.
    _exit: StopOnExit,
    receiver: mpsc::Receiver<CustodyCommand>,
}

impl CustodyCommandReceiver {
    pub(super) async fn recv(&mut self) -> Option<CustodyCommand> {
        self.receiver.recv().await
    }

    pub(super) fn refuse_queued(&mut self) {
        self.receiver.close();
        // The gate is already closed. Outstanding reservations can no longer
        // submit anything, so we need only answer messages currently queued.
        while let Ok(command) = self.receiver.try_recv() {
            command.refuse_unstarted();
        }
    }
}

pub(super) fn command_queue(
    capacity: usize,
    exit: StopOnExit,
) -> (CustodyTaskHandle, CustodyCommandReceiver) {
    let (commands, receiver) = mpsc::channel(capacity);
    (
        CustodyTaskHandle {
            commands,
            stop: exit.stop.clone(),
        },
        CustodyCommandReceiver {
            _exit: exit,
            receiver,
        },
    )
}

impl CustodyTaskHandle {
    pub async fn reserve(&self) -> Result<CustodyAdmission, CustodyQueueClosed> {
        let mut stopped = self.stop.0.subscribe();
        let permit = tokio::select! {
            biased;
            _ = stopped.wait_for(|state| state.stopped) => return Err(CustodyQueueClosed::Closed),
            permit = self.commands.clone().reserve_owned() => {
                permit.map_err(|_| CustodyQueueClosed::Closed)?
            }
        };
        if self.stop.0.borrow().stopped {
            return Err(CustodyQueueClosed::Closed);
        }
        Ok(CustodyAdmission {
            permit,
            stop: self.stop.clone(),
        })
    }

    pub async fn submit(&self, caller: Caller, payload: SubmissionPayload) -> Submission {
        let prepared = match QueuedSubmission::new(payload) {
            Ok(prepared) => prepared,
            Err(refusal) => return Submission::Refused(refusal),
        };
        match self.reserve().await {
            Ok(admission) => admission.submit_prepared(caller, prepared).resolve().await,
            Err(CustodyQueueClosed::Closed) => prepared.unavailable.reply(),
        }
    }

    pub async fn request_status(
        &self,
        caller: Caller,
        query: RequestStatusQuery,
    ) -> CustodyRequestStatus {
        match self.reserve().await {
            Ok(admission) => admission.request_status(caller, query).resolve().await,
            Err(CustodyQueueClosed::Closed) => CustodyRequestStatus::StatusUnavailable,
        }
    }

    pub async fn message_status(
        &self,
        caller: Caller,
        message_id: MessageId,
    ) -> Result<Option<MessageStatus>, MessageStatusError> {
        let submission = self.submit(caller, message_query(message_id)?).await;
        message_status_reply(submission)
    }

    pub async fn pending_writes(
        &self,
        caller: Caller,
    ) -> Result<Vec<PendingWrite>, CustodyLinkError> {
        match self.reserve().await {
            Ok(admission) => admission.pending_writes(caller).resolve().await,
            Err(CustodyQueueClosed::Closed) => Err(CustodyLinkError::TaskClosed),
        }
    }

    pub async fn waiting_for_session(
        &self,
        session: SessionRef,
    ) -> Result<Vec<MessageRecord>, CustodyLinkError> {
        match self.reserve().await {
            Ok(admission) => admission.waiting_for_session(session).resolve().await,
            Err(CustodyQueueClosed::Closed) => Err(CustodyLinkError::TaskClosed),
        }
    }
}

struct QueuedSubmission {
    payload: SubmissionPayload,
    lost_reply: Submission,
    unavailable: SubmissionUnavailable,
}

#[derive(Clone, Copy)]
pub(super) enum SubmissionUnavailable {
    Central,
    Outbox,
}

impl SubmissionUnavailable {
    fn reply(self) -> Submission {
        Submission::Refused(match self {
            Self::Central => SubmitRefusal::CentralUnavailable,
            Self::Outbox => SubmitRefusal::OutboxUnavailable,
        })
    }
}

impl QueuedSubmission {
    fn new(payload: SubmissionPayload) -> Result<Self, SubmitRefusal> {
        let (lost_reply, unavailable) = match choose_submission_route(payload.clone())? {
            SubmissionRoute::Outbox { request, payload } => (
                Submission::Uncertain {
                    request,
                    scope: wire_scope(payload.request_scope()),
                },
                SubmissionUnavailable::Outbox,
            ),
            SubmissionRoute::Immediate(wire) => (
                wire.write_request_id()
                    .map_or(Submission::ReadAnswerLost, |request| {
                        Submission::Uncertain {
                            request: request.clone(),
                            scope: wire.request_scope(),
                        }
                    }),
                SubmissionUnavailable::Central,
            ),
        };
        Ok(Self {
            payload,
            lost_reply,
            unavailable,
        })
    }
}

impl CustodyAdmission {
    pub fn submit(
        self,
        caller: Caller,
        payload: SubmissionPayload,
    ) -> PendingCustodyReply<Submission> {
        match QueuedSubmission::new(payload) {
            Ok(prepared) => self.submit_prepared(caller, prepared),
            Err(refusal) => PendingCustodyReply(CustodyReply::Ready(Submission::Refused(refusal))),
        }
    }

    fn submit_prepared(
        self,
        caller: Caller,
        prepared: QueuedSubmission,
    ) -> PendingCustodyReply<Submission> {
        let unavailable = prepared.unavailable;
        let closed = unavailable.reply();
        self.commit(
            move |reply| CustodyCommand::Submit {
                caller,
                payload: prepared.payload,
                unavailable,
                reply,
            },
            prepared.lost_reply,
            closed,
        )
    }

    pub fn request_status(
        self,
        caller: Caller,
        query: RequestStatusQuery,
    ) -> PendingCustodyReply<CustodyRequestStatus> {
        self.commit(
            move |reply| CustodyCommand::Status {
                caller,
                query,
                reply,
            },
            CustodyRequestStatus::StatusUnavailable,
            CustodyRequestStatus::StatusUnavailable,
        )
    }

    pub fn pending_writes(
        self,
        caller: Caller,
    ) -> PendingCustodyReply<Result<Vec<PendingWrite>, CustodyLinkError>> {
        self.commit(
            move |reply| CustodyCommand::Pending { caller, reply },
            Err(CustodyLinkError::TaskClosed),
            Err(CustodyLinkError::TaskClosed),
        )
    }

    pub fn waiting_for_session(
        self,
        session: SessionRef,
    ) -> PendingCustodyReply<Result<Vec<MessageRecord>, CustodyLinkError>> {
        self.commit(
            move |reply| CustodyCommand::Waiting { session, reply },
            Err(CustodyLinkError::TaskClosed),
            Err(CustodyLinkError::TaskClosed),
        )
    }

    fn commit<T>(
        self,
        command: impl FnOnce(oneshot::Sender<T>) -> CustodyCommand,
        lost_reply: T,
        unavailable: T,
    ) -> PendingCustodyReply<T> {
        let (sender, reply) = oneshot::channel();
        let command = command(sender);
        // The read guard spans the synchronous gate check and permit send,
        // with no await. Stop takes the same gate for writing.
        {
            let state = self.stop.0.borrow();
            if state.stopped {
                return PendingCustodyReply(CustodyReply::Ready(unavailable));
            }
            self.permit.send(command);
        }
        PendingCustodyReply(CustodyReply::Admitted { reply, lost_reply })
    }
}
