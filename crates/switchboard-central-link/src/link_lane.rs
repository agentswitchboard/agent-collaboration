//! The bounded channel between admitted-link consumers and the socket owner.

use std::{future::Future, pin::Pin, sync::Arc};

use central_link_client::{
    AdmittedConnection, ConnectionAttempt, LinkLaneCommand, LinkLaneDisposition, LinkLaneEvent,
    ReadyLinkLane, RegisteredConnection, WriteBatchCommand, WriteBatchDisposition,
    WriteBatchMemberDisposition,
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

const OPERATION_CAPACITY: usize = 16;
const EVENT_CAPACITY: usize = 64;

/// The command and observation handle created by `link_lane_channel` for admitted-link users.
#[derive(Clone)]
pub struct LinkLaneHandle {
    commands: mpsc::Sender<LaneRequest>,
    events: Arc<Mutex<mpsc::Receiver<LinkLaneEvent>>>,
    admitted: watch::Receiver<Option<AdmittedConnection>>,
    registered: watch::Receiver<Option<RegisteredConnection>>,
}

/// The socket-side endpoint created by `link_lane_channel` and owned by one running driver.
pub struct LinkLaneOwner {
    pub(super) commands: mpsc::Receiver<LaneRequest>,
    pub(super) events: mpsc::Sender<LinkLaneEvent>,
    pub(super) admitted: watch::Sender<Option<AdmittedConnection>>,
    _registered: watch::Sender<Option<RegisteredConnection>>,
}

/// Create the paired user handle and socket endpoint for one driver lifetime.
pub fn link_lane_channel() -> (LinkLaneHandle, LinkLaneOwner) {
    let (commands_out, commands) = mpsc::channel(OPERATION_CAPACITY);
    let (events, events_in) = mpsc::channel(EVENT_CAPACITY);
    let (admitted, watch_admitted) = watch::channel(None);
    let (registered, watch_registered) = watch::channel(None);
    (
        LinkLaneHandle {
            commands: commands_out,
            events: Arc::new(Mutex::new(events_in)),
            admitted: watch_admitted,
            registered: watch_registered,
        },
        LinkLaneOwner {
            commands,
            events,
            admitted,
            _registered: registered,
        },
    )
}

pub(super) enum LaneRequest {
    WriteBatch(WriteBatchCommand, oneshot::Sender<WriteBatchDisposition>),
    SingleCommand(LinkLaneCommand, oneshot::Sender<LinkLaneDisposition>),
}

impl LaneRequest {
    pub(super) fn not_reached(self) {
        match self {
            Self::WriteBatch(_, settled) => {
                let _ = settled.send(WriteBatchDisposition::NotReached);
            }
            Self::SingleCommand(_, settled) => {
                let _ = settled.send(LinkLaneDisposition::NotReached);
            }
        }
    }
}

impl LinkLaneOwner {
    pub(super) fn reject_queued(&mut self) {
        while let Ok(request) = self.commands.try_recv() {
            request.not_reached();
        }
    }
}

impl Drop for LinkLaneOwner {
    fn drop(&mut self) {
        self.commands.close();
        self.reject_queued();
    }
}

impl LinkLaneHandle {
    fn is_admitted(&self, connection: &ConnectionAttempt) -> bool {
        self.admitted
            .borrow()
            .as_ref()
            .is_some_and(|admitted| &admitted.connection == connection)
    }
}

impl ReadyLinkLane for LinkLaneHandle {
    fn watch_admitted(&self) -> watch::Receiver<Option<AdmittedConnection>> {
        self.admitted.clone()
    }

    fn watch_registered(&self) -> watch::Receiver<Option<RegisteredConnection>> {
        self.registered.clone()
    }

    fn submit_write_batch(
        &self,
        command: WriteBatchCommand,
    ) -> Pin<Box<dyn Future<Output = WriteBatchDisposition> + Send + '_>> {
        Box::pin(async move {
            if !self.is_admitted(&command.connection) {
                return WriteBatchDisposition::NotReached;
            }
            let uncertain = command
                .requests
                .requests()
                .iter()
                .map(|request| WriteBatchMemberDisposition::AnswerLost {
                    correlation_id: request.correlation_id().clone(),
                })
                .collect();
            let (settled, result) = oneshot::channel();
            if self
                .commands
                .try_send(LaneRequest::WriteBatch(command, settled))
                .is_err()
            {
                return WriteBatchDisposition::NotReached;
            }
            result
                .await
                .unwrap_or(WriteBatchDisposition::MemberResults { members: uncertain })
        })
    }

    fn submit_command(
        &self,
        command: LinkLaneCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition> + Send + '_>> {
        Box::pin(async move {
            let connection = match &command {
                LinkLaneCommand::SubmitReceipts { connection, .. }
                | LinkLaneCommand::RequestStatus { connection, .. }
                | LinkLaneCommand::ImmediateRequest { connection, .. } => connection,
            };
            if !self.is_admitted(connection) {
                return LinkLaneDisposition::NotReached;
            }
            let (settled, result) = oneshot::channel();
            if self
                .commands
                .try_send(LaneRequest::SingleCommand(command, settled))
                .is_err()
            {
                return LinkLaneDisposition::NotReached;
            }
            result.await.unwrap_or(LinkLaneDisposition::AnswerLost)
        })
    }

    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<LinkLaneEvent>> + Send + '_>> {
        Box::pin(async { self.events.lock().await.recv().await })
    }
}
