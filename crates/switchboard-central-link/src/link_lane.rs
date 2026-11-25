//! Bounded, connection-fenced handoff between link users and the socket owner.

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
};

use central_link_client::{
    AdmittedConnection, CheckedBatchResults, ConnectionAttempt, CorrelatedReceiptResults,
    CorrelatedRequestReply, CorrelatedStatusReply, ImmediateRequestCommand, LINK_LANE_FRAME_BYTES,
    LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane, RegisteredConnection, RequestStatusCommand,
    SubmitReceiptsCommand, UntouchedCommand, WRITE_BATCH_ENVELOPE_BYTES, WriteBatchCommand,
    WriteBatchDisposition, WriteBatchMemberDisposition, encoded_request_size,
};
use central_link_protocol::{DataFrame, LinkRequestBatch};
use router_clock::ClockInstant;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot, watch};

use crate::admitted_lane_policy::{ExpectedReply, LaneAnswer, LaneResult};

const OPERATION_CAPACITY: usize = 16;
const EVENT_CAPACITY: usize = 64;

type LaneFuture<'handle, Answer> = Pin<Box<dyn Future<Output = Answer> + Send + 'handle>>;

/// The user-side handle created by `link_lane_channel` for one driver lifetime.
#[derive(Clone)]
pub struct LinkLaneHandle {
    commands: mpsc::Sender<LaneRequest>,
    intake: Arc<CommandIntake>,
    events: Arc<AsyncMutex<mpsc::Receiver<LinkLaneEvent>>>,
    admitted: watch::Receiver<Option<AdmittedConnection>>,
    registered: watch::Receiver<Option<RegisteredConnection>>,
}

/// The socket-side endpoint created by `link_lane_channel` for one running driver.
pub struct LinkLaneOwner {
    pub(super) commands: mpsc::Receiver<LaneRequest>,
    pub(super) intake: Arc<CommandIntake>,
    pub(super) events: mpsc::Sender<LinkLaneEvent>,
    pub(super) admitted: watch::Sender<Option<AdmittedConnection>>,
    pub(super) registered: watch::Sender<Option<RegisteredConnection>>,
}

/// Create the paired user handle and socket endpoint for one driver lifetime.
pub fn link_lane_channel() -> (LinkLaneHandle, LinkLaneOwner) {
    let (commands_out, commands) = mpsc::channel(OPERATION_CAPACITY);
    let (events, events_in) = mpsc::channel(EVENT_CAPACITY);
    let (admitted, watch_admitted) = watch::channel(None);
    let (registered, watch_registered) = watch::channel(None);
    let intake = Arc::new(CommandIntake::default());
    (
        LinkLaneHandle {
            commands: commands_out,
            intake: intake.clone(),
            events: Arc::new(AsyncMutex::new(events_in)),
            admitted: watch_admitted,
            registered: watch_registered,
        },
        LinkLaneOwner {
            commands,
            intake,
            events,
            admitted,
            registered,
        },
    )
}

#[derive(Default)]
pub(super) struct CommandIntake(Mutex<Option<OpenIntake>>);

struct OpenIntake {
    connection: ConnectionAttempt,
    accepted: usize,
}

impl CommandIntake {
    pub(super) fn open(&self, connection: ConnectionAttempt) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(OpenIntake {
            connection,
            accepted: 0,
        });
    }

    pub(super) fn close(&self) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    pub(super) fn settle(&self, connection: &ConnectionAttempt) {
        if let Some(open) = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            && &open.connection == connection
        {
            open.accepted = open.accepted.saturating_sub(1);
        }
    }

    fn enqueue(
        &self,
        sender: &mpsc::Sender<LaneRequest>,
        request: LaneRequest,
        immediate: bool,
    ) -> Result<(), UntouchedCommand> {
        let mut intake = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(open) = intake
            .as_mut()
            .filter(|open| open.connection == request.connection)
        else {
            return Err(UntouchedCommand::ConnectionUnavailable);
        };
        if immediate && open.accepted != 0 {
            return Err(UntouchedCommand::WriterBusy);
        }
        sender.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => UntouchedCommand::WriterBusy,
            mpsc::error::TrySendError::Closed(_) => UntouchedCommand::ConnectionUnavailable,
        })?;
        open.accepted += 1;
        Ok(())
    }
}

pub(super) struct LaneRequest {
    pub(super) connection: ConnectionAttempt,
    pub(super) send_before: ClockInstant,
    pub(super) wire: String,
    pub(super) expected: ExpectedReply,
    pub(super) settled: oneshot::Sender<LaneResult>,
}

impl LinkLaneOwner {
    pub(super) fn reject_queued(&mut self) {
        while let Ok(request) = self.commands.try_recv() {
            let _ = request.settled.send(LaneResult::Untouched(
                UntouchedCommand::ConnectionUnavailable,
            ));
        }
    }
}

impl Drop for LinkLaneOwner {
    fn drop(&mut self) {
        self.intake.close();
        self.commands.close();
        self.reject_queued();
    }
}

struct FrameBuffer(Vec<u8>);

impl io::Write for FrameBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > LINK_LANE_FRAME_BYTES - self.0.len() {
            return Err(io::Error::other("link frame budget exhausted"));
        }
        let needed = self.0.len() + bytes.len();
        if needed > self.0.capacity() {
            let capacity = needed
                .max(self.0.capacity().saturating_mul(2))
                .min(LINK_LANE_FRAME_BYTES);
            self.0.reserve_exact(capacity - self.0.len());
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_user_frame(frame: &DataFrame) -> Result<String, UntouchedCommand> {
    let mut buffer = FrameBuffer(Vec::new());
    serde_json::to_writer(&mut buffer, frame).map_err(|error| {
        if error.is_io() {
            UntouchedCommand::FrameTooLarge
        } else {
            UntouchedCommand::InvalidFrame
        }
    })?;
    String::from_utf8(buffer.0).map_err(|_| UntouchedCommand::InvalidFrame)
}

fn check_batch_budget(requests: &LinkRequestBatch) -> Result<(), UntouchedCommand> {
    let mut remaining = LINK_LANE_FRAME_BYTES - WRITE_BATCH_ENVELOPE_BYTES;
    for (index, request) in requests.requests().iter().enumerate() {
        let length = encoded_request_size(request).map_err(|_| UntouchedCommand::InvalidFrame)?;
        // The writer uses the same serde JSON encoding. Reject before the
        // batch-wide serialization can clone or allocate an oversized frame.
        remaining = remaining
            .checked_sub(length)
            .and_then(|left| left.checked_sub(usize::from(index != 0)))
            .ok_or(UntouchedCommand::FrameTooLarge)?;
    }
    Ok(())
}

impl LinkLaneHandle {
    fn is_admitted(&self, connection: &ConnectionAttempt) -> bool {
        self.admitted
            .borrow()
            .as_ref()
            .is_some_and(|admitted| &admitted.connection == connection)
    }

    async fn handoff(
        &self,
        connection: ConnectionAttempt,
        send_before: ClockInstant,
        frame: DataFrame,
        expected: ExpectedReply,
        immediate: bool,
    ) -> LaneResult {
        if !self.is_admitted(&connection) {
            return LaneResult::Untouched(UntouchedCommand::ConnectionUnavailable);
        }
        let wire = match encode_user_frame(&frame) {
            Ok(wire) => wire,
            Err(cause) => return LaneResult::Untouched(cause),
        };
        let (settled, result) = oneshot::channel();
        let request = LaneRequest {
            connection,
            send_before,
            wire,
            expected,
            settled,
        };
        if let Err(cause) = self.intake.enqueue(&self.commands, request, immediate) {
            return LaneResult::Untouched(cause);
        }
        result.await.unwrap_or(LaneResult::SingleLost)
    }
}

fn single_disposition<Reply>(
    result: LaneResult,
    project: impl FnOnce(LaneAnswer) -> Option<Reply>,
) -> LinkLaneDisposition<Reply> {
    match result {
        LaneResult::Untouched(cause) => LinkLaneDisposition::NotReached { cause },
        LaneResult::SingleAnswered {
            answer,
            accepted_at,
        } => match project(answer) {
            Some(reply) => LinkLaneDisposition::Answered { reply, accepted_at },
            None => LinkLaneDisposition::AnswerLost,
        },
        LaneResult::Batch(_) | LaneResult::SingleLost => LinkLaneDisposition::AnswerLost,
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
    ) -> LaneFuture<'_, WriteBatchDisposition> {
        Box::pin(async move {
            if let Err(cause) = check_batch_budget(&command.requests) {
                return WriteBatchDisposition::NotReached { cause };
            }
            let uncertain = CheckedBatchResults::new(
                &command.requests,
                command.requests.requests().iter().map(|request| {
                    WriteBatchMemberDisposition::AnswerLost {
                        correlation_id: request.correlation_id().clone(),
                    }
                }),
            );
            let Ok(uncertain) = uncertain else {
                return WriteBatchDisposition::NotReached {
                    cause: UntouchedCommand::InvalidFrame,
                };
            };
            let expected = ExpectedReply::batch(&command.requests);
            let result = self
                .handoff(
                    command.connection,
                    command.send_before,
                    DataFrame::Batch {
                        requests: command.requests,
                    },
                    expected,
                    false,
                )
                .await;
            match result {
                LaneResult::Untouched(cause) => WriteBatchDisposition::NotReached { cause },
                LaneResult::Batch(results) => WriteBatchDisposition::MemberResults(results),
                LaneResult::SingleAnswered { .. } | LaneResult::SingleLost => {
                    WriteBatchDisposition::MemberResults(uncertain)
                }
            }
        })
    }

    fn submit_receipts(
        &self,
        command: SubmitReceiptsCommand,
    ) -> LaneFuture<'_, LinkLaneDisposition<CorrelatedReceiptResults>> {
        Box::pin(async move {
            let expected = ExpectedReply::ReceiptResults {
                correlation_id: command.correlation_id.clone(),
                length: command.receipts.receipts().len(),
            };
            let frame = DataFrame::Receipts {
                correlation_id: command.correlation_id,
                receipts: command.receipts,
            };
            single_disposition(
                self.handoff(
                    command.connection,
                    command.send_before,
                    frame,
                    expected,
                    false,
                )
                .await,
                |answer| {
                    if let LaneAnswer::ReceiptResults(reply) = answer {
                        Some(reply)
                    } else {
                        None
                    }
                },
            )
        })
    }

    fn request_status(
        &self,
        command: RequestStatusCommand,
    ) -> LaneFuture<'_, LinkLaneDisposition<CorrelatedStatusReply>> {
        Box::pin(async move {
            let expected = ExpectedReply::RequestStatus(command.correlation_id.clone());
            let frame = DataFrame::RequestStatus {
                correlation_id: command.correlation_id,
                caller: command.caller,
                query: command.query,
            };
            single_disposition(
                self.handoff(
                    command.connection,
                    command.send_before,
                    frame,
                    expected,
                    false,
                )
                .await,
                |answer| {
                    if let LaneAnswer::RequestStatus(reply) = answer {
                        Some(reply)
                    } else {
                        None
                    }
                },
            )
        })
    }

    fn submit_immediate_request(
        &self,
        command: ImmediateRequestCommand,
    ) -> LaneFuture<'_, LinkLaneDisposition<CorrelatedRequestReply>> {
        Box::pin(async move {
            let expected = ExpectedReply::immediate(&command.request);
            let frame = DataFrame::Request {
                request: command.request,
            };
            single_disposition(
                self.handoff(
                    command.connection,
                    command.send_before,
                    frame,
                    expected,
                    true,
                )
                .await,
                |answer| {
                    if let LaneAnswer::ImmediateRequest(reply) = answer {
                        Some(reply)
                    } else {
                        None
                    }
                },
            )
        })
    }

    fn next_event(&self) -> LaneFuture<'_, Option<LinkLaneEvent>> {
        Box::pin(async { self.events.lock().await.recv().await })
    }
}

#[cfg(test)]
#[path = "link_lane_test.rs"]
mod tests;
