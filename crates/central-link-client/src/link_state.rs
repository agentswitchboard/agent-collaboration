use crate::ClientError;
use crate::{
    FailedCall, SendFailure,
    pending_calls::{PendingCalls, ReplyDisposition},
};
use central_link_protocol::{
    CentralCallReply, CorrelationId, DataFrame, DeliveryMessage, LinkFrame, LinkRequest, MacCall,
    MachineLinkCloseCode, ReceiptBatch, ReceiptResult, ReplyPayload, RequestPayload, TimeLeft,
    TokenRenewalResult, TypedCallResult,
};
use router_clock::{ClockInstant, RuntimeClock};
use std::{sync::Arc, time::Duration};
use workspace_ids::MachineId;
use workspace_model::{Caller, ResolvedSender, Timestamp};

const PING_INTERVAL: Duration = Duration::from_secs(20);
const CALL_DEADLINE: Duration = Duration::from_secs(30);
const WELCOME_DEADLINE: Duration = Duration::from_secs(10);
const INITIAL_RETRY: Duration = Duration::from_secs(1);
const MAX_RETRY: Duration = Duration::from_secs(30);

/// Why the socket owner permanently retires an enrollment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnrollmentEnd {
    Revoked,
    Inactive,
    LeaseRefused,
    ConnectionEnrollmentEnded,
}

/// A local configuration or credential fault classified by the socket owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkFault {
    MalformedAddress,
    FreshCredentialRefused,
    LeaseSource,
}

/// The socket owner's disposition of the current connection attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptOutcome {
    Retry,
    EnrollmentEnded { cause: EnrollmentEnd },
    Fault { kind: LinkFault },
}

/// The client's transport progress and readiness, driven by the socket owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkStatus {
    Connecting,
    Welcomed { machine_id: MachineId },
    Connected { machine_id: MachineId },
    EnrollmentEnded { cause: EnrollmentEnd },
    Fault { kind: LinkFault },
}

/// An exact connection attempt minted by the client and carried by its socket owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionAttempt(CorrelationId);

enum ConnectionState {
    Waiting {
        reconnect_at: ClockInstant,
        last_attempt: Option<ConnectionAttempt>,
    },
    PreparingDial {
        attempt: ConnectionAttempt,
    },
    AwaitingWelcome {
        attempt: ConnectionAttempt,
        expires_at: ClockInstant,
    },
    Welcomed {
        readiness: Readiness,
        attempt: ConnectionAttempt,
        heartbeat: Heartbeat,
    },
    Parked {
        attempt: ConnectionAttempt,
        reason: ParkReason,
    },
}
enum Readiness {
    WaitingForRegistration,
    Ready,
}
enum ParkReason {
    EnrollmentEnded(EnrollmentEnd),
    Fault(LinkFault),
}
enum Heartbeat {
    IdleUntil(ClockInstant),
    FirstWritePending,
    FirstResponseDue(ClockInstant),
    SecondWritePending,
    SecondResponseDue(ClockInstant),
}

impl Heartbeat {
    fn deadline(&self) -> Option<ClockInstant> {
        match self {
            Self::IdleUntil(at) | Self::FirstResponseDue(at) | Self::SecondResponseDue(at) => {
                Some(*at)
            }
            Self::FirstWritePending | Self::SecondWritePending => None,
        }
    }
}

/// Work the client asks its socket owner to perform after an elapsed timer check.
#[derive(Debug, Eq, PartialEq)]
pub enum ClientAction {
    Connect { attempt: ConnectionAttempt },
    SendPing,
    Reconnect { failed: Vec<FailedCall> },
    CallsExpired { failed: Vec<FailedCall> },
}

/// A checked server frame produced by the client for its socket owner.
#[derive(Debug, Eq, PartialEq)]
pub enum ClientEvent {
    Welcomed {
        central_time: Timestamp,
        effective_expires_at: Timestamp,
    },
    Pong,
    Reply {
        correlation_id: CorrelationId,
        result: ReplyPayload,
    },
    LateReply {
        correlation_id: CorrelationId,
    },
    /// A message central delivered to this Router (2c); repeats are safe.
    Deliver {
        message: Box<DeliveryMessage>,
        sender: ResolvedSender,
        time_left: Option<TimeLeft>,
    },
    ReceiptResults {
        correlation_id: CorrelationId,
        results: Vec<ReceiptResult>,
    },
    /// Central's answer to a RenewMachineToken the Host sent.
    TokenRenewal {
        correlation_id: CorrelationId,
        result: TokenRenewalResult,
    },
    /// The Host owns typed-call dispatch and correlation on this connection.
    CallMac {
        correlation_id: CorrelationId,
        request: Box<MacCall>,
    },
    /// The Host must accept a reply only while its typed call is pending.
    CentralCallReply {
        correlation_id: CorrelationId,
        result: Box<TypedCallResult<CentralCallReply>>,
    },
}

struct PendingReceiptFrame {
    correlation_id: CorrelationId,
    count: usize,
    expires_at: ClockInstant,
}

/// Connection-attempt policy created and driven by the socket owner.
pub struct LinkClient {
    clock: Arc<dyn RuntimeClock>,
    machine: MachineId,
    state: ConnectionState,
    retry_delay: Duration,
    pending: PendingCalls,
    id_context: uuid::ContextV7,
    pending_receipts: Option<PendingReceiptFrame>,
}

impl LinkClient {
    pub fn new(machine: MachineId, clock: Arc<dyn RuntimeClock>) -> Self {
        Self {
            state: ConnectionState::Waiting {
                reconnect_at: clock.monotonic_now(),
                last_attempt: None,
            },
            clock,
            machine,
            retry_delay: INITIAL_RETRY,
            pending: PendingCalls::default(),
            id_context: uuid::ContextV7::new(),
            pending_receipts: None,
        }
    }

    pub fn status(&self) -> LinkStatus {
        match &self.state {
            ConnectionState::Waiting { .. }
            | ConnectionState::PreparingDial { .. }
            | ConnectionState::AwaitingWelcome { .. } => LinkStatus::Connecting,
            ConnectionState::Welcomed {
                readiness: Readiness::WaitingForRegistration,
                ..
            } => LinkStatus::Welcomed {
                machine_id: self.machine.clone(),
            },
            ConnectionState::Welcomed {
                readiness: Readiness::Ready,
                ..
            } => LinkStatus::Connected {
                machine_id: self.machine.clone(),
            },
            ConnectionState::Parked { reason, .. } => match reason {
                ParkReason::EnrollmentEnded(cause) => LinkStatus::EnrollmentEnded { cause: *cause },
                ParkReason::Fault(kind) => LinkStatus::Fault { kind: *kind },
            },
        }
    }

    /// Arm the Welcome deadline when the owner starts the dial with a usable lease.
    pub fn dial_started(&mut self, attempt: &ConnectionAttempt) -> Result<(), ClientError> {
        self.check_attempt(attempt)?;
        if !matches!(self.state, ConnectionState::PreparingDial { .. }) {
            return Err(ClientError::UnexpectedFrame);
        }
        self.state = ConnectionState::AwaitingWelcome {
            attempt: attempt.clone(),
            expires_at: self.clock.monotonic_now().after(WELCOME_DEADLINE),
        };
        Ok(())
    }

    /// The owner has observed the current connection's registration acknowledgement.
    pub fn ready(&mut self, attempt: &ConnectionAttempt) -> Result<(), ClientError> {
        self.check_attempt(attempt)?;
        let ConnectionState::Welcomed { readiness, .. } = &mut self.state else {
            return Err(ClientError::UnexpectedFrame);
        };
        *readiness = Readiness::Ready;
        self.retry_delay = INITIAL_RETRY;
        Ok(())
    }

    fn is_ready(&self) -> bool {
        matches!(
            self.state,
            ConnectionState::Welcomed {
                readiness: Readiness::Ready,
                ..
            }
        )
    }

    /// The Host drives this deadline through the same clock; no task is spawned here.
    pub fn next_deadline(&self) -> Option<ClockInstant> {
        let connection = match self.state {
            ConnectionState::Waiting { reconnect_at, .. } => Some(reconnect_at),
            ConnectionState::AwaitingWelcome { expires_at, .. } => Some(expires_at),
            ConnectionState::Welcomed { ref heartbeat, .. } => heartbeat.deadline(),
            ConnectionState::PreparingDial { .. } | ConnectionState::Parked { .. } => None,
        };
        connection
            .into_iter()
            .chain(self.pending.next_deadline())
            .chain(
                self.pending_receipts
                    .as_ref()
                    .map(|receipts| receipts.expires_at),
            )
            .min()
    }

    /// Drain due actions before processing socket events; call again until None.
    pub fn poll_time(&mut self) -> Option<ClientAction> {
        let now = self.clock.monotonic_now();
        if self
            .pending_receipts
            .as_ref()
            .is_some_and(|receipts| now >= receipts.expires_at)
        {
            let failed = self.disconnect();
            return Some(ClientAction::Reconnect { failed });
        }
        let failed = self.pending.expire(now);
        if !failed.is_empty() {
            return Some(ClientAction::CallsExpired { failed });
        }
        match &mut self.state {
            ConnectionState::Waiting { reconnect_at, .. } if now >= *reconnect_at => {
                let attempt = ConnectionAttempt(self.next_correlation());
                self.state = ConnectionState::PreparingDial {
                    attempt: attempt.clone(),
                };
                Some(ClientAction::Connect { attempt })
            }
            ConnectionState::AwaitingWelcome { expires_at, .. } if now >= *expires_at => {
                let failed = self.disconnect();
                Some(ClientAction::Reconnect { failed })
            }
            ConnectionState::Welcomed { heartbeat, .. }
                if heartbeat.deadline().is_some_and(|deadline| now >= deadline) =>
            {
                match heartbeat {
                    Heartbeat::SecondResponseDue(_) => {
                        let failed = self.disconnect();
                        Some(ClientAction::Reconnect { failed })
                    }
                    Heartbeat::IdleUntil(_) => {
                        *heartbeat = Heartbeat::FirstWritePending;
                        Some(ClientAction::SendPing)
                    }
                    Heartbeat::FirstResponseDue(_) => {
                        *heartbeat = Heartbeat::SecondWritePending;
                        Some(ClientAction::SendPing)
                    }
                    Heartbeat::FirstWritePending | Heartbeat::SecondWritePending => None,
                }
            }
            ConnectionState::Waiting { .. }
            | ConnectionState::PreparingDial { .. }
            | ConnectionState::Welcomed { .. }
            | ConnectionState::AwaitingWelcome { .. }
            | ConnectionState::Parked { .. } => None,
        }
    }

    /// Confirm the current pending heartbeat after the socket owner finishes its write.
    pub fn ping_written(
        &mut self,
        attempt: &ConnectionAttempt,
        completed_at: ClockInstant,
    ) -> Result<(), ClientError> {
        self.check_attempt(attempt)?;
        let ConnectionState::Welcomed { heartbeat, .. } = &mut self.state else {
            return Err(ClientError::UnexpectedFrame);
        };
        let deadline = completed_at.after(PING_INTERVAL);
        *heartbeat = match heartbeat {
            Heartbeat::FirstWritePending => Heartbeat::FirstResponseDue(deadline),
            Heartbeat::SecondWritePending => Heartbeat::SecondResponseDue(deadline),
            Heartbeat::IdleUntil(_)
            | Heartbeat::FirstResponseDue(_)
            | Heartbeat::SecondResponseDue(_) => return Err(ClientError::UnexpectedFrame),
        };
        Ok(())
    }

    /// Preserve the probe's remaining window while the owner pauses socket reads for backpressure.
    pub fn defer_heartbeat(&mut self, attempt: &ConnectionAttempt, paused_for: Duration) {
        let ConnectionState::Welcomed {
            attempt: current,
            heartbeat,
            ..
        } = &mut self.state
        else {
            return;
        };
        if current != attempt {
            return;
        }
        match heartbeat {
            Heartbeat::IdleUntil(at)
            | Heartbeat::FirstResponseDue(at)
            | Heartbeat::SecondResponseDue(at) => *at += paused_for,
            Heartbeat::FirstWritePending | Heartbeat::SecondWritePending => {}
        }
    }

    pub fn begin_request(
        &mut self,
        caller: Caller,
        payload: RequestPayload,
    ) -> Result<LinkFrame, ClientError> {
        if !self.is_ready() {
            return Err(ClientError::CentralUnavailable);
        }
        let request = LinkRequest::new(self.next_correlation(), caller, payload)
            .map_err(|_| ClientError::InvalidRequest)?;
        self.pending.insert(
            request.clone(),
            self.clock.monotonic_now().after(CALL_DEADLINE),
        )?;
        Ok(LinkFrame::Data(DataFrame::Request {
            request: Box::new(request),
        }))
    }

    /// One receipt frame is outstanding per connection. The Router owns its durable items.
    pub fn begin_receipts(&mut self, receipts: ReceiptBatch) -> Result<LinkFrame, ClientError> {
        if !self.is_ready() {
            return Err(ClientError::CentralUnavailable);
        }
        if self.pending_receipts.is_some() {
            return Err(ClientError::ReceiptFramePending);
        }
        let correlation_id = self.next_correlation();
        self.pending_receipts = Some(PendingReceiptFrame {
            correlation_id: correlation_id.clone(),
            count: receipts.receipts().len(),
            expires_at: self.clock.monotonic_now().after(CALL_DEADLINE),
        });
        Ok(LinkFrame::Data(DataFrame::Receipts {
            correlation_id,
            receipts,
        }))
    }

    /// Only DefinitelyNotSent guarantees no request bytes were sent.
    pub fn send_failed(
        &mut self,
        attempt: &ConnectionAttempt,
        correlation: &CorrelationId,
        receipt: SendFailure,
    ) -> Result<FailedCall, ClientError> {
        self.check_attempt(attempt)?;
        self.pending.send_failed(correlation, receipt)
    }

    /// Protocol errors require close; a typed LateReply event does not.
    pub fn receive(
        &mut self,
        attempt: &ConnectionAttempt,
        frame: LinkFrame,
    ) -> Result<ClientEvent, ClientError> {
        self.check_attempt(attempt)?;
        if let LinkFrame::Data(DataFrame::Welcome {
            machine_id,
            central_time,
            effective_expires_at,
        }) = &frame
        {
            if !matches!(self.state, ConnectionState::AwaitingWelcome { expires_at, .. } if self.clock.monotonic_now() < expires_at)
                || machine_id != &self.machine
            {
                return Err(ClientError::InvalidWelcome);
            }
            self.state = ConnectionState::Welcomed {
                readiness: Readiness::WaitingForRegistration,
                attempt: attempt.clone(),
                heartbeat: Heartbeat::IdleUntil(self.clock.monotonic_now().after(PING_INTERVAL)),
            };
            return Ok(ClientEvent::Welcomed {
                central_time: *central_time,
                effective_expires_at: *effective_expires_at,
            });
        }
        if !matches!(self.state, ConnectionState::Welcomed { .. }) {
            return Err(ClientError::UnexpectedFrame);
        }
        match frame {
            LinkFrame::Pong => {
                let ConnectionState::Welcomed { heartbeat, .. } = &mut self.state else {
                    return Err(ClientError::UnexpectedFrame);
                };
                match heartbeat {
                    Heartbeat::FirstResponseDue(at) | Heartbeat::SecondResponseDue(at) => {
                        *heartbeat = Heartbeat::IdleUntil(*at);
                    }
                    Heartbeat::SecondWritePending => *heartbeat = Heartbeat::FirstWritePending,
                    Heartbeat::IdleUntil(_) | Heartbeat::FirstWritePending => {}
                }
                Ok(ClientEvent::Pong)
            }
            LinkFrame::Data(DataFrame::Reply {
                correlation_id,
                result,
            }) => match self.pending.complete(&correlation_id, &result)? {
                ReplyDisposition::Completed => Ok(ClientEvent::Reply {
                    correlation_id,
                    result: *result,
                }),
                ReplyDisposition::AlreadyResolved => Ok(ClientEvent::LateReply { correlation_id }),
            },
            LinkFrame::Data(DataFrame::Deliver {
                message,
                sender,
                time_left,
            }) => Ok(ClientEvent::Deliver {
                message,
                sender,
                time_left,
            }),
            LinkFrame::Data(DataFrame::ReceiptResults {
                correlation_id,
                results,
            }) => {
                if !self.pending_receipts.as_ref().is_some_and(|pending| {
                    pending.correlation_id == correlation_id && pending.count == results.len()
                }) {
                    return Err(ClientError::InvalidReceiptResults);
                }
                self.pending_receipts = None;
                Ok(ClientEvent::ReceiptResults {
                    correlation_id,
                    results,
                })
            }
            // The Host owns renewal correlation (it sent the frame); the client
            // only surfaces central's answer.
            LinkFrame::Data(DataFrame::MachineTokenRenewal {
                correlation_id,
                result,
            }) => Ok(ClientEvent::TokenRenewal {
                correlation_id,
                result,
            }),
            LinkFrame::Data(DataFrame::CallMac {
                correlation_id,
                request,
            }) => Ok(ClientEvent::CallMac {
                correlation_id,
                request,
            }),
            LinkFrame::Data(DataFrame::CentralCallReply {
                correlation_id,
                result,
            }) => Ok(ClientEvent::CentralCallReply {
                correlation_id,
                result,
            }),
            LinkFrame::Ping
            | LinkFrame::Data(
                DataFrame::Welcome { .. }
                | DataFrame::Request { .. }
                | DataFrame::RequestStatus { .. }
                | DataFrame::RequestStatusReply { .. }
                | DataFrame::Batch { .. }
                | DataFrame::Receipts { .. }
                | DataFrame::CallCentral { .. }
                | DataFrame::MacCallReply { .. }
                | DataFrame::MacCapabilities(_)
                | DataFrame::RenewMachineToken { .. },
            ) => Err(ClientError::UnexpectedFrame),
        }
    }

    pub fn closed(&mut self, attempt: &ConnectionAttempt) -> Result<Vec<FailedCall>, ClientError> {
        self.check_most_recent_attempt(attempt)?;
        match self.state {
            ConnectionState::PreparingDial { .. }
            | ConnectionState::AwaitingWelcome { .. }
            | ConnectionState::Welcomed { .. } => Ok(self.disconnect()),
            ConnectionState::Waiting { .. } | ConnectionState::Parked { .. } => Ok(Vec::new()),
        }
    }

    fn disconnect(&mut self) -> Vec<FailedCall> {
        let last_attempt = self.most_recent_attempt().cloned();
        self.state = ConnectionState::Waiting {
            reconnect_at: self.clock.monotonic_now().after(self.retry_delay),
            last_attempt,
        };
        self.retry_delay = self.retry_delay.saturating_mul(2).min(MAX_RETRY);
        self.pending_receipts = None;
        self.pending.end_connection()
    }

    /// Retire the exact attempt with the outcome observed by its owner.
    pub fn finish_attempt(
        &mut self,
        attempt: &ConnectionAttempt,
        outcome: AttemptOutcome,
    ) -> Result<Vec<FailedCall>, ClientError> {
        match outcome {
            AttemptOutcome::Retry => self.closed(attempt),
            AttemptOutcome::EnrollmentEnded { cause } => {
                self.park(attempt, ParkReason::EnrollmentEnded(cause))
            }
            AttemptOutcome::Fault { kind } => self.park(attempt, ParkReason::Fault(kind)),
        }
    }

    /// Consume Central's decoded close code without interpreting its reason text.
    pub fn close_received(
        &mut self,
        attempt: &ConnectionAttempt,
        code: Option<MachineLinkCloseCode>,
    ) -> Result<Vec<FailedCall>, ClientError> {
        let outcome = match code {
            Some(MachineLinkCloseCode::Revoked) => AttemptOutcome::EnrollmentEnded {
                cause: EnrollmentEnd::Revoked,
            },
            Some(MachineLinkCloseCode::Inactive) => AttemptOutcome::EnrollmentEnded {
                cause: EnrollmentEnd::Inactive,
            },
            Some(MachineLinkCloseCode::Expired) | None => AttemptOutcome::Retry,
        };
        self.finish_attempt(attempt, outcome)
    }

    fn park(
        &mut self,
        attempt: &ConnectionAttempt,
        reason: ParkReason,
    ) -> Result<Vec<FailedCall>, ClientError> {
        self.check_most_recent_attempt(attempt)?;
        if let ConnectionState::Parked {
            reason: ParkReason::EnrollmentEnded(ended),
            ..
        } = self.state
        {
            return match reason {
                ParkReason::EnrollmentEnded(cause) if cause == ended => Ok(Vec::new()),
                _ => Err(ClientError::StaleConnection),
            };
        }
        self.state = ConnectionState::Parked {
            attempt: attempt.clone(),
            reason,
        };
        self.pending_receipts = None;
        Ok(self.pending.end_connection())
    }

    fn check_attempt(&self, attempt: &ConnectionAttempt) -> Result<(), ClientError> {
        match &self.state {
            ConnectionState::PreparingDial { attempt: current }
            | ConnectionState::AwaitingWelcome {
                attempt: current, ..
            }
            | ConnectionState::Welcomed {
                attempt: current, ..
            } if current == attempt => Ok(()),
            ConnectionState::Waiting { .. }
            | ConnectionState::PreparingDial { .. }
            | ConnectionState::AwaitingWelcome { .. }
            | ConnectionState::Welcomed { .. }
            | ConnectionState::Parked { .. } => Err(ClientError::StaleConnection),
        }
    }

    fn most_recent_attempt(&self) -> Option<&ConnectionAttempt> {
        match &self.state {
            ConnectionState::Waiting { last_attempt, .. } => last_attempt.as_ref(),
            ConnectionState::PreparingDial { attempt }
            | ConnectionState::AwaitingWelcome { attempt, .. }
            | ConnectionState::Welcomed { attempt, .. }
            | ConnectionState::Parked { attempt, .. } => Some(attempt),
        }
    }

    fn check_most_recent_attempt(&self, attempt: &ConnectionAttempt) -> Result<(), ClientError> {
        if self.most_recent_attempt() == Some(attempt) {
            Ok(())
        } else {
            Err(ClientError::StaleConnection)
        }
    }

    /// Allocate a correlation for one renewal the owner is about to publish on this socket.
    pub fn renewal_correlation(
        &mut self,
        attempt: &ConnectionAttempt,
    ) -> Result<CorrelationId, ClientError> {
        self.check_attempt(attempt)?;
        if !matches!(self.state, ConnectionState::Welcomed { .. }) {
            return Err(ClientError::CentralUnavailable);
        }
        Ok(self.next_correlation())
    }

    fn next_correlation(&mut self) -> CorrelationId {
        let millis = u64::try_from(self.clock.utc_now().timestamp_millis()).unwrap_or(0);
        let nanos = u32::try_from(millis % 1000)
            .unwrap_or(0)
            .saturating_mul(1_000_000);
        CorrelationId::mint(uuid::Timestamp::from_unix(
            &self.id_context,
            millis / 1000,
            nanos,
        ))
    }

    /// Rearm a fault after its owner supplies the permitted recovery input.
    /// The owner creates a new client when it observes a different enrollment binding.
    pub fn retry_after_fault(&mut self) -> bool {
        if !matches!(
            self.state,
            ConnectionState::Parked {
                reason: ParkReason::Fault(_),
                ..
            }
        ) {
            return false;
        }
        self.state = ConnectionState::Waiting {
            reconnect_at: self.clock.monotonic_now(),
            last_attempt: None,
        };
        true
    }
}

#[cfg(test)]
#[path = "link_state_test.rs"]
mod tests;
