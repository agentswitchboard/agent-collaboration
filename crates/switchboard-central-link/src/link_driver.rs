//! Socket custody and elapsed scheduling for the Mac Central link.

use std::{
    future::{Future, pending},
    sync::Arc,
    time::Duration,
};

use central_link_client::{
    AdmittedConnection, AttemptOutcome, ClientAction, ClientEvent, ConnectionAttempt, LinkClient,
    WelcomeClockAnchor,
};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, MachineLinkCloseCode, TokenRenewalResult, decode_text,
    encode_text,
};
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use router_clock::{ClockInstant, RuntimeClock};
use state_machine_core::StateMachine;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message};
use workspace_ids::MachineId;

use crate::{
    AddressError, DialAddress,
    admitted_lane_policy::SocketReply,
    admitted_lane_socket::{AdmittedSocketLane, EventPermit, FrameOffer, ReadAdmission},
    lane_writer::{FrameWrite, SendFence, write_user_frame},
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource, LinkLease},
    link_lane::{LaneRequest, LinkLaneOwner},
    link_policy::{DialFailure, LinkEffect, LinkInput, LinkPolicy, LinkState, LinkStatus},
    socket_upgrade::{MAX_CREDENTIAL_BYTES, MAX_OUTBOUND_TEXT_BYTES},
};

const WRITE_LIMIT: Duration = Duration::from_secs(5);
const MIN_LEASE_RETRY: Duration = Duration::from_secs(1);

/// The binding and validated address supplied by the enrollment and configuration owners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriverSettings<Binding> {
    pub binding: Option<Binding>,
    pub address: Result<DialAddress, AddressError>,
}

/// Owns the current socket during `run`. The caller owns the task and its shutdown sender.
pub struct LinkDriver<Binding, Source> {
    pub machine_id: MachineId,
    pub clock: Arc<dyn RuntimeClock>,
    pub lease_source: Arc<Source>,
    pub settings: watch::Receiver<DriverSettings<Binding>>,
    pub shutdown: watch::Receiver<bool>,
    pub status: watch::Sender<LinkStatus>,
    pub lane: LinkLaneOwner,
}

#[derive(Clone, Copy)]
enum Interrupted {
    SettingsChanged,
    Stopped,
}

struct Control<'a, Binding> {
    settings: &'a mut watch::Receiver<DriverSettings<Binding>>,
    shutdown: &'a mut watch::Receiver<bool>,
    observed: DriverSettings<Binding>,
}

impl<Binding: Clone + Eq> Control<'_, Binding> {
    async fn wait(&mut self) -> Interrupted {
        tokio::select! {
            biased;
            _ = self.shutdown.wait_for(|stop| *stop) => Interrupted::Stopped,
            update = self.settings.wait_for(|settings| *settings != self.observed) => match update {
                Ok(settings) => {
                    self.observed = settings.clone();
                    Interrupted::SettingsChanged
                }
                Err(_) => Interrupted::Stopped,
            },
        }
    }

    async fn during<T>(&mut self, work: impl Future<Output = T>) -> Result<T, Interrupted> {
        tokio::select! {
            biased;
            interruption = self.wait() => Err(interruption),
            result = work => Ok(result),
        }
    }
}

struct BindingDriver<'source, Binding, Source> {
    lease_source: &'source Source,
    clock: &'source dyn RuntimeClock,
    status: &'source watch::Sender<LinkStatus>,
    address: Result<DialAddress, AddressError>,
    machine_id: MachineId,
    state: LinkState<Binding>,
    client: LinkClient,
    lease_retry_at: Option<ClockInstant>,
}

enum OpenExit<Binding> {
    Resume,
    Final {
        binding: Binding,
        cause: central_link_client::EnrollmentEnd,
    },
}

enum SocketInput<Binding> {
    Timer,
    Frame(Option<Result<Message, WebSocketError>>, Option<EventPermit>),
    EventSpace,
    Lease(Result<LinkLease<Binding>, LeaseFailure>),
    Command(LaneRequest),
}

#[derive(Clone, Copy)]
enum SocketPriority {
    ReadFrame,
    AcquireLease,
    AcceptCommand,
}

enum SocketWriteFailure {
    InvalidFrame,
    SizeLimit,
    DeadlineReached,
    Transport,
}

impl<Binding, Source> LinkDriver<Binding, Source>
where
    Binding: Clone + Eq + Send + Sync,
    Source: LeaseSource<Binding>,
{
    /// Run the link until shutdown or until the configuration sender closes.
    pub async fn run(mut self) {
        let mut terminal_binding = None;
        loop {
            if *self.shutdown.borrow() {
                return;
            }
            let settings = self.settings.borrow_and_update().clone();
            let mut control = Control {
                settings: &mut self.settings,
                shutdown: &mut self.shutdown,
                observed: settings.clone(),
            };
            let Some(binding) = settings.binding else {
                if terminal_binding.is_none() {
                    self.status.send_replace(LinkStatus::NotEnrolled);
                }
                if matches!(control.wait().await, Interrupted::Stopped) {
                    return;
                }
                continue;
            };
            if terminal_binding.as_ref() == Some(&binding) {
                if matches!(control.wait().await, Interrupted::Stopped) {
                    return;
                }
                continue;
            }
            terminal_binding = None;
            let mut driver = BindingDriver {
                lease_source: self.lease_source.as_ref(),
                clock: self.clock.as_ref(),
                status: &self.status,
                address: settings.address,
                machine_id: self.machine_id.clone(),
                state: LinkState::new(self.machine_id.clone()),
                client: LinkClient::new(self.machine_id.clone(), self.clock.clone()),
                lease_retry_at: None,
            };
            match driver
                .run_binding(binding.clone(), &mut control, &mut self.lane)
                .await
            {
                (Interrupted::Stopped, _) => return,
                (Interrupted::SettingsChanged, true) => terminal_binding = Some(binding),
                (Interrupted::SettingsChanged, false) => {}
            }
        }
    }
}

impl<Binding: Clone + Eq + Send + Sync, Source: LeaseSource<Binding>>
    BindingDriver<'_, Binding, Source>
{
    fn apply(&mut self, event: LinkInput<Binding>) -> Option<Vec<LinkEffect<Binding>>> {
        let (state, effects) = LinkPolicy::transition(&self.state, event).ok()?;
        self.state = state;
        let next = self.state.status();
        self.status.send_if_modified(|current| {
            if *current == next {
                return false;
            }
            *current = next;
            true
        });
        Some(effects)
    }

    fn close(&mut self, attempt: &ConnectionAttempt, code: Option<MachineLinkCloseCode>) {
        let _ = self.client.close_received(attempt, code);
        let _ = self.apply(LinkInput::SocketClosed {
            attempt: attempt.clone(),
            code,
        });
    }

    async fn run_binding(
        &mut self,
        binding: Binding,
        control: &mut Control<'_, Binding>,
        lane_owner: &mut LinkLaneOwner,
    ) -> (Interrupted, bool) {
        let _ = self.apply(LinkInput::BindingAvailable(binding.clone()));
        loop {
            let interruption = if self.address.is_err() {
                self.status.send_replace(LinkStatus::Fault {
                    kind: central_link_client::LinkFault::MalformedAddress,
                });
                control.wait().await
            } else {
                match self.run_attempts(&binding, control, lane_owner).await {
                    Ok(OpenExit::Final { binding, cause }) => {
                        self.lease_source.end(&binding, cause);
                        return (control.wait().await, true);
                    }
                    Ok(OpenExit::Resume) => control.wait().await,
                    Err(reason) => reason,
                }
            };
            if matches!(interruption, Interrupted::Stopped)
                || control.observed.binding.as_ref() != Some(&binding)
            {
                return (interruption, false);
            }
            let next_address = control.observed.address.clone();
            if next_address != self.address
                && next_address.is_ok()
                && self.apply(LinkInput::AddressCorrected).is_some()
            {
                self.client.retry_after_fault();
            }
            self.address = next_address;
        }
    }

    async fn run_attempts(
        &mut self,
        binding: &Binding,
        control: &mut Control<'_, Binding>,
        lane_owner: &mut LinkLaneOwner,
    ) -> Result<OpenExit<Binding>, Interrupted> {
        loop {
            if let Some(cause) = self.state.ending() {
                return Ok(OpenExit::Final {
                    binding: binding.clone(),
                    cause,
                });
            }
            if let LinkStatus::Fault { .. } = self.state.status() {
                let Some(due) = self.state.next_deadline() else {
                    return Ok(OpenExit::Resume);
                };
                control.during(self.clock.sleep_until(due)).await?;
                let _ = self.apply(LinkInput::Tick {
                    now: self.clock.monotonic_now(),
                });
                self.client.retry_after_fault();
                continue;
            }
            let Some(newer_than) = self.state.dial_requirement() else {
                return Ok(OpenExit::Resume);
            };
            if let Some(deadline) = self.client.next_deadline() {
                control.during(self.clock.sleep_until(deadline)).await?;
            }
            if let Some(deadline) = self.lease_retry_at {
                control.during(self.clock.sleep_until(deadline)).await?;
                self.lease_retry_at = None;
            }
            let result = control
                .during(self.lease_source.acquire(binding, newer_than))
                .await?;
            let lease = match result {
                Ok(lease)
                    if &lease.window.binding == binding
                        && lease.window.usable_until > self.clock.monotonic_now()
                        && newer_than.is_none_or(|version| lease.window.version > version) =>
                {
                    lease
                }
                Ok(_) => {
                    self.lease_retry_at = Some(self.clock.monotonic_now().after(MIN_LEASE_RETRY));
                    continue;
                }
                Err(LeaseFailure::Unavailable { retry_after }) => {
                    self.lease_retry_at = Some(
                        self.clock
                            .monotonic_now()
                            .after(retry_after.max(MIN_LEASE_RETRY)),
                    );
                    continue;
                }
                Err(LeaseFailure::EnrollmentEnded) => {
                    let _ = self.apply(LinkInput::LeaseEnded {
                        binding: binding.clone(),
                    });
                    continue;
                }
                Err(LeaseFailure::Fault) => {
                    let _ = self.apply(LinkInput::LeaseFault {
                        binding: binding.clone(),
                    });
                    continue;
                }
            };
            let Some(ClientAction::Connect { attempt }) = self.client.poll_time() else {
                return Ok(OpenExit::Resume);
            };
            let started = self.clock.monotonic_now();
            if self
                .apply(LinkInput::DialStarted {
                    attempt: attempt.clone(),
                    lease: lease.window,
                    now: started,
                })
                .is_none()
                || self.client.dial_started(&attempt).is_err()
            {
                let _ = self.client.finish_attempt(&attempt, AttemptOutcome::Retry);
                return Ok(OpenExit::Resume);
            }
            let Some(deadline) = self.client.next_deadline() else {
                return Ok(OpenExit::Resume);
            };
            let Ok(address) = &self.address else {
                return Ok(OpenExit::Resume);
            };
            let result = control
                .during(async {
                    tokio::select! {
                        biased;
                        () = self.clock.sleep_until(deadline) => Err(DialFailure::Transport),
                        result = Box::pin(address.dial(&lease.token)) => result,
                    }
                })
                .await;
            let result = match result {
                Ok(result) => result,
                Err(reason) => return self.interrupted_attempt(&attempt, reason),
            };
            match result {
                Ok(mut socket) => {
                    let done = self
                        .open_socket(&mut socket, &attempt, binding, started, control, lane_owner)
                        .await;
                    lane_owner.reject_queued();
                    // Close is best-effort during cancellation. Dropping the stream
                    // terminates custody even when a blocked peer cannot take a frame.
                    let _ = socket.send(Message::Close(None)).now_or_never();
                    if let Err(reason) = done {
                        return self.interrupted_attempt(&attempt, reason);
                    }
                }
                Err(reason) => {
                    let _ = self.apply(LinkInput::DialFailed {
                        attempt: attempt.clone(),
                        reason,
                        now: self.clock.monotonic_now(),
                    });
                    let outcome = match self.state.status() {
                        LinkStatus::Fault { kind } => AttemptOutcome::Fault { kind },
                        _ => AttemptOutcome::Retry,
                    };
                    let _ = self.client.finish_attempt(&attempt, outcome);
                }
            }
        }
    }

    fn interrupted_attempt(
        &mut self,
        attempt: &ConnectionAttempt,
        reason: Interrupted,
    ) -> Result<OpenExit<Binding>, Interrupted> {
        if matches!(reason, Interrupted::SettingsChanged) {
            let _ = self.client.closed(attempt);
            let _ = self.apply(LinkInput::AddressChanged {
                attempt: attempt.clone(),
            });
        }
        Err(reason)
    }

    async fn open_socket(
        &mut self,
        socket: &mut (
                 impl Stream<Item = Result<Message, WebSocketError>>
                 + Sink<Message, Error = WebSocketError>
                 + Unpin
             ),
        attempt: &ConnectionAttempt,
        binding: &Binding,
        started: ClockInstant,
        control: &mut Control<'_, Binding>,
        lane_owner: &mut LinkLaneOwner,
    ) -> Result<(), Interrupted> {
        let mut acquiring: Option<LeaseFuture<'_, Binding>> = None;
        let mut renewal_sent_at = None;
        let mut priority = SocketPriority::ReadFrame;
        let mut admitted_lane = AdmittedSocketLane::new(lane_owner);
        let mut read_paused_at = None;
        loop {
            let now = self.clock.monotonic_now();
            admitted_lane.expire(now);
            let effects = self.apply(LinkInput::Tick { now }).unwrap_or_default();
            for effect in effects {
                match effect {
                    LinkEffect::NeedLease { newer_than, .. } => {
                        acquiring = Some(self.lease_source.acquire(binding, newer_than));
                    }
                    LinkEffect::BackOff { .. } => {
                        let _ = self.client.closed(attempt);
                        return Ok(());
                    }
                    LinkEffect::RenewOnSocket { .. } | LinkEffect::EndEnrollment { .. } => {}
                }
            }
            if self.state.ending().is_some() {
                return Ok(());
            }
            if self.state.dial_requirement().is_some() {
                let _ = self.client.closed(attempt);
                return Ok(());
            }
            let can_read = admitted_lane.can_read_without_delay();
            let client_action = if can_read {
                if let Some(paused_at) = read_paused_at.take() {
                    self.client
                        .defer_heartbeat(attempt, now.saturating_duration_since(paused_at));
                }
                self.client.poll_time()
            } else {
                read_paused_at.get_or_insert(now);
                None
            };
            match client_action {
                Some(ClientAction::SendPing) => {
                    if !self.write_heartbeat(socket, attempt, control).await? {
                        return Ok(());
                    }
                    continue;
                }
                Some(ClientAction::Reconnect { .. }) => {
                    self.close(attempt, None);
                    return Ok(());
                }
                Some(ClientAction::CallsExpired { .. }) => continue,
                Some(ClientAction::Connect { .. }) => {
                    self.close(attempt, None);
                    return Ok(());
                }
                None => {}
            }
            let deadline = can_read
                .then(|| self.client.next_deadline())
                .flatten()
                .into_iter()
                .chain(self.state.next_deadline())
                .chain(admitted_lane.reply_deadline())
                .min();
            let input = control
                .during(async {
                    tokio::select! {
                        biased;
                        () = sleep_if_due(self.clock, deadline) => SocketInput::Timer,
                        input = next_socket_input(socket, &mut acquiring, &mut lane_owner.commands, admitted_lane.can_offer(), admitted_lane.read_admission(), priority) => input,
                    }
                })
                .await?;
            // Rotate first poll across the three effect lanes. A Close already
            // read below ends the attempt before another lane can run.
            priority = match &input {
                SocketInput::Timer | SocketInput::EventSpace => priority,
                SocketInput::Frame(..) => SocketPriority::AcquireLease,
                SocketInput::Lease(_) => SocketPriority::AcceptCommand,
                SocketInput::Command(_) => SocketPriority::ReadFrame,
            };
            match input {
                SocketInput::Timer => {}
                SocketInput::EventSpace => {
                    let authority_live = self
                        .state
                        .renewal_deadline()
                        .is_some_and(|deadline| self.clock.monotonic_now() < deadline);
                    if authority_live && !admitted_lane.publish_admission() {
                        self.close(attempt, None);
                        return Ok(());
                    }
                }
                SocketInput::Frame(Some(Ok(Message::Close(frame))), _) => {
                    let code = frame
                        .and_then(|frame| MachineLinkCloseCode::from_code(u16::from(frame.code)));
                    log::trace!("Central link received a close classified as {code:?}");
                    self.close(attempt, code);
                    return Ok(());
                }
                SocketInput::Frame(Some(Ok(Message::Text(text))), permit) => {
                    let Ok(frame) = decode_text(text.as_str(), FrameDirection::ToRouter) else {
                        self.close(attempt, None);
                        return Ok(());
                    };
                    if !self.receive_frame(
                        attempt,
                        frame,
                        started,
                        renewal_sent_at,
                        &mut admitted_lane,
                        permit,
                    ) {
                        self.close(attempt, None);
                        return Ok(());
                    }
                }
                SocketInput::Frame(Some(Ok(Message::Ping(_) | Message::Pong(_))), _) => {}
                SocketInput::Frame(Some(Ok(_)) | Some(Err(_)) | None, _) => {
                    self.close(attempt, None);
                    return Ok(());
                }
                SocketInput::Command(request) => {
                    match admitted_lane.offer(request, self.clock.monotonic_now()) {
                        FrameOffer::Declined => {}
                        FrameOffer::Reconnect => {
                            self.close(attempt, None);
                            return Ok(());
                        }
                        FrameOffer::Queued { wire, send_before } => {
                            let Some(authority_until) = self.state.renewal_deadline() else {
                                self.close(attempt, None);
                                return Ok(());
                            };
                            let fence = SendFence {
                                send_before,
                                authority_until,
                                write_until: self.clock.monotonic_now().after(WRITE_LIMIT),
                            };
                            match control
                                .during(write_user_frame(socket, wire, fence, self.clock, || {
                                    admitted_lane.start_write();
                                }))
                                .await?
                            {
                                FrameWrite::Flushed => {
                                    admitted_lane.finish_write(self.clock.monotonic_now())
                                }
                                FrameWrite::Untouched(cause) => admitted_lane.reject_write(cause),
                                FrameWrite::PossiblyWritten => {
                                    self.close(attempt, None);
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                SocketInput::Lease(result) => {
                    acquiring = None;
                    match result {
                        Ok(lease) => {
                            let now = self.clock.monotonic_now();
                            let correlation_id = self
                                .client
                                .renewal_correlation(attempt)
                                .map_err(|_| Interrupted::Stopped)?;
                            let sent = self
                                .apply(LinkInput::RenewalLease {
                                    attempt: attempt.clone(),
                                    lease: lease.window,
                                    correlation_id: correlation_id.clone(),
                                    now,
                                })
                                .is_some();
                            if sent {
                                renewal_sent_at = Some(now);
                                let frame = LinkFrame::Data(DataFrame::RenewMachineToken {
                                    correlation_id,
                                    token: lease.token,
                                });
                                if self.write(socket, frame, control).await?.is_err() {
                                    self.close(attempt, None);
                                    return Ok(());
                                }
                            } else {
                                let _ = self.apply(LinkInput::RenewalUnavailable {
                                    attempt: attempt.clone(),
                                });
                            }
                        }
                        Err(LeaseFailure::EnrollmentEnded) => {
                            let _ = self.apply(LinkInput::LeaseEnded {
                                binding: binding.clone(),
                            });
                            return Ok(());
                        }
                        Err(LeaseFailure::Fault | LeaseFailure::Unavailable { .. }) => {
                            let _ = self.apply(LinkInput::RenewalUnavailable {
                                attempt: attempt.clone(),
                            });
                        }
                    }
                }
            }
        }
    }

    fn receive_frame(
        &mut self,
        attempt: &ConnectionAttempt,
        frame: LinkFrame,
        started: ClockInstant,
        renewal_sent_at: Option<ClockInstant>,
        admitted_lane: &mut AdmittedSocketLane,
        permit: Option<EventPermit>,
    ) -> bool {
        let reply = match frame {
            LinkFrame::Data(DataFrame::Reply {
                correlation_id,
                result,
            }) => SocketReply::Request {
                correlation_id,
                result,
            },
            LinkFrame::Data(DataFrame::RequestStatusReply {
                correlation_id,
                result,
            }) => SocketReply::RequestStatus {
                correlation_id,
                result,
            },
            LinkFrame::Data(DataFrame::ReceiptResults {
                correlation_id,
                results,
            }) => SocketReply::ReceiptResults {
                correlation_id,
                results,
            },
            other => {
                let Ok(event) = self.client.receive(attempt, other) else {
                    return false;
                };
                return self.receive_event(
                    attempt,
                    event,
                    started,
                    renewal_sent_at,
                    admitted_lane,
                    permit,
                );
            }
        };
        if !admitted_lane.is_admitted() {
            return false;
        }
        let now = self.clock.monotonic_now();
        if self
            .state
            .renewal_deadline()
            .is_some_and(|deadline| now < deadline)
        {
            admitted_lane.reply(attempt, reply, now);
        }
        true
    }

    fn receive_event(
        &mut self,
        attempt: &ConnectionAttempt,
        event: ClientEvent,
        started: ClockInstant,
        renewal_sent_at: Option<ClockInstant>,
        admitted_lane: &mut AdmittedSocketLane,
        permit: Option<EventPermit>,
    ) -> bool {
        match event {
            ClientEvent::Welcomed {
                central_time,
                effective_expires_at,
            } => {
                let lifetime = (effective_expires_at - central_time)
                    .to_std()
                    .unwrap_or_default();
                let now = self.clock.monotonic_now();
                let accepted = self
                    .apply(LinkInput::Welcome {
                        attempt: attempt.clone(),
                        machine_id: self.machine_id.clone(),
                        deadline: started.after(lifetime),
                        now,
                    })
                    .is_some();
                accepted
                    && (self.state.renewal_deadline().is_none()
                        || admitted_lane.admit(AdmittedConnection {
                            connection: attempt.clone(),
                            clock_anchor: WelcomeClockAnchor {
                                central_time,
                                observed_at: now,
                            },
                        }))
            }
            ClientEvent::TokenRenewal {
                correlation_id,
                result,
            } => {
                let event = match result {
                    TokenRenewalResult::Renewed {
                        central_time,
                        effective_expires_at,
                    } => {
                        let Some(sent_at) = renewal_sent_at else {
                            return true;
                        };
                        let lifetime = (effective_expires_at - central_time)
                            .to_std()
                            .unwrap_or_default();
                        LinkInput::RenewalAccepted {
                            attempt: attempt.clone(),
                            correlation_id,
                            deadline: sent_at.after(lifetime),
                            now: self.clock.monotonic_now(),
                        }
                    }
                    TokenRenewalResult::Refused { reason } => LinkInput::RenewalRefused {
                        attempt: attempt.clone(),
                        correlation_id,
                        reason,
                        now: self.clock.monotonic_now(),
                    },
                };
                let _ = self.apply(event);
                true
            }
            event @ ClientEvent::Deliver { .. } => admitted_lane.deliver(event, permit),
            ClientEvent::Pong => true,
            // Registered calls remain outside the admitted message lane.
            ClientEvent::CallMac { .. }
            | ClientEvent::CentralCallReply { .. }
            | ClientEvent::ReceiptResults { .. }
            | ClientEvent::LateReply { .. }
            | ClientEvent::Reply { .. } => false,
        }
    }

    async fn write_heartbeat(
        &mut self,
        socket: &mut (impl Sink<Message> + Unpin),
        attempt: &ConnectionAttempt,
        control: &mut Control<'_, Binding>,
    ) -> Result<bool, Interrupted> {
        if self.write(socket, LinkFrame::Ping, control).await?.is_ok()
            && self
                .client
                .ping_written(attempt, self.clock.monotonic_now())
                .is_ok()
        {
            return Ok(true);
        }
        self.close(attempt, None);
        Ok(false)
    }

    async fn write(
        &mut self,
        socket: &mut (impl Sink<Message> + Unpin),
        frame: LinkFrame,
        control: &mut Control<'_, Binding>,
    ) -> Result<Result<(), SocketWriteFailure>, Interrupted> {
        let wire = match bounded_wire_text(&frame) {
            Ok(wire) => wire,
            Err(reason) => return Ok(Err(reason)),
        };
        let deadline = self.write_deadline();
        control
            .during(async {
                tokio::select! {
                    biased;
                    () = self.clock.sleep_until(deadline) => Err(SocketWriteFailure::DeadlineReached),
                    result = socket.send(Message::Text(wire.into())) => result.map_err(|_| SocketWriteFailure::Transport),
                }
            })
            .await
    }

    fn write_deadline(&self) -> ClockInstant {
        let limit = self.clock.monotonic_now().after(WRITE_LIMIT);
        self.state
            .renewal_deadline()
            .map_or(limit, |deadline| deadline.min(limit))
    }
}

async fn next_socket_input<Binding>(
    socket: &mut (impl Stream<Item = Result<Message, WebSocketError>> + Unpin),
    acquisition: &mut Option<LeaseFuture<'_, Binding>>,
    commands: &mut mpsc::Receiver<LaneRequest>,
    can_offer: bool,
    read_admission: ReadAdmission,
    priority: SocketPriority,
) -> SocketInput<Binding> {
    let reading = read_socket(socket, read_admission);
    let leasing = acquire_if_pending(acquisition).map(SocketInput::Lease);
    let command = receive_command(commands, can_offer).map(SocketInput::Command);
    match priority {
        SocketPriority::ReadFrame => poll_in_order(reading, leasing, command).await,
        SocketPriority::AcquireLease => poll_in_order(leasing, command, reading).await,
        SocketPriority::AcceptCommand => poll_in_order(command, reading, leasing).await,
    }
}

async fn read_socket<Binding>(
    socket: &mut (impl Stream<Item = Result<Message, WebSocketError>> + Unpin),
    admission: ReadAdmission,
) -> SocketInput<Binding> {
    match admission {
        ReadAdmission::AwaitingWelcome => SocketInput::Frame(socket.next().await, None),
        ReadAdmission::Admitted(events) => match events.reserve_owned().await {
            Ok(permit) => SocketInput::Frame(socket.next().await, Some(permit)),
            Err(_) => SocketInput::Frame(None, None),
        },
        ReadAdmission::AwaitingEventSpace { events, slots } => {
            match events.reserve_many(slots).await {
                Ok(_) => SocketInput::EventSpace,
                Err(_) => SocketInput::Frame(None, None),
            }
        }
    }
}

async fn poll_in_order<Output>(
    first: impl Future<Output = Output>,
    second: impl Future<Output = Output>,
    third: impl Future<Output = Output>,
) -> Output {
    tokio::select! {
        biased;
        result = first => result,
        result = second => result,
        result = third => result,
    }
}

async fn receive_command(
    commands: &mut mpsc::Receiver<LaneRequest>,
    can_offer: bool,
) -> LaneRequest {
    if can_offer && let Some(command) = commands.recv().await {
        return command;
    }
    pending().await
}

fn bounded_wire_text(frame: &LinkFrame) -> Result<String, SocketWriteFailure> {
    if let LinkFrame::Data(DataFrame::RenewMachineToken { token, .. }) = frame
        && token.expose().len() > MAX_CREDENTIAL_BYTES
    {
        return Err(SocketWriteFailure::SizeLimit);
    }
    let wire = encode_text(frame, FrameDirection::ToCentral)
        .map_err(|_| SocketWriteFailure::InvalidFrame)?;
    if wire.len() > MAX_OUTBOUND_TEXT_BYTES {
        return Err(SocketWriteFailure::SizeLimit);
    }
    Ok(wire)
}

async fn sleep_if_due(clock: &dyn RuntimeClock, deadline: Option<ClockInstant>) {
    match deadline {
        Some(deadline) => clock.sleep_until(deadline).await,
        None => pending().await,
    }
}

async fn acquire_if_pending<Binding>(
    acquisition: &mut Option<LeaseFuture<'_, Binding>>,
) -> Result<crate::lease_source::LinkLease<Binding>, LeaseFailure> {
    match acquisition {
        Some(future) => future.await,
        None => pending().await,
    }
}

#[cfg(test)]
#[path = "link_driver_test.rs"]
mod tests;
