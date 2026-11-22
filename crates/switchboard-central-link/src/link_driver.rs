//! Socket custody and elapsed scheduling for the Mac Central link.

use std::{
    future::{Future, pending},
    sync::Arc,
    time::Duration,
};

use central_link_client::{
    AttemptOutcome, ClientAction, ClientEvent, ConnectionAttempt, LinkClient,
};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, MachineLinkCloseCode, TokenRenewalResult, decode_text,
    encode_text,
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use router_clock::{ClockInstant, RuntimeClock};
use state_machine_core::StateMachine;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;
use workspace_ids::MachineId;

use crate::{
    AddressError, DialAddress,
    lease_source::{LeaseFailure, LeaseFuture, LeaseSource},
    link_policy::{DialFailure, LinkEffect, LinkInput, LinkPolicy, LinkState, LinkStatus},
    socket_upgrade::Socket,
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
    pub inbox: mpsc::Sender<ClientEvent>,
}

#[derive(Clone, Copy)]
enum Interrupted {
    SettingsChanged,
    Stopped,
}

struct Control<'a, Binding> {
    settings: &'a mut watch::Receiver<DriverSettings<Binding>>,
    shutdown: &'a mut watch::Receiver<bool>,
}

impl<Binding> Control<'_, Binding> {
    async fn wait(&mut self) -> Interrupted {
        tokio::select! {
            biased;
            _ = self.shutdown.wait_for(|stop| *stop) => Interrupted::Stopped,
            update = self.settings.changed() => match update {
                Ok(()) => Interrupted::SettingsChanged,
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
    inbox: &'source mpsc::Sender<ClientEvent>,
    address: Result<DialAddress, AddressError>,
    machine_id: MachineId,
    state: LinkState<Binding>,
    client: LinkClient,
}

enum OpenExit<Binding> {
    Resume,
    Final {
        binding: Binding,
        cause: central_link_client::EnrollmentEnd,
    },
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
                inbox: &self.inbox,
                address: settings.address,
                machine_id: self.machine_id.clone(),
                state: LinkState::new(self.machine_id.clone()),
                client: LinkClient::new(self.machine_id.clone(), self.clock.clone()),
            };
            match driver.run_binding(binding.clone(), &mut control).await {
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
    ) -> (Interrupted, bool) {
        let _ = self.apply(LinkInput::BindingAvailable(binding.clone()));
        if self.address.is_err() {
            self.status.send_replace(LinkStatus::Fault {
                kind: central_link_client::LinkFault::MalformedAddress,
            });
            return (control.wait().await, false);
        }
        match self.run_attempts(&binding, control).await {
            Ok(OpenExit::Final { binding, cause }) => {
                // Retire exact credential authority even if its storage version advanced.
                // The lease owner commits the mark independently of the socket.
                self.lease_source.end(&binding, cause);
                (control.wait().await, true)
            }
            Ok(OpenExit::Resume) => (control.wait().await, false),
            Err(reason) => (reason, false),
        }
    }

    async fn run_attempts(
        &mut self,
        binding: &Binding,
        control: &mut Control<'_, Binding>,
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
                self.client.reenrolled();
                continue;
            }
            let Some(newer_than) = self.state.dial_requirement() else {
                return Ok(OpenExit::Resume);
            };
            if let Some(deadline) = self.client.next_deadline() {
                control.during(self.clock.sleep_until(deadline)).await?;
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
                    control
                        .during(self.clock.sleep_for(MIN_LEASE_RETRY))
                        .await?;
                    continue;
                }
                Err(LeaseFailure::Unavailable { retry_after }) => {
                    control
                        .during(self.clock.sleep_for(retry_after.max(MIN_LEASE_RETRY)))
                        .await?;
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
                .await?;
            match result {
                Ok(mut socket) => {
                    let done = self
                        .open_socket(&mut socket, &attempt, binding, started, control)
                        .await;
                    // Close is best-effort during cancellation. Dropping the stream
                    // terminates custody even when a blocked peer cannot take a frame.
                    let _ = socket.send(Message::Close(None)).now_or_never();
                    done?;
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

    async fn open_socket(
        &mut self,
        socket: &mut Socket,
        attempt: &ConnectionAttempt,
        binding: &Binding,
        started: ClockInstant,
        control: &mut Control<'_, Binding>,
    ) -> Result<(), Interrupted> {
        let mut acquiring: Option<LeaseFuture<'_, Binding>> = None;
        let mut renewal_sent_at = None;
        loop {
            let now = self.clock.monotonic_now();
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
            match self.client.poll_time() {
                Some(ClientAction::SendPing) => {
                    if !self.write(socket, LinkFrame::Ping, control).await? {
                        self.close(attempt, None);
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
            let deadline = self
                .client
                .next_deadline()
                .into_iter()
                .chain(self.state.next_deadline())
                .min();
            enum Input<Binding> {
                Timer,
                Frame(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
                Lease(Result<crate::lease_source::LinkLease<Binding>, LeaseFailure>),
            }
            let input = control
                .during(async {
                    tokio::select! {
                        biased;
                        () = sleep_if_due(self.clock, deadline) => Input::Timer,
                        frame = socket.next() => Input::Frame(frame),
                        lease = acquire_if_pending(&mut acquiring) => Input::Lease(lease),
                    }
                })
                .await?;
            match input {
                Input::Timer => {}
                Input::Frame(Some(Ok(Message::Close(frame)))) => {
                    let code = frame
                        .and_then(|frame| MachineLinkCloseCode::from_code(u16::from(frame.code)));
                    self.close(attempt, code);
                    return Ok(());
                }
                Input::Frame(Some(Ok(Message::Text(text)))) => {
                    let Ok(event) = decode_text(text.as_str(), FrameDirection::ToRouter)
                        .map_err(|_| ())
                        .and_then(|frame| self.client.receive(attempt, frame).map_err(|_| ()))
                    else {
                        self.close(attempt, None);
                        return Ok(());
                    };
                    if !self.receive(attempt, event, started, renewal_sent_at) {
                        self.close(attempt, None);
                        return Ok(());
                    }
                }
                Input::Frame(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
                Input::Frame(Some(Ok(_)) | Some(Err(_)) | None) => {
                    self.close(attempt, None);
                    return Ok(());
                }
                Input::Lease(result) => {
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
                                if !self.write(socket, frame, control).await? {
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

    fn receive(
        &mut self,
        attempt: &ConnectionAttempt,
        event: ClientEvent,
        started: ClockInstant,
        renewal_sent_at: Option<ClockInstant>,
    ) -> bool {
        match event {
            ClientEvent::Welcomed {
                central_time,
                effective_expires_at,
            } => {
                let lifetime = (effective_expires_at - central_time)
                    .to_std()
                    .unwrap_or_default();
                self.apply(LinkInput::Welcome {
                    attempt: attempt.clone(),
                    machine_id: self.machine_id.clone(),
                    deadline: started.after(lifetime),
                    now: self.clock.monotonic_now(),
                })
                .is_some()
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
                    },
                };
                let _ = self.apply(event);
                true
            }
            event @ ClientEvent::Deliver { .. } => self.inbox.try_send(event).is_ok(),
            ClientEvent::Pong
            | ClientEvent::ReceiptResults { .. }
            | ClientEvent::LateReply { .. } => true,
            // This cut registers no calls and exposes no call handle.
            ClientEvent::CallMac { .. }
            | ClientEvent::CentralCallReply { .. }
            | ClientEvent::Reply { .. } => false,
        }
    }

    async fn write(
        &mut self,
        socket: &mut Socket,
        frame: LinkFrame,
        control: &mut Control<'_, Binding>,
    ) -> Result<bool, Interrupted> {
        let Ok(wire) = encode_text(&frame, FrameDirection::ToCentral) else {
            return Ok(false);
        };
        let deadline = self
            .state
            .renewal_deadline()
            .unwrap_or_else(|| self.clock.monotonic_now().after(WRITE_LIMIT))
            .min(self.clock.monotonic_now().after(WRITE_LIMIT));
        control
            .during(async {
                tokio::select! {
                    biased;
                    () = self.clock.sleep_until(deadline) => false,
                    result = socket.send(Message::Text(wire.into())) => result.is_ok(),
                }
            })
            .await
    }
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
