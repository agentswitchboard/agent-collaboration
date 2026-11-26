use std::{io, sync::Arc, time::Duration};

use approval_request_protocol::{
    Answer, ApprovalDecideReply, ApprovalListFilter, ApprovalListReply, ApprovalReadReply,
    ApprovalRequestId,
};
use central_link_protocol::{
    CorrelationId, LinkCallRefusal,
    person_link_rpc::{
        DecidePersonApproval, ListOrganizationWorkspaces, ListPersonApprovals,
        OrganizationWorkspace, PERSON_WORKSPACE_PAGE_LIMIT, PersonWorkspacePageLimit,
        ReadPersonApproval,
    },
};
use fleet_rpc::Envelope;
use reqwest::{Certificate, Client, ClientBuilder};
use router_clock::{ClockInstant, RuntimeClock, timeout_at};
use state_machine_core::{StateMachine, TransitionError};
use workspace_ids::{RequestId, WorkspaceId};

use crate::{
    FleetCredentialStore, FleetLoginError, FleetTarget, PersonLinkError, clock_uuid,
    connection_machine::{
        CallAttempt, ConnectionEffect, ConnectionEvent, ConnectionMachine, ConnectionRefusal,
        ConnectionState, PersonCall, PersonReply,
    },
    fleet_credential::refresh_for_upgrade,
    fleet_login,
    person_socket::{PersonSocket, encode_call, receive_event, send_call, upgrade},
};

const CALL_BUDGET: Duration = Duration::from_secs(30);

#[derive(Debug, Eq, PartialEq)]
pub enum ApprovalDecisionOutcome {
    Reply(ApprovalDecideReply),
    Refused {
        reason: LinkCallRefusal,
    },
    /// A submission may have reached Central. Retry with the same request, answer and operation ID.
    Unknown {
        operation_id: RequestId,
        cause: PersonLinkError,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("Decision was not sent: {0}")]
pub struct ApprovalNotSent(#[source] PersonLinkError);

/// One sequential person connection shared by directory and approval calls.
pub struct PersonLinkClient {
    target: FleetTarget,
    store: Arc<dyn FleetCredentialStore>,
    clock: Arc<dyn RuntimeClock>,
    http: Client,
    state: Option<ConnectionState>,
    socket: Option<PersonSocket>,
}

impl PersonLinkClient {
    pub fn new(
        target: FleetTarget,
        store: Arc<dyn FleetCredentialStore>,
        clock: Arc<dyn RuntimeClock>,
    ) -> Result<Self, PersonLinkError> {
        Self::build(target, store, clock, Client::builder())
    }

    /// Connect to an issuer whose TLS root is outside the system trust bundle.
    pub fn with_trusted_root(
        target: FleetTarget,
        store: Arc<dyn FleetCredentialStore>,
        clock: Arc<dyn RuntimeClock>,
        root: Certificate,
    ) -> Result<Self, PersonLinkError> {
        Self::build(
            target,
            store,
            clock,
            Client::builder().add_root_certificate(root),
        )
    }

    fn build(
        target: FleetTarget,
        store: Arc<dyn FleetCredentialStore>,
        clock: Arc<dyn RuntimeClock>,
        builder: ClientBuilder,
    ) -> Result<Self, PersonLinkError> {
        let http = builder
            .https_only(true)
            .http1_only()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| PersonLinkError::HttpClient)?;
        Ok(Self {
            target,
            store,
            clock,
            http,
            state: None,
            socket: None,
        })
    }

    pub async fn login(
        &mut self,
        show_address: impl FnOnce(&url::Url) -> io::Result<()>,
    ) -> Result<(), FleetLoginError> {
        self.state = None;
        self.socket = None;
        fleet_login::login(
            &self.http,
            &self.target,
            self.store.clone(),
            self.clock.as_ref(),
            show_address,
        )
        .await
    }

    pub async fn list_workspaces(&mut self) -> Result<Vec<OrganizationWorkspace>, PersonLinkError> {
        let limit = PersonWorkspacePageLimit::try_from(PERSON_WORKSPACE_PAGE_LIMIT)
            .map_err(|_| PersonLinkError::WorkspaceCursor)?;
        let operation = RequestId::try_from(clock_uuid(self.clock.now().unix_millis()).to_string())
            .map_err(|_| PersonLinkError::RequestId)?;
        let mut after = None;
        let mut rows = Vec::new();
        loop {
            let reply = self
                .call(PersonCall::ListWorkspaces(ListOrganizationWorkspaces {
                    operation_id: operation.clone(),
                    after: after.clone(),
                    limit,
                }))
                .await
                .map_err(CallFailure::read_error)?;
            let PersonReply::Workspaces(page) = reply else {
                return Err(PersonLinkError::Protocol);
            };
            rows.extend(page.workspaces);
            match page.next {
                Some(next)
                    if after
                        .as_ref()
                        .is_none_or(|previous: &WorkspaceId| &next > previous) =>
                {
                    after = Some(next);
                }
                Some(_) => return Err(PersonLinkError::WorkspaceCursor),
                None => return Ok(rows),
            }
        }
    }

    pub async fn list_approvals(
        &mut self,
        filter: ApprovalListFilter,
    ) -> Result<ApprovalListReply, PersonLinkError> {
        let mut cursor = None;
        let mut approvals = Vec::new();
        loop {
            let reply = self
                .call(PersonCall::ListApprovals(ListPersonApprovals {
                    filter,
                    cursor: cursor.clone(),
                }))
                .await
                .map_err(CallFailure::read_error)?;
            let PersonReply::Approvals(ApprovalListReply::Page { items, next }) = reply else {
                return Err(PersonLinkError::Protocol);
            };
            approvals.extend(items);
            match next {
                Some(next) if cursor.as_ref().is_none_or(|previous| &next < previous) => {
                    cursor = Some(next);
                }
                Some(_) => return Err(PersonLinkError::ApprovalCursor),
                None => {
                    return Ok(ApprovalListReply::Page {
                        items: approvals,
                        next: None,
                    });
                }
            }
        }
    }

    pub async fn read_approval(
        &mut self,
        request_id: ApprovalRequestId,
    ) -> Result<ApprovalReadReply, PersonLinkError> {
        let reply = self
            .call(PersonCall::ReadApproval(ReadPersonApproval { request_id }))
            .await
            .map_err(CallFailure::read_error)?;
        let PersonReply::Approval(reply) = reply else {
            return Err(PersonLinkError::Protocol);
        };
        Ok(*reply)
    }

    pub async fn decide_approval(
        &mut self,
        request_id: ApprovalRequestId,
        operation_id: RequestId,
        answer: Answer,
    ) -> Result<ApprovalDecisionOutcome, ApprovalNotSent> {
        match self
            .call(PersonCall::DecideApproval(DecidePersonApproval {
                request_id,
                operation_id: operation_id.clone(),
                answer,
            }))
            .await
        {
            Ok(PersonReply::Decision(reply)) => Ok(ApprovalDecisionOutcome::Reply(reply)),
            Ok(_) => Ok(ApprovalDecisionOutcome::Unknown {
                operation_id,
                cause: PersonLinkError::Protocol,
            }),
            Err(CallFailure::AfterFirstWrite(PersonLinkError::CallRefused(reason))) => {
                Ok(ApprovalDecisionOutcome::Refused { reason })
            }
            Err(CallFailure::AfterReplay(PersonLinkError::CallRefused(reason))) => {
                // The refusal answers the replay. The prior attempt still has no answer.
                Ok(ApprovalDecisionOutcome::Unknown {
                    operation_id,
                    cause: PersonLinkError::ReplayRefused(reason),
                })
            }
            Err(CallFailure::AfterFirstWrite(cause) | CallFailure::AfterReplay(cause)) => {
                Ok(ApprovalDecisionOutcome::Unknown {
                    operation_id,
                    cause,
                })
            }
            Err(CallFailure::BeforeWrite(cause)) => Err(ApprovalNotSent(cause)),
        }
    }

    async fn call(&mut self, body: PersonCall) -> Result<PersonReply, CallFailure> {
        let deadline = self.clock.monotonic_now().after(CALL_BUDGET);
        let call = Envelope {
            target: self.target.organization().clone(),
            body,
        };
        let (mut state, mut effect) = match &self.state {
            None => ConnectionMachine::begin(call),
            Some(state) => step(
                state,
                ConnectionEvent::Call(CallAttempt {
                    call,
                    correlation: self.mint_correlation(),
                }),
            )?,
        };
        // The in-flight future owns the socket so cancellation also drops the pending call.
        self.state = None;
        let mut socket = self.socket.take();
        let mut write_progress = WriteProgress::Unwritten;
        let mut write_error = None;
        loop {
            let event = match effect {
                ConnectionEffect::Upgrade => match self.open_socket(deadline).await {
                    Ok(opened) => {
                        socket = Some(opened);
                        ConnectionEvent::Upgraded(self.mint_correlation())
                    }
                    Err(error) => ConnectionEvent::Failed(error),
                },
                ConnectionEffect::Send(attempt) => {
                    let active = socket.as_mut().ok_or_else(|| {
                        CallFailure::from_progress(PersonLinkError::ConnectionState, write_progress)
                    })?;
                    let (event, error) = self
                        .send_attempt(active, attempt, deadline, &mut write_progress)
                        .await;
                    write_error = error;
                    event
                }
                ConnectionEffect::Receive => {
                    let active = socket.as_mut().ok_or_else(|| {
                        CallFailure::from_progress(PersonLinkError::ConnectionState, write_progress)
                    })?;
                    self.wait_for_call_event(active, deadline, write_error)
                        .await
                }
                ConnectionEffect::RetireSocket => {
                    drop(socket.take());
                    ConnectionEvent::SocketRetired
                }
                ConnectionEffect::Finish(answer) => {
                    if matches!(state, ConnectionState::Connected(None)) {
                        self.socket = socket;
                    }
                    self.state = Some(state);
                    return answer
                        .map_err(|error| CallFailure::from_progress(error, write_progress));
                }
            };
            match ConnectionMachine::transition(&state, event) {
                Ok(transition) => {
                    (state, effect) = take_effect(transition)
                        .map_err(|error| CallFailure::from_progress(error, write_progress))?;
                }
                Err(TransitionError::Stale) => effect = ConnectionEffect::Receive,
                Err(error) => {
                    return Err(CallFailure::from_progress(
                        transition_error(error),
                        write_progress,
                    ));
                }
            }
        }
    }

    async fn send_attempt(
        &self,
        socket: &mut PersonSocket,
        attempt: CallAttempt,
        deadline: ClockInstant,
        write_progress: &mut WriteProgress,
    ) -> (ConnectionEvent, Option<PersonLinkError>) {
        let text = match encode_call(attempt) {
            Ok(text) => text,
            Err(error) => return (ConnectionEvent::Failed(error), None),
        };
        let write = async {
            write_progress.begin_attempt();
            send_call(socket, text).await
        };
        let write_error = match timeout_at(self.clock.as_ref(), deadline, write).await {
            Ok(outcome) => outcome.err(),
            Err(_) => return (ConnectionEvent::Failed(PersonLinkError::Deadline), None),
        };
        // A reply or reconnect can cross a failed write. Keep its error as a fallback through stale replies.
        (
            self.wait_for_call_event(socket, deadline, write_error)
                .await,
            write_error,
        )
    }

    async fn open_socket(&self, deadline: ClockInstant) -> Result<PersonSocket, PersonLinkError> {
        if self.clock.monotonic_now() >= deadline {
            return Err(PersonLinkError::Deadline);
        }
        let credential = refresh_for_upgrade(
            &self.http,
            &self.target,
            self.store.clone(),
            self.clock.as_ref(),
            deadline,
        )
        .await?;
        timeout_at(
            self.clock.as_ref(),
            deadline,
            upgrade(&self.http, &self.target, credential),
        )
        .await
        .map_err(|_| PersonLinkError::Deadline)?
    }

    async fn wait_for_call_event(
        &self,
        socket: &mut PersonSocket,
        deadline: ClockInstant,
        write_error: Option<PersonLinkError>,
    ) -> ConnectionEvent {
        if self.clock.monotonic_now() >= deadline {
            return ConnectionEvent::Failed(write_error.unwrap_or(PersonLinkError::Deadline));
        }
        match timeout_at(self.clock.as_ref(), deadline, receive_event(socket)).await {
            Ok(Ok(event)) => event,
            Ok(Err(error)) => ConnectionEvent::Failed(write_error.unwrap_or(error)),
            Err(_) => ConnectionEvent::Failed(write_error.unwrap_or(PersonLinkError::Deadline)),
        }
    }

    fn mint_correlation(&self) -> CorrelationId {
        central_link_protocol::FreshCorrelationId::mint_at(self.clock.now().unix_millis())
            .wire_id()
            .clone()
    }
}

#[derive(Clone, Copy)]
enum WriteProgress {
    Unwritten,
    FirstAttempt,
    Replay,
}

impl WriteProgress {
    fn begin_attempt(&mut self) {
        *self = match self {
            Self::Unwritten => Self::FirstAttempt,
            Self::FirstAttempt | Self::Replay => Self::Replay,
        };
    }
}

enum CallFailure {
    BeforeWrite(PersonLinkError),
    AfterFirstWrite(PersonLinkError),
    AfterReplay(PersonLinkError),
}

impl CallFailure {
    fn from_progress(cause: PersonLinkError, progress: WriteProgress) -> Self {
        match progress {
            WriteProgress::Unwritten => Self::BeforeWrite(cause),
            WriteProgress::FirstAttempt => Self::AfterFirstWrite(cause),
            WriteProgress::Replay => Self::AfterReplay(cause),
        }
    }

    fn read_error(self) -> PersonLinkError {
        let cause = match self {
            Self::BeforeWrite(cause) | Self::AfterFirstWrite(cause) | Self::AfterReplay(cause) => {
                cause
            }
        };
        if matches!(cause, PersonLinkError::CallEncoding) {
            PersonLinkError::Protocol
        } else {
            cause
        }
    }
}

impl From<PersonLinkError> for CallFailure {
    fn from(error: PersonLinkError) -> Self {
        Self::BeforeWrite(error)
    }
}

#[cfg(test)]
#[path = "person_link_client_test.rs"]
mod tests;

fn step(
    state: &ConnectionState,
    event: ConnectionEvent,
) -> Result<(ConnectionState, ConnectionEffect), PersonLinkError> {
    take_effect(ConnectionMachine::transition(state, event).map_err(transition_error)?)
}

fn transition_error(error: TransitionError<ConnectionRefusal>) -> PersonLinkError {
    match error {
        TransitionError::Domain(ConnectionRefusal::AlreadyRefused(reason)) => reason,
        TransitionError::NotAllowed { .. } | TransitionError::Stale => {
            PersonLinkError::ConnectionState
        }
    }
}

fn take_effect(
    (state, effects): (ConnectionState, Vec<ConnectionEffect>),
) -> Result<(ConnectionState, ConnectionEffect), PersonLinkError> {
    Ok((
        state,
        effects
            .into_iter()
            .next()
            .ok_or(PersonLinkError::ConnectionState)?,
    ))
}
