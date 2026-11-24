use std::{sync::Arc, time::Duration};

use central_link_protocol::{
    CorrelationId,
    person_link_rpc::{
        ListOrganizationWorkspaces, OrganizationWorkspace, OrganizationWorkspacePage,
        PERSON_WORKSPACE_PAGE_LIMIT, PersonWorkspacePageLimit,
    },
};
use fleet_rpc::Envelope;
use reqwest::{Certificate, Client, ClientBuilder};
use reqwest_websocket::WebSocket;
use router_clock::{ClockInstant, RuntimeClock, timeout_at};
use state_machine_core::{StateMachine, TransitionError};
use workspace_ids::{RequestId, WorkspaceId};

use crate::{
    FleetCredentialStore, FleetTarget, PersonLinkError, clock_uuid,
    connection_machine::{
        ConnectionEffect, ConnectionEvent, ConnectionMachine, ConnectionRefusal, ConnectionState,
        ReadAttempt,
    },
    fleet_credential::refresh_for_upgrade,
    person_socket::{receive_event, send_read, upgrade},
};

const READ_BUDGET: Duration = Duration::from_secs(30);

/// One sequential person connection. Each listing may span several correlated reads.
pub struct PersonWorkspaceClient {
    target: FleetTarget,
    store: Arc<dyn FleetCredentialStore>,
    clock: Arc<dyn RuntimeClock>,
    http: Client,
    state: Option<ConnectionState>,
    socket: Option<WebSocket>,
}

impl PersonWorkspaceClient {
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

    pub async fn list_workspaces(&mut self) -> Result<Vec<OrganizationWorkspace>, PersonLinkError> {
        let limit = PersonWorkspacePageLimit::try_from(PERSON_WORKSPACE_PAGE_LIMIT)
            .map_err(|_| PersonLinkError::WorkspaceCursor)?;
        let operation = RequestId::try_from(clock_uuid(self.clock.now().unix_millis()).to_string())
            .map_err(|_| PersonLinkError::RequestId)?;
        let mut after = None;
        let mut rows = Vec::new();
        loop {
            let page = self
                .read_page(ListOrganizationWorkspaces {
                    operation_id: operation.clone(),
                    after: after.clone(),
                    limit,
                })
                .await?;
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

    async fn read_page(
        &mut self,
        body: ListOrganizationWorkspaces,
    ) -> Result<OrganizationWorkspacePage, PersonLinkError> {
        let deadline = self.clock.monotonic_now().after(READ_BUDGET);
        let read = Envelope {
            target: self.target.organization().clone(),
            body,
        };
        let (mut state, mut effect) = match &self.state {
            None => ConnectionMachine::begin(read),
            Some(state) => step(
                state,
                ConnectionEvent::Read(ReadAttempt {
                    read,
                    correlation: self.mint_correlation(),
                }),
            )?,
        };
        // The in-flight future owns its socket. Cancellation drops the socket and its pending read.
        self.state = None;
        let mut socket = self.socket.take();
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
                    let active = socket.as_mut().ok_or(PersonLinkError::ConnectionState)?;
                    let sent =
                        timeout_at(self.clock.as_ref(), deadline, send_read(active, attempt)).await;
                    match sent {
                        Ok(Ok(())) => self.wait_for_read_event(active, deadline).await,
                        Ok(Err(PersonLinkError::Socket)) => {
                            // A revocation close can cross the write. Replay only if its typed close arrives.
                            match self.wait_for_read_event(active, deadline).await {
                                ConnectionEvent::LifetimeClosed => ConnectionEvent::LifetimeClosed,
                                _ => ConnectionEvent::Failed(PersonLinkError::Socket),
                            }
                        }
                        Ok(Err(error)) => ConnectionEvent::Failed(error),
                        Err(_) => ConnectionEvent::Failed(PersonLinkError::Deadline),
                    }
                }
                ConnectionEffect::Receive => {
                    let active = socket.as_mut().ok_or(PersonLinkError::ConnectionState)?;
                    self.wait_for_read_event(active, deadline).await
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
                    return answer;
                }
            };
            match ConnectionMachine::transition(&state, event) {
                Ok(transition) => (state, effect) = take_effect(transition)?,
                Err(TransitionError::Stale) => effect = ConnectionEffect::Receive,
                Err(error) => return Err(transition_error(error)),
            }
        }
    }

    async fn open_socket(&self, deadline: ClockInstant) -> Result<WebSocket, PersonLinkError> {
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

    async fn wait_for_read_event(
        &self,
        socket: &mut WebSocket,
        deadline: ClockInstant,
    ) -> ConnectionEvent {
        if self.clock.monotonic_now() >= deadline {
            return ConnectionEvent::Failed(PersonLinkError::Deadline);
        }
        match timeout_at(self.clock.as_ref(), deadline, receive_event(socket)).await {
            Ok(Ok(event)) => event,
            Ok(Err(error)) => ConnectionEvent::Failed(error),
            Err(_) => ConnectionEvent::Failed(PersonLinkError::Deadline),
        }
    }

    fn mint_correlation(&self) -> CorrelationId {
        central_link_protocol::FreshCorrelationId::mint_at(self.clock.now().unix_millis())
            .wire_id()
            .clone()
    }
}

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
