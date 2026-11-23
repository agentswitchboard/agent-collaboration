//! The applying boundary for the organization's session commands.
use central_engine_model::{Context, EngineTime, Machine, TransitionError, supported_time};
use central_storage::StoreFailure;
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    CreateSessionKind, CreateSessionRefusal, EndFleetSession, EndFleetSessionReply,
    EndSessionTarget, EnrollmentId, OrganizationId, PresentedSessionSecret, SessionCreator,
    SessionSecret,
};
use fleet_lent_transaction::CentralTransaction;
use fleet_machine_login::{
    SessionEnrollmentStatus, link_authority::ValidatedMachineConnection, read_session_enrollment,
};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use workspace_ids::{CentralSessionId, MachineId};

use crate::session_repository::SessionRepository;
use crate::{
    AddressAvailability, BoundSession, NewSessionAdmission, RecordedSessionCreator, SessionApply,
    SessionCreatorAdmission, SessionEndIdentity, SessionEvent, SessionLifecycle,
    SessionMachineRefusal, SessionReceipt, SessionReservation, SessionState,
};

const RESERVATION_DURATION_MILLIS: i64 = 600_000;

/// The physical organization owner lends its transaction with the scope it opened.
pub trait OrganizationTransaction: CentralTransaction {
    fn organization_id(&self) -> &OrganizationId;
}

/// Lookup keys for trusted facts reads.
pub struct SessionLookupScope {
    pub machine_id: MachineId,
    pub enrollment_id: EnrollmentId,
}

/// A rejection at link authority carries no session information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionCallOutcome<Reply> {
    Answered(Reply),
    Revoked,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionServiceError {
    #[error(transparent)]
    Store(#[from] StoreFailure),
    #[error("the session time is outside the supported range")]
    TimeOutOfRange,
    #[error("the supplied session ID source returned an invalid UUIDv7")]
    InvalidSessionId,
    #[error("the session secret source is unavailable")]
    SecretUnavailable,
    #[error("the request's create contents could not be digested")]
    InvalidCreateContents,
    #[error("the session transition refused the applying facts")]
    Machine(TransitionError<SessionMachineRefusal>),
}

/// Supplied by the applying owner from its secure random source.
pub trait SessionSecretSource {
    fn new_session_secret(&mut self) -> Result<SessionSecret, SessionServiceError>;
}

/// A host owns the transaction and commits only an Ok result.
pub struct SessionService<'a, Transaction> {
    repository: SessionRepository<'a, Transaction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedSession {
    pub session_id: CentralSessionId,
    pub machine_id: MachineId,
    pub enrollment_id: EnrollmentId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveSessionFacts {
    Owner,
    Delegate { parent: ScopedSession },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionAvailability {
    Unavailable,
    Live(LiveSessionFacts),
}

impl<Transaction: OrganizationTransaction> SessionService<'_, Transaction>
where
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    Option<String>: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    Option<i64>: Type<Transaction::Database> + for<'r> Decode<'r, Transaction::Database>,
    i64: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    for<'r> &'r str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    pub fn in_transaction(transaction: &mut Transaction) -> SessionService<'_, Transaction> {
        SessionService {
            repository: SessionRepository { tx: transaction },
        }
    }

    pub async fn create(
        &mut self,
        connection: &ValidatedMachineConnection,
        request: CreateFleetSession,
        context: &mut Context<'_>,
        secrets: &mut impl SessionSecretSource,
    ) -> Result<SessionCallOutcome<CreateFleetSessionReply>, SessionServiceError> {
        if self.repository.tx.organization_id() != connection.organization_id() {
            return Ok(SessionCallOutcome::Revoked);
        }
        let caller = &SessionLookupScope {
            machine_id: connection.machine_id().clone(),
            enrollment_id: connection.enrollment_id().clone(),
        };
        if !self.is_current(caller).await? {
            return Ok(SessionCallOutcome::Revoked);
        }
        let creator = match self
            .admit_creator(caller, &request.creator, context.now)
            .await?
        {
            Creator::Admitted(creator) => creator,
            Creator::NoSecret => {
                return Ok(SessionCallOutcome::Answered(
                    CreateFleetSessionReply::NoSecret {},
                ));
            }
            Creator::NoSuchSession => {
                return Ok(SessionCallOutcome::Answered(
                    CreateFleetSessionReply::NoSuchSession {},
                ));
            }
            Creator::SessionEnded => {
                return Ok(SessionCallOutcome::Answered(
                    CreateFleetSessionReply::SessionEnded {},
                ));
            }
            Creator::WrongSecret => {
                return Ok(SessionCallOutcome::Answered(
                    CreateFleetSessionReply::WrongSecret {},
                ));
            }
        };
        let prior = self
            .repository
            .by_operation(&caller.machine_id, &request.operation_id)
            .await?;
        let digest = request
            .contents_digest()
            .map_err(|_| SessionServiceError::InvalidCreateContents)?;
        let historical = self.historical_end(caller, &prior).await?;
        let first = if prior == SessionState::Missing {
            Some(Box::new(match creator.allows(&request.session_kind) {
                Ok(()) => NewSessionAdmission::Permitted(SessionReservation {
                    session_id: CentralSessionId::try_from(context.ids.next_id().to_string())
                        .map_err(|_| SessionServiceError::InvalidSessionId)?,
                    machine_id: caller.machine_id.clone(),
                    enrollment_id: caller.enrollment_id.clone(),
                    operation_id: request.operation_id,
                    contents_digest: digest.clone(),
                    creator: creator.identity(),
                    agent_program: request.agent_program,
                    kind: request.session_kind,
                    reserved_until: reserve_until(context.now)?,
                }),
                Err(reason) => NewSessionAdmission::Refused(reason),
            }))
        } else {
            None
        };
        let issue_secret = matches!(first.as_deref(), Some(NewSessionAdmission::Permitted(_)))
            || matches!(prior.read_at(context.now, historical),
                SessionState::Reserved(reserved) if reserved.allows_rotation(&digest, request.attempt));
        let candidate_secret = issue_secret
            .then(|| secrets.new_session_secret())
            .transpose()?;
        let receipt = self
            .apply(
                prior,
                SessionEvent::Create {
                    caller: SessionCreatorAdmission::Admitted,
                    first,
                    attempt: request.attempt,
                    contents_digest: digest,
                    candidate_secret,
                },
                historical,
                context,
            )
            .await?;
        match receipt {
            SessionReceipt::Create(reply) => Ok(SessionCallOutcome::Answered(reply)),
            SessionReceipt::Bind(_) | SessionReceipt::End(_) => {
                Err(corrupt("actor_sessions.create_receipt").into())
            }
        }
    }

    /// Applies Bind using the validated connection from Machine Login.
    ///
    /// A lookup scope containing only IDs cannot issue a command.
    ///
    /// ```compile_fail,E0308
    /// use fleet_actor_identity::{OrganizationTransaction, SessionLookupScope, SessionService};
    /// use fleet_identity_protocol::{BindFleetSession, OrganizationId};
    /// use central_engine_model::Context;
    /// use fleet_lent_transaction::CentralTransaction;
    /// use sqlx::{Sqlite, SqliteConnection};
    /// struct Lender<'a>(&'a mut SqliteConnection, OrganizationId);
    /// impl CentralTransaction for Lender<'_> {
    ///     type Database = Sqlite;
    ///     fn executor(&mut self) -> impl sqlx::Executor<'_, Database = Sqlite> { &mut *self.0 }
    ///     fn rows_affected(result: sqlx::sqlite::SqliteQueryResult) -> u64 { result.rows_affected() }
    /// }
    /// impl OrganizationTransaction for Lender<'_> {
    ///     fn organization_id(&self) -> &OrganizationId { &self.1 }
    /// }
    /// async fn apply_from_plain_fields(lender: &mut Lender<'_>, scope: &SessionLookupScope,
    ///                                 request: BindFleetSession, context: &mut Context<'_>) {
    ///     SessionService::in_transaction(lender).bind(scope, request, context).await;
    /// }
    /// ```
    pub async fn bind(
        &mut self,
        connection: &ValidatedMachineConnection,
        request: BindFleetSession,
        context: &mut Context<'_>,
    ) -> Result<SessionCallOutcome<BindFleetSessionReply>, SessionServiceError> {
        if self.repository.tx.organization_id() != connection.organization_id() {
            return Ok(SessionCallOutcome::Revoked);
        }
        let caller = &SessionLookupScope {
            machine_id: connection.machine_id().clone(),
            enrollment_id: connection.enrollment_id().clone(),
        };
        if !self.is_current(caller).await? {
            return Ok(SessionCallOutcome::Revoked);
        }
        let prior = self
            .repository
            .by_id(&caller.machine_id, &request.session_id)
            .await?;
        let historical = self.historical_end(caller, &prior).await?;
        let address = if let SessionState::Reserved(reserved) =
            prior.read_at(context.now, historical)
            && reserved.secret_hash.matches(&request.secret)
            && self
                .repository
                .at_address(&caller.machine_id, &request.harness_session_id)
                .await?
                .is_some()
        {
            AddressAvailability::Taken
        } else {
            AddressAvailability::Free
        };
        let receipt = self
            .apply(
                prior,
                SessionEvent::Bind {
                    harness_session_id: request.harness_session_id,
                    secret: request.secret,
                    address,
                },
                historical,
                context,
            )
            .await?;
        match receipt {
            SessionReceipt::Bind(reply) => Ok(SessionCallOutcome::Answered(reply)),
            SessionReceipt::Create(_) | SessionReceipt::End(_) => {
                Err(corrupt("actor_sessions.bind_receipt").into())
            }
        }
    }

    pub async fn end(
        &mut self,
        connection: &ValidatedMachineConnection,
        request: EndFleetSession,
        context: &mut Context<'_>,
    ) -> Result<SessionCallOutcome<EndFleetSessionReply>, SessionServiceError> {
        if self.repository.tx.organization_id() != connection.organization_id() {
            return Ok(SessionCallOutcome::Revoked);
        }
        let caller = &SessionLookupScope {
            machine_id: connection.machine_id().clone(),
            enrollment_id: connection.enrollment_id().clone(),
        };
        if !self.is_current(caller).await? {
            return Ok(SessionCallOutcome::Revoked);
        }
        let (prior, by) = match request.target {
            EndSessionTarget::CentralId { session_id } => (
                self.repository
                    .by_id(&caller.machine_id, &session_id)
                    .await?,
                SessionEndIdentity::CentralId,
            ),
            EndSessionTarget::OperationId {
                operation_id,
                contents_digest,
            } => (
                self.repository
                    .by_operation(&caller.machine_id, &operation_id)
                    .await?,
                SessionEndIdentity::OperationDigest(contents_digest),
            ),
        };
        let historical = self.historical_end(caller, &prior).await?;
        let receipt = self
            .apply(
                prior,
                SessionEvent::End {
                    by,
                    cause: request.cause,
                },
                historical,
                context,
            )
            .await?;
        match receipt {
            SessionReceipt::End(reply) => Ok(SessionCallOutcome::Answered(reply)),
            SessionReceipt::Create(_) | SessionReceipt::Bind(_) => {
                Err(corrupt("actor_sessions.end_receipt").into())
            }
        }
    }

    pub async fn session_facts(
        &mut self,
        caller: &SessionLookupScope,
        session: &CentralSessionId,
    ) -> Result<SessionAvailability, StoreFailure> {
        let Some(bound) = self.live_session(caller, session).await? else {
            return Ok(SessionAvailability::Unavailable);
        };
        match bound.reserved.reservation.kind {
            CreateSessionKind::Ordinary {} => {
                Ok(SessionAvailability::Live(LiveSessionFacts::Owner))
            }
            CreateSessionKind::Delegate { parent } => {
                let (machine_id, enrollment_id) = self
                    .repository
                    .ordinary_parent(session, &parent, &caller.machine_id, &caller.enrollment_id)
                    .await?
                    .ok_or_else(|| corrupt("actor_delegates.parent_actor_id"))?;
                Ok(SessionAvailability::Live(LiveSessionFacts::Delegate {
                    parent: ScopedSession {
                        session_id: parent,
                        machine_id,
                        enrollment_id,
                    },
                }))
            }
        }
    }

    pub(crate) async fn live_session(
        &mut self,
        caller: &SessionLookupScope,
        session: &CentralSessionId,
    ) -> Result<Option<BoundSession>, StoreFailure> {
        let SessionState::Active(bound) = self
            .repository
            .in_enrollment(&caller.machine_id, &caller.enrollment_id, session)
            .await?
        else {
            return Ok(None);
        };
        Ok(self.is_current(caller).await?.then_some(bound))
    }

    async fn apply(
        &mut self,
        prior: SessionState,
        event: SessionEvent,
        enrollment_ended_at: Option<EngineTime>,
        context: &mut Context<'_>,
    ) -> Result<SessionReceipt, SessionServiceError> {
        let (next, mut receipts) = SessionLifecycle::transition(
            &prior,
            SessionApply {
                event,
                enrollment_ended_at,
            },
            context,
        )
        .map_err(SessionServiceError::Machine)?;
        let receipt = receipts
            .pop()
            .filter(|_| receipts.is_empty())
            .ok_or_else(|| corrupt("actor_sessions.receipt"))?;
        self.repository.persist(&prior, &next, context.now).await?;
        Ok(receipt)
    }

    async fn is_current(&mut self, caller: &SessionLookupScope) -> Result<bool, StoreFailure> {
        Ok(read_session_enrollment(
            self.repository.tx,
            &caller.machine_id,
            &caller.enrollment_id,
        )
        .await?
            == SessionEnrollmentStatus::Current)
    }

    async fn historical_end(
        &mut self,
        caller: &SessionLookupScope,
        state: &SessionState,
    ) -> Result<Option<EngineTime>, StoreFailure> {
        let Some(reservation) = state.reservation() else {
            return Ok(None);
        };
        if reservation.enrollment_id == caller.enrollment_id
            || matches!(
                state,
                SessionState::Abandoned { .. } | SessionState::Ended { .. }
            )
        {
            return Ok(None);
        }
        match read_session_enrollment(
            self.repository.tx,
            &caller.machine_id,
            &reservation.enrollment_id,
        )
        .await?
        {
            SessionEnrollmentStatus::Ended { at_millis } => {
                EngineTime::from_timestamp_millis(at_millis)
                    .filter(|time| supported_time(*time))
                    .map(Some)
                    .ok_or_else(|| corrupt("actor_sessions.enrollment_end"))
            }
            SessionEnrollmentStatus::Current | SessionEnrollmentStatus::Unavailable => {
                Err(corrupt("actor_sessions.enrollment_id"))
            }
        }
    }

    async fn admit_creator(
        &mut self,
        caller: &SessionLookupScope,
        source: &SessionCreator,
        now: EngineTime,
    ) -> Result<Creator, StoreFailure> {
        match source {
            SessionCreator::RouterOperator {} => Ok(Creator::Admitted(CreatorScope::Operator)),
            SessionCreator::Session {
                presented: PresentedSessionSecret::Nothing {},
                ..
            } => Ok(Creator::NoSecret),
            SessionCreator::Session {
                session_id,
                presented: PresentedSessionSecret::Secret { secret },
            } => {
                let state = self
                    .repository
                    .by_id(&caller.machine_id, session_id)
                    .await?;
                let historical = self.historical_end(caller, &state).await?;
                match state.read_at(now, historical) {
                    SessionState::Missing
                    | SessionState::Reserved(_)
                    | SessionState::Abandoned { .. } => Ok(Creator::NoSuchSession),
                    SessionState::Ended { .. } => Ok(Creator::SessionEnded),
                    SessionState::Active(bound) => {
                        if !bound.reserved.secret_hash.matches(secret) {
                            return Ok(Creator::WrongSecret);
                        }
                        Ok(Creator::Admitted(CreatorScope::Session {
                            session_id: session_id.clone(),
                            kind: bound.reserved.reservation.kind,
                        }))
                    }
                }
            }
        }
    }
}

enum Creator {
    Admitted(CreatorScope),
    NoSecret,
    NoSuchSession,
    SessionEnded,
    WrongSecret,
}

enum CreatorScope {
    Operator,
    Session {
        session_id: CentralSessionId,
        kind: CreateSessionKind,
    },
}

impl CreatorScope {
    fn allows(&self, proposed: &CreateSessionKind) -> Result<(), CreateSessionRefusal> {
        match (self, proposed) {
            (
                Self::Session {
                    kind: CreateSessionKind::Delegate { .. },
                    ..
                },
                _,
            ) => Err(CreateSessionRefusal::CreatorNotAllowed {}),
            (
                Self::Operator
                | Self::Session {
                    kind: CreateSessionKind::Ordinary {},
                    ..
                },
                CreateSessionKind::Ordinary {},
            ) => Ok(()),
            (
                Self::Session {
                    session_id,
                    kind: CreateSessionKind::Ordinary {},
                    ..
                },
                CreateSessionKind::Delegate { parent },
            ) if parent == session_id => Ok(()),
            (
                Self::Operator
                | Self::Session {
                    kind: CreateSessionKind::Ordinary {},
                    ..
                },
                CreateSessionKind::Delegate { .. },
            ) => Err(CreateSessionRefusal::ParentNotValid {}),
        }
    }

    fn identity(self) -> RecordedSessionCreator {
        match self {
            Self::Operator => RecordedSessionCreator::Operator,
            Self::Session { session_id, .. } => RecordedSessionCreator::Session(session_id),
        }
    }
}

fn reserve_until(now: EngineTime) -> Result<EngineTime, SessionServiceError> {
    now.timestamp_millis()
        .checked_add(RESERVATION_DURATION_MILLIS)
        .and_then(EngineTime::from_timestamp_millis)
        .filter(|deadline| supported_time(*deadline))
        .ok_or(SessionServiceError::TimeOutOfRange)
}
