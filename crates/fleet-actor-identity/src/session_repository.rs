//! Apply the session machine's row changes on a single lent transaction.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use central_engine_model::EngineTime;
use central_storage::StoreFailure;
use fleet_identity_protocol::{CreateSessionKind, EnrollmentId};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, query::query, query_as::query_as, types::Type,
};
use workspace_ids::{CentralSessionId, MachineId, SessionId, SessionOperationId};

use crate::session_row::{
    AddressRow, ParentScopeRow, StoredSessionRow, abandonment_name, end_name, parse, program_name,
};
use crate::{RecordedSessionCreator, ReservedSession, SessionState};

pub(crate) struct SessionRepository<'a, Transaction> {
    pub(crate) tx: &'a mut Transaction,
}

macro_rules! read_one {
    ($row:ident, $file:literal $(, $input:expr)* $(,)?) => {{
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file_as!($row, $file $(, $input)*);
        query_as::<_, $row>(include_str!(concat!("../", $file)))$(.bind($input))*
    }};
}

macro_rules! change_one {
    ($tx:expr, $file:literal $(, $input:expr)* $(,)?) => {{
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file!($file $(, $input)*);
        let result = query(include_str!(concat!("../", $file)))$(.bind($input))*
            .execute($tx.executor()).await.map_err(store_error)?;
        require_one::<Transaction>(result)
    }};
}

fn require_one<Transaction: CentralTransaction>(
    result: <Transaction::Database as Database>::QueryResult,
) -> Result<(), StoreFailure> {
    if Transaction::rows_affected(result) == 1 {
        Ok(())
    } else {
        Err(StoreFailure::Unavailable)
    }
}

impl<Transaction: CentralTransaction> SessionRepository<'_, Transaction>
where
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    Option<String>: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    i64: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    for<'r> &'r str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    pub(crate) async fn by_id(
        &mut self,
        machine: &MachineId,
        session: &CentralSessionId,
    ) -> Result<SessionState, StoreFailure> {
        decode_optional(
            read_one!(
                StoredSessionRow,
                "src/queries/session_by_id.sql",
                machine.as_str().to_owned(),
                session.as_str().to_owned()
            )
            .fetch_optional(self.tx.executor())
            .await,
        )
    }

    pub(crate) async fn by_operation(
        &mut self,
        machine: &MachineId,
        operation: &SessionOperationId,
    ) -> Result<SessionState, StoreFailure> {
        decode_optional(
            read_one!(
                StoredSessionRow,
                "src/queries/session_by_operation.sql",
                machine.as_str().to_owned(),
                operation.as_str().to_owned()
            )
            .fetch_optional(self.tx.executor())
            .await,
        )
    }

    pub(crate) async fn in_enrollment(
        &mut self,
        machine: &MachineId,
        enrollment: &EnrollmentId,
        session: &CentralSessionId,
    ) -> Result<SessionState, StoreFailure> {
        decode_optional(
            read_one!(
                StoredSessionRow,
                "src/queries/session_in_enrollment.sql",
                session.as_str().to_owned(),
                machine.as_str().to_owned(),
                enrollment.as_str().to_owned()
            )
            .fetch_optional(self.tx.executor())
            .await,
        )
    }

    pub(crate) async fn at_address(
        &mut self,
        machine: &MachineId,
        harness: &SessionId,
    ) -> Result<Option<CentralSessionId>, StoreFailure> {
        let row = read_one!(
            AddressRow,
            "src/queries/session_at_address.sql",
            machine.as_str().to_owned(),
            harness.as_str().to_owned()
        )
        .fetch_optional(self.tx.executor())
        .await
        .map_err(store_error)?;
        row.map(|row| parse(&row.actor_id, "actor_id")).transpose()
    }

    pub(crate) async fn scope(
        &mut self,
        session: &CentralSessionId,
    ) -> Result<Option<(MachineId, EnrollmentId)>, StoreFailure> {
        let row = read_one!(
            ParentScopeRow,
            "src/queries/parent_scope.sql",
            session.as_str().to_owned()
        )
        .fetch_optional(self.tx.executor())
        .await
        .map_err(store_error)?;
        row.map(|row| {
            Ok((
                parse(&row.machine_id, "machine_id")?,
                EnrollmentId::try_from(row.enrollment_id).map_err(|_| corrupt("enrollment_id"))?,
            ))
        })
        .transpose()
    }

    pub(crate) async fn persist(
        &mut self,
        prior: &SessionState,
        next: &SessionState,
        now: EngineTime,
    ) -> Result<(), StoreFailure> {
        use SessionState::{Abandoned, Active, Ended, Missing, Reserved};
        if prior == next {
            return Ok(());
        }
        let updated_at = format_utc_millis(now);
        match (prior, next) {
            (Missing, Reserved(reserved)) => self.insert_reservation(reserved, updated_at).await,
            (Reserved(previous), Reserved(next)) => {
                let next_attempt = checked_attempt(next)?;
                let previous_attempt = checked_attempt(previous)?;
                let secret_hash = URL_SAFE_NO_PAD.encode(next.secret_hash.0);
                let actor_id = next.reservation.session_id.as_str().to_owned();
                change_one!(
                    self.tx,
                    "src/queries/rotate_reservation_secret.sql",
                    next_attempt,
                    secret_hash,
                    updated_at,
                    actor_id,
                    next_attempt
                )
                .and_then(|()| {
                    if previous_attempt < next_attempt {
                        Ok(())
                    } else {
                        Err(corrupt("actor_sessions.create_attempt"))
                    }
                })
            }
            (Reserved(_), Active(bound)) => {
                let harness_id = bound.harness_session_id.as_str().to_owned();
                let bound_at = format_utc_millis(bound.bound_at);
                let actor_id = bound.reserved.reservation.session_id.as_str().to_owned();
                change_one!(
                    self.tx,
                    "src/queries/bind_session_address.sql",
                    harness_id,
                    bound_at,
                    updated_at,
                    actor_id
                )
            }
            (
                Reserved(_),
                Abandoned {
                    reserved,
                    cause,
                    at,
                },
            ) => {
                self.record_settlement(
                    &reserved.reservation.session_id,
                    "reserved",
                    "abandoned",
                    abandonment_name(cause),
                    *at,
                    updated_at,
                )
                .await
            }
            (Active(_), Ended { bound, cause, at }) => {
                self.record_settlement(
                    &bound.reserved.reservation.session_id,
                    "active",
                    "ended",
                    end_name(cause),
                    *at,
                    updated_at,
                )
                .await
            }
            (
                Missing | Reserved(_) | Active(_) | Abandoned { .. } | Ended { .. },
                Missing | Reserved(_) | Active(_) | Abandoned { .. } | Ended { .. },
            ) => Err(corrupt("actor_sessions.transition")),
        }
    }

    async fn insert_reservation(
        &mut self,
        reserved: &ReservedSession,
        now: String,
    ) -> Result<(), StoreFailure> {
        let reservation = &reserved.reservation;
        let actor_id = reservation.session_id.as_str().to_owned();
        let machine = reservation.machine_id.as_str().to_owned();
        let enrollment = reservation.enrollment_id.as_str().to_owned();
        let operation = reservation.operation_id.as_str().to_owned();
        let digest = reservation.contents_digest.as_str().to_owned();
        let attempt = checked_attempt(reserved)?;
        let program = program_name(&reservation.agent_program).to_owned();
        let (kind, parent) = match &reservation.kind {
            CreateSessionKind::Ordinary {} => ("ordinary".to_owned(), None),
            CreateSessionKind::Delegate { parent } => {
                ("delegate".to_owned(), Some(parent.as_str().to_owned()))
            }
        };
        let (creator_kind, creator) = match &reservation.creator {
            RecordedSessionCreator::Operator => ("operator".to_owned(), None),
            RecordedSessionCreator::Session(creator) => {
                ("session".to_owned(), Some(creator.as_str().to_owned()))
            }
        };
        let secret_hash = URL_SAFE_NO_PAD.encode(reserved.secret_hash.0);
        let reserved_until = format_utc_millis(reservation.reserved_until);
        change_one!(
            self.tx,
            "src/queries/insert_session_actor.sql",
            actor_id.clone(),
            now.clone(),
            now.clone()
        )?;
        change_one!(
            self.tx,
            "src/queries/insert_reserved_session.sql",
            actor_id.clone(),
            machine,
            enrollment,
            operation,
            digest,
            attempt,
            program,
            kind,
            creator_kind,
            creator,
            secret_hash,
            reserved_until,
            now.clone(),
            now.clone()
        )?;
        if let Some(parent) = parent {
            change_one!(
                self.tx,
                "src/queries/insert_delegate.sql",
                actor_id,
                parent,
                now.clone(),
                now
            )?;
        }
        Ok(())
    }

    async fn record_settlement(
        &mut self,
        session: &CentralSessionId,
        previous_state: &str,
        final_state: &str,
        cause: &str,
        fact_at: EngineTime,
        updated_at: String,
    ) -> Result<(), StoreFailure> {
        let settled_at = format_utc_millis(fact_at);
        change_one!(
            self.tx,
            "src/queries/record_session_settlement.sql",
            final_state.to_owned(),
            cause.to_owned(),
            settled_at,
            updated_at,
            session.as_str().to_owned(),
            previous_state.to_owned()
        )
    }
}

fn checked_attempt(reserved: &ReservedSession) -> Result<i64, StoreFailure> {
    i64::try_from(reserved.attempt.get()).map_err(|_| corrupt("actor_sessions.create_attempt"))
}

fn decode_optional(
    result: Result<Option<StoredSessionRow>, sqlx_core::error::Error>,
) -> Result<SessionState, StoreFailure> {
    result
        .map_err(store_error)?
        .map_or(Ok(SessionState::Missing), StoredSessionRow::decode)
}

fn store_error(error: sqlx_core::error::Error) -> StoreFailure {
    match error {
        sqlx_core::error::Error::ColumnNotFound(column)
        | sqlx_core::error::Error::ColumnDecode { index: column, .. } => corrupt(&column),
        _ => StoreFailure::Unavailable,
    }
}
