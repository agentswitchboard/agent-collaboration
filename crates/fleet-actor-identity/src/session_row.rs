//! Decode named storage columns into checked lifecycle facts.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use central_engine_model::{EngineTime, supported_time};
use central_storage::{CorruptReason, StoreFailure};
use fleet_identity_protocol::{
    AbandonedSessionCause, CreateSessionAttempt, CreateSessionDigest, CreateSessionKind,
    EndedSessionCause, EnrollmentId, HarnessKind, SessionTime,
};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_ids::{CentralSessionId, MachineId, SessionId, SessionOperationId};

use crate::{
    BoundSession, RecordedSessionCreator, ReservedSession, SessionReservation, SessionSecretHash,
    SessionState,
};

#[derive(sqlx::FromRow)]
pub(crate) struct StoredSessionRow {
    pub(super) actor_id: String,
    pub(super) machine_id: String,
    pub(super) enrollment_id: String,
    pub(super) harness_session_id: Option<String>,
    pub(super) create_operation_id: String,
    pub(super) create_contents_digest: String,
    pub(super) create_attempt: i64,
    pub(super) agent_program: String,
    pub(super) session_kind: String,
    pub(super) creator_kind: String,
    pub(super) creator_actor_id: Option<String>,
    pub(super) secret_hash: String,
    pub(super) session_state: String,
    pub(super) reserved_until: String,
    pub(super) bound_at: Option<String>,
    pub(super) settled_cause: Option<String>,
    pub(super) settled_at: Option<String>,
    pub(super) parent_actor_id: Option<String>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct AddressRow {
    pub actor_id: String,
}

impl StoredSessionRow {
    pub(crate) fn decode(self) -> Result<SessionState, StoreFailure> {
        let creator = match (self.creator_kind.as_str(), self.creator_actor_id.as_deref()) {
            ("operator", None) => RecordedSessionCreator::Operator,
            ("session", Some(actor_id)) => {
                RecordedSessionCreator::Session(parse(actor_id, "creator_actor_id")?)
            }
            ("operator" | "session", _) => return Err(corrupt("creator_actor_id")),
            _ => return Err(unknown("creator_kind")),
        };
        let kind = match (self.session_kind.as_str(), self.parent_actor_id.as_deref()) {
            ("ordinary", None) => CreateSessionKind::Ordinary {},
            ("delegate", Some(parent)) => {
                let parent: CentralSessionId = parse(parent, "parent_actor_id")?;
                if creator != RecordedSessionCreator::Session(parent.clone()) {
                    return Err(corrupt("parent_actor_id"));
                }
                CreateSessionKind::Delegate { parent }
            }
            ("ordinary" | "delegate", _) => return Err(corrupt("parent_actor_id")),
            _ => return Err(unknown("session_kind")),
        };
        let session_id = parse::<CentralSessionId>(&self.actor_id, "actor_id")?;
        if matches!(&kind, CreateSessionKind::Delegate { parent } if parent == &session_id)
            || matches!(&creator, RecordedSessionCreator::Session(creator_id) if creator_id == &session_id)
        {
            return Err(corrupt("creator_actor_id"));
        }
        let reservation = SessionReservation {
            session_id,
            machine_id: parse::<MachineId>(&self.machine_id, "machine_id")?,
            enrollment_id: EnrollmentId::try_from(self.enrollment_id)
                .map_err(|_| corrupt("enrollment_id"))?,
            operation_id: parse::<SessionOperationId>(
                &self.create_operation_id,
                "create_operation_id",
            )?,
            contents_digest: CreateSessionDigest::try_from(self.create_contents_digest)
                .map_err(|_| corrupt("create_contents_digest"))?,
            creator,
            agent_program: decode_program(&self.agent_program)?,
            kind,
            reserved_until: decode_time(self.reserved_until)?,
        };
        let attempt = u64::try_from(self.create_attempt)
            .ok()
            .and_then(|attempt| CreateSessionAttempt::try_from(attempt).ok())
            .ok_or_else(|| corrupt("create_attempt"))?;
        let decoded_hash = URL_SAFE_NO_PAD
            .decode(self.secret_hash)
            .map_err(|_| corrupt("secret_hash"))?;
        let reserved = ReservedSession {
            reservation,
            attempt,
            secret_hash: SessionSecretHash(
                decoded_hash
                    .try_into()
                    .map_err(|_| corrupt("secret_hash"))?,
            ),
        };
        match (
            self.session_state.as_str(),
            self.harness_session_id,
            self.bound_at,
            self.settled_cause,
            self.settled_at,
        ) {
            ("reserved", None, None, None, None) => Ok(SessionState::Reserved(reserved)),
            ("active", Some(harness), Some(bound_at), None, None) => Ok(SessionState::Active(
                decode_bound(reserved, &harness, bound_at)?,
            )),
            ("abandoned", None, None, Some(cause), Some(at)) => Ok(SessionState::Abandoned {
                reserved,
                cause: decode_abandonment(&cause)?,
                at: decode_time(at)?,
            }),
            ("ended", Some(harness), Some(bound_at), Some(cause), Some(at)) => {
                Ok(SessionState::Ended {
                    bound: decode_bound(reserved, &harness, bound_at)?,
                    cause: decode_end(&cause)?,
                    at: decode_time(at)?,
                })
            }
            ("reserved" | "active" | "abandoned" | "ended", _, _, _, _) => {
                Err(corrupt("session_state"))
            }
            _ => Err(unknown("session_state")),
        }
    }
}

fn decode_bound(
    reserved: ReservedSession,
    harness: &str,
    bound_at: String,
) -> Result<BoundSession, StoreFailure> {
    let bound_at = decode_time(bound_at)?;
    if bound_at >= reserved.reservation.reserved_until {
        return Err(corrupt("actor_sessions.bound_at"));
    }
    Ok(BoundSession {
        reserved,
        harness_session_id: parse::<SessionId>(harness, "harness_session_id")?,
        bound_at,
    })
}

fn decode_time(raw: String) -> Result<EngineTime, StoreFailure> {
    let timestamp = SessionTime::try_from(raw).map_err(|_| corrupt("actor_sessions.timestamp"))?;
    timestamp
        .as_str()
        .parse::<EngineTime>()
        .ok()
        .filter(|time| supported_time(*time))
        .ok_or_else(|| corrupt("actor_sessions.timestamp"))
}

pub(crate) fn parse<Value: std::str::FromStr>(
    raw: &str,
    field: &str,
) -> Result<Value, StoreFailure> {
    raw.parse().map_err(|_| corrupt(field))
}

fn unknown(column: &str) -> StoreFailure {
    StoreFailure::Corrupt {
        reason: CorruptReason::UnknownKind {
            column: column.to_owned(),
        },
    }
}

fn decode_program(raw: &str) -> Result<HarnessKind, StoreFailure> {
    match raw {
        "codex" => Ok(HarnessKind::Codex),
        "claude" => Ok(HarnessKind::Claude),
        "cursor" => Ok(HarnessKind::Cursor),
        _ => Err(unknown("agent_program")),
    }
}

pub(crate) fn program_name(program: &HarnessKind) -> &'static str {
    match program {
        HarnessKind::Codex => "codex",
        HarnessKind::Claude => "claude",
        HarnessKind::Cursor => "cursor",
    }
}

fn decode_abandonment(raw: &str) -> Result<AbandonedSessionCause, StoreFailure> {
    match raw {
        "given_up" => Ok(AbandonedSessionCause::GivenUp {}),
        "revoked" => Ok(AbandonedSessionCause::Revoked {}),
        "machine_revoked" => Ok(AbandonedSessionCause::MachineRevoked {}),
        "time_ran_out" => Ok(AbandonedSessionCause::TimeRanOut {}),
        _ => Err(unknown("settled_cause")),
    }
}

pub(crate) fn abandonment_name(cause: &AbandonedSessionCause) -> &'static str {
    match cause {
        AbandonedSessionCause::GivenUp {} => "given_up",
        AbandonedSessionCause::Revoked {} => "revoked",
        AbandonedSessionCause::MachineRevoked {} => "machine_revoked",
        AbandonedSessionCause::TimeRanOut {} => "time_ran_out",
    }
}

fn decode_end(raw: &str) -> Result<EndedSessionCause, StoreFailure> {
    match raw {
        "ended_for_good" => Ok(EndedSessionCause::EndedForGood {}),
        "given_up" => Ok(EndedSessionCause::GivenUp {}),
        "revoked" => Ok(EndedSessionCause::Revoked {}),
        "machine_revoked" => Ok(EndedSessionCause::MachineRevoked {}),
        _ => Err(unknown("settled_cause")),
    }
}

pub(crate) fn end_name(cause: &EndedSessionCause) -> &'static str {
    match cause {
        EndedSessionCause::EndedForGood {} => "ended_for_good",
        EndedSessionCause::GivenUp {} => "given_up",
        EndedSessionCause::Revoked {} => "revoked",
        EndedSessionCause::MachineRevoked {} => "machine_revoked",
    }
}
