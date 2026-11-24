use central_engine_model::{
    Context, EngineTime, Machine, MachineKind, Transition, TransitionError, TransitionResult,
    supported_time,
};
use chrono::SecondsFormat;
use fleet_identity_protocol::{
    AbandonedSessionCause, BindFleetSessionReply, CreateFleetSessionReply, CreateSessionAttempt,
    CreateSessionDigest, EndFleetSessionReply, EndedSessionCause, NewAbandonmentCause,
    NewEndSettlement, RouterEndCause, SessionSecret, SessionSettlement, SessionTime,
};
use workspace_ids::{SessionId, SessionRef};

use crate::{
    AddressAvailability, BoundSession, NewSessionAdmission, ReservedSession, SessionApply,
    SessionCreatorAdmission, SessionEndIdentity, SessionEvent, SessionMachineRefusal,
    SessionReceipt, SessionSecretHash, SessionState,
};

pub struct SessionLifecycle;

type SessionChange = TransitionResult<SessionLifecycle>;
type Failure = TransitionError<SessionMachineRefusal>;

impl Machine for SessionLifecycle {
    const KIND: MachineKind = MachineKind("actor_session");
    type State = SessionState;
    type Event = SessionApply;
    type Effect = SessionReceipt;
    type Error = SessionMachineRefusal;

    fn transition(
        state: &SessionState,
        apply: SessionApply,
        context: &mut Context<'_>,
    ) -> SessionChange {
        let now = context.now;
        if !supported_time(now)
            || apply
                .enrollment_ended_at
                .is_some_and(|ended| !supported_time(ended))
        {
            return Err(TransitionError::Domain(
                SessionMachineRefusal::TimeOutOfRange,
            ));
        }
        match apply.event {
            SessionEvent::Create {
                caller,
                first,
                contents_digest,
                attempt,
                candidate_secret,
            } => create(
                state,
                caller,
                first,
                &contents_digest,
                attempt,
                candidate_secret,
                now,
                apply.enrollment_ended_at,
            ),
            SessionEvent::Bind {
                harness_session_id,
                secret,
                address,
            } => bind(
                state,
                harness_session_id,
                &secret,
                address,
                now,
                apply.enrollment_ended_at,
            ),
            SessionEvent::End { by, cause } => {
                end(state, &by, cause, now, apply.enrollment_ended_at)
            }
        }
    }

    fn transitions() -> &'static [Transition] {
        crate::session_edges::TRANSITIONS
    }

    fn next_due(_: &Self::State) -> Option<EngineTime> {
        None
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the reducer applies explicitly supplied Create facts"
)]
fn create(
    state: &SessionState,
    caller: SessionCreatorAdmission,
    first: Option<NewSessionAdmission>,
    digest: &CreateSessionDigest,
    attempt: CreateSessionAttempt,
    secret: Option<SessionSecret>,
    now: EngineTime,
    enrollment_ended_at: Option<EngineTime>,
) -> SessionChange {
    let refusal = match caller {
        SessionCreatorAdmission::Admitted => None,
        SessionCreatorAdmission::NoSecret => Some(CreateFleetSessionReply::NoSecret {}),
        SessionCreatorAdmission::NoSuchSession => Some(CreateFleetSessionReply::NoSuchSession {}),
        SessionCreatorAdmission::SessionEnded => Some(CreateFleetSessionReply::SessionEnded {}),
        SessionCreatorAdmission::WrongSecret => Some(CreateFleetSessionReply::WrongSecret {}),
    };
    if let Some(reply) = refusal {
        return unchanged(state, SessionReceipt::Create(reply));
    }
    if state
        .reservation()
        .is_some_and(|reserved| &reserved.contents_digest != digest)
    {
        return unchanged(
            state,
            SessionReceipt::Create(CreateFleetSessionReply::Conflict {}),
        );
    }
    match state.read_at(now, enrollment_ended_at) {
        SessionState::Missing => match first.ok_or(TransitionError::Domain(
            SessionMachineRefusal::MissingCreateAdmission,
        ))? {
            NewSessionAdmission::Permitted(reservation) => {
                if &reservation.contents_digest != digest
                    || reservation.reserved_until <= now
                    || !supported_time(reservation.reserved_until)
                {
                    return Err(TransitionError::Domain(
                        SessionMachineRefusal::ContradictoryReservation,
                    ));
                }
                let secret = secret.ok_or(TransitionError::Domain(
                    SessionMachineRefusal::MissingCandidateSecret,
                ))?;
                issue(
                    ReservedSession {
                        reservation: *reservation,
                        attempt,
                        secret_hash: SessionSecretHash::from_secret(&secret)
                            .map_err(TransitionError::Domain)?,
                    },
                    secret,
                )
            }
            NewSessionAdmission::Refused(reason) => unchanged(
                state,
                SessionReceipt::Create(CreateFleetSessionReply::Refused { reason }),
            ),
        },
        SessionState::Reserved(mut reserved) if reserved.allows_rotation(digest, attempt) => {
            let secret = secret.ok_or(TransitionError::Domain(
                SessionMachineRefusal::MissingCandidateSecret,
            ))?;
            reserved.attempt = attempt;
            reserved.secret_hash =
                SessionSecretHash::from_secret(&secret).map_err(TransitionError::Domain)?;
            issue(reserved, secret)
        }
        SessionState::Reserved(_) => unchanged(
            state,
            SessionReceipt::Create(CreateFleetSessionReply::Superseded {}),
        ),
        SessionState::Active(bound) => unchanged(
            state,
            SessionReceipt::Create(CreateFleetSessionReply::Active {
                session_id: bound.reserved.reservation.session_id.clone(),
                address: address_of(&bound),
            }),
        ),
        SessionState::Abandoned {
            reserved,
            cause,
            at,
        } => {
            let settlement = SessionSettlement::Abandoned {
                cause,
                time: wire_time(at)?,
            };
            let next = if reserved.attempt == attempt {
                state.clone()
            } else {
                state.read_at(now, enrollment_ended_at)
            };
            Ok((
                next,
                vec![SessionReceipt::Create(CreateFleetSessionReply::Settled {
                    settlement,
                })],
            ))
        }
        SessionState::Ended { bound, cause, at } => {
            let settlement = SessionSettlement::Ended {
                cause,
                time: wire_time(at)?,
            };
            let next = if bound.reserved.attempt == attempt {
                state.clone()
            } else {
                state.read_at(now, enrollment_ended_at)
            };
            Ok((
                next,
                vec![SessionReceipt::Create(CreateFleetSessionReply::Settled {
                    settlement,
                })],
            ))
        }
    }
}

fn issue(reserved: ReservedSession, secret: SessionSecret) -> SessionChange {
    let reply = CreateFleetSessionReply::Reserved {
        session_id: reserved.reservation.session_id.clone(),
        secret,
        reserved_until: wire_time(reserved.reservation.reserved_until)?,
        workspace: reserved.reservation.project.workspace.clone(),
    };
    Ok((
        SessionState::Reserved(reserved),
        vec![SessionReceipt::Create(reply)],
    ))
}

fn bind(
    state: &SessionState,
    harness_session_id: SessionId,
    secret: &SessionSecret,
    address: AddressAvailability,
    now: EngineTime,
    enrollment_ended_at: Option<EngineTime>,
) -> SessionChange {
    let effective = state.read_at(now, enrollment_ended_at);
    if let Some(settlement) = settlement_of(&effective)? {
        return Ok((
            effective,
            vec![SessionReceipt::Bind(BindFleetSessionReply::Settled {
                settlement,
            })],
        ));
    }
    match effective {
        SessionState::Reserved(reserved) if !reserved.secret_hash.matches(secret) => unchanged(
            state,
            SessionReceipt::Bind(BindFleetSessionReply::WrongSecret {}),
        ),
        SessionState::Active(bound) if !bound.reserved.secret_hash.matches(secret) => unchanged(
            state,
            SessionReceipt::Bind(BindFleetSessionReply::WrongSecret {}),
        ),
        SessionState::Missing => unchanged(
            state,
            SessionReceipt::Bind(BindFleetSessionReply::NoSuchStart {}),
        ),
        SessionState::Reserved(reserved) => match address {
            AddressAvailability::Taken => unchanged(
                state,
                SessionReceipt::Bind(BindFleetSessionReply::AddressTaken {}),
            ),
            AddressAvailability::Free => {
                let bound = BoundSession {
                    reserved,
                    harness_session_id,
                    bound_at: now,
                };
                let reply = BindFleetSessionReply::Active {
                    address: address_of(&bound),
                };
                Ok((
                    SessionState::Active(bound),
                    vec![SessionReceipt::Bind(reply)],
                ))
            }
        },
        SessionState::Active(bound) => {
            let reply = if bound.harness_session_id == harness_session_id {
                BindFleetSessionReply::Active {
                    address: address_of(&bound),
                }
            } else {
                BindFleetSessionReply::AlreadyBoundToDifferentId {}
            };
            unchanged(state, SessionReceipt::Bind(reply))
        }
        SessionState::Abandoned { .. } | SessionState::Ended { .. } => Err(
            TransitionError::Domain(SessionMachineRefusal::ContradictoryReservation),
        ),
    }
}

fn end(
    state: &SessionState,
    by: &SessionEndIdentity,
    cause: RouterEndCause,
    now: EngineTime,
    enrollment_ended_at: Option<EngineTime>,
) -> SessionChange {
    let Some(reservation) = state.reservation() else {
        return unchanged(
            state,
            SessionReceipt::End(EndFleetSessionReply::NoSuchStart {}),
        );
    };
    if let SessionEndIdentity::OperationDigest(digest) = by
        && digest != &reservation.contents_digest
    {
        return unchanged(
            state,
            SessionReceipt::End(EndFleetSessionReply::NotYoursToEnd {}),
        );
    }
    let effective = state.read_at(now, enrollment_ended_at);
    if let Some(settlement) = settlement_of(&effective)? {
        return Ok((
            effective,
            vec![SessionReceipt::End(EndFleetSessionReply::AlreadySettled {
                settlement,
            })],
        ));
    }
    let (next, settlement) = match effective {
        SessionState::Reserved(reserved) => (
            SessionState::Abandoned {
                reserved,
                cause: AbandonedSessionCause::GivenUp {},
                at: now,
            },
            NewEndSettlement::Abandoned {
                cause: NewAbandonmentCause::GivenUp {},
                time: wire_time(now)?,
            },
        ),
        SessionState::Active(_) if matches!(by, SessionEndIdentity::OperationDigest(_)) => {
            return unchanged(
                state,
                SessionReceipt::End(EndFleetSessionReply::NotYoursToEnd {}),
            );
        }
        SessionState::Active(bound) => {
            let stored_cause = match &cause {
                RouterEndCause::EndedForGood {} => EndedSessionCause::EndedForGood {},
                RouterEndCause::GivenUp {} => EndedSessionCause::GivenUp {},
            };
            (
                SessionState::Ended {
                    bound,
                    cause: stored_cause,
                    at: now,
                },
                NewEndSettlement::Ended {
                    cause,
                    time: wire_time(now)?,
                },
            )
        }
        SessionState::Missing | SessionState::Abandoned { .. } | SessionState::Ended { .. } => {
            return Err(TransitionError::Domain(
                SessionMachineRefusal::ContradictoryReservation,
            ));
        }
    };
    Ok((
        next,
        vec![SessionReceipt::End(EndFleetSessionReply::Settled {
            settlement,
        })],
    ))
}

fn address_of(bound: &BoundSession) -> SessionRef {
    SessionRef {
        machine_id: bound.reserved.reservation.machine_id.clone(),
        session_id: bound.harness_session_id.clone(),
    }
}

pub(crate) fn wire_time(time: EngineTime) -> Result<SessionTime, Failure> {
    SessionTime::try_from(time.to_rfc3339_opts(SecondsFormat::Millis, true))
        .map_err(|_| TransitionError::Domain(SessionMachineRefusal::TimeOutOfRange))
}

fn settlement_of(state: &SessionState) -> Result<Option<SessionSettlement>, Failure> {
    match state {
        SessionState::Abandoned { cause, at, .. } => Ok(Some(SessionSettlement::Abandoned {
            cause: cause.clone(),
            time: wire_time(*at)?,
        })),
        SessionState::Ended { cause, at, .. } => Ok(Some(SessionSettlement::Ended {
            cause: cause.clone(),
            time: wire_time(*at)?,
        })),
        SessionState::Missing | SessionState::Reserved(_) | SessionState::Active(_) => Ok(None),
    }
}

fn unchanged(state: &SessionState, receipt: SessionReceipt) -> SessionChange {
    Ok((state.clone(), vec![receipt]))
}

#[cfg(test)]
#[path = "session_machine_test.rs"]
mod tests;
