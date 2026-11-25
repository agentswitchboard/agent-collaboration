use fleet_identity_protocol::{CreateSessionAttempt, HarnessKind, SessionSettlement, SessionTime};
use state_machine_core::StateMachine;
use switchboard_session_start::{
    FirstInputId, FirstInputIntent, IssuedSessionProof, MakePlan, SealedSessionSecretRef,
    StartEffect, StartEvent, StartMachine, StartProgress, StartScope, StartSnapshot,
};
use workspace_ids::{CentralSessionId, SessionId, SessionRef};

pub(super) fn scope() -> StartScope {
    StartScope {
        operation_id: "019c0000-0000-7000-8000-000000000001".parse().unwrap(),
        organization: "019c0000-0000-7000-8000-000000000002"
            .to_owned()
            .try_into()
            .unwrap(),
        machine: "019c0000-0000-7000-8000-000000000003".parse().unwrap(),
        enrollment: "019c0000-0000-7000-8000-000000000004"
            .to_owned()
            .try_into()
            .unwrap(),
    }
}

pub(super) fn central_id() -> CentralSessionId {
    "019c0000-0000-7000-8000-000000000005".parse().unwrap()
}

pub(super) fn harness_id() -> SessionId {
    "harness-session-1".parse().unwrap()
}

pub(super) fn address() -> SessionRef {
    SessionRef {
        machine_id: scope().machine,
        session_id: harness_id(),
    }
}

pub(super) fn time() -> SessionTime {
    "2026-11-24T19:30:00.000Z".to_owned().try_into().unwrap()
}

pub(super) fn settlement() -> SessionSettlement {
    SessionSettlement::Ended {
        cause: fleet_identity_protocol::EndedSessionCause::EndedForGood {},
        time: time(),
    }
}

pub(super) fn attempt(value: u64) -> CreateSessionAttempt {
    value.try_into().unwrap()
}

pub(super) fn issue(value: u64) -> IssuedSessionProof {
    IssuedSessionProof {
        secret: SealedSessionSecretRef {
            start_id: scope().operation_id,
            central_id: central_id(),
            attempt: attempt(value),
        },
        reserved_until: time(),
    }
}

pub(super) fn from_progress(progress: StartProgress) -> StartSnapshot {
    StartSnapshot::restore(
        scope(),
        HarnessKind::Claude,
        MakePlan::HeldOnLaunch,
        None,
        progress,
    )
    .unwrap()
}

pub(super) fn first_input() -> FirstInputIntent {
    FirstInputIntent {
        input_id: FirstInputId("019c0000-0000-7000-8000-000000000006".parse().unwrap()),
        fingerprint: [5; 32],
    }
}

pub(super) fn native_snapshot(progress: StartProgress) -> StartSnapshot {
    StartSnapshot::restore(
        scope(),
        HarnessKind::Codex,
        MakePlan::FirstInputRequired(first_input()),
        None,
        progress,
    )
    .unwrap()
}

/// The driver contract under test. The next snapshot is eligible to persist before these effects.
pub(super) fn step(
    snapshot: &StartSnapshot,
    event: StartEvent,
    effects: &[StartEffect],
) -> StartSnapshot {
    let (next, actual_effects) = StartMachine::transition(snapshot, event).unwrap();
    assert_eq!(actual_effects, effects);
    next
}
