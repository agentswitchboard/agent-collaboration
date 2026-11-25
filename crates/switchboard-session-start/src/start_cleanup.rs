//! Settlement of the held conversation and its exact Central work.

use state_machine_core::TransitionError;
use workspace_ids::{CentralSessionId, SessionId};

use crate::{
    CentralCloseProgress, CentralEndObservation, CentralEndUnavailable, ClosingReason,
    ClosingSession, EndCallCertainty, EndOutcome, EndedCentralSession, HarnessCloseObservation,
    HarnessCloseProgress, StartEffect, StartProgress, StartReadiness, StartSnapshot,
};

use super::start_events::{MachineResult, move_to, wrong_phase};

pub(super) fn begin_closing(
    snapshot: &StartSnapshot,
    harness: &SessionId,
    reason: ClosingReason,
    central_close: CentralCloseProgress,
) -> MachineResult {
    let closing = ClosingSession {
        harness: harness.clone(),
        reason,
        harness_close: HarnessCloseProgress::Pending,
        central_close,
    };
    move_to(
        snapshot,
        StartProgress::Closing(closing),
        vec![
            StartEffect::RetireAdmission,
            StartEffect::CloseExact(harness.clone()),
        ],
    )
}

pub(super) fn resume_closing(
    snapshot: &StartSnapshot,
    closing: &ClosingSession,
    readiness: StartReadiness,
) -> MachineResult {
    let mut closing = closing.clone();
    if closing.harness_close == HarnessCloseProgress::AnswerLost {
        closing.harness_close = HarnessCloseProgress::Pending;
    }
    if let CentralCloseProgress::Owed {
        secret, purpose, ..
    } = &closing.central_close
    {
        closing.central_close = match readiness {
            StartReadiness::Ready => CentralCloseProgress::Owed {
                secret: secret.clone(),
                purpose: *purpose,
                certainty: EndCallCertainty::Pending,
            },
            StartReadiness::LinkUnavailable => closing.central_close,
            StartReadiness::EnrollmentEnded => CentralCloseProgress::Unavailable {
                secret: secret.clone(),
                reason: CentralEndUnavailable::EnrollmentEnded,
            },
        };
    }
    next_cleanup(snapshot, closing, readiness == StartReadiness::Ready)
}

pub(super) fn harness_closed(
    snapshot: &StartSnapshot,
    observation: HarnessCloseObservation,
) -> MachineResult {
    let StartProgress::Closing(closing) = snapshot.progress() else {
        return wrong_phase();
    };
    if closing.harness_close != HarnessCloseProgress::Pending {
        return Err(TransitionError::Stale);
    }
    let mut closing = closing.clone();
    closing.harness_close = match observation {
        HarnessCloseObservation::Closed => HarnessCloseProgress::Closed,
        HarnessCloseObservation::ExactOwnerUnavailable => {
            HarnessCloseProgress::ExactOwnerUnavailable
        }
        HarnessCloseObservation::Refused => HarnessCloseProgress::Refused,
        HarnessCloseObservation::AnswerLost => HarnessCloseProgress::AnswerLost,
    };
    next_cleanup(snapshot, closing, true)
}

pub(super) fn central_ended(
    snapshot: &StartSnapshot,
    target: &CentralSessionId,
    observation: CentralEndObservation,
) -> MachineResult {
    let StartProgress::Closing(closing) = snapshot.progress() else {
        return wrong_phase();
    };
    let CentralCloseProgress::Owed {
        secret,
        purpose,
        certainty: EndCallCertainty::Pending,
    } = &closing.central_close
    else {
        return Err(TransitionError::Stale);
    };
    if &secret.central_id != target || closing.harness_close == HarnessCloseProgress::Pending {
        return Err(TransitionError::Stale);
    }
    let mut next = closing.clone();
    next.central_close = match observation {
        CentralEndObservation::Settled(settlement) => CentralCloseProgress::Settled {
            secret: secret.clone(),
            settlement,
        },
        CentralEndObservation::Unavailable(reason) => CentralCloseProgress::Unavailable {
            secret: secret.clone(),
            reason,
        },
        CentralEndObservation::Retry => CentralCloseProgress::Owed {
            secret: secret.clone(),
            purpose: *purpose,
            certainty: EndCallCertainty::RetryNeeded,
        },
    };
    next_cleanup(snapshot, next, true)
}

fn next_cleanup(
    snapshot: &StartSnapshot,
    closing: ClosingSession,
    central_ready: bool,
) -> MachineResult {
    if closing.harness_close == HarnessCloseProgress::Pending {
        let harness = closing.harness.clone();
        return move_to(
            snapshot,
            StartProgress::Closing(closing),
            vec![StartEffect::CloseExact(harness)],
        );
    }
    let central = match &closing.central_close {
        CentralCloseProgress::NotOwed => EndedCentralSession::NoSession,
        CentralCloseProgress::SettledWithoutIssuingHere(settlement) => {
            EndedCentralSession::SettledWithoutIssuingHere(settlement.clone())
        }
        CentralCloseProgress::Settled { secret, settlement } => EndedCentralSession::Settled {
            id: secret.central_id.clone(),
            evidence: settlement.clone(),
        },
        CentralCloseProgress::Owed {
            secret,
            purpose,
            certainty: EndCallCertainty::Pending,
        } if central_ready => {
            let effect = StartEffect::EndCentral {
                id: secret.central_id.clone(),
                purpose: *purpose,
            };
            return move_to(snapshot, StartProgress::Closing(closing), vec![effect]);
        }
        CentralCloseProgress::Owed { .. } => {
            let effect = if central_ready {
                StartEffect::RetryAfterBackoff
            } else {
                StartEffect::AwaitReadiness
            };
            return move_to(snapshot, StartProgress::Closing(closing), vec![effect]);
        }
        CentralCloseProgress::Unavailable { .. } | CentralCloseProgress::UnverifiedCreate => {
            return move_to(
                snapshot,
                StartProgress::Closing(closing),
                vec![StartEffect::ReportOutcome],
            );
        }
    };
    if closing.harness_close == HarnessCloseProgress::AnswerLost {
        return move_to(
            snapshot,
            StartProgress::Closing(closing),
            vec![StartEffect::RetryAfterBackoff],
        );
    }
    move_to(
        snapshot,
        StartProgress::Ended(EndOutcome::Closed {
            harness: closing.harness,
            reason: closing.reason,
            harness_close: closing.harness_close,
            central,
        }),
        vec![StartEffect::ReportOutcome],
    )
}
