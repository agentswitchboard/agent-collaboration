use fleet_identity_protocol::{CreateSessionKind, HarnessKind};
use workspace_ids::SessionRef;

use super::{OpenedSession, SnapshotFacts, SnapshotPhase, StartSnapshot, StoredSnapshot};
use crate::{
    CandidateAsker, CheckedActive, ConversationFact, CorruptStartRecord, Exposure, LaunchFact,
    LaunchSpecRef, LocalStartDigest, OpeningCommit, PointerFact, Reservation, SecretFact,
    SecretIssue, StartCandidate, StartEnvelope, StartFacts, StartInstanceId, StartMachine,
    StartRevision, StartScope, StoredFacts,
};

fn specimen() -> StoredSnapshot {
    let enrollment = String::from("019c0000-0000-7000-8000-000000000001")
        .try_into()
        .unwrap();
    let envelope = StartEnvelope::new(StartCandidate {
        scope: StartScope {
            enrollment,
            operation_id: "019c0000-0000-7000-8000-000000000002".parse().unwrap(),
        },
        machine: "019c0000-0000-7000-8000-000000000003".parse().unwrap(),
        asker: CandidateAsker::Operator,
        agent_program: HarnessKind::Claude,
        kind: CreateSessionKind::Ordinary {},
        project: fleet_identity_protocol::StartProject::Named {
            project_ref: fleet_identity_protocol::ProjectRef {
                workspace: "019c0000-0000-7000-8000-000000000005".parse().unwrap(),
                project: "019c0000-0000-7000-8000-000000000006"
                    .to_owned()
                    .try_into()
                    .unwrap(),
            },
        },
        local_digest: LocalStartDigest::new([0; 32]),
        launch: LaunchSpecRef::new(1),
    })
    .unwrap();
    let attempt = 1.try_into().unwrap();
    let issue = SecretIssue {
        enrollment: envelope.scope().enrollment.clone(),
        session_id: "019c0000-0000-7000-8000-000000000005".parse().unwrap(),
        attempt,
    };
    let harness_session_id = "held".parse().unwrap();
    let address = SessionRef {
        machine_id: envelope.machine().clone(),
        session_id: "held".parse().unwrap(),
    };
    let phase = SnapshotPhase::Opened(OpenedSession {
        address: address.clone(),
        session_id: issue.session_id.clone(),
        harness_session_id,
        issue: issue.clone(),
    });
    let facts = StoredFacts {
        reservation: Some(Reservation {
            session_id: issue.session_id.clone(),
            reserved_until: String::from("2026-11-23T08:00:00.000Z").try_into().unwrap(),
            attempt,
        }),
        secret: SecretFact::Stored(issue.clone()),
        pointer: PointerFact::Committed(issue),
        launch: LaunchFact::Started,
        harness_session_id: Some("held".parse().unwrap()),
        conversation: ConversationFact::Held,
        bind_exposure: Exposure::Witnessed,
        active_proof: Some(CheckedActive { address }),
        ..Default::default()
    };
    StoredSnapshot {
        envelope,
        attempt,
        facts,
        phase,
    }
}

fn opened(stored: &mut StoredSnapshot) -> &mut OpenedSession {
    let SnapshotPhase::Opened(association) = &mut stored.phase else {
        panic!("opened test specimen")
    };
    association
}

fn pointer(stored: &mut StoredSnapshot) -> &mut SecretIssue {
    let PointerFact::Committed(issue) = &mut stored.facts.pointer else {
        panic!("opened test specimen")
    };
    issue
}

fn stored_issue(stored: &mut StoredSnapshot) -> &mut SecretIssue {
    let SecretFact::Stored(issue) = &mut stored.facts.secret else {
        panic!("opened test specimen")
    };
    issue
}

fn restore_with_scope(
    stored: StoredSnapshot,
    scope: &StartScope,
) -> Result<StartMachine, CorruptStartRecord> {
    // Model a corrupt internal snapshot without making unchecked construction
    // available to adapters or callers.
    let snapshot = StartSnapshot {
        start: Box::new(SnapshotFacts {
            envelope: stored.envelope,
            attempt: stored.attempt,
            facts: StartFacts {
                stored: stored.facts,
            },
        }),
        phase: stored.phase,
    };
    StartMachine::restored(instance(), scope, StartRevision::new(1), snapshot)
}

fn instance() -> StartInstanceId {
    StartInstanceId::new("019c0000-0000-7000-8000-000000000006".parse().unwrap())
}

fn rejects(stored: StoredSnapshot, error: CorruptStartRecord) {
    let scope = stored.envelope.scope().clone();
    assert_eq!(restore_with_scope(stored.clone(), &scope), Err(error));
    assert_eq!(
        StartSnapshot::from_stored(&scope, stored.clone()),
        Err(error)
    );
    assert!(stored.must_be_retained(&scope));
}

#[test]
fn scope_mismatch() {
    let stored = specimen();
    let mut scope = stored.envelope.scope().clone();
    scope.operation_id = "019c0000-0000-7000-8000-000000000010".parse().unwrap();
    assert_eq!(
        restore_with_scope(stored.clone(), &scope),
        Err(CorruptStartRecord::ScopeMismatch)
    );
    assert_eq!(
        StartSnapshot::from_stored(&scope, stored),
        Err(CorruptStartRecord::ScopeMismatch)
    );
}

#[test]
fn issue_enrollment_mismatch() {
    let mut stored = specimen();
    stored_issue(&mut stored).enrollment = String::from("019c0000-0000-7000-8000-000000000010")
        .try_into()
        .unwrap();
    rejects(stored, CorruptStartRecord::IssueEnrollmentMismatch);
}

#[test]
fn issue_session_mismatch() {
    let mut stored = specimen();
    stored_issue(&mut stored).session_id = "019c0000-0000-7000-8000-000000000010".parse().unwrap();
    rejects(stored, CorruptStartRecord::IssueSessionMismatch);
}

#[test]
fn issue_attempt_mismatch() {
    let mut stored = specimen();
    stored_issue(&mut stored).attempt = 2.try_into().unwrap();
    rejects(stored, CorruptStartRecord::IssueAttemptMismatch);
}

#[test]
fn pointer_without_reservation() {
    let mut stored = specimen();
    stored.phase = SnapshotPhase::Unknown(crate::UnknownReason::Opening);
    stored.facts.secret = SecretFact::None;
    stored.facts.reservation = None;
    rejects(stored, CorruptStartRecord::PointerWithoutReservation);
}

#[test]
fn association_machine_mismatch() {
    let mut stored = specimen();
    opened(&mut stored).address.machine_id =
        "019c0000-0000-7000-8000-000000000010".parse().unwrap();
    rejects(stored, CorruptStartRecord::AssociationMachineMismatch);
}

#[test]
fn association_harness_mismatch() {
    let mut stored = specimen();
    opened(&mut stored).harness_session_id = "other-harness".parse().unwrap();
    rejects(stored, CorruptStartRecord::AssociationHarnessMismatch);
}

#[test]
fn association_session_mismatch() {
    let mut stored = specimen();
    opened(&mut stored).session_id = "019c0000-0000-7000-8000-000000000010".parse().unwrap();
    rejects(stored, CorruptStartRecord::AssociationSessionMismatch);
}

#[test]
fn association_issue_mismatch() {
    let mut stored = specimen();
    opened(&mut stored).issue.attempt = 2.try_into().unwrap();
    rejects(stored, CorruptStartRecord::AssociationIssueMismatch);
}

#[test]
fn reservation_attempt_mismatch() {
    let mut stored = specimen();
    stored.phase = SnapshotPhase::CreateUnknown;
    stored.facts.pointer = PointerFact::None;
    stored.facts.secret = SecretFact::None;
    stored.attempt = 2.try_into().unwrap();
    rejects(stored, CorruptStartRecord::ReservationAttemptMismatch);
}

#[test]
fn secret_without_reservation() {
    let mut stored = specimen();
    stored.phase = SnapshotPhase::Failed(crate::FailureReason::SecretNotStored);
    stored.facts.pointer = PointerFact::None;
    stored.facts.reservation = None;
    rejects(stored, CorruptStartRecord::SecretWithoutReservation);
}

#[test]
fn opened_without_held_conversation() {
    let mut stored = specimen();
    stored.facts.conversation = ConversationFact::Closed;
    rejects(stored, CorruptStartRecord::OpenedWithoutHeldConversation);
}

#[test]
fn opened_without_committed_pointer() {
    let mut stored = specimen();
    stored.facts.pointer = PointerFact::PossiblyCommitted(pointer(&mut stored).clone());
    rejects(stored, CorruptStartRecord::OpenedWithoutCommittedPointer);
}

#[test]
fn reserved_without_reservation() {
    let mut stored = reserved_specimen();
    stored.facts.reservation = None;
    rejects(stored, CorruptStartRecord::ReservedWithoutReservation);
}

#[test]
fn reserved_without_secret_intent() {
    let mut stored = reserved_specimen();
    stored.facts.secret = SecretFact::None;
    rejects(stored, CorruptStartRecord::ReservedWithoutSecretIntent);
}

fn reserved_specimen() -> StoredSnapshot {
    let mut stored = specimen();
    let issue = pointer(&mut stored).clone();
    stored.phase = SnapshotPhase::Reserved;
    stored.facts = StoredFacts {
        reservation: stored.facts.reservation,
        secret: SecretFact::PossiblyStored(issue),
        ..Default::default()
    };
    stored
}

#[test]
fn opening_constructor_checks_address_and_preserves_checked_facts() {
    let mut stored = specimen();
    let phase = opened(&mut stored).clone();
    let facts = StartFacts::from_stored(stored.facts.clone()).unwrap();
    let mut bad = phase.address.clone();
    bad.machine_id = "019c0000-0000-7000-8000-000000000010".parse().unwrap();
    assert_eq!(
        OpeningCommit::new(&stored.envelope, &facts, bad),
        Err(CorruptStartRecord::AssociationMachineMismatch)
    );
    let mut bad = phase.address.clone();
    bad.session_id = "other-harness".parse().unwrap();
    assert_eq!(
        OpeningCommit::new(&stored.envelope, &facts, bad),
        Err(CorruptStartRecord::AssociationHarnessMismatch)
    );
    let opening = OpeningCommit::new(&stored.envelope, &facts, phase.address.clone()).unwrap();
    assert_eq!(opening.association(), &phase);
    assert_eq!(opening.opened().to_stored(), stored);
}

#[test]
fn opened_requires_a_stored_secret_for_its_pointer() {
    let mut stored = specimen();
    stored.facts.secret = SecretFact::None;
    rejects(
        stored.clone(),
        CorruptStartRecord::OpenedWithoutStoredSecret,
    );
    let address = opened(&mut stored).address.clone();
    let checked_facts = StartFacts::from_stored(stored.facts.clone()).unwrap();
    assert_eq!(
        OpeningCommit::new(&stored.envelope, &checked_facts, address),
        Err(CorruptStartRecord::OpenedWithoutStoredSecret)
    );
}

#[test]
fn opened_requires_witnessed_bind_exposure() {
    let mut stored = specimen();
    stored.facts.bind_exposure = Exposure::None;
    rejects(
        stored.clone(),
        CorruptStartRecord::OpenedWithoutWitnessedBind,
    );
    let address = opened(&mut stored).address.clone();
    let checked_facts = StartFacts::from_stored(stored.facts.clone()).unwrap();
    assert_eq!(
        OpeningCommit::new(&stored.envelope, &checked_facts, address),
        Err(CorruptStartRecord::OpenedWithoutWitnessedBind)
    );
}

fn abandoned() -> fleet_identity_protocol::SessionSettlement {
    use fleet_identity_protocol::{AbandonedSessionCause, SessionSettlement};
    SessionSettlement::Abandoned {
        cause: AbandonedSessionCause::GivenUp {},
        time: String::from("2026-11-23T08:01:00.000Z").try_into().unwrap(),
    }
}

#[test]
fn settled_requires_exact_settlement_before_restore_or_prune() {
    let mut stored = specimen();
    stored.phase = SnapshotPhase::Settled;
    stored.facts = StoredFacts {
        settlement: Some(abandoned()),
        ..Default::default()
    };
    assert!(!stored.must_be_retained(stored.envelope.scope()));
    stored.facts.settlement = None;
    rejects(stored, CorruptStartRecord::SettledWithoutSettlement);
}

#[test]
fn opened_requires_a_started_launch_and_its_exact_active_proof() {
    let mut no_launch = specimen();
    no_launch.facts.launch = LaunchFact::Intended;
    rejects(no_launch, CorruptStartRecord::OpenedWithoutLaunch);

    let mut no_proof = specimen();
    no_proof.facts.active_proof = None;
    rejects(no_proof, CorruptStartRecord::OpenedWithoutActiveProof);

    let mut wrong_proof = specimen();
    wrong_proof
        .facts
        .active_proof
        .as_mut()
        .unwrap()
        .address
        .session_id = "different-proof".parse().unwrap();
    rejects(
        wrong_proof,
        CorruptStartRecord::OpenedActiveProofAddressMismatch,
    );
}

#[test]
fn opened_refuses_settlement_and_a_cleanup_end_leg() {
    let mut settled = specimen();
    settled.facts.settlement = Some(abandoned());
    rejects(settled, CorruptStartRecord::OpenedWithSettlement);

    let mut ending = specimen();
    ending.facts.end = crate::EndLeg::Due;
    rejects(ending, CorruptStartRecord::OpenedWithEndLeg);
}

#[test]
fn an_end_leg_disagreeing_with_the_exact_settlement_is_corrupt_in_each_phase() {
    use crate::{CleanupOutcome, EndLeg, FailureReason};
    use fleet_identity_protocol::{NewAbandonmentCause, NewEndSettlement};
    for phase in [
        SnapshotPhase::Settled,
        SnapshotPhase::Fenced,
        SnapshotPhase::CleaningUp(CleanupOutcome::Failed(FailureReason::SetUpFailed)),
    ] {
        let mut stored = specimen();
        stored.phase = phase;
        stored.facts.settlement = Some(abandoned());
        let exact = NewEndSettlement::Abandoned {
            cause: NewAbandonmentCause::GivenUp {},
            time: String::from("2026-11-23T08:01:00.000Z").try_into().unwrap(),
        };
        stored.facts.end = EndLeg::Settled(exact);
        assert!(StartSnapshot::from_stored(stored.envelope.scope(), stored.clone()).is_ok());
        stored.facts.end = EndLeg::Settled(NewEndSettlement::Abandoned {
            cause: NewAbandonmentCause::GivenUp {},
            time: String::from("2026-11-23T08:02:00.000Z").try_into().unwrap(),
        });
        rejects(stored, CorruptStartRecord::EndSettlementDisagrees);
    }
}

#[test]
fn launch_and_bind_phases_require_their_own_evidence() {
    use crate::{CleanupOutcome, FactName, SnapshotKind};
    let mut launch = specimen();
    launch.phase = SnapshotPhase::LaunchIntended;
    launch.facts.launch = LaunchFact::Intended;
    assert!(StartSnapshot::from_stored(launch.envelope.scope(), launch.clone()).is_ok());
    launch.facts.pointer = PointerFact::None;
    rejects(
        launch,
        CorruptStartRecord::PhaseFactMissing {
            phase: SnapshotKind::LaunchIntended,
            fact: FactName::CommittedPointer,
        },
    );

    for (phase, kind) in [
        (SnapshotPhase::Held, SnapshotKind::Held),
        (SnapshotPhase::BindIntended, SnapshotKind::BindIntended),
        (SnapshotPhase::BindUnknown, SnapshotKind::BindUnknown),
    ] {
        let mut stored = specimen();
        stored.phase = phase;
        assert!(StartSnapshot::from_stored(stored.envelope.scope(), stored.clone()).is_ok());
        stored.facts.harness_session_id = None;
        rejects(
            stored,
            CorruptStartRecord::PhaseFactMissing {
                phase: kind,
                fact: FactName::HarnessSessionId,
            },
        );
    }

    let mut cleanup = specimen();
    cleanup.phase = SnapshotPhase::CleaningUp(CleanupOutcome::Settled);
    rejects(
        cleanup,
        CorruptStartRecord::PhaseFactMissing {
            phase: SnapshotKind::CleaningUp,
            fact: FactName::CleanupReason,
        },
    );
}
