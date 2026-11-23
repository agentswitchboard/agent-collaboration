use fleet_identity_protocol::{CreateSessionKind, HarnessKind};
use workspace_ids::SessionRef;

use super::{OpenedSession, SnapshotFacts, SnapshotPhase, StartSnapshot, StoredSnapshot};
use crate::{
    CandidateAsker, ConversationFact, CorruptStartRecord, LaunchFact, LaunchSpecRef,
    LocalStartDigest, OpeningCommit, PointerFact, Reservation, SecretFact, SecretIssue,
    StartCandidate, StartEnvelope, StartFacts, StartInstanceId, StartMachine, StartRevision,
    StartScope, StoredFacts,
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
        address,
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
    assert_eq!(StartSnapshot::from_stored(&scope, stored), Err(error));
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
