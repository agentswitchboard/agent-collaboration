//! The service-side test executor owns the fence, record, and taken directives.

use std::cell::{Cell, RefCell};

use fleet_identity_protocol::SessionSecret;
use state_machine_core::StateMachine;
use switchboard_session_start::{
    BindNotTaken, Directed, DirectiveId, DirectiveResult, EndLeg, LaunchNotTaken, LaunchResult,
    LinkNotTaken, LinkResult, OpeningCommit, OpeningNotCommitted, OpeningOutcome, PointerFact,
    PutSecretNotTaken, PutSecretResult, SaveResult, SecretFact, SecretIssue, SecretPutRefusal,
    SecretReadError, SessionSecretStore, SetUpNotTaken, SetUpResult, SnapshotKind, SnapshotPhase,
    StartAnswer, StartDirective, StartEffect, StartInput, StartMachine, StartNotification,
    StartRecord, StartRecordError, StartRevision, StartSaveRefusal, StartScope, StartSnapshot,
    StoredFacts,
};

use super::{choices, fixtures, model::Model};

#[path = "executor_effects.rs"]
mod effects;

type Record = (StartScope, StartRevision, StartSnapshot);

#[derive(Default)]
struct MemoryRecord {
    durable: RefCell<Option<Record>>,
    fence: Cell<bool>,
}

impl StartRecord for MemoryRecord {
    fn load(
        &self,
        scope: &StartScope,
    ) -> Result<Option<(StartRevision, StartSnapshot)>, StartRecordError> {
        Ok(self
            .durable
            .borrow()
            .as_ref()
            .filter(|(held, ..)| held == scope)
            .map(|(_, revision, snapshot)| (*revision, snapshot.clone())))
    }

    fn save(
        &self,
        scope: &StartScope,
        expected: Option<StartRevision>,
        snapshot: &StartSnapshot,
    ) -> Result<StartRevision, StartSaveRefusal> {
        let revision = self
            .durable
            .borrow()
            .as_ref()
            .filter(|(held, _, _)| held == scope)
            .map(|(_, revision, _)| *revision);
        if revision != expected {
            return Err(StartSaveRefusal::RevisionMismatch);
        }
        let next = StartRevision::new(revision.map_or(1, |revision| revision.get() + 1));
        *self.durable.borrow_mut() = Some((scope.clone(), next, snapshot.clone()));
        Ok(next)
    }

    fn commit_opening(
        &self,
        scope: &StartScope,
        expected: StartRevision,
        opening: &OpeningCommit,
    ) -> OpeningOutcome {
        if self.fence.get() {
            return OpeningOutcome::NotCommitted(OpeningNotCommitted::EnrollmentEnded);
        }
        assert_eq!(opening.opened().kind(), SnapshotKind::Opened);
        match self.save(scope, Some(expected), opening.opened()) {
            Ok(revision) => OpeningOutcome::Committed(revision),
            Err(StartSaveRefusal::RevisionMismatch) => {
                OpeningOutcome::NotCommitted(OpeningNotCommitted::RevisionMismatch)
            }
            Err(StartSaveRefusal::NotCommitted) => {
                OpeningOutcome::NotCommitted(OpeningNotCommitted::RecordUnavailable)
            }
            Err(StartSaveRefusal::OutcomeNotKnown) => OpeningOutcome::Uncertain,
        }
    }
}

#[derive(Default)]
struct MemorySecrets(RefCell<Vec<(SecretIssue, SessionSecret)>>);

impl SessionSecretStore for MemorySecrets {
    fn put(&self, issue: &SecretIssue, secret: SessionSecret) -> Result<(), SecretPutRefusal> {
        match self.read(issue) {
            Ok(current) if current == secret => Ok(()),
            Ok(_) => Err(SecretPutRefusal::IssueAlreadyHeld),
            Err(SecretReadError::NotFound) => {
                self.0.borrow_mut().push((issue.clone(), secret));
                Ok(())
            }
            Err(SecretReadError::Locked | SecretReadError::Unavailable) => {
                Err(SecretPutRefusal::NotStored)
            }
        }
    }

    fn read(&self, issue: &SecretIssue) -> Result<SessionSecret, SecretReadError> {
        self.0
            .borrow()
            .iter()
            .find(|(held, _)| held == issue)
            .map(|(_, secret)| secret.clone())
            .ok_or(SecretReadError::NotFound)
    }
}

#[derive(Debug)]
struct HistoryEntry {
    id: DirectiveId,
    kind: &'static str,
    taken: bool,
    completed: bool,
    delivered: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum OriginalHold {
    Held,
    ProvedGone,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Timing {
    BeforeFence,
    FenceBeforeTake,
    FenceAfterTake,
    RetireConnectionAfterTake,
}

pub(super) struct Executor {
    pub machine: StartMachine,
    model: Model,
    pub pending: Option<Directed>,
    taken_result: Option<(DirectiveResult, u64)>,
    returned: Option<(DirectiveResult, u64)>,
    connection: u64,
    admitted_bind: Option<u64>,
    original_hold: OriginalHold,
    close_attempts: usize,
    pending_secret: Option<(SecretIssue, SessionSecret)>,
    history: Vec<HistoryEntry>,
    record: MemoryRecord,
    secrets: MemorySecrets,
    stopped_by_save: bool,
    openings: usize,
    maybe_running_setup: bool,
}

impl Executor {
    pub(super) fn new() -> Self {
        let instance = fixtures::instance(101);
        Self {
            machine: StartMachine::new(instance.clone()),
            model: Model::new(instance),
            pending: None,
            taken_result: None,
            returned: None,
            connection: 1,
            admitted_bind: None,
            original_hold: OriginalHold::Unknown,
            close_attempts: 0,
            pending_secret: None,
            history: Vec::new(),
            record: MemoryRecord::default(),
            secrets: MemorySecrets::default(),
            stopped_by_save: false,
            openings: 0,
            maybe_running_setup: false,
        }
    }

    pub(super) const fn result_is_waiting(&self) -> bool {
        self.returned.is_some()
    }

    pub(super) fn interrupted() -> Self {
        let mut executor = Self::new();
        let snapshot = fixtures::snapshot(SnapshotPhase::CreateIntended, StoredFacts::default());
        let instance = fixtures::instance(103);
        let revision = StartRevision::new(1);
        executor.model = Model::interrupted(instance.clone(), revision, snapshot.start());
        executor.machine = fixtures::restore(instance, revision, snapshot);
        executor
    }

    pub(super) fn selectable_results(&self) -> usize {
        self.pending.as_ref().map_or(0, |directed| {
            choices::results(&directed.directive, StartRevision::new(1)).len()
        })
    }

    pub(super) fn feed(&mut self, input: StartInput) {
        let expected = self.model.feed(input.clone());
        let actual = StartMachine::transition(&self.machine, input.clone());
        let (next, effects) = match (expected, actual) {
            (Ok(effects), Ok((observed, outputs))) => {
                assert_eq!(
                    outputs,
                    effects,
                    "state {:?}, input {input:?}",
                    self.machine.kind()
                );
                (observed, outputs)
            }
            (Err(expected), Err(actual)) => {
                assert_eq!(
                    actual,
                    expected,
                    "state {:?}, input {input:?}",
                    self.machine.kind()
                );
                return;
            }
            (expected, actual) => panic!(
                "state {:?}, input {input:?}: expected {expected:?}, got {actual:?}",
                self.machine.kind()
            ),
        };
        assert_eq!(next.kind(), self.model.state);
        assert_eq!(next.envelope(), self.model.envelope.as_ref());
        assert_eq!(
            next.attempt()
                .map(fleet_identity_protocol::CreateSessionAttempt::get),
            self.model.envelope.as_ref().map(|_| self.model.attempt)
        );
        assert_eq!(
            next.facts()
                .map(switchboard_session_start::StartFacts::as_stored),
            self.model.envelope.as_ref().map(|_| &self.model.facts)
        );
        assert_eq!(next.revision(), self.model.revision);
        assert_eq!(
            next.recording(),
            self.model.envelope.as_ref().map(|_| self.model.recording)
        );
        assert_eq!(next.outstanding_id(), self.model.pending);
        self.assert_facts_preserved(&next);
        for effect in effects {
            match effect {
                StartEffect::Directive(directed) => {
                    assert!(!self.machine.kind().is_terminal());
                    assert!(!next.kind().is_terminal());
                    let kind = fixtures::directive_tag(&directed.directive);
                    if self.stopped_by_save {
                        assert_eq!(kind, "CloseConversation");
                        assert!(
                            !self
                                .history
                                .iter()
                                .any(|entry| entry.kind == "CloseConversation")
                        );
                    }
                    assert!(self.history.iter().all(|entry| entry.id != directed.id));
                    self.history.push(HistoryEntry {
                        id: directed.id.clone(),
                        kind,
                        taken: false,
                        completed: false,
                        delivered: false,
                    });
                    assert!(self.pending.replace(directed).is_none());
                }
                StartEffect::Notification(StartNotification::Tell {
                    answer: StartAnswer::Started { .. },
                    ..
                }) => {
                    assert_eq!(self.openings, 1);
                    assert_eq!(self.durable_kind(), Some(SnapshotKind::Opened));
                }
                StartEffect::Notification(StartNotification::DiscardPendingSecret(issue)) => {
                    if self
                        .pending_secret
                        .as_ref()
                        .is_some_and(|(held, _)| held == &issue)
                    {
                        self.pending_secret = None;
                    }
                }
                StartEffect::Notification(_) => {}
            }
        }
        self.machine = next;
    }

    fn assert_facts_preserved(&self, next: &StartMachine) {
        if let (Some(before), Some(after)) = (self.machine.facts(), next.facts()) {
            assert!(before.create_exposure <= after.create_exposure);
            assert!(before.bind_exposure <= after.bind_exposure);
            if before.reservation.is_some() {
                assert_eq!(before.reservation, after.reservation);
            }
            if before.settlement.is_some() {
                assert_eq!(before.settlement, after.settlement);
            }
            if matches!(before.secret, SecretFact::Stored(_)) {
                assert_eq!(before.secret, after.secret);
            }
            if before.harness_session_id.is_some() {
                assert_eq!(before.harness_session_id, after.harness_session_id);
            }
            if matches!(
                before.launch,
                switchboard_session_start::LaunchFact::Started
                    | switchboard_session_start::LaunchFact::ProvenNotTaken(_)
                    | switchboard_session_start::LaunchFact::FailedNoConversation
                    | switchboard_session_start::LaunchFact::NotKnown
            ) {
                assert_eq!(before.launch, after.launch);
            }
            if !matches!(before.end, EndLeg::NotDue(_) | EndLeg::Due) {
                assert_eq!(before.end, after.end);
            }
            if before.pointer != PointerFact::None {
                assert_eq!(before.pointer, after.pointer);
            }
        }
    }

    pub(super) fn wrong_id(&mut self, wrong_instance: bool) {
        let (mut id, result) = match self.pending.as_ref() {
            Some(pending) => (
                pending.id.clone(),
                choices::results(&pending.directive, StartRevision::new(1)).remove(0),
            ),
            None => (
                DirectiveId {
                    instance: fixtures::instance(101),
                    sequence: 10,
                },
                DirectiveResult::Save(SaveResult::NotCommitted),
            ),
        };
        if wrong_instance {
            id.instance = fixtures::instance(103);
        } else {
            id.sequence = id.sequence.saturating_sub(1);
        }
        self.feed(StartInput::DirectiveResult { id, result });
    }

    fn assert_take_allowed(&self, directive: &StartDirective) {
        if self.record.fence.get() {
            assert!(matches!(
                directive,
                StartDirective::Save { .. } | StartDirective::CloseConversation
            ));
        }
        let previous_takes = |kind| {
            self.history
                .iter()
                .filter(|entry| entry.kind == kind && entry.taken)
                .count()
        };
        match directive {
            StartDirective::Launch => {
                assert_eq!(self.durable_kind(), Some(SnapshotKind::LaunchIntended));
                assert_eq!(previous_takes("Launch"), 0);
            }
            StartDirective::SendBind { issue, .. } => {
                assert_eq!(self.durable_kind(), Some(SnapshotKind::BindIntended));
                assert!(self.secrets.read(issue).is_ok());
            }
            StartDirective::SendEnd(_) => {
                let record = self.record.durable.borrow();
                let (_, _, snapshot) = record.as_ref().unwrap();
                assert_eq!(snapshot.kind(), SnapshotKind::CleaningUp);
                assert_eq!(snapshot.start().facts.end, EndLeg::Due);
                assert_eq!(previous_takes("SendEnd"), 0);
            }
            StartDirective::CloseConversation => assert_eq!(previous_takes("CloseConversation"), 0),
            StartDirective::PutSecret { issue } => {
                let (_, snapshot) = self.durable_snapshot().unwrap();
                assert_eq!(
                    snapshot.reserved().map(|(_, recorded)| recorded),
                    Some(issue)
                );
            }
            StartDirective::Save { .. }
            | StartDirective::SendCreate(_)
            | StartDirective::SetUp
            | StartDirective::CommitOpening { .. } => {}
        }
    }

    fn durable_kind(&self) -> Option<SnapshotKind> {
        self.record
            .durable
            .borrow()
            .as_ref()
            .map(|entry| entry.2.kind())
    }

    pub(super) fn assert_terminal_custody(&self) {
        assert_eq!(
            self.machine.outstanding_id().is_none(),
            self.pending.is_none()
        );
        if self.machine.kind().is_terminal() {
            assert!(!self.maybe_running_setup);
            assert!(self.pending_secret.is_none());
            let incomplete = self.history.iter().filter(|entry| !entry.delivered).count();
            assert_eq!(incomplete, 0);
        }
    }
}

fn not_taken(result: &DirectiveResult) -> bool {
    matches!(
        result,
        DirectiveResult::SendCreate(LinkResult::NotTaken(_))
            | DirectiveResult::SendBind(LinkResult::NotTaken(_))
            | DirectiveResult::SendEnd(LinkResult::NotTaken(_))
            | DirectiveResult::PutSecret(PutSecretResult::NotTaken(_))
            | DirectiveResult::Launch(LaunchResult::NotTaken(_))
            | DirectiveResult::SetUp(SetUpResult::NotTaken(_))
    )
}

fn fenced_result(directive: &StartDirective) -> DirectiveResult {
    match directive {
        StartDirective::SendCreate(_) => {
            DirectiveResult::SendCreate(LinkResult::NotTaken(LinkNotTaken::EnrollmentEnded))
        }
        StartDirective::PutSecret { .. } => DirectiveResult::PutSecret(PutSecretResult::NotTaken(
            PutSecretNotTaken::EnrollmentEnded,
        )),
        StartDirective::Launch => {
            DirectiveResult::Launch(LaunchResult::NotTaken(LaunchNotTaken::EnrollmentEnded))
        }
        StartDirective::SetUp => {
            DirectiveResult::SetUp(SetUpResult::NotTaken(SetUpNotTaken::EnrollmentEnded))
        }
        StartDirective::SendBind { .. } => {
            DirectiveResult::SendBind(LinkResult::NotTaken(BindNotTaken::EnrollmentEnded))
        }
        StartDirective::CommitOpening { .. } => DirectiveResult::CommitOpening(
            OpeningOutcome::NotCommitted(OpeningNotCommitted::EnrollmentEnded),
        ),
        StartDirective::SendEnd(_) => {
            DirectiveResult::SendEnd(LinkResult::NotTaken(LinkNotTaken::EnrollmentEnded))
        }
        StartDirective::Save { .. } | StartDirective::CloseConversation => {
            panic!("local directive fenced")
        }
    }
}

#[test]
fn record_and_secret_ports_enforce_revision_fence_and_immutable_issue() {
    let record = MemoryRecord::default();
    let initial = fixtures::snapshot(SnapshotPhase::CreateIntended, StoredFacts::default());
    let envelope = fixtures::envelope();
    let scope = envelope.scope().clone();
    let revision = record.save(&scope, None, &initial).unwrap();
    assert_eq!(
        record.save(&scope, None, &initial),
        Err(StartSaveRefusal::RevisionMismatch)
    );
    assert_eq!(
        record.load(&scope).unwrap(),
        Some((revision, initial.clone()))
    );
    let issue = SecretIssue {
        enrollment: scope.enrollment.clone(),
        session_id: fixtures::central_id(),
        attempt: fixtures::attempt(1),
    };
    let secrets = MemorySecrets::default();
    assert_eq!(secrets.read(&issue), Err(SecretReadError::NotFound));
    assert_eq!(secrets.put(&issue, fixtures::secret(11)), Ok(()));
    assert_eq!(secrets.put(&issue, fixtures::secret(11)), Ok(()));
    assert_eq!(
        secrets.put(&issue, fixtures::secret(12)),
        Err(SecretPutRefusal::IssueAlreadyHeld)
    );
    let opening =
        OpeningCommit::new(&envelope, &fixtures::held_facts(), fixtures::address()).unwrap();
    record.fence.set(true);
    assert_eq!(
        record.commit_opening(&scope, revision, &opening),
        OpeningOutcome::NotCommitted(OpeningNotCommitted::EnrollmentEnded)
    );
    assert_eq!(record.load(&scope).unwrap(), Some((revision, initial)));
    record.fence.set(false);
    assert_eq!(
        record.commit_opening(&scope, StartRevision::new(0), &opening),
        OpeningOutcome::NotCommitted(OpeningNotCommitted::RevisionMismatch)
    );
    let OpeningOutcome::Committed(new_revision) = record.commit_opening(&scope, revision, &opening)
    else {
        panic!("expected commit")
    };
    assert_eq!(
        record.load(&scope).unwrap(),
        Some((new_revision, opening.opened().clone()))
    );
}
