use fleet_identity_protocol::BindFleetSessionReply;
use switchboard_session_start::{
    BindNotTaken, CloseResult, CreateAnswer, DirectiveResult, LaunchResult, LinkResult,
    OpeningNotCommitted, OpeningOutcome, PutSecretNotTaken, PutSecretResult, SaveResult,
    SecretPutRefusal, SetUpResult, SnapshotKind, StartDirective, StartInput, StartRevision,
};

use super::{Executor, OriginalHold, Timing, choices, fenced_result, fixtures, not_taken};

impl Executor {
    pub(in super::super) fn can_take(&self) -> bool {
        self.pending.is_some() && self.taken_result.is_none() && self.returned.is_none()
    }

    pub(in super::super) fn is_taken(&self) -> bool {
        self.taken_result.is_some()
    }

    pub(in super::super) fn end_enrollment(&mut self) {
        self.record.fence.set(true);
    }

    pub(in super::super) fn retire_connection(&mut self) {
        self.connection += 1;
    }

    pub(in super::super) fn lose_original_hold(&mut self) {
        self.original_hold = OriginalHold::Unknown;
        self.maybe_running_setup = false;
    }

    pub(in super::super) fn observe_original_gone(&mut self) {
        self.lose_original_hold();
        self.original_hold = OriginalHold::ProvedGone;
    }

    pub(in super::super) const fn close_attempts(&self) -> usize {
        self.close_attempts
    }

    pub(in super::super) fn setup_is_running(&self) -> bool {
        self.maybe_running_setup
    }

    pub(in super::super) fn take(&mut self, choice: usize) {
        assert!(self.can_take());
        let directed = self.pending.as_ref().unwrap();
        let mut results = choices::results(&directed.directive, StartRevision::new(1));
        let selected = results.swap_remove(choice);
        let local = matches!(
            directed.directive,
            StartDirective::Save { .. } | StartDirective::CloseConversation
        );
        let result = if self.record.fence.get() && !local {
            fenced_result(&directed.directive)
        } else if matches!(directed.directive, StartDirective::SendBind { .. })
            && self.original_hold != OriginalHold::Held
        {
            DirectiveResult::SendBind(LinkResult::NotTaken(BindNotTaken::HoldLost))
        } else {
            selected
        };
        let fenced = self.record.fence.get() && !local;
        let taken = !fenced && !not_taken(&result);
        if taken {
            self.assert_take_allowed(&directed.directive);
        }
        let entry = self.history.last_mut().unwrap();
        assert_eq!(entry.id, directed.id);
        entry.taken = taken;
        if taken && matches!(directed.directive, StartDirective::SetUp) {
            self.maybe_running_setup = true;
        }
        if let StartDirective::PutSecret { issue } = &directed.directive {
            let source = self.pending_secret.take_if(|(held, _)| held == issue);
            self.taken_secret = if taken {
                source.map(|(_, secret)| secret)
            } else {
                None
            };
        }
        self.taken_result = Some((result, self.connection));
    }

    pub(in super::super) fn complete(&mut self, persist_unknown: bool) {
        let (selected, connection) = self.taken_result.take().unwrap();
        let directed = self.pending.as_ref().unwrap();
        let secret = self.taken_secret.take();
        let mut result =
            self.complete_effect(&directed.directive, selected, persist_unknown, secret);
        match &result {
            DirectiveResult::Launch(
                LaunchResult::Started { .. } | LaunchResult::FailedDuplicateId,
            ) => self.original_hold = OriginalHold::Held,
            DirectiveResult::SetUp(
                SetUpResult::Done | SetUpResult::Failed | SetUpResult::NotTaken(_),
            ) => {
                self.maybe_running_setup = false;
            }
            DirectiveResult::CloseConversation(close) => {
                result = DirectiveResult::CloseConversation(self.close_original(*close));
            }
            _ => {}
        }
        let entry = self.history.last_mut().unwrap();
        assert!(!entry.completed);
        entry.completed = true;
        self.returned = Some((result, connection));
    }

    fn close_original(&mut self, result: CloseResult) -> CloseResult {
        match self.original_hold {
            OriginalHold::Held => {
                self.stop_setup();
                self.perform_close(result);
                result
            }
            OriginalHold::ProvedGone => CloseResult::Closed,
            OriginalHold::Unknown => CloseResult::OutcomeNotKnown,
        }
    }

    fn stop_setup(&mut self) {
        self.maybe_running_setup = false;
    }

    fn perform_close(&mut self, result: CloseResult) {
        assert!(
            !self.maybe_running_setup,
            "close acted before the original setup stopped"
        );
        self.close_attempts += 1;
        if result == CloseResult::Closed {
            self.original_hold = OriginalHold::ProvedGone;
        }
    }

    fn opening_refusal(&self) -> Option<OpeningNotCommitted> {
        if self.original_hold != OriginalHold::Held {
            Some(OpeningNotCommitted::HoldLost)
        } else if self.admitted_bind != Some(self.connection) {
            Some(OpeningNotCommitted::ConnectionRetired)
        } else {
            None
        }
    }

    fn complete_effect(
        &self,
        directive: &StartDirective,
        selected: DirectiveResult,
        persist_unknown: bool,
        taken_secret: Option<fleet_identity_protocol::SessionSecret>,
    ) -> DirectiveResult {
        use switchboard_session_start::{SessionSecretStore, StartRecord};

        let scope = self.machine.envelope().unwrap().scope();
        match (directive, selected) {
            (
                StartDirective::Save { expected, snapshot },
                DirectiveResult::Save(SaveResult::Saved(_)),
            ) => DirectiveResult::Save(SaveResult::Saved(
                self.record.save(scope, *expected, snapshot).unwrap(),
            )),
            (
                StartDirective::Save { expected, snapshot },
                result @ DirectiveResult::Save(SaveResult::OutcomeNotKnown),
            ) if persist_unknown => {
                self.record.save(scope, *expected, snapshot).unwrap();
                result
            }
            (StartDirective::PutSecret { issue }, DirectiveResult::PutSecret(result)) => {
                self.complete_put(issue, result, persist_unknown, taken_secret)
            }
            (StartDirective::CommitOpening { .. }, DirectiveResult::CommitOpening(result))
                if result != OpeningOutcome::NotCommitted(OpeningNotCommitted::EnrollmentEnded)
                    && self.opening_refusal().is_some() =>
            {
                DirectiveResult::CommitOpening(OpeningOutcome::NotCommitted(
                    self.opening_refusal().unwrap(),
                ))
            }
            (
                StartDirective::CommitOpening { expected, opening },
                DirectiveResult::CommitOpening(OpeningOutcome::Committed(_)),
            ) => {
                self.secrets.read(opening.issue()).unwrap();
                DirectiveResult::CommitOpening(
                    self.record.commit_opening(scope, *expected, opening),
                )
            }
            (
                StartDirective::CommitOpening { expected, opening },
                result @ DirectiveResult::CommitOpening(OpeningOutcome::Uncertain),
            ) if persist_unknown => {
                self.record.commit_opening(scope, *expected, opening);
                result
            }
            (_, result) => result,
        }
    }

    fn complete_put(
        &self,
        issue: &switchboard_session_start::SecretIssue,
        result: PutSecretResult,
        persist_unknown: bool,
        pending: Option<fleet_identity_protocol::SessionSecret>,
    ) -> DirectiveResult {
        use switchboard_session_start::SessionSecretStore;
        let result = match (result, pending) {
            (refusal @ PutSecretResult::NotTaken(_), _) => refusal,
            (_, None) => PutSecretResult::NotTaken(PutSecretNotTaken::PendingSecretMissing),
            (PutSecretResult::HeldWithDifferentValue, Some(pending)) => {
                self.secrets.put(issue, fixtures::secret(37)).unwrap();
                assert_eq!(
                    self.secrets.put(issue, pending),
                    Err(SecretPutRefusal::IssueAlreadyHeld)
                );
                PutSecretResult::HeldWithDifferentValue
            }
            (PutSecretResult::Stored, Some(pending)) => {
                self.secrets.put(issue, pending).unwrap();
                PutSecretResult::Stored
            }
            (PutSecretResult::OutcomeNotKnown, Some(pending)) if persist_unknown => {
                self.secrets.put(issue, pending).unwrap();
                PutSecretResult::OutcomeNotKnown
            }
            (other, Some(_)) => other,
        };
        DirectiveResult::PutSecret(result)
    }

    pub(in super::super) fn deliver(&mut self) {
        let (completed, connection) = self.returned.take().unwrap();
        let directed = self.pending.take().unwrap();
        let entry = self.history.last_mut().unwrap();
        assert_eq!(entry.id, directed.id);
        assert!(entry.completed && !entry.delivered);
        entry.delivered = true;
        let result = self.at_delivery(completed, connection);
        match result {
            DirectiveResult::Save(
                SaveResult::NotCommitted
                | SaveResult::RevisionMismatch
                | SaveResult::OutcomeNotKnown,
            ) => self.stopped_by_save = true,
            DirectiveResult::CommitOpening(OpeningOutcome::Committed(_)) => {
                self.openings += 1;
                assert_eq!(self.openings, 1);
            }
            _ => {}
        }
        self.feed_link_result(directed.id, result, 17);
    }

    fn at_delivery(&mut self, result: DirectiveResult, connection: u64) -> DirectiveResult {
        let current = self.connection == connection && !self.record.fence.get();
        match &result {
            DirectiveResult::SendCreate(LinkResult::Answered(
                CreateAnswer::Reserved { .. } | CreateAnswer::Active { .. },
            )) if !current => {
                return DirectiveResult::SendCreate(LinkResult::AnswerLost);
            }
            DirectiveResult::SendBind(LinkResult::Answered(BindFleetSessionReply::Active {
                ..
            })) if !current => {
                return DirectiveResult::SendBind(LinkResult::AnswerLost);
            }
            DirectiveResult::SendBind(LinkResult::Answered(BindFleetSessionReply::Active {
                ..
            })) => {
                self.admitted_bind = Some(connection);
            }
            _ => {}
        }
        result
    }

    pub(in super::super) fn feed_link_result(
        &mut self,
        id: switchboard_session_start::DirectiveId,
        result: DirectiveResult,
        secret_seed: u8,
    ) -> bool {
        let staged =
            if let DirectiveResult::SendCreate(LinkResult::Answered(CreateAnswer::Reserved {
                session_id,
                ..
            })) = &result
            {
                let issue = switchboard_session_start::SecretIssue {
                    enrollment: self.machine.envelope().unwrap().scope().enrollment.clone(),
                    session_id: session_id.clone(),
                    attempt: self.machine.attempt().unwrap(),
                };
                Some((issue, fixtures::secret(secret_seed)))
            } else {
                None
            };
        self.feed_with_staged(StartInput::DirectiveResult { id, result }, staged)
    }

    pub(in super::super) fn complete_step(
        &mut self,
        choice: usize,
        timing: Timing,
        persist_unknown: bool,
    ) {
        if matches!(timing, Timing::FenceBeforeTake) {
            self.end_enrollment();
        }
        self.take(choice);
        if matches!(timing, Timing::FenceAfterTake) {
            self.end_enrollment();
        }
        self.complete(persist_unknown);
        if matches!(timing, Timing::RetireConnectionAfterTake) {
            self.retire_connection();
        }
        self.deliver();
    }

    pub(in super::super) fn durable_snapshot(
        &self,
    ) -> Option<(StartRevision, switchboard_session_start::StartSnapshot)> {
        self.record
            .durable
            .borrow()
            .as_ref()
            .map(|(_, revision, snapshot)| (*revision, snapshot.clone()))
    }

    pub(in super::super) fn has_pending_secret(&self) -> bool {
        self.pending_secret.is_some()
    }

    pub(in super::super) fn secret_at(
        &self,
        issue: &switchboard_session_start::SecretIssue,
    ) -> Option<fleet_identity_protocol::SessionSecret> {
        use switchboard_session_start::SessionSecretStore;
        self.secrets.read(issue).ok()
    }

    pub(in super::super) fn has_opened_record(&self) -> bool {
        self.durable_kind() == Some(SnapshotKind::Opened)
    }
}
