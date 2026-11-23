use fleet_identity_protocol::{
    BindFleetSessionReply, CreateFleetSessionReply, EndFleetSession, EndFleetSessionReply,
    EndReplyContext, EndSessionTarget, RouterEndCause,
};
use hegel::generators as gs;
use switchboard_session_start::{
    CandidateAsker, CentralStanding, CloseResult, CreateAnswer, CreateReplyKind, DirectiveResult,
    EndContradiction, EndLeg, EndReplyKind, Exposure, FailureReason, FinishedOutcome,
    IntendedOutcome, LinkResult, SaveResult, SecretFact, SetUpResult, SnapshotKind, StartAnswer,
    StartDirective, StartEnvelope, StartFault, StartNotification, StartStateKind,
};

use super::{
    central_contract::checked_create,
    executor::{Executor, Timing},
    fixtures::{self, Journey},
};

pub(super) fn drive_to_exact(executor: &mut Executor, target: StartStateKind) {
    if executor.machine.kind() == StartStateKind::NotStarted {
        executor.feed(fixtures::ask(
            1,
            Ok(fixtures::envelope()),
            CentralStanding::Ready,
        ));
    }
    for _ in 0..12 {
        if executor.machine.kind() == target {
            return;
        }
        executor.complete_step(0, Timing::BeforeFence, false);
    }
    assert_eq!(executor.machine.kind(), target);
}

fn started_result(journey: &Journey) -> &StartAnswer {
    let Some(StartNotification::Tell { answer, .. }) = journey.notices.last() else {
        panic!("waiter outcome missing")
    };
    answer
}

#[test]
fn stopped_keeps_active_pair_after_failed_final_save() {
    for save in [
        SaveResult::NotCommitted,
        SaveResult::RevisionMismatch,
        SaveResult::OutcomeNotKnown,
    ] {
        let mut journey = Journey::creating();
        let prior_revision = journey.machine.revision();
        let session_id = fixtures::central_id();
        let address = fixtures::address();
        journey.result(DirectiveResult::SendCreate(LinkResult::Answered(
            CreateAnswer::Active {
                session_id: session_id.clone(),
                address: address.clone(),
            },
        )));
        journey.result(DirectiveResult::Save(save));
        let StartAnswer::Finished {
            outcome:
                FinishedOutcome::Stopped {
                    while_saving,
                    intended,
                    ..
                },
            ..
        } = started_result(&journey)
        else {
            panic!("expected stopped outcome")
        };
        assert_eq!(*while_saving, SnapshotKind::Failed);
        assert_eq!(
            *intended,
            Some(IntendedOutcome::Failed(
                FailureReason::ActiveWithoutLocalProof {
                    session_id,
                    address
                }
            ))
        );
        assert_eq!(journey.machine.revision(), prior_revision);
        assert!(journey.pending.is_none());
    }
}

#[test]
fn stopped_keeps_cleanup_reason_through_the_only_close() {
    let mut journey = Journey::setting_up();
    journey.result(DirectiveResult::SetUp(SetUpResult::Failed));
    journey.result(DirectiveResult::Save(SaveResult::RevisionMismatch));
    journey.result(DirectiveResult::CloseConversation(CloseResult::Closed));
    let StartAnswer::Finished {
        outcome:
            FinishedOutcome::Stopped {
                while_saving,
                intended,
                ..
            },
        ..
    } = started_result(&journey)
    else {
        panic!("expected stopped outcome")
    };
    assert_eq!(*while_saving, SnapshotKind::CleaningUp);
    assert_eq!(
        *intended,
        Some(IntendedOutcome::CleaningUp(FailureReason::SetUpFailed))
    );
    assert!(journey.pending.is_none());
}

#[test]
fn stopped_keeps_exact_settlement_when_terminal_save_is_uncertain() {
    let mut journey = Journey::setting_up();
    journey.result(DirectiveResult::SetUp(SetUpResult::Failed));
    journey.saved();
    let settlement = fixtures::abandoned();
    journey.result(DirectiveResult::SendEnd(LinkResult::Answered(
        EndFleetSessionReply::AlreadySettled {
            settlement: settlement.clone(),
        },
    )));
    journey.result(DirectiveResult::CloseConversation(
        CloseResult::OutcomeNotKnown,
    ));
    journey.result(DirectiveResult::Save(SaveResult::OutcomeNotKnown));
    let StartAnswer::Finished {
        outcome:
            FinishedOutcome::Stopped {
                while_saving,
                intended,
                ..
            },
        facts,
        ..
    } = started_result(&journey)
    else {
        panic!("expected stopped outcome")
    };
    assert_eq!(*while_saving, SnapshotKind::Settled);
    assert_eq!(*intended, Some(IntendedOutcome::Settled));
    assert_eq!(facts.settlement.as_ref(), Some(&settlement));
    assert_eq!(facts.end, EndLeg::AlreadySettled(settlement));
}

#[test]
fn secret_put_waits_for_reserved_and_can_be_found_after_restart() {
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::CreateSent);
    executor.take(0);
    executor.complete(false);
    assert!(
        !executor.has_pending_secret(),
        "a completed reply is not yet delivered"
    );
    executor.deliver();
    assert!(executor.has_pending_secret());
    assert_eq!(executor.machine.kind(), StartStateKind::SavingReservation);
    assert_eq!(
        executor.durable_snapshot().unwrap().1.kind(),
        SnapshotKind::CreateIntended
    );
    executor.complete_step(0, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::StoringSecret);
    executor.take(0);
    executor.complete(false);
    let (revision, durable) = executor.durable_snapshot().unwrap();
    let (reservation, issue) = durable.reserved().unwrap();
    assert_eq!(reservation.session_id, fixtures::central_id());
    assert_eq!(executor.secret_at(issue), Some(fixtures::secret(17)));
    assert!(!executor.has_pending_secret());
    let restored = fixtures::restore(fixtures::instance(55), revision, durable.clone());
    assert_eq!(restored.kind(), StartStateKind::Interrupted);
    assert_eq!(restored.interrupted_snapshot(), Some(&durable));
    assert!(restored.outstanding_id().is_none());
}

#[test]
fn unsaved_reservation_discards_pending_secret_and_never_writes_the_store() {
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::SavingReservation);
    assert!(executor.has_pending_secret());
    executor.complete_step(3, Timing::BeforeFence, true);
    assert_eq!(executor.machine.kind(), StartStateKind::Stopped);
    assert!(!executor.has_pending_secret());
    assert_eq!(executor.secret_at(&fixtures::issue()), None);
    assert!(executor.pending.is_none());
}

#[test]
fn stale_reserved_at_delivery_never_stages_a_secret() {
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::CreateSent);
    executor.take(0);
    executor.complete(false);
    executor.retire_connection();
    executor.deliver();
    assert!(!executor.has_pending_secret());
    assert_eq!(executor.machine.facts().unwrap().reservation, None);
    executor.complete_step(0, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::CreateUnknown);
}

#[test]
fn collision_preserves_existing_secret_and_enters_cleanup_without_launch() {
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::StoringSecret);
    let (_, reserved) = executor.durable_snapshot().unwrap();
    let (_, issue) = reserved.reserved().unwrap();
    executor.complete_step(4, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::SavingCleanUp);
    assert_eq!(
        executor.machine.facts().unwrap().secret,
        SecretFact::PossiblyStored(issue.clone())
    );
    assert_eq!(executor.secret_at(issue), Some(fixtures::secret(37)));
    assert!(!executor.has_pending_secret());
    assert!(matches!(
        executor.pending.as_ref().map(|pending| &pending.directive),
        Some(StartDirective::Save { snapshot, .. }) if snapshot.kind() == SnapshotKind::CleaningUp
    ));
}

#[test]
fn bind_completed_on_a_retired_connection_cannot_open_on_delivery() {
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::BindSent);
    executor.take(0);
    executor.complete(false);
    executor.retire_connection();
    executor.deliver();
    assert_eq!(executor.machine.kind(), StartStateKind::SavingOutcome);
    assert_eq!(
        executor.machine.facts().unwrap().bind_exposure,
        Exposure::MayHaveReached
    );
    assert!(!executor.has_opened_record());
    executor.complete_step(0, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::BindUnknown);
}

#[test]
fn retired_connection_at_opening_take_requires_a_fresh_checked_bind() {
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::Opening);
    assert_eq!(
        executor.machine.facts().unwrap().bind_exposure,
        Exposure::Witnessed
    );
    executor.retire_connection();
    executor.complete_step(0, Timing::BeforeFence, false);
    assert!(
        !executor.has_opened_record(),
        "opening committed on the retired connection"
    );
    executor.complete_step(0, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::BindUnknown);
    executor.feed(fixtures::ask(
        2,
        Ok(fixtures::envelope()),
        CentralStanding::Ready,
    ));
    executor.complete_step(0, Timing::BeforeFence, false);
    executor.complete_step(0, Timing::BeforeFence, false);
    executor.complete_step(0, Timing::BeforeFence, false);
    assert!(executor.has_opened_record());
}

#[test]
fn lost_original_hold_precedes_connection_retirement_at_opening_take() {
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::Opening);
    executor.retire_connection();
    executor.lose_original_hold();
    executor.complete_step(0, Timing::BeforeFence, false);
    assert!(!executor.has_opened_record());
    assert_eq!(executor.machine.kind(), StartStateKind::SavingCleanUp);
    assert_eq!(executor.machine.facts().unwrap().end, EndLeg::Due);
    assert_eq!(
        executor.machine.facts().unwrap().harness_session_id,
        Some(fixtures::harness_id())
    );
}

#[test]
fn opening_checks_hold_and_connection_at_the_write_and_preserves_committed_results() {
    for (lose_hold, expected) in [
        (true, StartStateKind::SavingCleanUp),
        (false, StartStateKind::SavingOutcome),
    ] {
        let mut executor = Executor::new();
        drive_to_exact(&mut executor, StartStateKind::Opening);
        executor.take(0);
        executor.retire_connection();
        if lose_hold {
            executor.lose_original_hold();
        }
        executor.complete(false);
        assert!(!executor.has_opened_record());
        executor.deliver();
        assert_eq!(executor.machine.kind(), expected);
    }
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::Opening);
    executor.take(0);
    executor.complete(false);
    assert!(executor.has_opened_record());
    executor.retire_connection();
    executor.lose_original_hold();
    executor.deliver();
    assert_eq!(executor.machine.kind(), StartStateKind::Opened);
}

#[hegel::test(test_cases = 100)]
fn generated_delivery_and_opening_custody_changes(case: hegel::TestCase) {
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::BindSent);
    executor.take(0);
    executor.complete(false);
    let retire_before_delivery = case.draw(gs::booleans());
    if retire_before_delivery {
        if case.draw(gs::booleans()) {
            executor.retire_connection();
        } else {
            executor.end_enrollment();
        }
        executor.deliver();
        assert_eq!(executor.machine.kind(), StartStateKind::SavingOutcome);
        return;
    }
    executor.deliver();
    let lose_hold = case.draw(gs::booleans());
    let retire = case.draw(gs::booleans());
    if lose_hold {
        executor.lose_original_hold();
    }
    if retire {
        executor.retire_connection();
    }
    executor.complete_step(0, Timing::BeforeFence, false);
    let expected = match (lose_hold, retire) {
        (true, _) => StartStateKind::SavingCleanUp,
        (false, true) => StartStateKind::SavingOutcome,
        (false, false) => StartStateKind::Opened,
    };
    assert_eq!(executor.machine.kind(), expected);
    assert_eq!(
        executor.has_opened_record(),
        expected == StartStateKind::Opened
    );
}

#[test]
fn uncertain_setup_remains_observable_until_close_stops_it() {
    let mut executor = Executor::new();
    drive_to_exact(&mut executor, StartStateKind::SettingUp);
    executor.take(4);
    assert!(executor.setup_is_running());
    executor.complete(false);
    executor.deliver();
    assert!(executor.setup_is_running());
    executor.take(0);
    assert!(executor.setup_is_running());
    executor.complete(false);
    assert!(!executor.setup_is_running());
    assert_eq!(executor.close_attempts(), 1);
    executor.deliver();
    executor.complete_step(0, Timing::BeforeFence, false);
    assert_eq!(executor.machine.kind(), StartStateKind::Unknown);
    assert_eq!(
        executor.machine.facts().unwrap().harness_session_id,
        Some(fixtures::harness_id())
    );
}

#[test]
fn close_does_not_act_after_ownership_of_the_original_is_lost() {
    use switchboard_session_start::ConversationFact;
    for (proved_gone, expected) in [
        (true, ConversationFact::Closed),
        (false, ConversationFact::NotKnown),
    ] {
        let mut executor = Executor::new();
        drive_to_exact(&mut executor, StartStateKind::SettingUp);
        executor.complete_step(4, Timing::BeforeFence, false);
        assert_eq!(executor.machine.kind(), StartStateKind::Closing);
        executor.take(1);
        if proved_gone {
            executor.observe_original_gone();
        } else {
            executor.lose_original_hold();
        }
        executor.complete(false);
        assert_eq!(executor.close_attempts(), 0);
        executor.deliver();
        assert_eq!(executor.machine.facts().unwrap().conversation, expected);
    }
}

#[test]
fn checked_contradiction_keeps_observed_reservation_and_no_credential() {
    let mut journey = Journey::new();
    let mut candidate = fixtures::candidate();
    candidate.asker = CandidateAsker::Session {
        session_id: fixtures::central_id(),
    };
    journey.send(fixtures::ask(
        1,
        StartEnvelope::new(candidate),
        CentralStanding::Ready,
    ));
    journey.saved();
    let StartDirective::SendCreate(request) = &journey.pending.as_ref().unwrap().directive else {
        panic!("expected create")
    };
    let (result, pending) = checked_create(
        request,
        CreateFleetSessionReply::Reserved {
            session_id: fixtures::central_id(),
            reserved_until: fixtures::time(),
            secret: fixtures::secret(89),
        },
    );
    assert!(pending.is_none());
    journey.result(result);
    assert_eq!(
        journey
            .snapshot()
            .start()
            .facts
            .reservation
            .as_ref()
            .map(|reservation| (&reservation.session_id, &reservation.reserved_until)),
        Some((&fixtures::central_id(), &fixtures::time()))
    );
    assert_eq!(journey.snapshot().start().facts.secret, SecretFact::None);
    assert!(matches!(
        journey.notices.as_slice(),
        [StartNotification::ReportFault(
            StartFault::CreateContradicted {
                reply: Some(CreateReplyKind::Reserved),
                ..
            }
        )]
    ));
}

#[test]
fn checked_central_id_not_yours_is_reported_as_end_not_yours() {
    let mut journey = Journey::setting_up();
    journey.result(DirectiveResult::SetUp(SetUpResult::Failed));
    journey.saved();
    let request = EndFleetSession {
        target: EndSessionTarget::CentralId {
            session_id: fixtures::central_id(),
        },
        cause: RouterEndCause::GivenUp {},
    };
    assert!(
        EndReplyContext::from_request(&request)
            .check(EndFleetSessionReply::NotYoursToEnd {})
            .is_err()
    );
    journey.result(DirectiveResult::SendEnd(LinkResult::Contradicted(
        EndContradiction {
            reply: Some(EndReplyKind::NotYoursToEnd),
        },
    )));
    assert!(matches!(
        journey.notices.as_slice(),
        [StartNotification::ReportFault(StartFault::EndNotYours(_))]
    ));
    assert_eq!(journey.machine.facts().unwrap().end, EndLeg::NotKnown);
}

#[test]
fn close_preserves_the_harness_id_for_every_close_disposition() {
    for outcome in [
        CloseResult::Closed,
        CloseResult::CloseFailed,
        CloseResult::OutcomeNotKnown,
    ] {
        let mut journey = Journey::setting_up();
        journey.result(DirectiveResult::SetUp(SetUpResult::OutcomeNotKnown));
        journey.result(DirectiveResult::CloseConversation(outcome));
        assert_eq!(
            journey.snapshot().start().facts.harness_session_id,
            Some(fixtures::harness_id())
        );
        journey.saved();
        assert_eq!(
            journey.machine.facts().unwrap().harness_session_id,
            Some(fixtures::harness_id())
        );
    }
}

#[test]
fn end_proved_not_written_was_transmitted_and_is_not_applied() {
    let mut journey = Journey::binding();
    journey.result(DirectiveResult::SendBind(LinkResult::Answered(
        BindFleetSessionReply::AddressTaken {},
    )));
    journey.saved();
    journey.result(DirectiveResult::SendEnd(LinkResult::ProvenNotWritten));
    journey.result(DirectiveResult::CloseConversation(CloseResult::Closed));
    journey.saved();
    assert_eq!(
        journey.machine.facts().unwrap().end,
        EndLeg::NotApplied(switchboard_session_start::EndNotApplied::ProvenNotWritten)
    );
}
