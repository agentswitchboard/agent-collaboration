use fleet_identity_protocol::{BindFleetSessionReply, EndFleetSessionReply};
use switchboard_session_start::{
    BindContradiction, CandidateAsker, CentralStanding, CloseResult, ConversationFact,
    CreateAnswer, CreateContradiction, DirectiveResult, EndLeg, EnvelopeRefusal, Exposure,
    FinishedOutcome, LinkResult, LocalStartDigest, OpeningOutcome, PointerFact, PutSecretResult,
    Recording, SaveResult, SetUpResult, SnapshotKind, SnapshotPhase, StartAnswer, StartDirective,
    StartEnvelope, StartNotification, StartRefusal, StartRevision, StartStateKind, StoredFacts,
    UnknownReason,
};

use super::fixtures::{
    Journey, abandoned, address, ask, candidate, central_id, envelope, instance, machine_id,
    restore, snapshot,
};

fn bind_active() -> DirectiveResult {
    DirectiveResult::SendBind(LinkResult::Answered(BindFleetSessionReply::Active {
        address: address(),
    }))
}

fn complete_local_wind_down(journey: &mut Journey) {
    while let Some(directed) = &journey.pending {
        match directed.directive {
            StartDirective::Save { .. } => journey.saved(),
            StartDirective::CloseConversation => {
                journey.result(DirectiveResult::CloseConversation(CloseResult::Closed))
            }
            ref unexpected => panic!("unexpected directive while finishing: {unexpected:?}"),
        }
    }
}

fn expect_finished_without_end(journey: &mut Journey, kind: StartStateKind) {
    complete_local_wind_down(journey);
    assert_eq!(journey.machine.kind(), kind);
    assert!(!journey.effects.contains(&"SendEnd"));
}

#[test]
fn earlier_bind_result_id_cannot_open_the_retried_bind() {
    let mut journey = Journey::binding();
    let earlier = journey.pending.as_ref().unwrap().id.clone();
    journey.result(DirectiveResult::SendBind(LinkResult::AnswerLost));
    journey.saved();
    journey.ask();
    journey.saved();
    assert_ne!(journey.pending.as_ref().unwrap().id, earlier);
    journey.refuse(
        earlier,
        bind_active(),
        StartRefusal::NotTheOutstandingDirective,
    );
    assert_eq!(journey.machine.kind(), StartStateKind::BindSent);
    assert!(!journey.effects.contains(&"CommitOpening"));
    journey.result(bind_active());
    assert_eq!(journey.machine.kind(), StartStateKind::Opening);
}

#[test]
fn unsaved_cleanup_cannot_send_end() {
    let mut journey = Journey::setting_up();
    journey.result(DirectiveResult::SetUp(SetUpResult::Failed));
    assert_eq!(journey.snapshot().kind(), SnapshotKind::CleaningUp);
    assert_eq!(journey.snapshot().start().facts.end, EndLeg::Due);
    journey.result(DirectiveResult::Save(SaveResult::NotCommitted));
    expect_finished_without_end(&mut journey, StartStateKind::Stopped);
}

#[test]
fn create_conflict_cannot_send_end() {
    let mut journey = Journey::creating();
    journey.result(DirectiveResult::SendCreate(LinkResult::Answered(
        CreateAnswer::Conflict,
    )));
    expect_finished_without_end(&mut journey, StartStateKind::Failed);
}

#[test]
fn active_create_without_local_proof_cannot_send_end() {
    let mut journey = Journey::creating();
    let observed = address();
    journey.result(DirectiveResult::SendCreate(LinkResult::Answered(
        CreateAnswer::Active {
            session_id: central_id(),
            address: observed.clone(),
        },
    )));
    let SnapshotPhase::Failed(switchboard_session_start::FailureReason::ActiveWithoutLocalProof {
        session_id,
        address,
    }) = journey.snapshot().phase()
    else {
        panic!("active evidence missing from {:?}", journey.snapshot())
    };
    assert_eq!(*session_id, central_id());
    assert_eq!(*address, observed);
    expect_finished_without_end(&mut journey, StartStateKind::Failed);
}

#[test]
fn wrong_secret_cannot_send_end() {
    let mut journey = Journey::binding();
    journey.result(DirectiveResult::SendBind(LinkResult::Answered(
        BindFleetSessionReply::WrongSecret {},
    )));
    expect_finished_without_end(&mut journey, StartStateKind::Failed);
}

#[test]
fn different_bound_id_cannot_send_end() {
    let mut journey = Journey::binding();
    journey.result(DirectiveResult::SendBind(LinkResult::Answered(
        BindFleetSessionReply::AlreadyBoundToDifferentId {},
    )));
    expect_finished_without_end(&mut journey, StartStateKind::Failed);
}

#[test]
fn contradicted_bind_cannot_send_end() {
    let mut journey = Journey::binding();
    journey.result(DirectiveResult::SendBind(LinkResult::Contradicted(
        BindContradiction::default(),
    )));
    expect_finished_without_end(&mut journey, StartStateKind::Unknown);
}

#[test]
fn contradicted_create_cannot_send_end() {
    let mut journey = Journey::creating();
    journey.result(DirectiveResult::SendCreate(LinkResult::Contradicted(
        CreateContradiction::default(),
    )));
    expect_finished_without_end(&mut journey, StartStateKind::Unknown);
}

#[test]
fn uncertain_secret_put_cannot_launch() {
    let mut journey = Journey::storing_secret();
    journey.result(DirectiveResult::PutSecret(PutSecretResult::OutcomeNotKnown));
    let intent = journey.snapshot().kind();
    journey.saved();
    assert!(
        !journey.effects.contains(&"Launch"),
        "an uncertain secret put issued Launch"
    );
    assert_eq!(intent, SnapshotKind::CleaningUp);
    assert!(matches!(
        journey.machine.facts().unwrap().secret,
        switchboard_session_start::SecretFact::PossiblyStored(_)
    ));
    assert_eq!(journey.machine.kind(), StartStateKind::Ending);
}

#[test]
fn uncertain_save_does_not_report_not_recorded() {
    let mut journey = Journey::new();
    journey.ask();
    journey.result(DirectiveResult::Save(SaveResult::OutcomeNotKnown));
    assert_eq!(journey.machine.kind(), StartStateKind::Stopped);
    assert_eq!(journey.machine.recording(), Some(Recording::NotKnown));
    assert!(matches!(
        journey.notices.last(),
        Some(StartNotification::Tell {
            answer: StartAnswer::Finished {
                recording: Recording::NotKnown,
                ..
            },
            ..
        })
    ));
}

#[test]
fn record_conflict_closes_once_without_saving_again() {
    let mut journey = Journey::setting_up();
    journey.result(DirectiveResult::SetUp(SetUpResult::Done));
    journey.result(DirectiveResult::Save(SaveResult::RevisionMismatch));
    assert_eq!(journey.machine.kind(), StartStateKind::Closing);
    journey.result(DirectiveResult::CloseConversation(CloseResult::Closed));
    assert!(
        journey.pending.is_none(),
        "issued after conflict: {:?}",
        journey.pending
    );
    assert_eq!(journey.machine.kind(), StartStateKind::Stopped);
    assert_eq!(journey.machine.recording(), Some(Recording::Conflict));
}

#[test]
fn wrong_bind_address_cannot_be_retried() {
    let mut journey = Journey::binding();
    journey.result(DirectiveResult::SendBind(LinkResult::Contradicted(
        BindContradiction::default(),
    )));
    complete_local_wind_down(&mut journey);
    journey.ask();
    if matches!(
        journey.pending.as_ref().map(|item| &item.directive),
        Some(StartDirective::Save { .. })
    ) {
        journey.saved();
    }
    assert!(journey.pending.is_none());
    assert_eq!(journey.machine.kind(), StartStateKind::Unknown);
    assert_eq!(
        journey
            .effects
            .iter()
            .filter(|tag| **tag == "SendBind")
            .count(),
        1
    );
    assert!(matches!(
        journey.notices.last(),
        Some(StartNotification::Tell {
            answer: StartAnswer::Finished {
                outcome: FinishedOutcome::Unknown(UnknownReason::Contradicted),
                ..
            },
            ..
        })
    ));
}

#[test]
fn closing_preserves_the_end_settlement_even_if_close_is_uncertain() {
    let mut journey = Journey::setting_up();
    journey.result(DirectiveResult::SetUp(SetUpResult::Failed));
    journey.saved();
    let settlement = abandoned();
    journey.result(DirectiveResult::SendEnd(LinkResult::Answered(
        EndFleetSessionReply::AlreadySettled {
            settlement: settlement.clone(),
        },
    )));
    journey.result(DirectiveResult::CloseConversation(
        CloseResult::OutcomeNotKnown,
    ));
    assert_eq!(journey.snapshot().kind(), SnapshotKind::Settled);
    assert_eq!(
        journey.snapshot().start().facts.settlement.as_ref(),
        Some(&settlement)
    );
    journey.saved();
    assert_eq!(journey.machine.kind(), StartStateKind::Settled);
    assert_eq!(
        journey.machine.facts().unwrap().end,
        EndLeg::AlreadySettled(settlement)
    );
}

#[test]
fn a_failed_close_never_causes_a_second_close() {
    let mut journey = Journey::setting_up();
    journey.result(DirectiveResult::SetUp(SetUpResult::OutcomeNotKnown));
    journey.result(DirectiveResult::CloseConversation(CloseResult::CloseFailed));
    assert_eq!(journey.snapshot().kind(), SnapshotKind::Unknown);
    journey.saved();
    assert_eq!(
        journey
            .effects
            .iter()
            .filter(|tag| **tag == "CloseConversation")
            .count(),
        1
    );
    assert_eq!(
        journey.machine.facts().unwrap().conversation,
        ConversationFact::CloseFailed
    );
}

#[test]
fn later_proven_not_written_preserves_create_exposure() {
    let mut journey = Journey::creating();
    journey.result(DirectiveResult::SendCreate(LinkResult::AnswerLost));
    journey.saved();
    journey.ask();
    journey.saved();
    journey.result(DirectiveResult::SendCreate(LinkResult::ProvenNotWritten));
    journey.saved();
    assert_eq!(journey.machine.kind(), StartStateKind::Failed);
    assert_eq!(
        journey.machine.facts().unwrap().create_exposure,
        Exposure::MayHaveReached
    );
}

#[test]
fn possibly_committed_pointer_is_not_unpointed() {
    let mut journey = Journey::saving_launch();
    assert!(matches!(
        journey.snapshot().start().facts.pointer,
        PointerFact::Committed(_)
    ));
    assert_eq!(journey.machine.facts().unwrap().pointer, PointerFact::None);
    journey.result(DirectiveResult::Save(SaveResult::OutcomeNotKnown));
    assert!(matches!(
        journey.machine.facts().unwrap().pointer,
        PointerFact::PossiblyCommitted(_)
    ));
    assert!(journey.pending.is_none());
}

fn opened() -> Journey {
    let mut journey = Journey::binding();
    journey.result(bind_active());
    journey.result(DirectiveResult::CommitOpening(OpeningOutcome::Committed(
        StartRevision::new(20),
    )));
    assert_eq!(journey.machine.kind(), StartStateKind::Opened);
    journey
}

fn different_request_cannot_see_opened_session(changed: switchboard_session_start::StartCandidate) {
    let mut journey = opened();
    let before = journey.machine.clone();
    journey.notices.clear();
    journey.send(ask(
        23,
        StartEnvelope::new(changed),
        CentralStanding::NotEnrolled,
    ));
    assert_eq!(journey.machine, before);
    assert_eq!(
        journey.notices,
        vec![StartNotification::Tell {
            slot: switchboard_session_start::WaiterSlot::new(23),
            answer: StartAnswer::RefusedDifferentRequest,
        }]
    );
}

#[test]
fn different_machine_cannot_see_stored_start() {
    let mut changed = candidate();
    changed.machine = machine_id(10);
    different_request_cannot_see_opened_session(changed);
}

#[test]
fn different_asker_cannot_see_stored_start() {
    let mut changed = candidate();
    changed.asker = CandidateAsker::Session {
        session_id: central_id(),
    };
    different_request_cannot_see_opened_session(changed);
}

#[test]
fn nonterminal_snapshots_restore_as_interrupted_and_are_retained() {
    let mut snapshots = [
        SnapshotPhase::CreateIntended,
        SnapshotPhase::CreateUnknown,
        SnapshotPhase::LaunchIntended,
        SnapshotPhase::Held,
        SnapshotPhase::BindIntended,
        SnapshotPhase::BindUnknown,
        SnapshotPhase::CleaningUp(switchboard_session_start::CleanupOutcome::Settled),
    ]
    .into_iter()
    .map(|phase| snapshot(phase, StoredFacts::default()))
    .collect::<Vec<_>>();
    snapshots.push(Journey::saving_reservation().snapshot().clone());
    for snapshot in snapshots {
        assert!(snapshot.must_be_retained());
        let mut journey = Journey::on(restore(
            instance(91),
            StartRevision::new(8),
            snapshot.clone(),
        ));
        assert_eq!(
            journey.machine.kind(),
            StartStateKind::Interrupted,
            "{snapshot:?}"
        );
        assert_eq!(journey.machine.interrupted_snapshot(), Some(&snapshot));
        journey.ask();
        assert!(journey.pending.is_none());
        assert!(matches!(
            journey.notices.last(),
            Some(StartNotification::Tell {
                answer: StartAnswer::Interrupted { .. },
                ..
            })
        ));
    }
}

#[test]
fn settled_restores_as_settled() {
    let facts = StoredFacts {
        settlement: Some(abandoned()),
        ..Default::default()
    };
    let snapshot = snapshot(SnapshotPhase::Settled, facts.clone());
    let mut journey = Journey::on(restore(instance(91), StartRevision::new(8), snapshot));
    journey.ask();
    assert_eq!(journey.machine.kind(), StartStateKind::Settled);
    assert_eq!(
        journey
            .machine
            .facts()
            .map(switchboard_session_start::StartFacts::as_stored),
        Some(&facts)
    );
    assert!(matches!(
        journey.notices.last(),
        Some(StartNotification::Tell {
            answer: StartAnswer::Finished {
                outcome: FinishedOutcome::Settled,
                ..
            },
            ..
        })
    ));
}

#[test]
fn settled_with_unknown_close_must_be_retained() {
    for conversation in [ConversationFact::CloseFailed, ConversationFact::NotKnown] {
        let facts = StoredFacts {
            conversation,
            create_exposure: Exposure::Witnessed,
            settlement: Some(abandoned()),
            ..Default::default()
        };
        assert!(snapshot(SnapshotPhase::Settled, facts).must_be_retained());
    }
}

#[test]
fn ask_refusals_produce_exactly_one_notice_in_precedence_order() {
    let mut changed = candidate();
    changed.local_digest = LocalStartDigest::new([7; 32]);
    for (mut journey, candidate, answer) in [
        (
            opened(),
            Err(EnvelopeRefusal::NoProvableAsker),
            StartAnswer::RefusedNoIdentity,
        ),
        (
            opened(),
            Err(EnvelopeRefusal::UnsupportedAgentProgram),
            StartAnswer::Refused(EnvelopeRefusal::UnsupportedAgentProgram),
        ),
        (
            opened(),
            StartEnvelope::new(changed),
            StartAnswer::RefusedDifferentRequest,
        ),
        (
            Journey::new(),
            Ok(envelope()),
            StartAnswer::RefusedNoCentral(CentralStanding::NotEnrolled),
        ),
    ] {
        let before = journey.machine.clone();
        journey.notices.clear();
        journey.send(ask(35, candidate, CentralStanding::NotEnrolled));
        assert_eq!(journey.machine, before);
        assert_eq!(
            journey.notices,
            vec![StartNotification::Tell {
                slot: switchboard_session_start::WaiterSlot::new(35),
                answer,
            }]
        );
        assert!(journey.pending.is_none());
    }
}

#[test]
fn directive_refusals_have_no_output_and_do_not_consume_the_directive() {
    let mut journey = Journey::binding();
    let id = journey.pending.as_ref().unwrap().id.clone();
    journey.refuse(
        id.clone(),
        DirectiveResult::SetUp(SetUpResult::Done),
        StartRefusal::NotExpectedHere,
    );
    journey.refuse(
        switchboard_session_start::DirectiveId {
            instance: instance(91),
            ..id
        },
        bind_active(),
        StartRefusal::NotTheOutstandingDirective,
    );
    journey.result(bind_active());
    assert_eq!(journey.machine.kind(), StartStateKind::Opening);
}
