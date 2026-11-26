use super::{OpeningEvent as Event, OpeningPhase as Phase, OpeningPolicy};
use hegel::generators as gs;
use state_machine_core::check_state_machine_transitions;
use switchboard_harness_contracts::LifecycleMove as Move;

#[test]
fn every_declared_opening_transition_is_taken_and_every_taken_transition_is_declared() {
    check_state_machine_transitions::<OpeningPolicy>(
        Phase::ALL
            .iter()
            .flat_map(|&phase| Event::ALL.iter().map(move |&event| (phase, event))),
    )
    .expect("the declared opening table and reducer must agree in both directions");
}

// The owner chooses from the work it actually has at this point. This oracle
// states the approved owner menu directly, without consulting policy edges.
fn owner_moves(phase: Phase) -> &'static [Move] {
    match phase {
        Phase::UnboundIdle => &[Move::Reopen],
        Phase::BoundIdle => &[Move::LoadBoundSession],
        Phase::QueuedCreate
        | Phase::QueuedRestore
        | Phase::AwaitingCreate
        | Phase::AwaitingRestore
        | Phase::ConfiguringCreate
        | Phase::StartingReader
        | Phase::CapturingReplay => &[Move::WaitForOpening],
        Phase::HeldCreate | Phase::HeldRestore => &[Move::ContinueHeldOpening],
        Phase::ConfiguredCreate | Phase::PreparedRestore => &[Move::ActivateOpening],
        Phase::Active => &[Move::KeepActiveRun, Move::CloseActiveRun, Move::Retire],
        Phase::OfferingClose | Phase::ClosingRestore | Phase::ClosingLive | Phase::Readmitting => {
            &[Move::WaitForClose]
        }
        Phase::CloseRefusedLive => &[Move::AdmitAgain, Move::Retire],
        Phase::RouteLost => &[Move::Retire],
        Phase::StoppedAwaitingReply => &[Move::StopOwner],
        Phase::RetiredAwaitingReply | Phase::Terminal => &[],
    }
}

fn event_for(phase: Phase, action: Move) -> Event {
    match action {
        Move::Reopen => Event::OpenCreate,
        Move::LoadBoundSession => Event::OpenRestore,
        Move::WaitForOpening => Event::WaitOpening,
        Move::ContinueHeldOpening if phase == Phase::HeldCreate => Event::Configure,
        Move::ContinueHeldOpening => Event::AcceptSettings,
        Move::ActivateOpening => Event::Activate,
        Move::KeepActiveRun => Event::KeepActive,
        Move::CloseActiveRun => Event::CloseAsked,
        Move::WaitForClose => Event::WaitClose,
        Move::AdmitAgain => Event::AdmitAgain,
        Move::Retire => Event::Retire,
        Move::StopOwner => Event::StopOnWriterLoss,
        other => panic!("unexpected phase guidance: {other:?} for {phase:?}"),
    }
}

fn check_owner_moves(phase: Phase) {
    let actual = phase.refusal().valid_moves();
    assert_eq!(actual, owner_moves(phase), "owner moves for {phase:?}");
    for &action in actual {
        assert!(
            phase.accepts(event_for(phase, action)),
            "{phase:?} advertised an event it cannot take: {action:?}"
        );
    }
}

#[test]
fn the_bound_slot_offers_its_saved_session_instead_of_creating() {
    check_owner_moves(Phase::BoundIdle);
    assert!(!Phase::BoundIdle.accepts(Event::OpenCreate));
}

#[test]
fn no_route_close_cannot_move_the_originating_opening() {
    for phase in [
        Phase::UnboundIdle,
        Phase::BoundIdle,
        Phase::QueuedCreate,
        Phase::QueuedRestore,
        Phase::AwaitingCreate,
        Phase::HeldCreate,
        Phase::ConfiguringCreate,
        Phase::ConfiguredCreate,
    ] {
        let mut actual = phase;
        assert!(!actual.advance(Event::CloseAsked), "{phase:?}");
        assert_eq!(actual, phase);
        check_owner_moves(actual);
    }
}

#[hegel::test]
fn opening_guidance_matches_the_owner_menu_through_generated_calls_and_facts(
    case: hegel::TestCase,
) {
    let initial = case.draw(gs::integers::<usize>().max_value(Phase::ALL.len() - 1));
    let mut phase = Phase::ALL[initial];
    for step in 0..case.draw(gs::integers::<u8>().min_value(1).max_value(60)) {
        check_owner_moves(phase);
        let event = if step % 2 == 0 && !owner_moves(phase).is_empty() {
            let moves = owner_moves(phase);
            let choice = case.draw(gs::integers::<usize>().max_value(moves.len() - 1));
            event_for(phase, moves[choice])
        } else {
            let choice = case.draw(gs::integers::<usize>().max_value(Event::ALL.len() - 1));
            Event::ALL[choice]
        };
        let prior = phase;
        let advanced = phase.advance(event);
        match (prior, event) {
            (Phase::ClosingRestore, Event::LateOpenReply) => {
                assert!(advanced);
                assert_eq!(phase, Phase::ClosingRestore);
            }
            (Phase::ClosingRestore, Event::CloseRefused) => {
                assert!(advanced);
                assert_eq!(phase, Phase::Terminal);
            }
            (Phase::ClosingLive, Event::CloseRefused) => {
                assert!(advanced);
                assert_eq!(phase, Phase::CloseRefusedLive);
            }
            (Phase::CloseRefusedLive, Event::AdmitAgain) => {
                assert!(advanced);
                assert_eq!(phase, Phase::Readmitting);
            }
            _ => {}
        }
    }
    check_owner_moves(phase);
}
