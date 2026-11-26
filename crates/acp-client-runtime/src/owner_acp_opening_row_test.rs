use super::*;
use provider_session_machines::harness_lifecycle::CreateAttempt;
use uuid::Uuid;

fn queued_create(attempt: OpenAttempt, instance: OpeningInstance) -> OpeningRow {
    let row = OpeningRow::vacant(attempt, instance.clone(), false);
    let accepted = row.apply(
        &instance,
        Command::OpenCreate(Box::new(OpenCreateRequest {
            setup: OpenSetup {
                cwd: "/".into(),
                work_roots: Default::default(),
                scratch_folder: None,
            },
            requested: RequestedProviderSettings::default(),
            stopped: CancellationToken::new(),
        })),
    );
    let Ok((Some(row), Effect::OfferCreate(_))) = accepted else {
        panic!("the vacant step must own its create offer");
    };
    row
}

#[test]
fn owned_writer_step_commits_the_declared_awaiting_variant() {
    let attempt = OpenAttempt::Create(CreateAttempt(Uuid::now_v7()));
    let instance = OpeningInstance::default();
    let row = queued_create(attempt, instance.clone());

    let outcome = row.apply(&instance, Command::WriterAccepted);
    if let Ok((stored, Effect::Fault(fault))) = &outcome {
        assert!(stored.is_none(), "the table must discard a mismatching row");
        assert_eq!(fault.phase, OpeningPhase::QueuedCreate);
        assert_eq!(fault.edge, Edge::WriterAccepted);
        assert_eq!(fault.declared, OpeningPhase::AwaitingCreate);
        panic!("writer acceptance produced an isolated typed fault");
    }
    let Ok((Some(row), Effect::None)) = outcome else {
        panic!("writer acceptance must keep the exact opening and add no release effect");
    };

    assert_eq!(row.phase(), OpeningPhase::AwaitingCreate);
    assert_eq!(row.attempt(), attempt);
    assert!(row.instance().is_same_as(&instance));
}

#[test]
fn a_stale_step_keeps_the_entire_original_opening() {
    let attempt = OpenAttempt::Create(CreateAttempt(Uuid::now_v7()));
    let instance = OpeningInstance::default();
    let row = queued_create(attempt, instance.clone());

    let Err(refused) = row.apply(&OpeningInstance::default(), Command::WriterAccepted) else {
        panic!("a stale writer receipt cannot change a current opening");
    };

    assert_eq!(refused.rejection.reason, LifecycleRefusal::AttemptMismatch);
    assert_eq!(refused.row.phase(), OpeningPhase::QueuedCreate);
    assert_eq!(refused.row.attempt(), attempt);
    assert!(refused.row.instance().is_same_as(&instance));
    assert!(matches!(
        *refused.rejection._command,
        Command::WriterAccepted
    ));
}
