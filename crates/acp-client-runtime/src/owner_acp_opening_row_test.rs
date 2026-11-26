use super::*;
use provider_session_machines::harness_lifecycle::CreateAttempt;
use uuid::Uuid;

#[test]
fn owned_writer_step_commits_the_declared_awaiting_variant() {
    let attempt = OpenAttempt::Create(CreateAttempt(Uuid::now_v7()));
    let instance = OpeningInstance::default();
    let row = OpeningRow::create(
        attempt,
        instance.clone(),
        OpeningOffer::default(),
        RequestedProviderSettings::default(),
        CancellationToken::new(),
    );

    let Ok((Some(row), Effect::None)) = row.apply(&instance, Command::WriterAccepted) else {
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
    let row = OpeningRow::create(
        attempt,
        instance.clone(),
        OpeningOffer::default(),
        RequestedProviderSettings::default(),
        CancellationToken::new(),
    );

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
