use super::*;
use std::sync::mpsc;

fn test_wake_id(suffix: u8) -> DueDrainWakeId {
    DueDrainWakeId(uuid::Uuid::from_bytes([
        0, 0, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, suffix,
    ]))
}

#[test]
fn competing_replies_coalesce_and_the_last_accepted_reply_gets_the_transfer() {
    let state = Arc::new(DueBatchState::default());
    let (scheduled, owner) = mpsc::channel();
    state.accept_reply(test_wake_id(1), move |claim| scheduled.send(claim).unwrap());
    let claim = owner.try_recv().unwrap();
    let (scheduled, chosen) = mpsc::channel();
    for suffix in 2..=4 {
        let scheduled = scheduled.clone();
        state.accept_reply(test_wake_id(suffix), move |claim| {
            scheduled.send(claim).unwrap()
        });
        assert!(matches!(chosen.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }
    assert!(matches!(
        state.alarm_fired(test_wake_id(5)),
        Some(AlarmDrainAdmission::PublishWhileOwned)
    ));
    drop(claim);
    let transferred = chosen.try_recv().unwrap();
    assert_eq!(transferred.wake_id(), test_wake_id(4));
    assert!(matches!(chosen.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert!(matches!(
        transferred.transition(DueDrainEvent::HandoffStarted),
        Some(DueDrainEffect::RunPass { pass, .. }) if pass.number() == 1
    ));
}

#[test]
fn an_old_claim_drop_cannot_release_the_current_owner() {
    let state = Arc::new(DueBatchState::default());
    let Some(AlarmDrainAdmission::RunPass { claim: earlier, .. }) =
        state.alarm_fired(test_wake_id(1))
    else {
        panic!("the alarm did not claim idle work");
    };
    assert!(matches!(
        earlier.transition(DueDrainEvent::ShortPassFinished),
        Some(DueDrainEffect::PublishDurableAlarm(_))
    ));
    earlier.transition(DueDrainEvent::AlarmPublicationSettled);
    let Some(AlarmDrainAdmission::RunPass { claim: current, .. }) =
        state.alarm_fired(test_wake_id(2))
    else {
        panic!("the next alarm could not claim idle work");
    };
    drop(earlier);
    assert!(matches!(
        state.alarm_fired(test_wake_id(3)),
        Some(AlarmDrainAdmission::PublishWhileOwned)
    ));
    drop(current);
    assert!(matches!(
        state.alarm_fired(test_wake_id(4)),
        Some(AlarmDrainAdmission::RunPass { .. })
    ));
}
