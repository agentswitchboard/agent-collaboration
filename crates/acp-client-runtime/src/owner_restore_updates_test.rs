use super::*;
use provider_session_machines::{SessionGeneration, session_owner::OwnerIncarnation};
use uuid::Uuid;

#[test]
fn a_late_wire_answer_does_not_settle_a_replacement_tap_for_the_same_generation() {
    let updates = RestoreUpdates::default();
    let run = TransportRun {
        incarnation: OwnerIncarnation(Uuid::now_v7()),
        generation: SessionGeneration::from_number(1),
    };
    let (old, _old_updates) = updates.open(run, "saved-session".into());
    updates.retire(run);
    let (replacement, _fresh_updates) = updates.open(run, "saved-session".into());
    updates.reply_settled(run, &old);
    updates.discard_unoffered(run, &old);
    {
        let taps = updates.0.lock().expect("tap table");
        let current = taps.get(&run).expect("replacement tap is still registered");
        assert!(
            current.awaiting_reply,
            "an old reply must not settle the replacement tap"
        );
        assert!(current.updates.is_some());
    }
    updates.retire(run);
    updates.reply_settled(run, &replacement);
    assert!(updates.0.lock().expect("tap table").is_empty());
}
