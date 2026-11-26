use super::*;
use agent_client_protocol::UntypedMessage;
use hegel::generators as gs;
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
        let current = taps
            .iter()
            .find(|tap| tap.names(run, &replacement))
            .expect("replacement tap is still registered");
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

const SAVED_SESSION: &str = "saved-session";
const OTHER_SESSION: &str = "other-session";

fn run(incarnation: OwnerIncarnation, generation: u64) -> TransportRun {
    TransportRun {
        incarnation,
        generation: SessionGeneration::from_number(generation),
    }
}

fn notification(session: &str, method: &str) -> Dispatch {
    Dispatch::Notification(
        UntypedMessage::new(method, serde_json::json!({ "sessionId": session }))
            .expect("construct the public ACP notification"),
    )
}

fn observed(
    updates: &RestoreUpdates,
    current: TransportRun,
    stamp: &OpeningInstance,
    receive: &mut mpsc::UnboundedReceiver<Dispatch>,
    session: &str,
    method: &str,
) -> bool {
    assert!(
        updates
            .forward(
                session,
                RestoreSelection::Registered {
                    run: current,
                    instance: stamp.clone()
                },
                notification(session, method)
            )
            .is_none(),
        "the current restore consumes or drops the frame"
    );
    match receive.try_recv() {
        Ok(Dispatch::Notification(update)) => {
            assert_eq!(update.method(), method);
            assert_eq!(update.params()["sessionId"], session);
            true
        }
        Err(mpsc::error::TryRecvError::Empty) => false,
        other => panic!("unexpected tap result: {other:?}"),
    }
}

#[test]
fn a_same_generation_replacement_keeps_its_earlier_unanswered_state_marker() {
    let updates = RestoreUpdates::default();
    let current = run(OwnerIncarnation(Uuid::now_v7()), 1);
    let (prior, _old_updates) = updates.open(current, SAVED_SESSION.into());
    updates.retire(current);
    let (current_stamp, mut receive) = updates.open(current, SAVED_SESSION.into());
    assert!(
        !observed(
            &updates,
            current,
            &current_stamp,
            &mut receive,
            SAVED_SESSION,
            STATE_METHOD
        ),
        "earlier same-generation marker was erased before its exact wire reply"
    );
    assert!(observed(
        &updates,
        current,
        &current_stamp,
        &mut receive,
        SAVED_SESSION,
        "session/update"
    ));
    updates.reply_settled(current, &prior);
    assert!(observed(
        &updates,
        current,
        &current_stamp,
        &mut receive,
        SAVED_SESSION,
        STATE_METHOD
    ));
}

#[hegel::test]
fn replay_custody_agrees_with_the_overlap_oracle_for_reused_and_fresh_generations(
    case: hegel::TestCase,
) {
    let prior_count = case.draw(gs::integers::<usize>().min_value(1).max_value(5));
    let operations: Vec<_> = (0..case.draw(gs::integers::<u8>().min_value(6).max_value(40)))
        .map(|_| {
            (
                case.draw(gs::integers::<usize>().max_value(prior_count + 1)),
                case.draw(gs::integers::<u8>().max_value(2)),
            )
        })
        .collect();

    for reuse in [false, true] {
        let updates = RestoreUpdates::default();
        let owner = OwnerIncarnation(Uuid::now_v7());
        let prior: Vec<_> = (0..prior_count)
            .map(|index| {
                let route = run(owner, if reuse { 1 } else { (index + 1) as u64 });
                let (stamp, _retired) = updates.open(route, SAVED_SESSION.into());
                updates.retire(route);
                (route, stamp)
            })
            .collect();
        let current = run(owner, if reuse { 1 } else { (prior_count + 1) as u64 });
        let (current_stamp, mut receive) = updates.open(current, SAVED_SESSION.into());
        let other = run(owner, (prior_count + 2) as u64);
        let (other_stamp, mut other_receive) = updates.open(other, OTHER_SESSION.into());
        let mut earlier_can_answer = vec![true; prior_count];

        for &(true_source, operation) in &operations {
            if operation == 0 {
                if let Some((route, stamp)) = prior.get(true_source) {
                    updates.reply_settled(*route, stamp);
                    earlier_can_answer[true_source] = false;
                } else if true_source == prior_count {
                    updates.reply_settled(current, &current_stamp);
                } else {
                    updates.reply_settled(other, &other_stamp);
                }
                continue;
            }

            let method = if operation == 1 {
                STATE_METHOD
            } else {
                "session/update"
            };
            // The generator knows the real source. ACP only discloses its session.
            // Ordinary updates enter current replay even when their source retired.
            let actual = if true_source == prior_count + 1 {
                observed(
                    &updates,
                    other,
                    &other_stamp,
                    &mut other_receive,
                    OTHER_SESSION,
                    method,
                )
            } else {
                observed(
                    &updates,
                    current,
                    &current_stamp,
                    &mut receive,
                    SAVED_SESSION,
                    method,
                )
            };
            let expected = true_source == prior_count + 1
                || operation == 2
                || !earlier_can_answer.iter().any(|&awaiting| awaiting);
            assert_eq!(
                actual, expected,
                "reuse={reuse}, source={true_source}, method={method}, earlier={earlier_can_answer:?}"
            );
        }
    }
}
