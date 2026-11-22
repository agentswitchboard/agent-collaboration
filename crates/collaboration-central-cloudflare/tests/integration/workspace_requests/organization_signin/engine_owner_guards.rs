//! The same observed-row guard journeys run on the two persistent hosts.
#![cfg(test)]

use super::{engine_admin_support::*, support::FIXTURE_MACHINE};
use central_storage::SqlValue;

const NOTICE: &str = "019f0000-0000-7000-8000-000000000201";

#[derive(Clone, Copy)]
enum OwnerCase {
    Wake,
    Schedule,
    Expiry,
}

impl OwnerCase {
    const ALL: [Self; 3] = [Self::Wake, Self::Schedule, Self::Expiry];

    fn kind(self) -> &'static str {
        match self {
            Self::Wake => "wake_timer",
            Self::Schedule => "automation_timer",
            Self::Expiry => "system_message_expiry",
        }
    }

    fn read_row(self) -> &'static str {
        match self {
            Self::Wake => "SELECT state,updated_at FROM automation_wakes WHERE wake_id=?1",
            Self::Schedule => {
                "SELECT state,updated_at,evaluated_through_at,next_due_at FROM automation_schedules WHERE automation_id=?1"
            }
            Self::Expiry => {
                "SELECT state,updated_at,failure_reason FROM mail_messages WHERE message_id=?1"
            }
        }
    }

    fn read_full_row(self) -> &'static str {
        match self {
            Self::Wake => "SELECT * FROM automation_wakes WHERE wake_id=?1",
            Self::Schedule => "SELECT * FROM automation_schedules WHERE automation_id=?1",
            Self::Expiry => "SELECT * FROM mail_messages WHERE message_id=?1",
        }
    }

    fn move_version(self) -> &'static str {
        match self {
            Self::Wake => "UPDATE automation_wakes SET updated_at=?1 WHERE wake_id=?2",
            Self::Schedule => {
                "UPDATE automation_schedules SET updated_at=?1 WHERE automation_id=?2"
            }
            Self::Expiry => "UPDATE mail_messages SET updated_at=?1 WHERE message_id=?2",
        }
    }

    fn replace_state(self) -> &'static str {
        match self {
            Self::Wake => "UPDATE automation_wakes SET state=?1,updated_at=?2 WHERE wake_id=?3",
            Self::Schedule => {
                "UPDATE automation_schedules SET state=?1,updated_at=?2 WHERE automation_id=?3"
            }
            Self::Expiry => "UPDATE mail_messages SET state=?1,updated_at=?2 WHERE message_id=?3",
        }
    }

    fn state_pair(self) -> (&'static str, &'static str) {
        match self {
            Self::Wake | Self::Schedule => ("active", "disabled"),
            Self::Expiry => ("queued", "cancelled"),
        }
    }

    async fn seed(self, engine: &EngineOrg, due: Time, damaged: bool) -> String {
        match self {
            Self::Wake => {
                let id = engine.wake(due, "wake guard").await;
                if damaged {
                    engine
                        .raw(
                            "UPDATE automation_wakes SET target=x'ff' WHERE wake_id=?1",
                            vec![text(&id)],
                        )
                        .await;
                }
                id
            }
            Self::Schedule => {
                let id = engine.automation(due, "schedule guard").await;
                if damaged {
                    engine.raw("UPDATE automation_schedules SET timing_rule=x'ff' WHERE automation_id=?1", vec![text(&id)]).await;
                }
                id
            }
            Self::Expiry => {
                let reason = if damaged {
                    text("damaged")
                } else {
                    SqlValue::Null
                };
                engine.raw(
                    "INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,failure_reason,created_at,updated_at,delivery_window_ends_at) VALUES (?1,'{}',?2,'{}','notice','{}','queued',?3,?4,?4,?5)",
                    vec![text(NOTICE), text(FIXTURE_MACHINE), reason, stamp(due - chrono::Duration::hours(24)), stamp(due)],
                ).await;
                NOTICE.to_owned()
            }
        }
    }
}

#[tokio::test]
async fn owner_transitions_refuse_stale_rows_and_apply_nullable_guards() {
    for owner in OwnerCase::ALL {
        let engine = EngineOrg::new().await;
        let due = next_mark(engine.now() + minutes(1));
        let key = owner.seed(&engine, due, false).await;
        let mut before = engine.rows(owner.read_row(), vec![text(&key)]).await;
        match owner {
            OwnerCase::Wake => assert_eq!(before[0][0], text("active")),
            OwnerCase::Schedule => {
                assert_eq!(before[0][0], text("active"));
                assert_eq!(before[0][2], SqlValue::Null);
            }
            OwnerCase::Expiry => {
                assert_eq!(before[0][0], text("queued"));
                assert_eq!(before[0][2], SqlValue::Null);
            }
        }

        let runtime = engine.runtime().await;
        let replacement = due - chrono::Duration::milliseconds(1);
        let (runnable, terminal) = owner.state_pair();
        let held = runtime.hold_engine("afterEngineLoad", &key).await;
        let mutate = async {
            runtime.until_held_engine(held).await;
            engine
                .raw(
                    owner.replace_state(),
                    vec![text(terminal), stamp(replacement), text(&key)],
                )
                .await;
            runtime.release_engine(held).await;
        };
        engine.set(due);
        tokio::join!(engine.after_event(), mutate);

        before[0][0] = text(terminal);
        before[0][1] = stamp(replacement);
        assert_eq!(
            engine.rows(owner.read_row(), vec![text(&key)]).await,
            before,
            "{}",
            owner.kind()
        );
        assert!(engine.record(owner.kind(), &key).await.is_empty());

        engine
            .raw(
                owner.replace_state(),
                vec![text(runnable), stamp(replacement), text(&key)],
            )
            .await;
        engine.set(due + chrono::Duration::seconds(31));
        engine.after_event().await;
        let after = engine.rows(owner.read_row(), vec![text(&key)]).await;
        match owner {
            OwnerCase::Wake => {
                assert_eq!(after[0][0], text("done"));
                assert_eq!(engine.wake_messages(&key).await, 1);
            }
            OwnerCase::Schedule => {
                assert_eq!(after[0][0], text("done"));
                assert_eq!(after[0][2], stamp(due + chrono::Duration::seconds(31)));
                assert_eq!(after[0][3], SqlValue::Null);
            }
            OwnerCase::Expiry => {
                assert_eq!(after[0][0], text("cancelled"));
                assert_eq!(after[0][2], text("expired24Hours"));
            }
        }

        engine.close().await;
    }
}

#[tokio::test]
async fn owner_attempts_preserve_raw_damage_and_refuse_a_stale_snapshot() {
    for owner in OwnerCase::ALL {
        let engine = EngineOrg::new().await;
        let due = next_mark(engine.now() + minutes(1));
        let key = owner.seed(&engine, due, true).await;
        let runtime = engine.runtime().await;
        let replacement = due - chrono::Duration::milliseconds(1);
        let held = runtime.hold_engine("beforeEngineFailureWrite", &key).await;
        let mutate = async {
            runtime.until_held_engine(held).await;
            engine
                .raw(owner.move_version(), vec![stamp(replacement), text(&key)])
                .await;
            runtime.release_engine(held).await;
        };
        engine.set(due);
        tokio::join!(engine.after_event(), mutate);

        assert!(
            engine.record(owner.kind(), &key).await.is_empty(),
            "{}",
            owner.kind()
        );

        engine.set(due + chrono::Duration::seconds(31));
        engine.after_event().await;
        let attempt = engine.record(owner.kind(), &key).await;
        assert_eq!(attempt.len(), 1, "{}", owner.kind());
        assert_eq!(attempt[0][0], text("failing"));
        assert_eq!(attempt[0][1], integer(1));
        assert_eq!(attempt[0][3], text("corrupt_row"));

        engine.close().await;
    }
}

#[tokio::test]
async fn expiry_preserves_a_concurrent_reason_only_write() {
    let engine = EngineOrg::new().await;
    let due = next_mark(engine.now() + minutes(1));
    let key = OwnerCase::Expiry.seed(&engine, due, false).await;
    let read = OwnerCase::Expiry.read_row();
    let mut expected = engine.rows(read, vec![text(&key)]).await;
    assert_eq!(expected[0][0], text("queued"));
    assert_eq!(expected[0][2], SqlValue::Null);

    let runtime = engine.runtime().await;
    let held = runtime.hold_engine("afterEngineLoad", &key).await;
    let competing = async {
        runtime.until_held_engine(held).await;
        engine
            .raw(
                "UPDATE mail_messages SET failure_reason='competing-reason' WHERE message_id=?1",
                vec![text(&key)],
            )
            .await;
        runtime.release_engine(held).await;
    };
    engine.set(due);
    tokio::join!(engine.after_event(), competing);

    expected[0][2] = text("competing-reason");
    assert_eq!(engine.rows(read, vec![text(&key)]).await, expected);
    engine.close().await;
}

#[derive(Clone, Copy)]
enum AttemptRace {
    Delete,
    Update,
}

async fn run_prior_attempt_race(owner: OwnerCase, race: AttemptRace) {
    let engine = EngineOrg::new().await;
    let due = next_mark(engine.now() + minutes(1));
    let key = owner.seed(&engine, due, true).await;
    let runtime = engine.runtime().await;
    engine.set(due);
    engine.after_event().await;
    let attempt_key = || vec![text(owner.kind()), text(&key)];
    let read_attempt = "SELECT failure_count,next_try_at,attempt_id,machine_kind,subject_key,state,last_failure,last_failure_detail,first_failed_at,last_failed_at,retired_at,created_at,updated_at FROM engine_work_attempts WHERE machine_kind=?1 AND subject_key=?2";
    let attempts = engine.rows(read_attempt, attempt_key()).await;
    let [first] = attempts.as_slice() else {
        panic!(
            "expected one recorded failure for {}: {attempts:?}",
            owner.kind()
        );
    };
    assert_eq!(first[0], integer(1));
    assert_eq!(first[5], text("failing"));
    let SqlValue::Text(next_try) = &first[1] else {
        panic!("first failure has no scheduled retry: {first:?}");
    };
    let retry: Time = next_try.parse().unwrap();
    let owner_read = owner.read_full_row();
    let owner_before = engine.rows(owner_read, vec![text(&key)]).await;

    let held = runtime.hold_engine("beforeEngineFailureWrite", &key).await;
    let competing = async {
        runtime.until_held_engine(held).await;
        match race {
            AttemptRace::Delete => engine.raw(
                "DELETE FROM engine_work_attempts WHERE machine_kind=?1 AND subject_key=?2", attempt_key(),
            ).await,
            AttemptRace::Update => engine.raw(
                "UPDATE engine_work_attempts SET failure_count=7 WHERE machine_kind=?1 AND subject_key=?2", attempt_key(),
            ).await,
        }
        runtime.release_engine(held).await;
    };
    engine.set(retry);
    tokio::join!(engine.after_event(), competing);

    assert_eq!(
        engine.rows(owner_read, vec![text(&key)]).await,
        owner_before
    );
    let actual = engine.rows(read_attempt, attempt_key()).await;
    match race {
        AttemptRace::Delete => assert!(
            actual.is_empty(),
            "deleted prior {} attempt reappeared: {actual:?}",
            owner.kind()
        ),
        AttemptRace::Update => {
            let mut expected = first.clone();
            expected[0] = integer(7);
            assert_eq!(
                actual,
                vec![expected],
                "competing {} attempt changed",
                owner.kind()
            );
        }
    }
    engine.close().await;
}

#[tokio::test]
async fn wake_attempt_does_not_resurrect_deleted_prior() {
    run_prior_attempt_race(OwnerCase::Wake, AttemptRace::Delete).await;
}

#[tokio::test]
async fn schedule_attempt_does_not_resurrect_deleted_prior() {
    run_prior_attempt_race(OwnerCase::Schedule, AttemptRace::Delete).await;
}

#[tokio::test]
async fn expiry_attempt_does_not_resurrect_deleted_prior() {
    run_prior_attempt_race(OwnerCase::Expiry, AttemptRace::Delete).await;
}

#[tokio::test]
async fn owner_attempts_preserve_the_exact_competing_prior_update() {
    for owner in OwnerCase::ALL {
        run_prior_attempt_race(owner, AttemptRace::Update).await;
    }
}
