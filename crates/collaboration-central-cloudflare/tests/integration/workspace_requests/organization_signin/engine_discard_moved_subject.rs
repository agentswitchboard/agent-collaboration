//! A competing change during discard retains the refusal without changing the engine subject.

#![cfg(test)]

use super::*;

#[tokio::test]
async fn moved_subject_retains_stale_without_domain_writes() {
    let engine = EngineOrg::new().await;
    // A subject changed meanwhile: the discard's reload finds it disabled.
    let (moved, moved_attempt) = retired_automation(&engine).await;
    let attempt_row = engine
        .rows(
            "SELECT * FROM engine_work_attempts WHERE attempt_id=?1",
            vec![text(&moved_attempt)],
        )
        .await;
    let due_rows = engine.rows(
        "SELECT * FROM engine_due_entries WHERE work_kind='automation_timer' AND subject_key=?1",
        vec![text(&moved)],
    ).await;
    let operation_id = engine.fresh();
    let runtime = engine.runtime().await;
    let send = engine.change(EngineAttemptAction::Discard, &operation_id, &moved_attempt);
    let held = runtime.hold_engine("afterEngineLoad", &moved).await;
    let change = async {
        runtime.until_held_engine(held).await;
        engine
            .raw(
                "UPDATE automation_schedules SET state='disabled' WHERE automation_id=?",
                vec![text(&moved)],
            )
            .await;
        let moved_row = engine
            .rows(
                "SELECT * FROM automation_schedules WHERE automation_id=?1",
                vec![text(&moved)],
            )
            .await;
        runtime.release_engine(held).await;
        moved_row
    };
    let (stale, moved_row) = tokio::join!(send, change);
    assert_eq!(stale, refused("stale"));
    assert_eq!(
        engine
            .rows(
                "SELECT request_id,answer FROM runtime_request_log WHERE request_id=?1",
                vec![text(&operation_id)],
            )
            .await,
        [[
            text(&operation_id),
            text(r#"{"result":"refused","reason":"stale"}"#)
        ]]
    );
    assert_eq!(
        engine
            .rows(
                "SELECT * FROM engine_work_attempts WHERE attempt_id=?1",
                vec![text(&moved_attempt)],
            )
            .await,
        attempt_row
    );
    assert_eq!(engine.rows(
        "SELECT * FROM engine_due_entries WHERE work_kind='automation_timer' AND subject_key=?1",
        vec![text(&moved)],
    ).await, due_rows);
    assert_eq!(
        engine
            .rows(
                "SELECT * FROM automation_schedules WHERE automation_id=?1",
                vec![text(&moved)],
            )
            .await,
        moved_row
    );
    assert_eq!(
        engine.record("automation_timer", &moved).await[0][..2],
        [text("retired"), integer(2)],
        "nothing changed: the record stays"
    );
    engine.close().await;
}
