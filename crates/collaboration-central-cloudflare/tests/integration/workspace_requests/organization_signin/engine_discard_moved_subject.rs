//! A competing change during discard leaves only the admitted refusal writes.

#![cfg(test)]

use super::*;

#[tokio::test]
async fn moved_subject_retains_stale_with_proof_without_domain_writes() {
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
    let proof_id = engine.fresh();
    let operation_id = engine.fresh();
    let organization = super::super::support::ORG;
    let body =
        json!({"organizationId":organization,"operationId":operation_id,"attemptId":moved_attempt});
    let intent = json!({"v":1,"operation":"EngineDiscard","organization":organization,"target":organization,"operationId":operation_id,"params":{"attemptId":moved_attempt}});
    let person = engine.repairer();
    let runtime = engine.runtime().await;
    let send = async {
        let headers = person
            .headers(DISCARD, &intent, &proof_id, engine.now().timestamp())
            .unwrap();
        engine.f.post_bytes(DISCARD, &body, &headers).await
    };
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
    let ((status, stale), moved_row) = tokio::join!(send, change);
    assert_eq!(status, 200);
    assert_eq!(stale, br#"{"result":"refused","reason":"stale"}"#);
    assert_eq!(
        engine
            .rows(
                "SELECT id,operation_id FROM admin_used_requests WHERE id=?1",
                vec![text(&proof_id)],
            )
            .await,
        [[text(&proof_id), text(&operation_id)]]
    );
    assert_eq!(
        engine
            .rows(
                "SELECT request_id,answer FROM runtime_request_log WHERE request_id=?1",
                vec![text(&operation_id)],
            )
            .await,
        [[
            text(&operation_id),
            text(std::str::from_utf8(&stale).unwrap())
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
