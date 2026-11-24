use super::super::{engine_admin_support::*, support::text};
use serde_json::json;

const FIRST: &str = "019f0000-0000-7000-8000-000000000101";
const SECOND: &str = "019f0000-0000-7000-8000-000000000102";
const THIRD: &str = "019f0000-0000-7000-8000-000000000103";

async fn seed_retired_attempts(engine: &EngineOrg, tags: [&str; 3]) {
    for (attempt, tag) in [FIRST, SECOND, THIRD].into_iter().zip(tags) {
        seed_retired_attempt(engine, attempt, tag).await;
    }
}

async fn assert_visible_walk(tags: [&str; 3], first_visible: &str) {
    let engine = EngineOrg::new().await;
    seed_retired_attempts(&engine, tags).await;

    let first_page = engine.retired(None, 1).await;
    let second_page = engine.retired(Some(first_visible), 1).await;

    for (page, expected_id, cursor) in [
        (first_page, first_visible, Some(first_visible)),
        (second_page, THIRD, None),
    ] {
        assert_eq!(page["result"], "engineRetired");
        let items = page["items"].as_array().unwrap();
        let [item] = items.as_slice() else {
            panic!("expected one visible attempt: {page}")
        };
        assert_eq!(item["attemptId"], expected_id);
        assert_eq!(page["next"], json!(cursor));
    }
    engine.close().await;
}

#[tokio::test]
async fn retired_paging_continues_past_a_leading_hidden_kind() {
    assert_visible_walk(["engine_probe", "wake_timer", "automation_timer"], SECOND).await;
}

#[tokio::test]
async fn retired_paging_continues_past_a_hidden_kind_between_visible_attempts() {
    assert_visible_walk(["wake_timer", "owed_reply", "automation_timer"], FIRST).await;
}

async fn take_page_reads(engine: &EngineOrg, arm: bool) -> Option<Vec<usize>> {
    engine
        .runtime()
        .await
        .runtime()
        .statement_host()
        .take_retired_page_read_observations(arm)
}

async fn seed_retired_attempt(engine: &EngineOrg, attempt: &str, kind: &str) {
    engine.raw(
        "INSERT INTO engine_work_attempts(attempt_id,machine_kind,subject_key,state,failure_count,last_failure,first_failed_at,last_failed_at,retired_at,created_at,updated_at) VALUES (?,?,?,'retired',2,'statement_rejected',?4,?4,?4,?4,?4)",
        vec![text(attempt), text(kind), text(attempt), stamp(engine.now())],
    ).await;
}

#[tokio::test]
async fn retired_paging_reports_an_unknown_kind_after_a_hidden_chunk() {
    let engine = EngineOrg::new().await;
    seed_retired_attempts(&engine, ["engine_probe", "owed_reply", "unrecognized_kind"]).await;
    assert!(engine.retired_result(None, 1).await.is_err());
    engine.close().await;
}

#[tokio::test]
async fn retired_paging_bounds_every_read_and_resumes_from_empty_pages() {
    let engine = EngineOrg::new().await;
    engine.raw(
        "WITH RECURSIVE fixture_rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture_rows WHERE n<12) INSERT INTO engine_work_attempts(attempt_id,machine_kind,subject_key,state,failure_count,next_try_at,last_failure,first_failed_at,last_failed_at,retired_at,created_at,updated_at) SELECT printf('019f0000-0000-7000-8000-%012d',n), 'engine_probe', printf('hidden-%d',n), CASE n%2 WHEN 0 THEN 'retired' ELSE 'failing' END, 2, CASE n%2 WHEN 1 THEN ?1 END, 'statement_rejected', ?1, ?1, CASE n%2 WHEN 0 THEN ?1 END, ?1, ?1 FROM fixture_rows",
        vec![stamp(engine.now())],
    ).await;
    let visible = "019f0000-0000-7000-8000-000000000013";
    seed_retired_attempt(&engine, visible, "wake_timer").await;

    assert_eq!(take_page_reads(&engine, true).await, None);
    let mut after = None;
    for expected in [
        Some("019f0000-0000-7000-8000-000000000004"),
        Some("019f0000-0000-7000-8000-000000000008"),
        Some("019f0000-0000-7000-8000-000000000012"),
        None,
    ] {
        let page = engine.retired(after.as_deref(), 1).await;
        let reads = take_page_reads(&engine, true).await.unwrap();
        println!("retired scan after {after:?} read {reads:?}");
        assert_eq!(page["result"], "engineRetired");
        assert!(
            matches!(reads.as_slice(), [count] if *count <= 4),
            "expected one scan of at most four rows at {after:?}, observed {reads:?}"
        );
        let items = page["items"].as_array().unwrap();
        if expected.is_some() {
            assert!(items.is_empty(), "{page}");
        } else {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0]["attemptId"], visible);
        }
        assert_eq!(page["next"], json!(expected));
        after = page["next"].as_str().map(str::to_owned);
    }

    seed_retired_attempt(
        &engine,
        "019f0000-0000-7000-8000-000000000014",
        "unrecognized_kind",
    )
    .await;
    let page = engine
        .retired(Some("019f0000-0000-7000-8000-000000000012"), 1)
        .await;
    assert_eq!(page["items"][0]["attemptId"], visible);
    assert_eq!(page["next"], visible);
    assert!(
        matches!(take_page_reads(&engine, true).await.as_deref(), Some([count]) if *count <= 4)
    );
    assert!(
        engine
            .retired_result(page["next"].as_str(), 1)
            .await
            .is_err()
    );
    assert!(
        matches!(take_page_reads(&engine, false).await.as_deref(), Some([count]) if *count <= 4)
    );
    engine.close().await;
}

#[tokio::test]
async fn retired_paging_rejects_noncanonical_v7_storage_keys() {
    let engine = EngineOrg::new().await;
    let mut statuses = Vec::new();
    for attempt in [
        "019F0000-0000-7000-8000-000000000101",
        "urn:uuid:019f0000-0000-7000-8000-000000000102",
    ] {
        seed_retired_attempt(&engine, attempt, "wake_timer").await;
        statuses.push(engine.retired_result(None, 1).await.is_err());
        engine
            .raw(
                "DELETE FROM engine_work_attempts WHERE attempt_id=?1",
                vec![text(attempt)],
            )
            .await;
    }
    assert_eq!(
        statuses,
        [true, true],
        "uppercase and URN keys must both be rejected"
    );
    engine.close().await;
}

#[tokio::test]
async fn retired_paging_rejects_a_corrupt_projected_failure_count() {
    let engine = EngineOrg::new().await;
    seed_retired_attempt(&engine, FIRST, "wake_timer").await;
    engine
        .raw(
            "UPDATE engine_work_attempts SET failure_count='damaged-count' WHERE attempt_id=?1",
            vec![text(FIRST)],
        )
        .await;

    assert!(engine.retired_result(None, 1).await.is_err());
    engine.close().await;
}
