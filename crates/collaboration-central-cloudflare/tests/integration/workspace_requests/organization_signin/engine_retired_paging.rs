use super::super::{engine_admin_support::*, support::ORG};
use serde_json::json;

const FIRST: &str = "019f0000-0000-7000-8000-000000000101";
const SECOND: &str = "019f0000-0000-7000-8000-000000000102";
const THIRD: &str = "019f0000-0000-7000-8000-000000000103";

async fn seed_retired_attempts(engine: &EngineOrg, tags: [&str; 3]) {
    for (attempt, tag) in [FIRST, SECOND, THIRD].into_iter().zip(tags) {
        engine.raw(
            "INSERT INTO engine_work_attempts(attempt_id,machine_kind,subject_key,state,failure_count,last_failure,first_failed_at,last_failed_at,retired_at,created_at,updated_at) VALUES (?,?,?,'retired',2,'statement_rejected',?4,?4,?4,?4,?4)",
            vec![text(attempt), text(tag), text(attempt), stamp(engine.now())],
        ).await;
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

#[tokio::test]
async fn retired_paging_reports_an_unknown_kind_after_a_hidden_chunk() {
    let engine = EngineOrg::new().await;
    seed_retired_attempts(&engine, ["engine_probe", "owed_reply", "unrecognized_kind"]).await;
    let organization = ORG;
    let person = engine.person(&[READ_AUDIT], false);
    let body = json!({"organizationId":organization,"after":null,"limit":1});
    let intent = json!({"v":1,"operation":"EngineListRetired","organization":organization,"target":organization,"operationId":null,"params":{"after":null,"limit":1}});
    engine.runtime().await;
    let headers = person
        .headers(RETIRED, &intent, &engine.fresh(), engine.now().timestamp())
        .unwrap();
    let (status, _) = engine.f.post_with(RETIRED, &body, &headers).await;
    assert_eq!(status, 503);
    engine.close().await;
}
