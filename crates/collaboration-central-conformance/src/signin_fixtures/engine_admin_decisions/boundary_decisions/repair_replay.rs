//! The repair operation ledger's cutoff, pruning and identity semantics.

use super::{
    ApplyingControl, ENGINE_OPERATION, EngineAdminHarness, EngineAdminRequest, EngineAdminRoute,
    NOW_SECONDS, ORGANIZATION, Person, SqlValue, assert_domain_unchanged, assert_proof_added,
    authorized_person, fresh_request_id, json, read_admin_facts, route_success, seed_attempt,
    signed_request,
};
use crate::ConformanceResult;
use chrono::{DateTime, Duration};

const OLD_RECEIPT: &str = "019f0000-0000-7000-8000-000000000110";
const LIVE_RECEIPT: &str = "019f0000-0000-7000-8000-000000000111";
const NOT_FOUND: &[u8] = br#"{"result":"refused","reason":"notFound"}"#;

async fn read_ledger(host: &impl EngineAdminHarness) -> ConformanceResult<Vec<Vec<SqlValue>>> {
    host.read_admin_rows(
        "SELECT request_id,caller,content_digest,answer,created_at,updated_at FROM runtime_request_log ORDER BY request_id",
        vec![],
    ).await
}

async fn seed_receipt_times(
    host: &impl EngineAdminHarness,
    at_cutoff: bool,
) -> ConformanceResult<()> {
    let now = DateTime::from_timestamp(NOW_SECONDS, 0).ok_or("fixture time is out of range")?;
    let cutoff = now - Duration::hours(24);
    let first_receipt_time = if at_cutoff {
        cutoff
    } else {
        cutoff + Duration::milliseconds(1)
    };
    host.apply_admin_fixture(
        "UPDATE runtime_request_log SET created_at=?1, updated_at=?1 WHERE request_id=?2",
        vec![
            SqlValue::Text(first_receipt_time.to_rfc3339()),
            SqlValue::Text(ENGINE_OPERATION.into()),
        ],
    )
    .await?;
    for (receipt_id, created) in [
        (OLD_RECEIPT, cutoff - Duration::seconds(1)),
        (LIVE_RECEIPT, cutoff + Duration::seconds(1)),
    ] {
        host.apply_admin_fixture(
            "INSERT INTO runtime_request_log (request_id,caller,content_digest,answer,created_at,updated_at) SELECT ?1,caller,content_digest,answer,?2,?2 FROM runtime_request_log WHERE request_id=?3",
            vec![SqlValue::Text(receipt_id.into()), SqlValue::Text(created.to_rfc3339()), SqlValue::Text(ENGINE_OPERATION.into())],
        ).await?;
    }
    Ok(())
}

pub(super) async fn check_cutoff(
    host: &impl EngineAdminHarness,
    route: EngineAdminRoute,
    at_cutoff: bool,
) -> ConformanceResult<()> {
    let person = authorized_person(route)?;
    let initial = signed_request(route, &person, &fresh_request_id(NOW_SECONDS)?, None)?;
    let response = host
        .send_admin(&initial, ApplyingControl::Immediate)
        .await?;
    assert_eq!(response, (200, NOT_FOUND.to_vec()));
    let (success, success_body) =
        route_success(route).ok_or("retention requires a repair route")?;
    let subject = seed_attempt(host, success).await?;
    seed_receipt_times(host, at_cutoff).await?;
    let before = read_admin_facts(host).await?;
    let ledger_before = read_ledger(host).await?;
    let proof_id = fresh_request_id(NOW_SECONDS)?;
    let fresh = signed_request(route, &person, &proof_id, None)?;

    let answer = host.send_admin(&fresh, ApplyingControl::Immediate).await?;

    let after = read_admin_facts(host).await?;
    let ledger_after = read_ledger(host).await?;
    assert_proof_added(&before, &after, &proof_id);
    assert_eq!(after.actors, before.actors, "existing actor is reused");
    if at_cutoff {
        assert_eq!(answer, (200, success_body.to_vec()));
        super::super::attempt_fixtures::check_domain_writes(success, &subject, &before, &after);
        let now = DateTime::from_timestamp(NOW_SECONDS, 0).ok_or("fixture time is out of range")?;
        assert_replaced_and_pruned(
            &ledger_before,
            &ledger_after,
            std::str::from_utf8(success_body)?,
            &now.to_rfc3339(),
        );
    } else {
        assert_eq!(answer, (200, NOT_FOUND.to_vec()));
        assert_eq!(
            ledger_after, ledger_before,
            "retained replay preserves even expired unrelated rows"
        );
        assert_domain_unchanged(&before, &after);
    }
    Ok(())
}

fn assert_replaced_and_pruned(
    before: &[Vec<SqlValue>],
    after: &[Vec<SqlValue>],
    answer: &str,
    now: &str,
) {
    let expired = SqlValue::Text(OLD_RECEIPT.into());
    let operation_id = SqlValue::Text(ENGINE_OPERATION.into());
    let live = SqlValue::Text(LIVE_RECEIPT.into());
    assert!(before.iter().any(|row| row.first() == Some(&expired)));
    assert!(before.iter().any(|row| row.first() == Some(&operation_id)));
    assert!(before.iter().any(|row| row.first() == Some(&live)));
    let expected: Vec<_> = before
        .iter()
        .filter(|row| row.first() != Some(&expired))
        .map(|row| match row.as_slice() {
            [id, actor, digest, _, _, _] if id == &operation_id => vec![
                id.clone(),
                actor.clone(),
                digest.clone(),
                SqlValue::Text(answer.into()),
                SqlValue::Text(now.into()),
                SqlValue::Text(now.into()),
            ],
            _ => row.clone(),
        })
        .collect();
    assert_eq!(after, expected);
}

pub(super) async fn check_other_actor(
    host: &impl EngineAdminHarness,
    route: EngineAdminRoute,
) -> ConformanceResult<()> {
    let owner = authorized_person(route)?;
    let initial = signed_request(route, &owner, &fresh_request_id(NOW_SECONDS)?, None)?;
    assert_eq!(
        host.send_admin(&initial, ApplyingControl::Immediate)
            .await?,
        (200, NOT_FOUND.to_vec())
    );
    let mut member = Person::member(ORGANIZATION, NOW_SECONDS)?;
    member.permissions = owner.permissions.clone();
    let floor = EngineAdminRequest {
        method: reqwest::Method::POST,
        path: "/internal/organization-authorization-revisions",
        body: json!({
            "kind":"person","organizationId":ORGANIZATION,"personId":member.id,
            "subjectId":member.subject,"revision":member.revision,"state":"active"
        }),
        headers: vec![],
    };
    assert_eq!(
        host.send_admin(&floor, ApplyingControl::Immediate).await?.0,
        200
    );
    let before = read_admin_facts(host).await?;
    let ledger = read_ledger(host).await?;
    let proof_id = fresh_request_id(NOW_SECONDS)?;
    let conflicting = signed_request(route, &member, &proof_id, None)?;
    assert_eq!(conflicting.body, initial.body);

    let answer = host
        .send_admin(&conflicting, ApplyingControl::Immediate)
        .await?;

    assert_eq!(
        answer,
        (200, br#"{"result":"refused","reason":"conflict"}"#.to_vec())
    );
    let after = read_admin_facts(host).await?;
    assert_proof_added(&before, &after, &proof_id);
    let proof_actor = host
        .read_admin_rows(
            "SELECT actor_person_id FROM admin_used_requests WHERE id=?1",
            vec![SqlValue::Text(proof_id)],
        )
        .await?;
    assert_eq!(proof_actor, [[SqlValue::Text(member.id)]]);
    assert_eq!(read_ledger(host).await?, ledger);
    assert_eq!(
        after.actors, before.actors,
        "conflict cannot mint another actor"
    );
    assert_domain_unchanged(&before, &after);
    Ok(())
}
