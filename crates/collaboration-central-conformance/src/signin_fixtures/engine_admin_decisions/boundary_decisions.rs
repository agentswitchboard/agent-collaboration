//! Signed HTTP witnesses for the admin transport and transaction boundaries.

use super::{
    AdminBatchLoss, ApplyingControl, ENGINE_ATTEMPT, ENGINE_OPERATION, EngineAdminCase,
    EngineAdminHarness, EngineAdminRequest, EngineAdminRoute, NOW_SECONDS, ORGANIZATION, Person,
    attempt_fixtures::{
        AdminFacts, AttemptStanding, LAST_HIDDEN_ATTEMPT, insert_attempt, read_admin_facts,
        seed_attempt,
    },
    fresh_request_id,
};
use crate::ConformanceResult;
use central_storage::SqlValue;
use serde_json::{Value, json};

mod repair_replay;

#[derive(Clone, Copy)]
pub enum BoundaryCase {
    OversizedUpload,
    BeforeApply,
    AfterCommit,
    RetiredDeleteGuard,
    InsideReceiptCutoff,
    AtReceiptCutoff,
    DifferentActor,
    FullHiddenScan,
    InvalidAttemptState,
}

pub async fn check_boundary_case(
    host: &impl EngineAdminHarness,
    route: EngineAdminRoute,
    case: BoundaryCase,
) -> ConformanceResult<()> {
    match case {
        BoundaryCase::OversizedUpload => check_oversized_upload(host, route).await,
        BoundaryCase::BeforeApply => {
            check_lost_reply(host, route, AdminBatchLoss::BeforeApply).await
        }
        BoundaryCase::AfterCommit => {
            check_lost_reply(host, route, AdminBatchLoss::AfterCommit).await
        }
        BoundaryCase::RetiredDeleteGuard => check_retry_delete_guard(host).await,
        BoundaryCase::InsideReceiptCutoff => repair_replay::check_cutoff(host, route, false).await,
        BoundaryCase::AtReceiptCutoff => repair_replay::check_cutoff(host, route, true).await,
        BoundaryCase::DifferentActor => repair_replay::check_other_actor(host, route).await,
        BoundaryCase::FullHiddenScan => check_full_hidden_scan(host).await,
        BoundaryCase::InvalidAttemptState => check_invalid_attempt_state(host).await,
    }
}

fn authorized_person(route: EngineAdminRoute) -> ConformanceResult<Person> {
    let mut person = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    person.permissions = vec![route.permission().to_owned()];
    Ok(person)
}

fn signed_request(
    route: EngineAdminRoute,
    person: &Person,
    proof_id: &str,
    after: Option<&str>,
) -> ConformanceResult<EngineAdminRequest> {
    EngineAdminRequest::signed_page(
        route,
        EngineAdminCase::NotFound,
        person,
        proof_id,
        ENGINE_ATTEMPT,
        after,
    )
}

fn status_and_body(reply: (u16, Vec<u8>)) -> ConformanceResult<(u16, Value)> {
    Ok((reply.0, serde_json::from_slice(&reply.1)?))
}

fn expect_empty(reply: (u16, Vec<u8>), status: u16) {
    assert_eq!(reply, (status, vec![]));
}

async fn check_oversized_upload(
    host: &impl EngineAdminHarness,
    route: EngineAdminRoute,
) -> ConformanceResult<()> {
    let person = authorized_person(route)?;
    let mut request = signed_request(route, &person, &fresh_request_id(NOW_SECONDS)?, None)?;
    request
        .body
        .as_object_mut()
        .ok_or("expected an addressed request body")?
        .insert("extra".to_owned(), Value::String("x".repeat(2_097_152)));
    let before = read_admin_facts(host).await?;

    let (status, body) = host
        .send_admin(&request, ApplyingControl::Immediate)
        .await?;

    assert_eq!(status, 413);
    host.assert_oversized_reply(&body);
    assert_eq!(read_admin_facts(host).await?, before);
    Ok(())
}

fn route_success(route: EngineAdminRoute) -> Option<(EngineAdminCase, &'static [u8])> {
    match route {
        EngineAdminRoute::Retry => {
            Some((EngineAdminCase::Retried, br#"{"result":"engineRetried"}"#))
        }
        EngineAdminRoute::Discard => Some((
            EngineAdminCase::Discarded,
            br#"{"result":"engineDiscarded"}"#,
        )),
        EngineAdminRoute::Health | EngineAdminRoute::Retired => None,
    }
}

fn assert_domain_unchanged(before: &AdminFacts, after: &AdminFacts) {
    assert_eq!(before.attempts, after.attempts);
    assert_eq!(before.wakes, after.wakes);
    assert_eq!(before.due, after.due);
    assert_eq!(before.alarm_millis, after.alarm_millis);
}

fn assert_proof_added(before: &AdminFacts, after: &AdminFacts, proof_id: &str) {
    let mut expected = before.proofs.clone();
    expected.push(vec![SqlValue::Text(proof_id.into())]);
    sort_by_text_id(&mut expected);
    assert_eq!(after.proofs, expected);
}

fn sort_by_text_id(rows: &mut [Vec<SqlValue>]) {
    fn text_id(row: &[SqlValue]) -> Option<&str> {
        match row.first() {
            Some(SqlValue::Text(id)) => Some(id.as_str()),
            _ => None,
        }
    }
    rows.sort_by(|left, right| text_id(left).cmp(&text_id(right)));
}

async fn check_lost_reply(
    host: &impl EngineAdminHarness,
    route: EngineAdminRoute,
    loss: AdminBatchLoss,
) -> ConformanceResult<()> {
    let person = authorized_person(route)?;
    let repair = if let Some((case, reply)) = route_success(route) {
        Some((case, reply, seed_attempt(host, case).await?))
    } else {
        None
    };
    let proof_id = fresh_request_id(NOW_SECONDS)?;
    let request = signed_request(route, &person, &proof_id, None)?;
    let before = read_admin_facts(host).await?;
    host.arm_admin_batch_loss(&proof_id, loss).await?;

    expect_empty(
        host.send_admin(&request, ApplyingControl::Immediate)
            .await?,
        503,
    );

    let lost = read_admin_facts(host).await?;
    let recovery_proof = fresh_request_id(NOW_SECONDS)?;
    let recovery = signed_request(route, &person, &recovery_proof, None)?;
    match (loss, repair) {
        (AdminBatchLoss::BeforeApply, Some((case, reply, subject))) => {
            assert_eq!(lost, before);
            let recovered = host
                .send_admin(&recovery, ApplyingControl::Immediate)
                .await?;
            assert_eq!(recovered, (200, reply.to_vec()));
            let recovered = read_admin_facts(host).await?;
            assert_proof_added(&before, &recovered, &recovery_proof);
            super::attempt_fixtures::check_domain_writes(case, &subject, &before, &recovered);
            assert_repair_receipt(&before, &recovered, reply)?;
        }
        (AdminBatchLoss::AfterCommit, Some((case, reply, subject))) => {
            assert_proof_added(&before, &lost, &proof_id);
            // Discard publishes after the storage call returns. Its lost answer can
            // leave the alarm to the ordinary post-event path, so compare the durable facts.
            super::attempt_fixtures::check_committed_repair(case, &subject, &before, &lost);
            assert_repair_receipt(&before, &lost, reply)?;
            let recovered = host
                .send_admin(&recovery, ApplyingControl::Immediate)
                .await?;
            assert_eq!(recovered, (200, reply.to_vec()));
            let recovered = read_admin_facts(host).await?;
            assert_proof_added(&lost, &recovered, &recovery_proof);
            assert_eq!(recovered.receipts, lost.receipts);
            assert_eq!(recovered.actors, lost.actors);
            assert_domain_unchanged(&lost, &recovered);
        }
        (AdminBatchLoss::AfterCommit, None) => {
            assert_proof_added(&before, &lost, &proof_id);
            assert_eq!(lost.receipts, before.receipts);
            assert_eq!(lost.actors, before.actors);
            assert_domain_unchanged(&before, &lost);
            let consumed = host
                .send_admin(&request, ApplyingControl::Immediate)
                .await?;
            assert_eq!(
                status_and_body(consumed)?,
                (200, json!({"result":"refused","reason":"requestReused"}))
            );
            let fresh = host
                .send_admin(&recovery, ApplyingControl::Immediate)
                .await?;
            assert_eq!(fresh.0, 200);
            let recovered = read_admin_facts(host).await?;
            assert_proof_added(&lost, &recovered, &recovery_proof);
            assert_eq!(recovered.receipts, lost.receipts);
            assert_eq!(recovered.actors, lost.actors);
            assert_domain_unchanged(&lost, &recovered);
        }
        (AdminBatchLoss::BeforeApply, None) => {
            return Err("before-apply recovery requires a repair route".into());
        }
    }
    Ok(())
}

fn assert_repair_receipt(
    before: &AdminFacts,
    facts: &AdminFacts,
    body: &[u8],
) -> ConformanceResult<()> {
    let bytes = std::str::from_utf8(body)?;
    verify_repair_receipt(before, facts, bytes);
    Ok(())
}

fn verify_repair_receipt(before: &AdminFacts, facts: &AdminFacts, bytes: &str) {
    let mut receipts = before.receipts.clone();
    receipts.push(vec![
        SqlValue::Text(ENGINE_OPERATION.into()),
        SqlValue::Text(bytes.into()),
    ]);
    sort_by_text_id(&mut receipts);
    assert_eq!(facts.receipts, receipts);
    assert_eq!(facts.actors.len(), before.actors.len() + 1);
}

async fn check_retry_delete_guard(host: &impl EngineAdminHarness) -> ConformanceResult<()> {
    let route = EngineAdminRoute::Retry;
    let person = authorized_person(route)?;
    let request = signed_request(route, &person, &fresh_request_id(NOW_SECONDS)?, None)?;
    seed_attempt(host, EngineAdminCase::Retried).await?;
    let mut expected = read_admin_facts(host).await?;
    assert_eq!(expected.attempts.len(), 1);
    expected.attempts.clear(); // The fixture's competing delete is the only write.

    let response = host
        .send_admin(
            &request,
            ApplyingControl::MoveAttemptAfterSampling {
                attempt_id: ENGINE_ATTEMPT,
            },
        )
        .await?;

    assert_eq!(
        status_and_body(response)?,
        (200, json!({"result":"refused","reason":"notFound"}))
    );
    assert_eq!(read_admin_facts(host).await?, expected);
    Ok(())
}

async fn check_full_hidden_scan(host: &impl EngineAdminHarness) -> ConformanceResult<()> {
    const HIDDEN: [&str; 4] = [
        ENGINE_ATTEMPT,
        "019f0000-0000-7000-8000-000000000102",
        "019f0000-0000-7000-8000-000000000103",
        LAST_HIDDEN_ATTEMPT,
    ];
    let route = EngineAdminRoute::Retired;
    let person = authorized_person(route)?;
    for hidden in HIDDEN {
        insert_attempt(
            host,
            hidden,
            "engine_probe",
            hidden,
            AttemptStanding::Retired,
        )
        .await?;
    }
    let before = read_admin_facts(host).await?;
    let first_proof = fresh_request_id(NOW_SECONDS)?;
    let first = signed_request(route, &person, &first_proof, None)?;
    let next_proof = fresh_request_id(NOW_SECONDS)?;
    let next = signed_request(route, &person, &next_proof, Some(LAST_HIDDEN_ATTEMPT))?;

    let page = host.send_admin(&first, ApplyingControl::Immediate).await?;
    let between = read_admin_facts(host).await?;
    let end = host.send_admin(&next, ApplyingControl::Immediate).await?;

    assert_eq!(
        status_and_body(page)?,
        (
            200,
            json!({"result":"engineRetired","items":[],"next":LAST_HIDDEN_ATTEMPT})
        )
    );
    assert_eq!(
        status_and_body(end)?,
        (
            200,
            json!({"result":"engineRetired","items":[],"next":null})
        )
    );
    let after = read_admin_facts(host).await?;
    assert_proof_added(&before, &between, &first_proof);
    assert_proof_added(&between, &after, &next_proof);
    assert_domain_unchanged(&before, &after);
    assert_eq!(after.receipts, before.receipts);
    assert_eq!(after.actors, before.actors);
    Ok(())
}

async fn check_invalid_attempt_state(host: &impl EngineAdminHarness) -> ConformanceResult<()> {
    let route = EngineAdminRoute::Retired;
    let person = authorized_person(route)?;
    let request = signed_request(route, &person, &fresh_request_id(NOW_SECONDS)?, None)?;
    insert_attempt(
        host,
        ENGINE_ATTEMPT,
        "wake_timer",
        ENGINE_ATTEMPT,
        AttemptStanding::Retired,
    )
    .await?;
    host.apply_admin_fixture(
        "UPDATE engine_work_attempts SET state='unrecognized' WHERE attempt_id=?1",
        vec![SqlValue::Text(ENGINE_ATTEMPT.into())],
    )
    .await?;
    let before = read_admin_facts(host).await?;

    expect_empty(
        host.send_admin(&request, ApplyingControl::Immediate)
            .await?,
        503,
    );

    assert_eq!(read_admin_facts(host).await?, before);
    Ok(())
}
