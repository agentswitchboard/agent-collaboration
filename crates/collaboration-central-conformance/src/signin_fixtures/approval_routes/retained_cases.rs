//! Both hosts validate exact C3 replay against immutable endings and probe visibility by destination key.

use central_storage::SqlValue;
use serde_json::json;

use crate::ConformanceResult;
use crate::signin_fixtures::approval_rows::{AnswerClass, InitialApproval, id, seed, text, time};
use crate::signin_fixtures::{NOW_SECONDS, ORGANIZATION, OWNER, Person};

use super::request::{SignedRequest, decide, read};
use super::{
    PersonApprovalHarness, assert_count, expect, expect_status, publish_person_floor, require,
    saved,
};

#[derive(Clone, Copy)]
pub enum RetainedCase {
    SavedDecisionMissingEnding,
    AlreadyDecidedWrongWinner,
    AlreadyDecidedWrongTime,
    PeopleVisibilityUsesDestinationIndex,
}

pub async fn run_retained_case(
    host: &impl PersonApprovalHarness,
    owner: &Person,
    case: RetainedCase,
) -> ConformanceResult<()> {
    match case {
        RetainedCase::SavedDecisionMissingEnding => {
            saved_decision_missing_ending(host, owner).await
        }
        RetainedCase::AlreadyDecidedWrongWinner => {
            corrupted_winner(
                host,
                owner,
                json!({"kind":"person","personId":id(500)}),
                time(0)?,
            )
            .await
        }
        RetainedCase::AlreadyDecidedWrongTime => {
            corrupted_winner(
                host,
                owner,
                json!({"kind":"person","personId":OWNER}),
                time(1)?,
            )
            .await
        }
        RetainedCase::PeopleVisibilityUsesDestinationIndex => visibility_shape(host, owner).await,
    }
}

async fn saved_decision_missing_ending(
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    let request = id(10);
    let operation = id(100);
    let answer = AnswerClass::Once.answer();
    let call = decide(&request, &operation, &answer);

    expect(
        call.send(host, owner).await?,
        200,
        json!({"kind":"notVisible"}),
    )?;
    seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    expect(
        call.send(host, owner).await?,
        200,
        json!({"kind":"notVisible"}),
    )?;

    // The forged reply agrees with the signed intent and actor. The request itself
    // has never committed a decision, which makes this replay invalid.
    host.write(
        "UPDATE runtime_request_log SET answer = ?1 WHERE request_id = ?2",
        vec![text(saved(&answer)), text(operation)],
    )
    .await?;
    expect_corrupt_replay(&call, host, owner).await?;
    assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 2).await?;
    assert_count(
        host,
        "SELECT COUNT(*) FROM approval_requests WHERE record_state = 'waiting'",
        1,
    )
    .await
}

async fn corrupted_winner(
    host: &impl PersonApprovalHarness,
    owner: &Person,
    fabricated_winner: serde_json::Value,
    fabricated_time: approval_request_protocol::CentralTime,
) -> ConformanceResult<()> {
    let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    let answer = AnswerClass::Once.answer();
    expect(
        decide(&request, &id(100), &answer)
            .send(host, owner)
            .await?,
        200,
        saved(&answer),
    )?;
    let operation = id(101);
    let losing_call = decide(&request, &operation, &answer);
    expect(
        losing_call.send(host, owner).await?,
        200,
        json!({"kind":"alreadyDecided","by":{"kind":"person","personId":OWNER},"at":time(0)?}),
    )?;

    host.write(
        "UPDATE runtime_request_log SET answer = ?1 WHERE request_id = ?2",
        vec![
            text(json!({"kind":"alreadyDecided","by":fabricated_winner,"at":fabricated_time})),
            text(operation),
        ],
    )
    .await?;
    expect_corrupt_replay(&losing_call, host, owner).await?;

    publish_person_floor(host, 5, "active").await?;
    let mut after_permission_loss = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    after_permission_loss.revision = 5;
    after_permission_loss
        .permissions
        .retain(|permission| permission != "approval:decide");
    expect_corrupt_replay(&losing_call, host, &after_permission_loss).await?;

    assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 2).await?;
    assert_count(
        host,
        "SELECT COUNT(*) FROM approval_requests WHERE record_state = 'ended'",
        1,
    )
    .await
}

async fn expect_corrupt_replay(
    call: &SignedRequest,
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    let body = expect_status(call.send(host, owner).await?, 503)?;
    require(
        body.is_empty(),
        &json!({"expected":"empty corrupt-log refusal"}),
    )
}

// The shared host harness asks each owning SQLite store to plan the exact C3 SQL.
const VISIBILITY_PLAN: &str = concat!(
    "EXPLAIN QUERY PLAN ",
    include_str!("../../../../fleet-approvals/src/queries/read_people_approval_request.sql")
);

async fn visibility_shape(
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    let hidden = seed(host, 10, AnswerClass::Once, InitialApproval::Hidden).await?;
    let missing = id(11);
    let hidden_plan = host
        .read_with_args(VISIBILITY_PLAN, vec![text(ORGANIZATION), text(&hidden)])
        .await?;
    let missing_plan = host
        .read_with_args(VISIBILITY_PLAN, vec![text(ORGANIZATION), text(&missing)])
        .await?;
    require(
        hidden_plan == missing_plan,
        &json!({"expected":"plan identical for an absent destination"}),
    )?;
    let [destination, request] = hidden_plan.as_slice() else {
        return Err(format!("expected two point searches, got {hidden_plan:?}").into());
    };
    let [_, _, _, SqlValue::Text(destination)] = destination.as_slice() else {
        return Err("expected destination plan detail".into());
    };
    let [_, _, _, SqlValue::Text(request)] = request.as_slice() else {
        return Err("expected request plan detail".into());
    };
    require(
        destination.starts_with("SEARCH destination ")
            && destination.contains("approval_destinations_people_lookup")
            && destination.contains("(organization_id=? AND request_id=?)")
            && request.starts_with("SEARCH requests "),
        &json!({"expected":"destination point search before reading the request","plan":[destination,request]}),
    )?;

    let hidden_reply = read(&hidden).send(host, owner).await?;
    let missing_reply = read(&missing).send(host, owner).await?;
    require(
        hidden_reply == missing_reply,
        &json!({"expected":"identical absent-destination reply"}),
    )?;
    expect(hidden_reply, 200, json!({"kind":"notVisible"}))
}
