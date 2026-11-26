//! Both hosts validate exact C3 replay against immutable endings and probe visibility by destination key.

use central_storage::SqlValue;
use serde_json::json;

use crate::ConformanceResult;
use crate::signin_fixtures::approval_rows::{AnswerClass, InitialApproval, id, seed, text, time};
use crate::signin_fixtures::{NOW_SECONDS, ORGANIZATION, OWNER, Person};

use super::request::{ApprovalCall, decide, read};
use super::{
    PersonApprovalHarness, assert_count, assert_no_proof_rows, expect, publish_person_floor,
    require, saved,
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
    let answer = json!({"kind":"approval","optionId":15});
    let call = decide(&request, &operation, &answer);

    expect(call.send(host, owner).await?, json!({"kind":"notVisible"}))?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 0).await?;
    seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    let refused = call.send(host, owner).await?.answer()?;
    let refused: serde_json::Value = serde_json::from_slice(&refused)?;
    assert_eq!(
        refused.get("kind").and_then(serde_json::Value::as_str),
        Some("invalid")
    );
    assert_count(
        host,
        "SELECT COUNT(*) FROM runtime_request_log WHERE approval_row_id IS NOT NULL",
        1,
    )
    .await?;

    // The forged reply agrees with the decision intent and actor. The request itself
    // has never committed a decision, which makes this replay invalid.
    host.write(
        "UPDATE runtime_request_log SET answer = ?1 WHERE request_id = ?2",
        vec![text(saved(&answer)), text(operation)],
    )
    .await?;
    expect_corrupt_replay(&call, host, owner).await?;
    assert_no_proof_rows(host).await?;
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
        saved(&answer),
    )?;
    let operation = id(101);
    let losing_call = decide(&request, &operation, &answer);
    expect(
        losing_call.send(host, owner).await?,
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

    assert_no_proof_rows(host).await?;
    assert_count(
        host,
        "SELECT COUNT(*) FROM approval_requests WHERE record_state = 'ended'",
        1,
    )
    .await
}

async fn expect_corrupt_replay(
    call: &ApprovalCall,
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    super::assert_lost_reply(call.send(host, owner).await?)
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
    expect(hidden_reply, json!({"kind":"notVisible"}))
}
