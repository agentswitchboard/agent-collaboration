//! Independent boundary assertions for the signed C3 routes on both hosts.
use central_storage::SqlValue;
use serde_json::{Value, json};

use super::request::{decide, decide_in_organization, list, read};
use super::{
    AnswerClass, InitialApproval, NOW_SECONDS, ORGANIZATION, Person, PersonApprovalHarness,
    REVISIONS, assert_count, assert_proof_count, expect, expect_status, fresh_request_id, id,
    refusal, require, saved, seed, time,
};
use crate::ConformanceResult;
use crate::signin_fixtures::approval_rows::seed_in_organization;

const FOREIGN_ORGANIZATION: &str = "019f0000-0000-7000-8000-00000000e201";
const OTHER_PERSON: &str = "019f0000-0000-7000-8000-00000000e202";
const OTHER_SUBJECT: &str = "019f0000-0000-7000-8000-00000000e203";

pub enum ExpandedCase {
    Binding(SignedMutation),
    SparseWaitingPage,
    OtherDestinationAnswer,
    StoredMilliseconds,
    SignedInDecisions,
    Applying(ClockBoundary),
}

#[derive(Clone, Copy)]
pub enum SignedMutation {
    Option,
    RequestId,
    Organization,
    OperationId,
    Route,
    Filter,
    Cursor,
}

#[derive(Clone, Copy)]
pub enum ClockBoundary {
    TokenExpiry,
    DeadlineCutoff,
}

pub(super) async fn check_expanded(
    host: &impl PersonApprovalHarness,
    owner: Person,
    case: ExpandedCase,
) -> ConformanceResult<()> {
    match case {
        ExpandedCase::Binding(mutation) => check_binding(host, &owner, mutation).await,
        ExpandedCase::SparseWaitingPage => check_sparse_page(host, &owner).await,
        ExpandedCase::OtherDestinationAnswer => check_other_destination(host, &owner).await,
        ExpandedCase::StoredMilliseconds => check_stored_milliseconds(host, &owner).await,
        ExpandedCase::SignedInDecisions => check_signed_in_decisions(host, owner).await,
        ExpandedCase::Applying(boundary) => check_applying_boundary(host, owner, boundary).await,
    }
}

async fn check_binding(
    host: &impl PersonApprovalHarness,
    owner: &Person,
    case: SignedMutation,
) -> ConformanceResult<()> {
    let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    let second = seed(host, 11, AnswerClass::Once, InitialApproval::Waiting).await?;
    let operation = id(100);
    let answer = AnswerClass::Once.answer();
    let mut call = match case {
        SignedMutation::Organization => {
            decide_in_organization(FOREIGN_ORGANIZATION, &request, &operation, &answer)
        }
        SignedMutation::Route => read(&request),
        SignedMutation::Filter => list("waiting", None),
        SignedMutation::Cursor => list("waiting", Some(&second)),
        SignedMutation::Option | SignedMutation::RequestId | SignedMutation::OperationId => {
            decide(&request, &operation, &answer)
        }
    };
    let actual_path = call.path;
    if matches!(case, SignedMutation::Route) {
        // Sign the same valid read intent and body for a different private route.
        call.path = list("waiting", None).path;
    }
    let headers = call.headers(owner, &fresh_request_id(NOW_SECONDS)?, &|_| {})?;
    call.path = actual_path;

    let field_change = match case {
        SignedMutation::Option => Some(("answer", json!({"kind":"approval","optionId":2}))),
        SignedMutation::RequestId => Some(("requestId", json!(second))),
        SignedMutation::Organization => Some(("organization", json!(ORGANIZATION))),
        SignedMutation::OperationId => Some(("operationId", json!(id(101)))),
        SignedMutation::Filter => Some(("filter", json!("all"))),
        SignedMutation::Cursor => Some(("cursor", json!(request))),
        SignedMutation::Route => None,
    };
    let mut changed = call.body.clone();
    if let Some((name, value)) = field_change {
        changed
            .as_object_mut()
            .ok_or("fixture request must be an object")?
            .insert(name.to_owned(), value);
    }

    let before = snapshot(host).await?;
    let reply = host.post(actual_path, &changed, headers.clone()).await?;
    require(
        reply == (401, Vec::new()),
        &json!({"expectedStatus":401, "actualStatus":reply.0, "bodyBytes":reply.1.len()}),
    )?;
    require_unchanged(host, before).await?;

    // The matching request succeeds. For route and organization, sign the
    // correct target; the other cases can spend the exact rejected proof.
    let control = match case {
        SignedMutation::Organization => {
            decide(&request, &operation, &answer)
                .send(host, owner)
                .await?
        }
        SignedMutation::Route => call.send(host, owner).await?,
        SignedMutation::Option
        | SignedMutation::RequestId
        | SignedMutation::OperationId
        | SignedMutation::Filter
        | SignedMutation::Cursor => host.post(call.path, &call.body, headers).await?,
    };
    let body = expect_status(control, 200)?;
    let reply: Value = serde_json::from_slice(&body)?;
    let kind = match case {
        SignedMutation::Route => "request",
        SignedMutation::Filter | SignedMutation::Cursor => "page",
        SignedMutation::Option
        | SignedMutation::RequestId
        | SignedMutation::Organization
        | SignedMutation::OperationId => "savedDecision",
    };
    require(
        reply.get("kind").and_then(Value::as_str) == Some(kind),
        &reply,
    )?;
    assert_proof_count(host, 1).await
}

async fn check_sparse_page(
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    for serial in 1..=101 {
        let state = match serial {
            2 | 101 => InitialApproval::PastDeadline,
            _ => InitialApproval::Waiting,
        };
        seed(host, serial, AnswerClass::Once, state).await?;
    }
    seed(host, 102, AnswerClass::Once, InitialApproval::Hidden).await?;
    // Put the foreign-tagged row in the owning store to exercise the query's
    // organization predicate rather than relying on physical store isolation.
    seed_in_organization(
        host,
        FOREIGN_ORGANIZATION,
        103,
        AnswerClass::Once,
        InitialApproval::PastDeadline,
    )
    .await?;

    let raised = time(-10)?;
    let deadline = time(3590)?;
    let summary = |serial| {
        json!({"requestId":id(serial),"kind":"providerApproval","state":{"kind":"waiting"},
            "raisedAt":raised,"deadline":deadline})
    };
    let first_items: Vec<_> = (2..=101)
        .rev()
        .filter(|serial| !matches!(serial, 2 | 101))
        .map(summary)
        .collect();
    let first_page = list("waiting", None).send(host, owner).await?;
    expect(
        first_page,
        200,
        json!({"kind":"page","items":first_items,"next":id(2)}),
    )?;
    expect(
        list("waiting", Some(&id(2))).send(host, owner).await?,
        200,
        json!({"kind":"page","items":[summary(1)],"next":null}),
    )?;
    let ended = host
        .read("SELECT request_id FROM approval_requests WHERE record_state='ended' ORDER BY request_id")
        .await?;
    require(
        ended == vec![vec![SqlValue::Text(id(2))], vec![SqlValue::Text(id(101))]],
        &json!({"expected":"only the two expired visible requests were settled"}),
    )?;
    assert_count(host, "SELECT COUNT(*) FROM actor_persons", 0).await?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 0).await?;
    assert_proof_count(host, 2).await
}

async fn check_other_destination(
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    let request = seed(host, 10, AnswerClass::Question, InitialApproval::Waiting).await?;
    let mut giver = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    giver.id = OTHER_PERSON.to_owned();
    giver.subject = OTHER_SUBJECT.to_owned();
    expect_status(
        host.post(
            REVISIONS,
            &json!({"kind":"person","organizationId":ORGANIZATION,"personId":OTHER_PERSON,
                "subjectId":OTHER_SUBJECT,"revision":giver.revision,"state":"active"}),
            Vec::new(),
        )
        .await?,
        200,
    )?;

    let answer = AnswerClass::Question.answer();
    let by = json!({"kind":"person","personId":OTHER_PERSON});
    expect(
        decide(&request, &id(100), &answer)
            .send(host, &giver)
            .await?,
        200,
        json!({"kind":"savedDecision","decision":{"answer":answer,"by":by}}),
    )?;
    let view = |ending| -> ConformanceResult<Value> {
        Ok(json!({"kind":"request","view":{
            "kind":"providerQuestion","requestId":request,
            "offer":{"prompt":"Private question","fields":[{"kind":"boolean",
                "fieldId":1,"label":"Continue","description":null,"required":true}]},
            "window":{"raisedAt":time(-10)?,"deadline":time(3590)?},
            "state":{"kind":"ended","endedAt":time(0)?,"delivery":{"kind":"owed"},"ending":ending}
        }}))
    };
    expect(
        read(&request).send(host, owner).await?,
        200,
        view(json!({"kind":"standing","ending":{"kind":"decided","by":by}}))?,
    )?;
    expect(
        read(&request).send(host, &giver).await?,
        200,
        view(
            json!({"kind":"giver","decision":{"by":by,"answer":{"kind":"answered",
            "fields":[{"fieldId":1,"value":{"kind":"boolean","value":true}}]}}}),
        )?,
    )?;
    assert_count(host, "SELECT COUNT(*) FROM actor_persons", 1).await?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 1).await?;
    assert_proof_count(host, 3).await
}

async fn check_applying_boundary(
    host: &impl PersonApprovalHarness,
    owner: Person,
    case: ClockBoundary,
) -> ConformanceResult<()> {
    let (answer_class, initial, advance_ms, expected) = match case {
        ClockBoundary::TokenExpiry => (
            AnswerClass::Once,
            InitialApproval::Waiting,
            1_000,
            refusal("expired"),
        ),
        ClockBoundary::DeadlineCutoff => (
            AnswerClass::Once,
            InitialApproval::DeadlineInOneSecond,
            1_000,
            json!({"kind":"tooLate"}),
        ),
    };

    let request = seed(host, 10, answer_class, initial).await?;
    let call = decide(&request, &id(100), &answer_class.answer());
    let headers = call.headers(&owner, &fresh_request_id(NOW_SECONDS)?, &|claims| {
        if matches!(case, ClockBoundary::TokenExpiry)
            && let Some(fields) = claims.as_object_mut()
        {
            fields.insert("exp".to_owned(), json!(NOW_SECONDS + 1));
        }
    })?;
    let before = snapshot(host).await?;
    let reply = host
        .post_after_verification(call.path, &call.body, headers, advance_ms)
        .await?;
    expect(reply, 200, expected)?;

    match case {
        ClockBoundary::TokenExpiry => require_unchanged(host, before).await,
        ClockBoundary::DeadlineCutoff => {
            assert_count(
                host,
                "SELECT COUNT(*) FROM approval_requests WHERE record_state='ended' AND decided_operation_id IS NULL AND delivery_state='owed'",
                1,
            )
            .await?;
            assert_proof_count(host, 1).await?;
            assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 1).await
        }
    }
}

/// Compare both domain rows and proof, actor, and operation-log writes.
type ApplySnapshot = [Vec<Vec<SqlValue>>; 3];

async fn snapshot(host: &impl PersonApprovalHarness) -> ConformanceResult<ApplySnapshot> {
    Ok([
        host.read("SELECT * FROM approval_requests ORDER BY request_id")
            .await?,
        host.read("SELECT * FROM approval_request_destinations ORDER BY approval_destination_id")
            .await?,
        host.read(
            "SELECT (SELECT COUNT(*) FROM admin_used_requests),
                    (SELECT COUNT(*) FROM runtime_request_log),
                    (SELECT COUNT(*) FROM actor_persons),
                    (SELECT COUNT(*) FROM approval_applying_time)",
        )
        .await?,
    ])
}

async fn require_unchanged(
    host: &impl PersonApprovalHarness,
    before: ApplySnapshot,
) -> ConformanceResult<()> {
    require(
        snapshot(host).await? == before,
        &json!({"expected":"no approval, proof, actor, operation-log, or floor write"}),
    )
}

async fn check_signed_in_decisions(
    host: &impl PersonApprovalHarness,
    mut owner: Person,
) -> ConformanceResult<()> {
    owner.second_factor_at = None;
    let examples = [
        (10, AnswerClass::HeldStartAllow),
        (11, AnswerClass::HeldEndDecline),
        (12, AnswerClass::KeptUntilChanged),
        (13, AnswerClass::LastingDecline),
        (14, AnswerClass::LastingAbort),
        (15, AnswerClass::UnverifiedLifetime),
    ];
    for (serial, answer_class) in examples {
        let request = seed(host, serial, answer_class, InitialApproval::Waiting).await?;
        let answer = answer_class.answer();
        let operation = id(100 + serial);
        expect(
            decide(&request, &operation, &answer)
                .send(host, &owner)
                .await?,
            200,
            saved(&answer),
        )?;
    }
    assert_count(
        host,
        "SELECT COUNT(*) FROM approval_requests WHERE record_state = 'ended'",
        6,
    )
    .await?;
    assert_proof_count(host, 6).await?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 6).await?;
    assert_count(host, "SELECT COUNT(*) FROM actor_persons", 1).await
}

/// Freeze literal timestamp values in the observed JSON and in the owning rows.
async fn check_stored_milliseconds(
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    const RAISED_AT: &str = "2030-03-17T17:46:30.123Z";
    const DEADLINE: &str = "2030-03-17T18:46:30.123Z";
    const ENDED_AT: &str = "2030-03-17T17:46:40.456Z";

    let class = AnswerClass::HeldEndDecline;
    let request = seed(
        host,
        10,
        class,
        InitialApproval::WaitingAt(RAISED_AT.parse()?),
    )
    .await?;
    let answer = class.answer();
    let decide_now = decide(&request, &id(100), &answer);
    let headers = decide_now.headers(owner, &fresh_request_id(NOW_SECONDS)?, &|_| {})?;

    // Advance only after the valid signed call reaches the applying hold.
    let first = host
        .post_after_verification(decide_now.path, &decide_now.body, headers, 456)
        .await?;
    expect(first, 200, saved(&answer))?;
    let stored = host
        .read("SELECT first_saved_at, deadline_ms, ended_at FROM approval_requests")
        .await?;
    require(
        stored
            == vec![vec![
                SqlValue::Text(RAISED_AT.to_owned()),
                SqlValue::Integer(1_900_003_590_123_i64.try_into()?),
                SqlValue::Text(ENDED_AT.to_owned()),
            ]],
        &json!({"stored":stored}),
    )?;

    expect(
        list("all", None).send(host, owner).await?,
        200,
        json!({"kind":"page","items":[{
            "requestId":request, "kind":"unprovenCall", "state":{"kind":"ended","ending":"decided"},
            "raisedAt":RAISED_AT, "deadline":DEADLINE
        }],"next":null}),
    )?;
    expect(
        read(&request).send(host, owner).await?,
        200,
        json!({"kind":"request", "view":{
            "kind":"unprovenCall", "requestId":request,
            "offer":{"operation":{"kind":"endSessionForGood","sessionId":"019f0000-0000-7000-8000-00000000c304"}},
            "state":{"kind":"ended", "ending":{"kind":"giver","decision":"decline","personId":owner.id},
                "endedAt":ENDED_AT, "delivery":{"kind":"owed"}},
            "window":{"raisedAt":RAISED_AT,"deadline":DEADLINE}
        }}),
    )?;

    let losing_decide = decide(&request, &id(101), &answer);
    let earlier_decision = losing_decide.send(host, owner).await?;
    expect(
        earlier_decision.clone(),
        200,
        json!({"kind":"alreadyDecided","by":{"kind":"person","personId":owner.id},"at":ENDED_AT}),
    )?;
    require(
        losing_decide.send(host, owner).await? == earlier_decision,
        &json!({"expected":"byte-identical replay of the stored fractional decision time"}),
    )
}
