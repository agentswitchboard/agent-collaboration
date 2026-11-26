//! Approval decision and time-boundary assertions on both hosts.
use central_storage::SqlValue;
use serde_json::{Value, json};

use super::request::{decide, list, read};
use super::{
    AnswerClass, InitialApproval, NOW_SECONDS, ORGANIZATION, Person, PersonApprovalHarness,
    assert_count, assert_no_proof_rows, expect, expect_http_status, id, require, saved, seed, time,
};
use crate::ConformanceResult;
use crate::signin_fixtures::approval_rows::seed_in_organization;

const FOREIGN_ORGANIZATION: &str = "019f0000-0000-7000-8000-00000000e201";
const OTHER_PERSON: &str = "019f0000-0000-7000-8000-00000000e202";
const OTHER_SUBJECT: &str = "019f0000-0000-7000-8000-00000000e203";

pub enum ExpandedCase {
    SparseWaitingPage,
    OtherDestinationAnswer,
    StoredMilliseconds,
    SignedInDecisions,
    ApplyingDeadline,
}

pub(super) async fn check_expanded(
    host: &impl PersonApprovalHarness,
    owner: Person,
    case: ExpandedCase,
) -> ConformanceResult<()> {
    match case {
        ExpandedCase::SparseWaitingPage => check_sparse_page(host, &owner).await,
        ExpandedCase::OtherDestinationAnswer => check_other_destination(host, &owner).await,
        ExpandedCase::StoredMilliseconds => check_stored_milliseconds(host, &owner).await,
        ExpandedCase::SignedInDecisions => check_signed_in_decisions(host, owner).await,
        ExpandedCase::ApplyingDeadline => check_applying_deadline(host, &owner).await,
    }
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
        json!({"kind":"page","items":first_items,"next":id(2)}),
    )?;
    expect(
        list("waiting", Some(&id(2))).send(host, owner).await?,
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
    assert_no_proof_rows(host).await
}

async fn check_other_destination(
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    let request = seed(host, 10, AnswerClass::Question, InitialApproval::Waiting).await?;
    let mut giver = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    giver.id = OTHER_PERSON.to_owned();
    giver.subject = OTHER_SUBJECT.to_owned();
    expect_http_status(
        host.post_revision(
            &json!({"kind":"person","organizationId":ORGANIZATION,"personId":OTHER_PERSON,
                "subjectId":OTHER_SUBJECT,"revision":giver.revision,"state":"active"}),
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
        view(json!({"kind":"standing","ending":{"kind":"decided","by":by}}))?,
    )?;
    expect(
        read(&request).send(host, &giver).await?,
        view(
            json!({"kind":"giver","decision":{"by":by,"answer":{"kind":"answered",
            "fields":[{"fieldId":1,"value":{"kind":"boolean","value":true}}]}}}),
        )?,
    )?;
    assert_count(host, "SELECT COUNT(*) FROM actor_persons", 1).await?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 1).await?;
    assert_no_proof_rows(host).await
}

async fn check_applying_deadline(
    host: &impl PersonApprovalHarness,
    owner: &Person,
) -> ConformanceResult<()> {
    let answer_class = AnswerClass::Once;
    let request = seed(host, 10, answer_class, InitialApproval::DeadlineInOneSecond).await?;
    let call = decide(&request, &id(100), &answer_class.answer());
    let mut socket = host.person_socket().connect(owner).await?;
    let reply = host
        .advance_at_approval_sample(call.call(&mut socket), 1_000)
        .await?;
    expect(reply, json!({"kind":"tooLate"}))?;
    assert_count(
        host,
        "SELECT COUNT(*) FROM approval_requests WHERE record_state='ended' AND decided_operation_id IS NULL AND delivery_state='owed'",
        1,
    )
    .await?;
    assert_no_proof_rows(host).await?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 1).await
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
            saved(&answer),
        )?;
    }
    assert_count(
        host,
        "SELECT COUNT(*) FROM approval_requests WHERE record_state = 'ended'",
        6,
    )
    .await?;
    assert_no_proof_rows(host).await?;
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
    let mut socket = host.person_socket().connect(owner).await?;

    // Advance only after the socket call reaches the applying hold.
    let first = host
        .advance_at_approval_sample(decide_now.call(&mut socket), 456)
        .await?;
    expect(first, saved(&answer))?;
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
        json!({"kind":"page","items":[{
            "requestId":request, "kind":"unprovenCall", "state":{"kind":"ended","ending":"decided"},
            "raisedAt":RAISED_AT, "deadline":DEADLINE
        }],"next":null}),
    )?;
    expect(
        read(&request).send(host, owner).await?,
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
        json!({"kind":"alreadyDecided","by":{"kind":"person","personId":owner.id},"at":ENDED_AT}),
    )?;
    require(
        losing_decide.send(host, owner).await? == earlier_decision,
        &json!({"expected":"byte-identical replay of the stored fractional decision time"}),
    )
}
