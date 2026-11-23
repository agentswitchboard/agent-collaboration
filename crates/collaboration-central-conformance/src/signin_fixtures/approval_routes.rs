//! One signed HTTP and owning-store decision table, run by both hosts.
use central_storage::SqlValue;
use serde_json::{Value, json};

use super::approval_rows::{InitialApproval, id, seed, text, time};
use super::{NOW_SECONDS, ORGANIZATION, OWNER, OWNER_SUBJECT, Person, fresh_request_id};
use crate::ConformanceResult;

pub use super::{approval_harness::PersonApprovalHarness, approval_rows::AnswerClass};

#[cfg(feature = "session-link-test-support")]
mod holder_delivery;
mod request;
#[cfg(feature = "session-link-test-support")]
use request::decide_in_organization;
use request::{READ, decide, list, read};

#[cfg(feature = "session-link-test-support")]
pub use holder_delivery::check_signed_holder_delivery;

const REVISIONS: &str = "/internal/organization-authorization-revisions";

pub enum Case {
    ListPage,
    ListDeadlinePrecedence,
    ReadRequest,
    ReadDeadlinePrecedence,
    ReadNotVisible,
    DecideNotVisible,
    DecideTimeout,
    DecideCancelled,
    ApplyingTimeFloor,
    AlreadyDecided,
    InvalidAnswer,
    Replay(ReplayCase),
    BadAddress,
    Authentication(AuthenticationCase),
    ReplayedProof,
    Admission(AdmissionCase),
    Factor { answer: AnswerClass, fresh: bool },
}

pub enum ReplayCase {
    OperationConflict,
    Exact,
    AfterPermissionLoss,
}

pub enum AuthenticationCase {
    MissingProof,
    BadProof,
    ExpiredToken,
    ForeignOrganization,
}

pub enum AdmissionCase {
    MissingPermission,
    OutdatedFloor,
    MissingFloor,
    Watermark,
    InactivePerson,
}

pub async fn check(host: &impl PersonApprovalHarness, case: Case) -> ConformanceResult<()> {
    let owner = Person::owner(ORGANIZATION, NOW_SECONDS)?;
    let operation = id(100);
    let answer = AnswerClass::Once.answer();
    match case {
        Case::ListPage => {
            let first = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
            let latest = seed(host, 20, AnswerClass::Question, InitialApproval::Waiting).await?;
            seed(host, 30, AnswerClass::Once, InitialApproval::Hidden).await?;
            let list = list("all", None);
            let page = list.send(host, &owner).await?;
            let summary = |request: &str, kind: &str| -> ConformanceResult<Value> {
                Ok(
                    json!({"requestId":request, "kind":kind,"state":{"kind":"waiting"},
                    "raisedAt": time(-10)?, "deadline":time(3590)?}),
                )
            };
            expect(
                page,
                200,
                json!({"kind":"page","items":[
                summary(&latest,"providerQuestion")?,summary(&first,"providerApproval")?
            ],"next":null}),
            )?;
            let earlier = self::list("all", Some(&latest)).send(host, &owner).await?;
            expect(
                earlier,
                200,
                json!({"kind":"page","items":[summary(&first,"providerApproval")?],"next":null}),
            )?;
            assert_count(host, "SELECT COUNT(*) FROM actor_persons", 0).await?;
            assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 2).await?;
            assert_count(host, "SELECT COUNT(*) FROM approval_applying_time", 0).await?;
        }
        Case::ListDeadlinePrecedence => {
            let expired = seed(host, 10, AnswerClass::Once, InitialApproval::PastDeadline).await?;
            let page = list("waiting", None).send(host, &owner).await?;
            expect(page, 200, json!({"kind":"page","items":[],"next":null}))?;
            let all = list("all", None).send(host, &owner).await?;
            expect(
                all,
                200,
                json!({"kind":"page","items":[{"requestId":expired,
                "kind":"providerApproval","state":{"kind":"ended","ending":"timedOut"},
                "raisedAt":time(-3600)?,"deadline":time(0)?}],"next":null}),
            )?;
        }
        Case::ReadRequest => {
            let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
            let view = expect_status(read(&request).send(host, &owner).await?, 200)?;
            let view: Value = serde_json::from_slice(&view)?;
            require(
                view.get("kind").and_then(Value::as_str) == Some("request")
                    && view.pointer("/view/requestId").and_then(Value::as_str)
                        == Some(request.as_str()),
                &view,
            )?;
            require(view.to_string().contains("Private offer details"), &view)?;
            assert_count(host, "SELECT COUNT(*) FROM actor_persons", 0).await?;
            assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 1).await?;
        }
        Case::ReadDeadlinePrecedence => {
            let request = seed(host, 10, AnswerClass::Once, InitialApproval::PastDeadline).await?;
            let view = expect_status(read(&request).send(host, &owner).await?, 200)?;
            let view: Value = serde_json::from_slice(&view)?;
            require(
                view.get("kind").and_then(Value::as_str) == Some("request")
                    && view.pointer("/view/requestId").and_then(Value::as_str)
                        == Some(request.as_str()),
                &view,
            )?;
            require(view.to_string().contains("\"kind\":\"timedOut\""), &view)?;
            assert_count(
                host,
                "SELECT COUNT(*) FROM approval_requests WHERE record_state='ended'",
                1,
            )
            .await?;
            assert_count(host, "SELECT COUNT(*) FROM actor_persons", 0).await?;
        }
        Case::ReadNotVisible | Case::DecideNotVisible => {
            let hidden = seed(host, 10, AnswerClass::Once, InitialApproval::Hidden).await?;
            let missing = id(9);
            let request = |id, operation_id| {
                if matches!(case, Case::ReadNotVisible) {
                    read(id)
                } else {
                    decide(id, operation_id, &answer)
                }
            };
            let hidden_reply = request(&hidden, &operation).send(host, &owner).await?;
            let missing_reply = request(&missing, &id(101)).send(host, &owner).await?;
            require(
                hidden_reply == missing_reply,
                &json!({"expected":"identical visibility replies"}),
            )?;
            expect(hidden_reply, 200, json!({"kind":"notVisible"}))?;
            let decide_was_admitted = matches!(case, Case::DecideNotVisible);
            assert_count(
                host,
                "SELECT COUNT(*) FROM actor_persons",
                i64::from(decide_was_admitted),
            )
            .await?;
            if decide_was_admitted {
                seed(host, 9, AnswerClass::Once, InitialApproval::Waiting).await?;
                let retry = decide(&missing, &id(101), &answer)
                    .send(host, &owner)
                    .await?;
                expect(retry, 200, json!({"kind":"notVisible"}))?;
                assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 2).await?;
            }
        }
        Case::DecideTimeout | Case::DecideCancelled => {
            let initial = if matches!(case, Case::DecideTimeout) {
                InitialApproval::PastDeadline
            } else {
                InitialApproval::Cancelled
            };
            let request = seed(host, 10, AnswerClass::Once, initial).await?;
            expect(
                decide(&request, &operation, &answer)
                    .send(host, &owner)
                    .await?,
                200,
                json!({"kind":"tooLate"}),
            )?;
            assert_count(
                host,
                "SELECT COUNT(*) FROM approval_requests WHERE decided_operation_id IS NOT NULL",
                0,
            )
            .await?;
        }
        Case::ApplyingTimeFloor => {
            let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
            let happened_at = time(3590)?;
            let floor_ms = happened_at.timestamp_millis();
            let stored_at = happened_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            host.write(
                "INSERT INTO approval_applying_time (organization_id, observed_ms, created_at, updated_at) VALUES (?1, ?2, ?3, ?3)",
                vec![text(ORGANIZATION), SqlValue::Integer(floor_ms.try_into()?), text(&stored_at)],
            ).await?;
            // The proof is current on the admin clock, while the stored approval window
            // has elapsed at the holder's durable applying time.
            expect(
                decide(&request, &operation, &answer)
                    .send(host, &owner)
                    .await?,
                200,
                json!({"kind":"tooLate"}),
            )?;
            let ended = host.read("SELECT ended_at FROM approval_requests").await?;
            require(
                ended == vec![vec![text(stored_at)]],
                &json!({"expected":"ending at the retained approval floor"}),
            )?;
            assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 1).await?;
        }
        Case::AlreadyDecided => {
            let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
            expect(
                decide(&request, &operation, &answer)
                    .send(host, &owner)
                    .await?,
                200,
                saved(&answer),
            )?;
            expect(
                decide(&request, &id(101), &json!({"kind":"approval","optionId":2}))
                    .send(host, &owner)
                    .await?,
                200,
                json!({
                    "kind":"alreadyDecided","by":{"kind":"person","personId":OWNER},"at":time(0)?
                }),
            )?;
            assert_count(host, "SELECT COUNT(*) FROM actor_persons", 1).await?;
            assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 2).await?;
        }
        Case::InvalidAnswer => {
            let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
            let unknown_option = json!({"kind":"approval","optionId":7});
            let call = decide(&request, &operation, &unknown_option);
            let invalid = expect_status(call.send(host, &owner).await?, 200)?;
            let invalid_value: Value = serde_json::from_slice(&invalid)?;
            require(
                invalid_value.get("kind").and_then(Value::as_str) == Some("invalid")
                    && invalid_value
                        .pointer("/reason/kind")
                        .and_then(Value::as_str)
                        == Some("invalidAnswer"),
                &invalid_value,
            )?;
            let replay = expect_status(call.send(host, &owner).await?, 200)?;
            require(invalid == replay, &invalid_value)?;
            assert_count(
                host,
                "SELECT COUNT(*) FROM approval_requests WHERE record_state='waiting'",
                1,
            )
            .await?;
            assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 1).await?;
        }
        Case::Replay(case) => {
            check_replay(host, owner, case, &operation, &answer).await?;
        }
        Case::BadAddress => {
            let request = read(&id(10));
            let headers = request.headers(&owner, &fresh_request_id(NOW_SECONDS)?, &|_| {})?;
            let body = expect_status(
                host.post(READ, &json!({"requestId":id(10)}), headers)
                    .await?,
                400,
            )?;
            require(body.is_empty(), &json!({"expected":"empty 400 body"}))?;
            assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 0).await?;
        }
        Case::Authentication(case) => {
            let request = read(&id(10));
            let headers = match case {
                AuthenticationCase::MissingProof => Vec::new(),
                AuthenticationCase::BadProof => {
                    let mut headers =
                        request.headers(&owner, &fresh_request_id(NOW_SECONDS)?, &|_| {})?;
                    if let Some((_, value)) = headers.iter_mut().find(|(name, _)| *name == "dpop") {
                        *value = "broken".to_owned();
                    }
                    headers
                }
                AuthenticationCase::ExpiredToken => {
                    request.headers(&owner, &fresh_request_id(NOW_SECONDS)?, &|claims| {
                        claims["exp"] = json!(NOW_SECONDS);
                    })?
                }
                AuthenticationCase::ForeignOrganization => {
                    let person =
                        Person::owner("019f0000-0000-7000-8000-00000000e201", NOW_SECONDS)?;
                    request.headers(&person, &fresh_request_id(NOW_SECONDS)?, &|_| {})?
                }
            };
            let body = expect_status(host.post(request.path, &request.body, headers).await?, 401)?;
            require(body.is_empty(), &json!({"expected":"empty 401 body"}))?;
            assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 0).await?;
        }
        Case::ReplayedProof => {
            let request = read(&id(10));
            let headers = request.headers(&owner, &fresh_request_id(NOW_SECONDS)?, &|_| {})?;
            expect(
                host.post(READ, &request.body, headers.clone()).await?,
                200,
                json!({"kind":"notVisible"}),
            )?;
            expect(
                host.post(READ, &request.body, headers).await?,
                200,
                refusal("requestReused"),
            )?;
            assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 1).await?;
        }
        Case::Admission(case) => {
            let mut owner = owner;
            let reason = match case {
                AdmissionCase::MissingPermission => {
                    owner
                        .permissions
                        .retain(|permission| permission != "approval:decide");
                    "notPermitted"
                }
                AdmissionCase::OutdatedFloor => {
                    owner.revision -= 1;
                    "refreshRequired"
                }
                AdmissionCase::MissingFloor => {
                    host.write(
                        "DELETE FROM signin_person_authorization_floors WHERE person_id=?1",
                        vec![text(OWNER)],
                    )
                    .await?;
                    "authorizationSyncRequired"
                }
                AdmissionCase::Watermark => {
                    host.write(
                        "UPDATE signin_authorization_floors SET request_reject_through_ms=?1",
                        vec![SqlValue::Integer((NOW_SECONDS * 1000).try_into()?)],
                    )
                    .await?;
                    "requestTime"
                }
                AdmissionCase::InactivePerson => {
                    publish_person_floor(host, 5, "inactive").await?;
                    owner.revision = 5;
                    "notPermitted"
                }
            };
            expect(
                read(&id(10)).send(host, &owner).await?,
                200,
                refusal(reason),
            )?;
            assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 0).await?;
        }
        Case::Factor { answer, fresh } => check_factor(host, owner, answer, fresh).await?,
    }
    Ok(())
}

async fn check_factor(
    host: &impl PersonApprovalHarness,
    mut owner: Person,
    class: AnswerClass,
    fresh: bool,
) -> ConformanceResult<()> {
    let request = seed(host, 10, class, InitialApproval::Waiting).await?;
    let operation = id(100);
    owner.second_factor_at = Some(NOW_SECONDS - if fresh { 900 } else { 901 });
    let answer = class.answer();
    let call = decide(&request, &operation, &answer);
    let reply = call.send(host, &owner).await?;
    if class.requires_factor() && !fresh {
        expect(
            reply,
            200,
            json!({"kind":"factorRequired","withinSeconds":900}),
        )?;
        assert_count(
            host,
            "SELECT COUNT(*) FROM approval_requests WHERE record_state='waiting'",
            1,
        )
        .await?;
        assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 0).await?;
        assert_count(host, "SELECT COUNT(*) FROM actor_persons", 0).await?;
        assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 1).await?;
        assert_count(host, "SELECT COUNT(*) FROM approval_applying_time", 0).await?;
        owner.second_factor_at = Some(NOW_SECONDS);
        expect(call.send(host, &owner).await?, 200, saved(&answer))?;
    } else {
        expect(reply, 200, saved(&answer))?;
    }
    assert_count(host, "SELECT COUNT(*) FROM actor_persons", 1).await?;
    assert_count(host, "SELECT COUNT(*) FROM approval_applying_time", 1).await?;
    assert_count(
        host,
        "SELECT COUNT(*) FROM approval_requests WHERE delivery_state='owed'",
        1,
    )
    .await?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 1).await
}

async fn check_replay(
    host: &impl PersonApprovalHarness,
    mut owner: Person,
    case: ReplayCase,
    operation: &str,
    answer: &Value,
) -> ConformanceResult<()> {
    let request = seed(host, 10, AnswerClass::Once, InitialApproval::Waiting).await?;
    let call = decide(&request, operation, answer);
    if matches!(case, ReplayCase::AfterPermissionLoss) {
        host.lose_reply_after_apply(operation).await?;
    }

    let first = call.send(host, &owner).await?;
    match case {
        ReplayCase::AfterPermissionLoss => {
            let lost = expect_status(first, 503)?;
            require(
                lost.is_empty(),
                &json!({"expected":"no decision delivered"}),
            )?;
            assert_count(
                host,
                "SELECT COUNT(*) FROM approval_requests WHERE record_state='ended'",
                1,
            )
            .await?;
            assert_count(host, "SELECT COUNT(*) FROM admin_used_requests", 1).await?;
            let rows = host.read("SELECT answer FROM runtime_request_log").await?;
            let [row] = rows.as_slice() else {
                return Err("expected the only retained answer".into());
            };
            let [SqlValue::Text(retained)] = row.as_slice() else {
                return Err("expected retained answer bytes".into());
            };
            publish_person_floor(host, 5, "active").await?;
            owner.revision = 5;
            owner
                .permissions
                .retain(|permission| permission != "approval:decide");
            let retry = call.send(host, &owner).await?;
            require(
                retry.1 == retained.as_bytes(),
                &json!({"expected":"exact retained response bytes"}),
            )?;
            expect(retry, 200, saved(answer))?;
            let altered = decide(
                &request,
                operation,
                &json!({"kind":"approval","optionId":2}),
            );
            expect(
                altered.send(host, &owner).await?,
                200,
                refusal("notPermitted"),
            )?;
            expect(
                decide(&request, &id(101), answer)
                    .send(host, &owner)
                    .await?,
                200,
                refusal("notPermitted"),
            )?;
        }
        ReplayCase::Exact => {
            let retry = call.send(host, &owner).await?;
            require(first == retry, &json!({"expected":"exact saved response"}))?;
            expect(retry, 200, saved(answer))?;
        }
        ReplayCase::OperationConflict => {
            let different = decide(
                &request,
                operation,
                &json!({"kind":"approval","optionId":2}),
            );
            expect(
                different.send(host, &owner).await?,
                200,
                json!({"kind":"operationConflict"}),
            )?;
            let retry = call.send(host, &owner).await?;
            require(
                first == retry,
                &json!({"expected":"first decision preserved"}),
            )?;
        }
    }
    assert_count(host, "SELECT COUNT(*) FROM actor_persons", 1).await?;
    assert_count(host, "SELECT COUNT(*) FROM runtime_request_log", 1).await
}

async fn publish_person_floor(
    host: &impl PersonApprovalHarness,
    revision: i64,
    state: &str,
) -> ConformanceResult<()> {
    let body = json!({"kind":"person","organizationId":ORGANIZATION,"personId":OWNER,
        "subjectId":OWNER_SUBJECT,"revision":revision,"state":state});
    expect_status(host.post(REVISIONS, &body, Vec::new()).await?, 200)?;
    Ok(())
}

async fn assert_count(
    host: &impl PersonApprovalHarness,
    statement: &'static str,
    expected: i64,
) -> ConformanceResult<()> {
    let actual = host.read(statement).await?;
    let expected = vec![vec![SqlValue::Integer(expected.try_into()?)]];
    if actual != expected {
        return Err(format!("{statement} yielded {actual:?}, expected {expected:?}").into());
    }
    Ok(())
}

fn saved(answer: &Value) -> Value {
    json!({"kind":"savedDecision","decision":{"by":{"kind":"person","personId":OWNER},"answer":answer}})
}

fn refusal(reason: &str) -> Value {
    json!({"result":"refused","reason":reason})
}

fn require(condition: bool, evidence: &Value) -> ConformanceResult<()> {
    if condition {
        Ok(())
    } else {
        Err(format!("approval response mismatch {evidence}").into())
    }
}

fn expect_status(reply: (u16, Vec<u8>), expected: u16) -> ConformanceResult<Vec<u8>> {
    if reply.0 != expected {
        return Err(format!(
            "expected HTTP {expected}, got {} {}",
            reply.0,
            String::from_utf8_lossy(&reply.1)
        )
        .into());
    }
    Ok(reply.1)
}

fn expect(reply: (u16, Vec<u8>), status: u16, expected: Value) -> ConformanceResult<()> {
    let actual: Value = serde_json::from_slice(&expect_status(reply, status)?)?;
    require(
        actual == expected,
        &json!({"expected":expected,"actual":actual}),
    )
}

pub const fn factor(answer: AnswerClass, fresh: bool) -> Case {
    Case::Factor { answer, fresh }
}
