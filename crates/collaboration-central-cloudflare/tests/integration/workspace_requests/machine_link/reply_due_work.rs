use super::fixture::{ConnectedLink, Fixture};
use central_link_protocol::{DataFrame, ReplyPayload, RequestPayload};
use central_storage::{BatchOutcome, SqlValue, Statement};
use collaboration_central_conformance::{
    ConformanceResult as Result,
    due_drain_scenarios::{
        DueReplyRoute, competing_reply_between_due_passes, observe_event,
        reply_arrived_before_first_due_pass,
    },
};
use collaboration_central_core::engine_probe::SeedProbe;
use fleet_contracts::{ORGANIZATION_STATEMENT_DIGEST_ROUTE, OrganizationStatementDigestReply};
use router_clock::RuntimeClock;
use workspace_model::{
    WorkspaceOperation, WorkspaceRequest,
    central_mail::{CentralMailOperation, CentralMailRequest, NoticeCadence},
};

const BEFORE_DEFERRED: &str = "beforeDeferredDueWork";

#[tokio::test]
async fn public_replies_leave_before_the_first_due_pass() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = exercise(&fixture).await;
    fixture.close().await?;
    result
}

async fn exercise(fixture: &Fixture) -> Result<()> {
    let runtime = fixture.runtime();
    let (mut link, _, _, _, _) = super::baseline(fixture).await?;
    let initial: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
    let log_start = fixture.host.captured_logs().len();
    request(DueReplyRoute::SignInHttp, fixture, &mut link).await?;
    let mut counts = Vec::new();
    for _ in 0..64 {
        let logs = fixture.host.captured_logs();
        counts = logs
            .iter()
            .skip(log_start)
            .filter(|line| {
                line.contains("Central due candidate SELECTs:")
                    || line.contains("Central alarm input SELECTs:")
            })
            .cloned()
            .collect();
        if counts
            .iter()
            .any(|line| line.contains("Central due candidate SELECTs:"))
            && counts
                .iter()
                .any(|line| line.contains("Central alarm input SELECTs:"))
        {
            break;
        }
        let _: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
    }
    if !counts
        .iter()
        .any(|line| line.contains("Central due candidate SELECTs: 3"))
        || !counts
            .iter()
            .any(|line| line.contains("Central alarm input SELECTs: 3"))
    {
        return Err(format!("empty Workerd event did not read one indexed snapshot and one alarm snapshot with fixture witnesses: {counts:?}").into());
    }
    println!("empty Workerd event, fixture SELECT dispatches: {counts:?}");
    let empty: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
    if empty != initial || empty.1 != 0 {
        return Err("an empty event scheduled Workerd reply work".into());
    }
    for route in DueReplyRoute::ALL {
        let key: uuid::Uuid = route.request_key().parse()?;
        let due = fixture.clock.utc_now() + chrono::Duration::seconds(5);
        let seeded = runtime
            .seed_probe(SeedProbe {
                key,
                group: key,
                due: Some(due),
                claim: format!("held due {key}"),
            })
            .await?;
        if !matches!(seeded, BatchOutcome::Applied { .. }) {
            return Err("due probe was not stored".into());
        }
        let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
        let before: u64 = runtime
            .control("mac-delivery-count", &BEFORE_DEFERRED)
            .await?;
        let _: bool = runtime.control("hold-central", &BEFORE_DEFERRED).await?;
        let state = || async {
            let result = runtime
                .read(Statement {
                    sql: "SELECT state FROM engine_probe_items WHERE item_id=?",
                    args: vec![SqlValue::Text(key.to_string())],
                })
                .await?;
            if let [row] = result.rows.as_slice()
                && let [SqlValue::Text(state)] = row.as_slice()
            {
                return Ok(state.clone());
            }
            Err("due probe state is missing".into())
        };
        let proof = reply_arrived_before_first_due_pass(
            request(route, fixture, &mut link),
            state,
            || async {
                let count: u64 = runtime
                    .control("mac-delivery-count", &BEFORE_DEFERRED)
                    .await?;
                let current: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
                Ok(count > before && current == (activity.0 + 1, 1))
            },
            || async {
                let _: bool = runtime.control("release-central", &BEFORE_DEFERRED).await?;
                Ok(())
            },
            || async {
                let current: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
                Ok(state().await? == "fired" && current == (activity.0 + 1, 0))
            },
        )
        .await;
        if let Err(error) = proof {
            let counts: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
            let candidates: serde_json::Value = runtime.control("engine-alarm-inputs", &()).await?;
            let alarm: Option<i64> = runtime.control("raw-alarm-time", &()).await?;
            let reached: u64 = runtime
                .control("mac-delivery-count", &BEFORE_DEFERRED)
                .await?;
            return Err(format!("{route:?} reply proof failed ({error}); activity before={activity:?} now={counts:?}, arrivals before={before} now={reached}; inputs={candidates}; alarm={alarm:?}; due={due}; logs={:?}", due_logs(fixture)).into());
        }
    }
    link.close().await
}

async fn request(route: DueReplyRoute, fixture: &Fixture, link: &mut ConnectedLink) -> Result<()> {
    let payload = match route {
        DueReplyRoute::WorkspaceLink => RequestPayload::Workspace(WorkspaceRequest {
            request_id: Some(route.request_key().parse()?),
            workspace_id: None,
            operation: WorkspaceOperation::CreateWorkspace {
                name: "Reply-first workspace".to_owned().try_into()?,
            },
        }),
        DueReplyRoute::MailLink => RequestPayload::Mail(CentralMailRequest {
            request_id: route.request_key().parse()?,
            operation: CentralMailOperation::SetNoticeCadence {
                cadence: NoticeCadence::M25,
            },
        }),
        DueReplyRoute::SignInHttp => return sign_in_digest(fixture).await,
    };
    let (_, replies) = link.batch(vec![payload]).await?;
    match replies.as_slice() {
        [DataFrame::Reply { result, .. }]
            if matches!(
                result.as_ref(),
                ReplyPayload::Workspace(workspace_model::WorkspaceReply {
                    result: workspace_model::WorkspaceResult::Done(_),
                    ..
                }) | ReplyPayload::Mail(workspace_model::central_mail::CentralMailResult::Done(_))
            ) =>
        {
            Ok(())
        }
        _ => Err(format!("wire operation did not succeed: {replies:?}").into()),
    }
}

async fn sign_in_digest(fixture: &Fixture) -> Result<()> {
    let answer = fixture
        .client
        .post(format!(
            "{}{ORGANIZATION_STATEMENT_DIGEST_ROUTE}",
            fixture.host.signin_origin()
        ))
        .json(&fleet_contracts::ReadOrganizationStatementDigest {
            organization_id: fixture.organization.clone(),
        })
        .send()
        .await?
        .error_for_status()?
        .json::<OrganizationStatementDigestReply>()
        .await?;
    if answer.digest.as_str().is_empty() {
        return Err("sign-in digest reply was empty".into());
    }
    Ok(())
}

const BEFORE_CONTINUATION: &str = "beforeDueContinuation";

#[tokio::test]
async fn a_competing_wire_reply_leaves_while_the_second_of_three_batches_waits() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = competing_reply(&fixture).await;
    fixture.close().await?;
    result
}

async fn competing_reply(fixture: &Fixture) -> Result<()> {
    let runtime = fixture.runtime();
    let mut link = ConnectedLink::open(fixture).await?;
    let group: uuid::Uuid = "019f0000-0000-7000-8000-00000000db04".parse()?;
    let due = fixture.clock.utc_now() + chrono::Duration::seconds(5);
    for number in 0..600 {
        runtime
            .seed_probe(SeedProbe {
                key: format!("019f0000-0000-7000-8000-{number:012}").parse()?,
                group,
                due: Some(due),
                claim: format!("continuation {number}"),
            })
            .await?;
    }
    let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
    let before: u64 = runtime
        .control("mac-delivery-count", &BEFORE_CONTINUATION)
        .await?;
    let _: bool = runtime
        .control("hold-central", &BEFORE_CONTINUATION)
        .await?;
    let effects = || async {
        let rows = runtime
            .read(Statement {
                sql: "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
                args: vec![SqlValue::Text(group.to_string())],
            })
            .await?
            .rows;
        if let [row] = rows.as_slice()
            && let [SqlValue::Integer(count)] = row.as_slice()
        {
            return Ok(u64::try_from(count.get())?);
        }
        Err("probe group count was absent".into())
    };
    competing_reply_between_due_passes(
        request(DueReplyRoute::WorkspaceLink, fixture, &mut link),
        || async {
            Ok(runtime
                .control::<u64>("mac-delivery-count", &BEFORE_CONTINUATION)
                .await?
                > before)
        },
        effects,
        sign_in_digest(fixture),
        || async {
            let _: bool = runtime
                .control("release-central", &BEFORE_CONTINUATION)
                .await?;
            Ok(())
        },
    )
    .await?;
    if !observe_event(|| async {
        Ok(runtime
            .control::<(u64, u64)>("raw-reply-due-activity", &())
            .await?
            == (activity.0 + 1, 0))
    })
    .await?
    {
        return Err("the owned continuation still has pending Worker work".into());
    }
    link.close().await
}

#[tokio::test]
async fn a_dropped_post_reply_task_leaves_its_item_to_the_physical_fallback() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = dropped_task(&fixture).await;
    if result.is_err() {
        eprintln!("{:?}", due_logs(&fixture));
    }
    fixture.close().await?;
    result
}

async fn dropped_task(fixture: &Fixture) -> Result<()> {
    let runtime = fixture.runtime();
    let key: uuid::Uuid = "019f0000-0000-7000-8000-00000000db05".parse()?;
    let due = fixture.clock.utc_now() + chrono::Duration::seconds(5);
    runtime
        .seed_probe(SeedProbe {
            key,
            group: key,
            due: Some(due),
            claim: "fallback proof".into(),
        })
        .await?;
    let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
    let _: bool = runtime.control("drop-reply-due-work", &()).await?;
    sign_in_digest(fixture).await?;
    if !observe_event(|| async {
        Ok(runtime
            .control::<(u64, u64)>("raw-reply-due-activity", &())
            .await?
            == (activity.0 + 1, 0))
    })
    .await?
    {
        return Err("the dropped task did not consume and settle one waitUntil".into());
    }
    if runtime
        .control::<Option<i64>>("raw-alarm-time", &())
        .await?
        != Some(due.timestamp_millis())
    {
        return Err("the dropped task changed the stored fallback".into());
    }
    fixture.clock.advance(std::time::Duration::from_secs(5))?;
    if runtime.dispatch_due_alarm().await?.is_none() {
        return Err("the physically stored fallback did not fire".into());
    }
    let effects = runtime
        .read(Statement {
            sql: "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
            args: vec![SqlValue::Text(key.to_string())],
        })
        .await?
        .rows;
    if effects != [vec![SqlValue::Integer(1.try_into()?)]]
        || runtime
            .control::<Option<i64>>("raw-alarm-time", &())
            .await?
            .is_some()
    {
        return Err("the physical fallback must finish the abandoned item once and clear".into());
    }
    Ok(())
}

fn due_logs(fixture: &Fixture) -> Vec<String> {
    fixture
        .host
        .captured_logs()
        .into_iter()
        .filter(|line| {
            line.contains("Central due yield")
                || line.contains("engine alarm")
                || line.contains("different request")
                || line.contains("async function")
                || line.contains("workerd/jsg")
        })
        .collect()
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "propagate fixture and transport errors while keeping assertion diffs for observed receipts and storage"
)]
async fn a_sent_worker_batch_member_keeps_the_drain_when_the_next_member_errors() -> Result<()> {
    use super::fixture::{receive, send, session_caller};

    let fixture = Fixture::new().await?;
    let runtime = fixture.runtime();
    let mut link = ConnectedLink::open(&fixture).await?;
    let key: uuid::Uuid = "019f0000-0000-7000-8000-00000000db26".parse()?;
    let due = fixture.clock.utc_now() + chrono::Duration::seconds(5);
    runtime
        .seed_probe(SeedProbe {
            key,
            group: key,
            due: Some(due),
            claim: "sent batch member".into(),
        })
        .await?;
    let make_request = |key: &str, name: &str| -> Result<WorkspaceRequest> {
        Ok(WorkspaceRequest {
            request_id: Some(key.parse()?),
            workspace_id: None,
            operation: WorkspaceOperation::CreateWorkspace {
                name: name.to_owned().try_into()?,
            },
        })
    };
    let DataFrame::Request { request: first } = link.request_as(
        session_caller()?,
        make_request("019f0000-0000-7000-8000-00000000db27", "Sent member")?,
    )?
    else {
        return Err("first workspace request was not framed".into());
    };
    let DataFrame::Request { request: second } = link.request_as(
        session_caller()?,
        make_request("019f0000-0000-7000-8000-00000000db28", "Failed member")?,
    )?
    else {
        return Err("second workspace request was not framed".into());
    };
    let expected_reply = first.correlation_id().clone();
    let activity: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
    runtime
        .fail_machine_restore_after(
            1,
            central_storage::StoreFailure::Corrupt {
                reason: central_storage::CorruptReason::BadValue {
                    column: "fixture.second_batch_member".into(),
                },
            },
        )
        .await?;
    send(
        &mut link.socket,
        &DataFrame::Batch {
            requests: central_link_protocol::LinkRequestBatch::new(vec![*first, *second])?,
        },
    )
    .await?;
    assert!(
        matches!(receive(&mut link.socket).await?, DataFrame::Reply { correlation_id, .. } if correlation_id == expected_reply)
    );
    if !observe_event(|| async {
        let observed: (u64, u64) = runtime.control("raw-reply-due-activity", &()).await?;
        Ok(observed == (activity.0 + 1, 0))
    })
    .await?
    {
        return Err("the sent member lost its due drain when the next member failed".into());
    }
    let facts = runtime.read(Statement {
        sql: "SELECT (SELECT effect_count FROM engine_probe_groups WHERE group_id=?), (SELECT COUNT(*) FROM directory_workspaces WHERE name='Failed member')",
        args: vec![SqlValue::Text(key.to_string())],
    }).await?.rows;
    assert_eq!(
        facts,
        [vec![
            SqlValue::Integer(1.try_into()?),
            SqlValue::Integer(0.try_into()?)
        ]]
    );
    assert!(fixture.clock.utc_now() < due);
    fixture.close().await
}

#[path = "reply_due_recovery.rs"]
mod recovery;

#[path = "reply_due_retry.rs"]
mod retry;

#[path = "reply_due_receipts.rs"]
mod receipts;

#[path = "reply_due_fairness.rs"]
mod fairness;
