use super::fixture::{Fixture, receive, send, start_fixture};
use super::link_support::{caller, connect_client, create_workspace};
use central_link_client::{ClientEvent, ConnectionAttempt, LinkClient};
use central_link_protocol::{ReplyPayload, RequestPayload};
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
use fleet_runtime::PausePoint;
use reqwest_websocket::WebSocket;
use router_clock::RuntimeClock;
use workspace_model::central_mail::{CentralMailOperation, CentralMailRequest, NoticeCadence};

#[tokio::test]
async fn public_replies_leave_before_the_first_due_pass() -> Result<()> {
    let fixture = start_fixture().await?;
    let result = exercise(&fixture).await;
    fixture.close().await?;
    result
}

async fn exercise(fixture: &Fixture) -> Result<()> {
    let runtime = fixture.host.central(&fixture.organization_id).await?;
    let (mut client, attempt, mut socket, _) = connect_client(fixture, &fixture.token).await?;
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
        let store = runtime.runtime().statement_host();
        let pauses = runtime.runtime().pause_points();
        let before = pauses.central_count(PausePoint::BeforeDeferredDueWork);
        let held = pauses.hold_central(PausePoint::BeforeDeferredDueWork);
        let state = || async {
            let result = store
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
        reply_arrived_before_first_due_pass(
            request(route, fixture, &mut client, &attempt, &mut socket),
            state,
            || async {
                tokio::task::yield_now().await;
                Ok(pauses.central_count(PausePoint::BeforeDeferredDueWork) > before)
            },
            || async {
                held.release();
                Ok(())
            },
            || async { Ok(state().await? == "fired" && store.alarm().is_none()) },
        )
        .await?;
    }
    Ok(())
}

async fn request(
    route: DueReplyRoute,
    fixture: &Fixture,
    client: &mut LinkClient,
    attempt: &ConnectionAttempt,
    socket: &mut WebSocket,
) -> Result<()> {
    let payload = match route {
        DueReplyRoute::WorkspaceLink => {
            create_workspace(route.request_key(), "Reply-first workspace")?
        }
        DueReplyRoute::MailLink => RequestPayload::Mail(CentralMailRequest {
            request_id: route.request_key().parse()?,
            operation: CentralMailOperation::SetNoticeCadence {
                cadence: NoticeCadence::M25,
            },
        }),
        DueReplyRoute::SignInHttp => return sign_in_digest(fixture).await,
    };
    let request = client.begin_request(caller()?, payload)?;
    let expected = super::link_support::correlation(&request)?;
    send(socket, &request).await?;
    match client.receive(attempt, receive(socket).await?)? {
        ClientEvent::Reply {
            correlation_id,
            result: ReplyPayload::Workspace(_) | ReplyPayload::Mail(_),
        } if correlation_id == expected => Ok(()),
        _ => Err("expected the corresponding workspace or mail wire reply".into()),
    }
}

async fn sign_in_digest(fixture: &Fixture) -> Result<()> {
    let answer = reqwest::Client::builder()
        .no_proxy()
        .unix_socket(fixture.signin_socket.clone())
        .build()?
        .post(format!(
            "http://central{ORGANIZATION_STATEMENT_DIGEST_ROUTE}"
        ))
        .json(&fleet_contracts::ReadOrganizationStatementDigest {
            organization_id: fixture.organization_id.clone(),
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

#[tokio::test]
async fn a_competing_wire_reply_leaves_while_the_second_of_three_batches_waits() -> Result<()> {
    let fixture = start_fixture().await?;
    let result = competing_reply(&fixture).await;
    fixture.close().await?;
    result
}

async fn competing_reply(fixture: &Fixture) -> Result<()> {
    let runtime = fixture.host.central(&fixture.organization_id).await?;
    let (mut client, attempt, mut socket, _) = connect_client(fixture, &fixture.token).await?;
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
    let store = runtime.runtime().statement_host();
    let pauses = runtime.runtime().pause_points();
    let before = pauses.central_count(PausePoint::BeforeDueContinuation);
    let held = pauses.hold_central(PausePoint::BeforeDueContinuation);
    let mut publishing = pauses.hold_central(PausePoint::BeforeDueAlarmPublication);
    let effects = || async {
        let rows = store
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
        request(
            DueReplyRoute::WorkspaceLink,
            fixture,
            &mut client,
            &attempt,
            &mut socket,
        ),
        || async {
            let _ = store
                .read(Statement {
                    sql: "SELECT 1",
                    args: vec![],
                })
                .await?;
            Ok(pauses.central_count(PausePoint::BeforeDueContinuation) > before)
        },
        effects,
        sign_in_digest(fixture),
        || async {
            held.release();
            Ok(())
        },
    )
    .await?;
    publishing.until_held().await;
    if store.alarm().is_none() {
        return Err(
            "the physical alarm must still be armed while final publication is held".into(),
        );
    }
    use futures_util::FutureExt as _;
    if runtime
        .runtime()
        .until_reply_drains_settle()
        .now_or_never()
        .is_some()
    {
        return Err("the reply drain settled before its publication".into());
    }
    publishing.release();
    runtime.runtime().until_reply_drains_settle().await;
    if store.alarm().is_some() {
        return Err("the completed continuation did not clear the alarm".into());
    }
    Ok(())
}

#[tokio::test]
async fn a_dropped_post_reply_task_leaves_its_item_to_the_physical_fallback() -> Result<()> {
    let fixture = start_fixture().await?;
    let result = dropped_task(&fixture).await;
    fixture.close().await?;
    result
}

async fn dropped_task(fixture: &Fixture) -> Result<()> {
    let runtime = fixture.host.central(&fixture.organization_id).await?;
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
    runtime.recompute_alarm().await?;
    let store = runtime.runtime().statement_host();
    let faults = runtime.runtime().test_faults();
    faults.arm_deferred_due_work_drop();
    sign_in_digest(fixture).await?;
    if !observe_event(|| async {
        tokio::task::yield_now().await;
        Ok(!faults.deferred_due_work_drop_pending())
    })
    .await?
    {
        return Err("the drop fault was never consumed".into());
    }
    runtime.runtime().until_reply_drains_settle().await;
    if store.alarm().map(|alarm| alarm.unix_millis()) != Some(due.timestamp_millis()) {
        return Err("the dropped task changed the stored fallback".into());
    }
    let before = runtime.runtime().alarm_completion_count();
    fixture.clock.advance(std::time::Duration::from_secs(5))?;
    runtime.runtime().until_alarm_completed(before + 1).await;
    let effects = store
        .read(Statement {
            sql: "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
            args: vec![SqlValue::Text(key.to_string())],
        })
        .await?
        .rows;
    if effects != [vec![SqlValue::Integer(1.try_into()?)]] || store.alarm().is_some() {
        return Err("the physical fallback must finish the abandoned item once and clear".into());
    }
    Ok(())
}

#[path = "reply_due_recovery.rs"]
mod recovery;

#[path = "reply_due_receipts.rs"]
mod receipts;

#[path = "reply_due_fairness.rs"]
mod fairness;
