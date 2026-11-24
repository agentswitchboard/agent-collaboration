//! One real hibernation wake settles each well-formed write independently.
use super::super::workerd_host::c2_controls::{
    AttachmentSnapshot, DiagnosticDisposition, EnrollmentReadOutcome, EnrollmentReadQuery,
    HeldDiagnostic, RestoreOutcome, WitnessEvent, WitnessPhase, WitnessRecord,
};
use super::fixture::{Fixture, MACHINE_ID, session_caller};
use super::receipt::{AreaReceipt, BatchHibernationReceipt};
use super::{baseline, correlation, create_area, workspace_reply};
use central_link_protocol::{DataFrame, ReplyPayload, RequestPayload};
use central_storage::{SqlValue, Statement};
use collaboration_central_conformance::ConformanceResult as Result;
use collaboration_central_conformance::fixture_link_reader::read_fixture_frame;
use futures_util::SinkExt;
use reqwest_websocket::Message;
use std::{io::Write, path::PathBuf};
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRefusal, CentralMailRequest,
    CentralMailResult, TimingRequirement,
};
use workspace_model::{
    Caller, ItemRef, ItemView, WorkspaceAnswer, WorkspaceId, WorkspaceOperation, WorkspaceRequest,
    WorkspaceResult,
};

const PAST_WAKE_REQUEST: &str = "019f0000-0000-7000-8000-00000000cf41";
const AREA_A: &str = "Batch area A";
const AREA_B: &str = "Batch area B";

#[tokio::test]
async fn one_hibernation_wake_processes_independent_writes_and_replies() -> Result<()> {
    let destination =
        std::env::var_os("CODEX_ROUTER_C2_BATCH_HIBERNATION_RECEIPT").map(PathBuf::from);
    if destination.as_ref().is_some_and(|path| !path.is_absolute()) {
        return Err("batch hibernation receipt destination must be absolute".into());
    }
    run_with_held_callback(3, destination).await
}

#[tokio::test]
async fn the_first_alarm_callback_cannot_delay_later_batch_replies() -> Result<()> {
    run_with_held_callback(1, None).await
}

#[tokio::test]
async fn refused_alarm_and_restoration_diagnostics_preserve_correlated_replies() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = refused_diagnostic_journey(&fixture).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn each_reply_observes_the_alarm_committed_for_that_member() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = committed_alarm_journey(&fixture).await;
    fixture.close().await?;
    result
}

async fn committed_alarm_journey(fixture: &Fixture) -> Result<()> {
    let (mut link, _, _, _, _) = baseline(fixture).await?;
    let Caller::Session(target) = session_caller()? else {
        return Err("wake needs the fixture's session caller".into());
    };
    let later: workspace_model::Timestamp = "2030-03-17T17:55:00Z".parse()?;
    let earlier: workspace_model::Timestamp = "2030-03-17T17:50:00Z".parse()?;
    let payloads = [
        ("019f0000-0000-7000-8000-00000000cf51", later),
        ("019f0000-0000-7000-8000-00000000cf52", earlier),
    ]
    .into_iter()
    .map(|(request_id, due_at)| {
        Ok(RequestPayload::Mail(CentralMailRequest {
            request_id: request_id.parse()?,
            operation: CentralMailOperation::SetWake {
                target: target.clone(),
                due_at,
                text: "C2 alarm outcome".to_owned().try_into()?,
            },
        }))
    })
    .collect::<Result<Vec<_>>>()?;
    fixture.host.c2_witness(WitnessPhase::Deciding).await?;
    let (batch, replies) = link.batch(payloads).await?;
    let correlations = batch_correlations(&batch)?;
    let records = fixture
        .host
        .c2_settle_alarms(WitnessPhase::Deciding, &correlations)
        .await?;
    assert_eq!(replies.len(), 2);
    for ((reply, correlation), expected) in replies.iter().zip(correlations).zip([later, earlier]) {
        let DataFrame::Reply { result, .. } = reply else {
            return Err("wake did not produce its reply".into());
        };
        assert!(
            matches!(result.as_ref(), ReplyPayload::Mail(CentralMailResult::Done(
            CentralMailAnswer::WakeSet { due_at, .. }
        )) if *due_at == expected)
        );
        let snapshots = records
            .iter()
            .filter_map(|record| match &record.event {
                WitnessEvent::AlarmSnapshot {
                    correlation_id,
                    earliest_due_ms,
                    alarm_ms,
                    ..
                } if correlation_id == correlation => Some((
                    earliest_due_ms.map(central_storage::SafeInteger::get),
                    alarm_ms.map(central_storage::SafeInteger::get),
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        let expected = Some(expected.timestamp_millis());
        assert_eq!(snapshots, [(expected, expected)]);
    }
    let physical: Option<i64> = fixture.runtime().control("raw-alarm-time", &()).await?;
    assert_eq!(physical, Some(earlier.timestamp_millis()));
    link.close().await
}

async fn refused_diagnostic_journey(fixture: &Fixture) -> Result<()> {
    let (mut link, _, workspace, _, _) = baseline(fixture).await?;
    fixture
        .host
        .c2_hold_deciding_diagnostic(
            HeldDiagnostic::RestoreResult,
            1,
            DiagnosticDisposition::Refuse,
        )
        .await?;
    let single = link.call(create_area(&workspace)?).await?;
    let single_id = correlation(&single.request)?;
    created_area(&single.reply, super::AREA_NAME)?;
    fixture
        .host
        .c2_settle_refused_diagnostic(single_id, &[single_id])
        .await?;

    fixture
        .host
        .c2_hold_deciding_diagnostic(
            HeldDiagnostic::AlarmSnapshot,
            1,
            DiagnosticDisposition::Refuse,
        )
        .await?;
    // A genuine independent two-write batch. The recorder refuses only after
    // both replies reached the client, on the same socket as the single reply.
    let mut payloads = writes(&workspace)?;
    payloads.remove(1);
    let (batch, replies) = link.batch(payloads).await?;
    let [first, second] = replies.as_slice() else {
        return Err("two independent writes must return two Replies".into());
    };
    created_area(first, AREA_A)?;
    created_area(second, AREA_B)?;
    let [first, second] = batch_correlations(&batch)?[..] else {
        return Err("two independent writes must have two correlations".into());
    };
    fixture
        .host
        .c2_settle_refused_diagnostic(first, &[second])
        .await?;

    fixture
        .host
        .c2_hold_deciding_diagnostic(
            HeldDiagnostic::AlarmSnapshot,
            1,
            DiagnosticDisposition::Refuse,
        )
        .await?;
    let single = link
        .call(WorkspaceRequest {
            request_id: Some("019f0000-0000-7000-8000-00000000cf12".parse()?),
            workspace_id: Some(workspace),
            operation: WorkspaceOperation::CreateArea {
                name: "Area after recorder refusal".to_owned().try_into()?,
                description: "Test description".to_owned().try_into()?,
            },
        })
        .await?;
    created_area(&single.reply, "Area after recorder refusal")?;
    fixture
        .host
        .c2_settle_refused_diagnostic(correlation(&single.request)?, &[])
        .await?;
    link.close().await
}

fn batch_correlations(frame: &DataFrame) -> Result<Vec<&central_link_protocol::CorrelationId>> {
    let DataFrame::Batch { requests } = frame else {
        return Err("deciding frame must be the actual Batch".into());
    };
    Ok(requests
        .requests()
        .iter()
        .map(central_link_protocol::LinkRequest::correlation_id)
        .collect())
}

async fn run_with_held_callback(ordinal: usize, destination: Option<PathBuf>) -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = batch_journey(&fixture, ordinal).await;
    fixture.close().await?;
    let mut receipt = result?;
    receipt.disposed = true;
    if let Some(destination) = destination {
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)?;
        output.write_all(&serde_json::to_vec_pretty(&receipt)?)?;
    }
    Ok(())
}

async fn batch_journey(fixture: &Fixture, held_callback: usize) -> Result<BatchHibernationReceipt> {
    let (mut link, baseline, workspace, before_events, attachment) = baseline(fixture).await?;
    let payloads = writes(&workspace)?;
    fixture.host.c2_witness(WitnessPhase::Evicting).await?;
    fixture.host.c2_evict(&fixture.organization).await?;
    fixture.host.c2_witness(WitnessPhase::Heartbeat).await?;
    let mut heartbeat_events = Vec::new();
    for _ in 0..3 {
        link.socket.send(Message::Text("ping".into())).await?;
        match read_fixture_frame(&mut link.socket, "batch link frame").await? {
            Some(Ok(Message::Text(text))) if text == "pong" => {}
            _ => return Err("batch journey did not receive literal platform pong".into()),
        }
        let events = fixture.host.c2_witness(WitnessPhase::Heartbeat).await?;
        if events != before_events {
            return Err("batch heartbeat woke a constructor or handler".into());
        }
        heartbeat_events.push(events);
    }
    fixture
        .host
        .c2_hold_deciding_diagnostic(
            HeldDiagnostic::AlarmSnapshot,
            held_callback,
            DiagnosticDisposition::Record,
        )
        .await?;
    let (batch_request, batch_replies) = link.batch(payloads).await?;
    let correlations = batch_correlations(&batch_request)?;
    // The recorder holds its callback until this read, which happens only after
    // every reply. The settling read returns only when all three records arrive.
    let deciding_events = fixture
        .host
        .c2_settle_alarms(WitnessPhase::Deciding, &correlations)
        .await?;
    assert_member_witnesses(
        &before_events,
        &deciding_events,
        &attachment,
        &batch_request,
    )?;
    let [first, middle, last] = batch_replies.as_slice() else {
        return Err("one batch must return exactly three independent Replies".into());
    };
    let area_a = created_area(first, AREA_A)?;
    let DataFrame::Reply { result, .. } = middle else {
        return Err("middle batch result was not a Reply".into());
    };
    if result.as_ref()
        != &ReplyPayload::Mail(CentralMailResult::Refused(
            CentralMailRefusal::TimingViolation {
                field: "dueAt".into(),
                requirement: TimingRequirement::FutureTime,
            },
        ))
    {
        return Err(
            "well-formed middle wake must return its actual FutureTime semantic refusal".into(),
        );
    }
    let area_b = created_area(last, AREA_B)?;
    if area_a.area_id == area_b.area_id {
        return Err("independent successful writes reused an area ID".into());
    }
    let areas = vec![area_a, area_b];
    fixture.host.c2_witness(WitnessPhase::Settling).await?;
    let mut read_requests = Vec::new();
    let mut read_replies = Vec::new();
    for expected in &areas {
        let exchange = link
            .call(WorkspaceRequest {
                request_id: None,
                workspace_id: Some(workspace.clone()),
                operation: WorkspaceOperation::Show {
                    item: ItemRef::Area(expected.area_id.clone()),
                },
            })
            .await?;
        match &workspace_reply(&exchange.reply)?.result {
            WorkspaceResult::Done(WorkspaceAnswer::Item {
                item: ItemView::Area(area),
            }) if area.area_id == expected.area_id
                && area.workspace_id == workspace
                && area.name.as_str() == expected.name => {}
            _ => {
                return Err(
                    "batch Show did not return the independently created area and literal name"
                        .into(),
                );
            }
        }
        read_requests.push(exchange.request);
        read_replies.push(exchange.reply);
    }
    let store = fixture
        .runtime()
        .inspect_workspace(
            &workspace,
            Statement {
                sql: "SELECT area_id,name FROM workspace_areas ORDER BY name",
                args: vec![],
            },
        )
        .await?;
    let expected_rows = areas
        .iter()
        .map(|area| {
            vec![
                SqlValue::Text(area.area_id.to_string()),
                SqlValue::Text(area.name.into()),
            ]
        })
        .collect::<Vec<_>>();
    if store.columns != vec!["area_id", "name"] || store.rows != expected_rows {
        return Err(
            "batch owning store must contain exactly the two successful area IDs and names".into(),
        );
    }
    let wakes = fixture
        .runtime()
        .read(Statement {
            sql: "SELECT COUNT(*) AS wake_count FROM automation_wakes",
            args: vec![],
        })
        .await?;
    if wakes.columns != vec!["wake_count"]
        || wakes.rows != vec![vec![SqlValue::Integer(0.try_into()?)]]
    {
        return Err("semantically refused batch member wrote a wake row".into());
    }
    let past_wake_writes = fixture
        .runtime()
        .read(Statement {
            sql: "SELECT (SELECT COUNT(*) FROM engine_due_entries WHERE work_kind='wake_timer'),
                           (SELECT COUNT(*) FROM runtime_request_log WHERE request_id=?)",
            args: vec![SqlValue::Text(PAST_WAKE_REQUEST.into())],
        })
        .await?;
    if past_wake_writes.rows != vec![vec![SqlValue::Integer(0.try_into()?); 2]] {
        return Err("semantically refused wake wrote a due entry or retained request".into());
    }
    let welcome = link.welcome.clone();
    link.close().await?;
    Ok(BatchHibernationReceipt {
        kind: "c2BatchHibernation",
        organization: fixture.organization.clone(),
        machine_id: MACHINE_ID.parse()?,
        welcome,
        baseline_request: baseline.request,
        baseline_reply: baseline.reply,
        batch_request,
        batch_replies,
        read_requests,
        read_replies,
        before_events,
        heartbeat_events,
        deciding_events,
        areas,
        store,
        wakes,
        disposed: false,
    })
}

fn writes(workspace: &WorkspaceId) -> Result<Vec<RequestPayload>> {
    let Caller::Session(target) = session_caller()? else {
        return Err("batch wake target must be the ordinary caller's session".into());
    };
    let area = |request_id: &str, name: &str| -> Result<RequestPayload> {
        Ok(RequestPayload::Workspace(WorkspaceRequest {
            request_id: Some(request_id.parse()?),
            workspace_id: Some(workspace.clone()),
            operation: WorkspaceOperation::CreateArea {
                name: name.to_owned().try_into()?,
                description: "Test description".to_owned().try_into().unwrap(),
            },
        }))
    };
    Ok(vec![
        area("019f0000-0000-7000-8000-00000000cf40", AREA_A)?,
        RequestPayload::Mail(CentralMailRequest {
            request_id: PAST_WAKE_REQUEST.parse()?,
            operation: CentralMailOperation::SetWake {
                target,
                due_at: "2030-03-17T17:45:00Z".parse()?,
                text: "Past wake must refuse".to_owned().try_into()?,
            },
        }),
        area("019f0000-0000-7000-8000-00000000cf42", AREA_B)?,
    ])
}

fn created_area(frame: &DataFrame, name: &'static str) -> Result<AreaReceipt> {
    let WorkspaceResult::Done(WorkspaceAnswer::Created {
        item: ItemRef::Area(area_id),
        ..
    }) = &workspace_reply(frame)?.result
    else {
        return Err("successful batch member must return an actual created area".into());
    };
    Ok(AreaReceipt {
        area_id: area_id.clone(),
        name,
    })
}

fn assert_member_witnesses(
    before: &[WitnessRecord],
    after: &[WitnessRecord],
    attachment: &AttachmentSnapshot,
    frame: &DataFrame,
) -> Result<()> {
    let DataFrame::Batch { requests } = frame else {
        return Err("deciding frame must be the actual Batch".into());
    };
    let first = requests
        .requests()
        .first()
        .ok_or("batch has no first member")?;
    // This establishes one fresh constructor/handler and the original closed attachment.
    let (instance, _) = super::witness_assertions::reconstruction(
        before,
        after,
        attachment,
        first.correlation_id(),
        RestoreOutcome::Admitted,
    )?;
    let fresh = after
        .get(before.len()..)
        .ok_or("batch witness baseline changed")?;
    let handler = fresh
        .iter()
        .find(|record| matches!(record.event, WitnessEvent::Handler { .. }))
        .ok_or("batch handler witness missing")?;
    let mut floor = handler.sequence;
    for member in requests.requests() {
        let restored = fresh.iter().filter(|record| matches!(&record.event,WitnessEvent::RestoreResult { correlation_id, .. } if correlation_id == member.correlation_id())).collect::<Vec<_>>();
        let [restored] = restored.as_slice() else {
            return Err("each batch member needs exactly one matching restoration result".into());
        };
        if restored.instance_id != instance
            || restored.sequence <= floor
            || !matches!(
                restored.event,
                WitnessEvent::RestoreResult {
                    outcome: RestoreOutcome::Admitted,
                    ..
                }
            )
        {
            return Err(
                "each batch member must restore live authority on the new serving instance".into(),
            );
        }
        let required = [
            EnrollmentReadQuery::ByEnrollment,
            EnrollmentReadQuery::MachineById,
            EnrollmentReadQuery::ActiveEnrollment,
        ];
        let mut next = 0;
        for record in fresh {
            if record.instance_id == instance
                && record.sequence > floor
                && record.sequence < restored.sequence
                && let WitnessEvent::EnrollmentRead {
                    query,
                    outcome: EnrollmentReadOutcome::Read,
                    ..
                } = &record.event
                && required.get(next) == Some(query)
            {
                next += 1;
            }
        }
        if next != required.len() {
            return Err("batch member reused earlier live reads instead of reading after the previous terminal alarm snapshot".into());
        }
        super::witness_assertions::alarm_snapshot(
            after,
            member.correlation_id(),
            instance,
            restored.sequence,
        )?;
        let alarm = fresh.iter().find(|record| matches!(&record.event,WitnessEvent::AlarmSnapshot { correlation_id, .. } if correlation_id == member.correlation_id())).ok_or("batch terminal alarm snapshot missing")?;
        floor = alarm.sequence;
    }
    if fresh
        .iter()
        .filter(|record| matches!(record.event, WitnessEvent::RestoreResult { .. }))
        .count()
        != 3
        || fresh
            .iter()
            .filter(|record| matches!(record.event, WitnessEvent::AlarmSnapshot { .. }))
            .count()
            != 3
    {
        return Err(
            "batch must have exactly three member restoration results and terminal alarm snapshots"
                .into(),
        );
    }
    Ok(())
}
