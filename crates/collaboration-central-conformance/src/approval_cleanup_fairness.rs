//! Applying the existing lane to tied cleanup sources on both hosts.
use approval_request_protocol::{Raise, RegisterRunReply};
use central_link_protocol::approval_rpc::ApprovalHolderTarget;
use central_storage::{SafeInteger, SqlValue, Statement};
use reqwest_websocket::CloseCode;

use crate::{
    ConformanceResult as Result,
    approval_link_scenarios::{
        ApprovalLinkFixture, held_start, raise, register_run, require_receipt,
    },
};

const MARK_SECONDS: i64 = 300;

pub async fn tied_cleanup_work_yields_the_approval_lane(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let (target, waiting, deadline_mark) = prepare(fixture, 250, false).await?;
    let first = deadline_mark - 2 * MARK_SECONDS;
    apply_mark(fixture, first).await?;
    count(
        fixture,
        "SELECT count(*) FROM approval_erased_requests",
        250,
    )
    .await?;
    count(fixture, "SELECT count(*) FROM approval_router_runs WHERE enrollment_cleanup_ms IS NOT NULL AND enrollment_cleanup_due_ms IS NULL", 250).await?;
    apply_mark(fixture, first + MARK_SECONDS).await?;
    count(fixture, "SELECT count(*) FROM approval_erased_requests", 0).await?;
    count(
        fixture,
        "SELECT count(*) FROM approval_router_runs WHERE enrollment_cleanup_due_ms IS NOT NULL",
        250,
    )
    .await?;
    apply_mark(fixture, deadline_mark).await?;
    require_end(fixture, &waiting).await?;
    apply_mark(fixture, deadline_mark + MARK_SECONDS).await?;
    count(
        fixture,
        "SELECT count(*) FROM approval_router_runs WHERE enrollment_cleanup_ms IS NOT NULL",
        0,
    )
    .await?;
    count(fixture, "SELECT count(*) FROM engine_due_entries WHERE subject_key LIKE 'ended-enrollment:%' OR subject_key LIKE 'retired-run:%'", 0).await?;
    assert_eq!(fixture.target()?.router_run, target.router_run);
    Ok(())
}

pub async fn corrupt_cleanup_is_charged_without_stopping_healthy_work(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let (_, waiting, mark) = prepare(fixture, 1, true).await?;
    apply_mark(fixture, mark).await?;
    require_end(fixture, &waiting).await?;
    count(fixture, "SELECT count(*) FROM engine_work_attempts WHERE machine_kind='approval_request' AND subject_key LIKE 'ended-enrollment:%'", 1).await?;
    count(fixture, "SELECT count(*) FROM approval_erased_requests", 0).await?;
    Ok(())
}

async fn prepare(
    fixture: &impl ApprovalLinkFixture,
    runs: u16,
    corrupt: bool,
) -> Result<(ApprovalHolderTarget, Raise, i64)> {
    let target = fixture.target()?;
    let deadline = fixture.now_seconds() + 3_600;
    let waiting_mark = deadline + (-deadline).rem_euclid(MARK_SECONDS);
    let cleanup_mark = if corrupt {
        waiting_mark
    } else {
        waiting_mark - 2 * MARK_SECONDS
    };
    for ordinal in 0..runs {
        seed_cleanup(fixture, &target, ordinal, cleanup_mark, corrupt).await?;
    }
    let mut socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut socket, &target.organization_id, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let waiting = held_start(&target.router_run, 800)?;
    require_receipt(raise(&mut socket, &target.organization_id, waiting.clone()).await?)?;
    socket.close(CloseCode::Normal, None).await?;
    Ok((target, waiting, waiting_mark))
}

async fn seed_cleanup(
    fixture: &impl ApprovalLinkFixture,
    target: &ApprovalHolderTarget,
    ordinal: u16,
    mark: i64,
    corrupt: bool,
) -> Result<()> {
    let identity =
        |space, ordinal: u16| format!("019f0000-0000-7000-8000-0000{space:04x}{ordinal:04x}");
    let enrollment = identity(0xd100, ordinal);
    let run = identity(0xd200, ordinal);
    let retired_at = chrono::DateTime::from_timestamp(mark, 0)
        .ok_or("fixture cleanup time")?
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let at = SqlValue::Integer(SafeInteger::new(mark * 1000)?);
    let text = |value: &str| SqlValue::Text(value.to_owned());
    fixture.fixture_write(Statement {
        sql: "INSERT INTO login_machine_approvals(approval_id,request_id,machine_id,label,allowed_workspaces,state,end_reason,approved_by_person_id,permission_checked,base_generation,code_expires_at,machine_client_id,ended_at,created_at,updated_at)
              SELECT ?1,?2,machine_id,label,allowed_workspaces,'ended','owner',approved_by_person_id,permission_checked,base_generation,code_expires_at,?1,?3,created_at,?3
              FROM login_machine_approvals WHERE approval_id=?4",
        args: vec![text(&enrollment), text(&identity(0xd300, ordinal)), text(&retired_at), text(target.enrollment_id.as_str())],
    }).await?;
    fixture.fixture_write(Statement {
        sql: "INSERT INTO approval_router_runs(router_run_id,organization_id,machine_id,enrollment_id,registration_order,
          registered_at,retired_at,created_at,updated_at,enrollment_cleanup_ms,enrollment_cleanup_due_ms,marker_cleanup_due_ms)
          VALUES (?1,?2,?3,?4,?5,?6,?6,?6,?6,?7,?7,?7)",
        args: vec![text(&run),text(target.organization_id.as_str()),text(target.machine_id.as_str()),text(&enrollment),
            SqlValue::Integer(SafeInteger::new(1)?),text(&retired_at),at.clone()],
    }).await?;
    fixture.fixture_write(Statement {
        sql: "INSERT INTO approval_erased_requests(approval_row_id,organization_id,request_id,router_run_id,ending_json,created_at,updated_at)
              VALUES (?1,?2,?3,?4,?5,?6,?6)",
        args: vec![text(&identity(0xd400,ordinal)),text(target.organization_id.as_str()),
            text(&identity(0xd500,ordinal)),text(&run),text(r#"{"kind":"timedOut"}"#),text(&retired_at)],
    }).await?;
    for (space, prefix) in [(0xd600, "ended-enrollment:"), (0xd700, "retired-run:")] {
        fixture.fixture_write(Statement {
            sql: "INSERT INTO engine_due_entries(due_entry_id,work_kind,entry_role,subject_key,occurrence_ms,base_due_ms,effective_due_ms,created_at,updated_at)
                 VALUES (?1,'approval_request','item',?2,?3,?3,?3,?4,?4)",
            args: vec![text(&identity(space,ordinal)),text(&format!("{prefix}{}", if corrupt && prefix == "ended-enrollment:" { "bad-key" } else { &run })),at.clone(),text(&retired_at)],
        }).await?;
    }
    Ok(())
}

async fn apply_mark(fixture: &impl ApprovalLinkFixture, mark: i64) -> Result<()> {
    fixture
        .advance(u64::try_from(mark - fixture.now_seconds())?)
        .await?;
    fixture.observe_due_alarm().await
}

async fn require_end(fixture: &impl ApprovalLinkFixture, request: &Raise) -> Result<()> {
    let rows = fixture
        .read(Statement {
            sql: "SELECT record_state FROM approval_requests WHERE request_id=?",
            args: vec![SqlValue::Text(request.request_id().to_string())],
        })
        .await?
        .rows;
    assert_eq!(
        rows,
        [vec![SqlValue::Text("ended".to_owned())]],
        "later Waiting deadline must apply on its grid mark"
    );
    Ok(())
}

async fn count(fixture: &impl ApprovalLinkFixture, sql: &'static str, expected: i64) -> Result<()> {
    let rows = fixture.read(Statement { sql, args: vec![] }).await?.rows;
    assert_eq!(
        rows,
        [vec![SqlValue::Integer(SafeInteger::new(expected)?)]],
        "{sql}"
    );
    Ok(())
}
