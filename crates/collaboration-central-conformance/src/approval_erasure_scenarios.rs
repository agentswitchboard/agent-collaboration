//! Exercise the real holder and engine entry points on both hosts.
use approval_request_protocol::{
    ErasedApprovalMarker, Raise, RaiseReply, RegisterRunReply, RouterRunId, RunRefusal,
    StopReceiptEnding, StopReply, StopRequest,
};
use central_link_protocol::TypedCallResult;
use central_storage::{SafeInteger, SqlValue, Statement};
use reqwest_websocket::{CloseCode, WebSocket};
use std::future::Future;

use crate::{
    ConformanceResult as Result,
    approval_link_scenarios::{
        ApprovalLinkFixture, held_start, raise, register_run, require_receipt, stop, stop_request,
    },
};

const RETENTION_SECONDS: i64 = 5_184_000;
const FIVE_MINUTES: i64 = 300;
const REPLACEMENT_RUN: &str = "019f0000-0000-7000-8000-00000000e601";

pub trait MissedApprovalCleanupFixture: ApprovalLinkFixture {
    fn miss_cleanup_at(&self, mark: i64) -> impl Future<Output = Result<()>>;
    fn prepare_catch_up(
        &self,
        socket: WebSocket,
        mark: i64,
    ) -> impl Future<Output = Result<WebSocket>>;
}

pub async fn missed_cleanup_is_recovered_by_the_next_mark(
    fixture: &impl MissedApprovalCleanupFixture,
) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let mut socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let request = held_start(&target.router_run, 10)?;
    let ending = raise_and_stop(&mut socket, organization, &request).await?;
    let marker = RaiseReply::AlreadyEnded {
        marker: ErasedApprovalMarker {
            request_id: request.request_id().clone(),
            router_run: target.router_run.clone(),
            ending,
        },
    };
    let cutoff = fixture.now_seconds() + RETENTION_SECONDS;
    let first_mark = cutoff + FIVE_MINUTES - cutoff.rem_euclid(FIVE_MINUTES);
    assert_ne!(cutoff.rem_euclid(FIVE_MINUTES), 0);
    socket.close(CloseCode::Normal, None).await?;

    fixture.set_wall_seconds(cutoff - 1).await?;
    let mut socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    require_receipt(raise(&mut socket, organization, request.clone()).await?)?;
    fixture.miss_cleanup_at(first_mark).await?;
    assert_eq!(fixture.now_seconds(), first_mark + 1);
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 1).await?;
    assert_count(
        fixture,
        "SELECT count(*) FROM approval_request_destinations",
        1,
    )
    .await?;
    assert_count(fixture, "SELECT count(*) FROM approval_erased_requests", 0).await?;
    assert_eq!(
        raise(&mut socket, organization, request.clone()).await?,
        marker
    );
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 1).await?;
    assert_count(fixture, "SELECT count(*) FROM approval_erased_requests", 0).await?;
    let mut socket = fixture
        .prepare_catch_up(socket, first_mark + FIVE_MINUTES)
        .await?;
    advance_to_next_grid(fixture).await?;
    assert_eq!(fixture.now_seconds(), first_mark + FIVE_MINUTES);
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 0).await?;
    assert_count(
        fixture,
        "SELECT count(*) FROM approval_request_destinations",
        0,
    )
    .await?;
    assert_count(fixture, "SELECT count(*) FROM approval_erased_requests", 1).await?;
    assert_count(
        fixture,
        "SELECT count(*) FROM engine_due_entries WHERE work_kind='approval_request'",
        0,
    )
    .await?;
    assert_eq!(raise(&mut socket, organization, request).await?, marker);
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 0).await?;
    assert_count(
        fixture,
        "SELECT count(*) FROM approval_request_destinations",
        0,
    )
    .await?;
    assert_count(fixture, "SELECT count(*) FROM approval_erased_requests", 1).await?;
    assert_count(
        fixture,
        "SELECT count(*) FROM engine_due_entries WHERE work_kind='approval_request'",
        0,
    )
    .await?;
    socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

pub async fn cutoff_and_correlated_marker(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let mut socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let request = held_start(&target.router_run, 10)?;
    let standing = raise_and_stop(&mut socket, organization, &request).await?;
    let matching_marker = RaiseReply::AlreadyEnded {
        marker: ErasedApprovalMarker {
            request_id: request.request_id().clone(),
            router_run: target.router_run.clone(),
            ending: standing,
        },
    };
    let cutoff = fixture.now_seconds() + RETENTION_SECONDS;
    assert_ne!(
        cutoff.rem_euclid(FIVE_MINUTES),
        0,
        "fixture separates cutoff from grid"
    );
    assert_count(
        fixture,
        "SELECT count(*) FROM _sqlx_migrations WHERE version=15 AND success=1",
        1,
    )
    .await?;
    socket.close(CloseCode::Normal, None).await?;

    fixture.set_wall_seconds(cutoff - 1).await?;
    socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    require_receipt(raise(&mut socket, organization, request.clone()).await?)?;
    let wrong_digest = mismatched_stop(&request)?;
    assert_eq!(
        stop(&mut socket, organization, wrong_digest.clone()).await?,
        answer(StopReply::NotVisible {}),
        "the existing digest check still applies before cutoff",
    );
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 1).await?;
    assert_count(fixture, "SELECT count(*) FROM approval_erased_requests", 0).await?;

    fixture.advance(1).await?;
    assert_eq!(
        raise(&mut socket, organization, request.clone()).await?,
        matching_marker
    );
    assert_eq!(
        stop(&mut socket, organization, wrong_digest.clone()).await?,
        answer(StopReply::Stands { ending: standing }),
    );
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 1).await?;
    assert_count(fixture, "SELECT count(*) FROM approval_erased_requests", 0).await?;

    advance_to_next_grid(fixture).await?;
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 0).await?;
    assert_count(
        fixture,
        "SELECT count(*) FROM approval_request_destinations",
        0,
    )
    .await?;
    assert_count(fixture, "SELECT count(*) FROM approval_erased_requests", 1).await?;
    assert_count(
        fixture,
        "SELECT count(*) FROM engine_due_entries WHERE work_kind='approval_request'",
        0,
    )
    .await?;
    assert_eq!(
        raise(&mut socket, organization, request.clone()).await?,
        matching_marker
    );
    assert_eq!(
        stop(&mut socket, organization, wrong_digest).await?,
        answer(StopReply::Stands { ending: standing }),
    );
    require_receipt(
        raise(
            &mut socket,
            organization,
            held_start(&target.router_run, 11)?,
        )
        .await?,
    )?;

    socket.close(CloseCode::Normal, None).await?;
    let replacement: RouterRunId = REPLACEMENT_RUN.parse()?;
    socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut socket, organization, &replacement).await?,
        RegisterRunReply::Registered {}
    );
    assert_eq!(
        raise(&mut socket, organization, request.clone()).await?,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunNotRegistered {}
        },
    );
    socket.close(CloseCode::Normal, None).await?;
    socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::RunRetired {},
    );
    assert_eq!(
        register_run(&mut socket, organization, &replacement).await?,
        RegisterRunReply::Registered {},
    );
    // The retired marker must have the same effect as an absent record for a new run.
    require_receipt(raise(&mut socket, organization, held_start(&replacement, 10)?).await?)?;
    socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

pub async fn erase_batches_isolate_corruption_and_retain_only_live_markers(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let mut socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    for ordinal in 1..=251 {
        raise_and_stop(
            &mut socket,
            organization,
            &held_start(&target.router_run, ordinal)?,
        )
        .await?;
    }
    let cutoff = fixture.now_seconds() + RETENTION_SECONDS;
    let corrupt_id = held_start(&target.router_run, 1)?.request_id().clone();
    fixture
        .fixture_write(Statement {
            sql: "UPDATE approval_requests SET ending_json='broken ending' WHERE request_id=?",
            args: vec![SqlValue::Text(corrupt_id.to_string())],
        })
        .await?;
    socket.close(CloseCode::Normal, None).await?;
    fixture.set_wall_seconds(cutoff - 1).await?;
    socket = fixture.connect_without_capabilities().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 251).await?;
    let first_mark = cutoff + (-cutoff).rem_euclid(FIVE_MINUTES);
    fixture
        .advance(u64::try_from(first_mark - fixture.now_seconds())?)
        .await?;
    fixture.observe_due_alarm().await?;
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 2).await?;
    assert_count(
        fixture,
        "SELECT count(*) FROM approval_erased_requests",
        249,
    )
    .await?;

    advance_to_next_grid(fixture).await?;
    assert_count(fixture, "SELECT count(*) FROM approval_requests", 1).await?;
    assert_count(
        fixture,
        "SELECT count(*) FROM approval_erased_requests",
        250,
    )
    .await?;
    let unresolved = fixture
        .read(Statement {
            sql: "SELECT request_id FROM approval_requests",
            args: vec![],
        })
        .await?;
    assert_eq!(
        unresolved.rows,
        [vec![SqlValue::Text(corrupt_id.to_string())]]
    );
    assert_count(
        fixture,
        "SELECT count(*) FROM engine_work_attempts WHERE machine_kind='approval_request'",
        1,
    )
    .await?;

    socket.close(CloseCode::Normal, None).await?;
    socket = fixture.connect_without_capabilities().await?;
    let replacement: RouterRunId = REPLACEMENT_RUN.parse()?;
    assert_eq!(
        register_run(&mut socket, organization, &replacement).await?,
        RegisterRunReply::Registered {}
    );
    assert_count(
        fixture,
        "SELECT count(*) FROM approval_erased_requests",
        250,
    )
    .await?;
    assert_eq!(
        raise(
            &mut socket,
            organization,
            held_start(&target.router_run, 2)?
        )
        .await?,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunNotRegistered {}
        },
    );
    for remaining in [186, 122, 58, 0] {
        advance_to_next_grid(fixture).await?;
        assert_count(
            fixture,
            "SELECT count(*) FROM approval_erased_requests",
            remaining,
        )
        .await?;
        assert_count(
            fixture,
            "SELECT count(*) FROM engine_due_entries WHERE work_kind='approval_request' AND subject_key LIKE 'retired-run:%'",
            i64::from(remaining != 0),
        ).await?;
    }
    socket.close(CloseCode::Normal, None).await?;
    assert_count(fixture, "SELECT count(*) FROM approval_router_runs", 2).await?;
    fixture
        .end_enrollment_through_login(crate::approval_link_scenarios::EnrollmentLoginEnd::Revoke)
        .await?;
    advance_to_next_grid(fixture).await?;
    assert_count(fixture, "SELECT count(*) FROM approval_router_runs", 0).await?;
    Ok(())
}

async fn raise_and_stop(
    socket: &mut reqwest_websocket::WebSocket,
    organization: &fleet_identity_protocol::OrganizationId,
    request: &Raise,
) -> Result<StopReceiptEnding> {
    require_receipt(raise(socket, organization, request.clone()).await?)?;
    let reply = stop(socket, organization, stop_request(request)?).await?;
    let TypedCallResult::Answered {
        reply: StopReply::Stands { ending },
    } = reply
    else {
        return Err(format!("Stop did not settle the fixture: {reply:?}").into());
    };
    Ok(ending)
}

fn mismatched_stop(request: &Raise) -> Result<StopRequest> {
    let StopRequest::UnprovenCall {
        request_id,
        router_run,
        cause,
        ..
    } = stop_request(request)?
    else {
        return Err("retention fixture needs a parked call".into());
    };
    Ok(StopRequest::UnprovenCall {
        request_id,
        router_run,
        contents_digest: approval_request_protocol::sha256_url(b"contents for another request")
            .try_into()?,
        cause,
    })
}

fn answer(reply: StopReply) -> TypedCallResult<StopReply> {
    TypedCallResult::Answered { reply }
}

async fn advance_to_next_grid(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let seconds = FIVE_MINUTES - fixture.now_seconds().rem_euclid(FIVE_MINUTES);
    fixture.advance(u64::try_from(seconds)?).await?;
    fixture.observe_due_alarm().await
}

async fn assert_count(
    fixture: &impl ApprovalLinkFixture,
    sql: &'static str,
    count: i64,
) -> Result<()> {
    let rows = fixture
        .read(Statement {
            sql,
            args: Vec::new(),
        })
        .await?
        .rows;
    assert_eq!(
        rows,
        [vec![SqlValue::Integer(SafeInteger::try_from(count)?)]],
        "{sql}"
    );
    Ok(())
}
