//! Physical carrier, enrollment and failure-boundary approval journeys.
use crate::{
    ConformanceResult as Result,
    approval_link_scenarios::{
        ApprovalLinkFixture, EnrollmentLoginEnd, central_call, heartbeat, held_start, next_ending,
        raise, read_request, register_run, registered_waiting_request, renew, require_receipt,
        snapshot, stop, stop_request,
    },
};
use approval_request_protocol::{
    DeliverEndingRequest, RaiseReply, RegisterRunReply, RouterRunId, RunRefusal, StopReply,
};
use central_link_protocol::TypedCallResult;
use central_storage::{SafeInteger, SqlValue, Statement};

const OTHER_RUN: &str = "019f0000-0000-7000-8000-000000005411";

pub async fn approval_carrier_binding(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let (target, mut original, waiting) = registered_waiting_request(fixture).await?;
    let other: RouterRunId = OTHER_RUN.parse()?;
    let organization = &target.organization_id;
    let before = snapshot(fixture).await?;
    assert_eq!(
        register_run(&mut original, organization, &other).await?,
        RegisterRunReply::RunNotRegistered {}
    );
    assert_eq!(
        register_run(&mut original, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    assert_eq!(
        raise(&mut original, organization, held_start(&other, 8)?).await?,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunNotRegistered {}
        }
    );
    assert_eq!(snapshot(fixture).await?, before);

    let mut replacement = fixture.connect().await?;
    assert_eq!(
        raise(
            &mut replacement,
            organization,
            held_start(&target.router_run, 9)?
        )
        .await?,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunNotRegistered {}
        }
    );
    assert_eq!(
        stop(&mut replacement, organization, stop_request(&waiting)?).await?,
        TypedCallResult::Answered {
            reply: StopReply::AuthorityRefused {
                refusal: RunRefusal::RunNotRegistered {}
            }
        }
    );
    assert_eq!(snapshot(fixture).await?, before);
    assert_eq!(
        register_run(&mut replacement, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    require_receipt(raise(&mut replacement, organization, waiting).await?)?;
    assert_eq!(snapshot(fixture).await?, before);
    Ok(())
}

pub async fn enrollment_end_settles_approval_run(
    fixture: &impl ApprovalLinkFixture,
    event: EnrollmentLoginEnd,
    overdue: bool,
) -> Result<()> {
    let (target, mut socket, request) = registered_waiting_request(fixture).await?;
    use crate::approval_provider_scenarios::{ProviderAsk, create_bound, provider_raise};
    let (session, _) = create_bound(&mut socket, &target.organization_id, 1, None).await?;
    let decided = provider_raise(&target, &session, 3, ProviderAsk::Approval)?;
    require_receipt(raise(&mut socket, &target.organization_id, decided.clone()).await?)?;
    fixture
        .decide_provider_approval(decided.request_id())
        .await?;
    let (_, offered) = next_ending(&mut socket, &target).await?;
    assert_eq!(offered.request_id(), decided.request_id());
    let stored_ending = fixture
        .read(Statement {
            sql: "SELECT ending_json FROM approval_requests WHERE request_id=?",
            args: vec![text(decided.request_id().as_str())],
        })
        .await?;
    if overdue {
        fixture.advance(3601).await?;
    }
    fixture.end_enrollment_through_login(event).await?;
    let preserved = fixture
        .read(Statement {
            sql: "SELECT ending_json FROM approval_requests WHERE request_id=?",
            args: vec![text(decided.request_id().as_str())],
        })
        .await?;
    assert_eq!(preserved.rows, stored_ending.rows);
    assert_eq!(
        read_request(fixture, &decided).await?,
        [text("ended"), text("closed_without_receipt")]
    );
    let runs = fixture
        .read(Statement {
            sql: "SELECT retired_at FROM approval_router_runs WHERE router_run_id=?",
            args: vec![text(target.router_run.as_str())],
        })
        .await?;
    assert!(matches!(runs.rows.as_slice(), [row] if matches!(row.as_slice(), [SqlValue::Text(_)])));
    let ended = fixture
        .read(Statement {
            sql: "SELECT delivery_state,ending_json FROM approval_requests WHERE request_id=?",
            args: vec![text(request.request_id().as_str())],
        })
        .await?;
    let [row] = ended.rows.as_slice() else {
        return Err("missing ended row".into());
    };
    let [SqlValue::Text(delivery), SqlValue::Text(ending)] = row.as_slice() else {
        return Err("enrollment end did not record an ending".into());
    };
    assert_eq!(delivery, "closed_without_receipt");
    let ending: DeliverEndingRequest = serde_json::from_str(ending)?;
    let expected = if overdue {
        approval_request_protocol::Ending::TimedOut {}
    } else {
        approval_request_protocol::Ending::Cancelled {
            reason: approval_request_protocol::CancelReason::EnrollmentEnded {},
        }
    };
    assert_eq!(ending.into_ending(), expected);
    Ok(())
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

pub async fn durable_floor_wins_after_clock_rollback(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let (target, mut socket, waiting) = registered_waiting_request(fixture).await?;
    let start = fixture.now_seconds();
    for _ in 0..7 {
        fixture.advance(500).await?;
        renew(&mut socket, fixture.refresh().await?).await?;
    }
    fixture.advance(101).await?;
    let later = held_start(&target.router_run, 2)?;
    require_receipt(raise(&mut socket, &target.organization_id, later).await?)?;
    assert_eq!(
        read_request(fixture, &waiting).await?,
        [text("waiting"), SqlValue::Null]
    );
    let stored = fixture
        .read(Statement {
            sql: "SELECT observed_ms FROM approval_applying_time WHERE organization_id=?",
            args: vec![text(target.organization_id.as_str())],
        })
        .await?;
    assert_eq!(
        stored.rows,
        [vec![SqlValue::Integer(SafeInteger::try_from(
            (start + 3601) * 1000
        )?)]]
    );
    fixture.set_wall_seconds(start + 3599).await?;
    fixture.reopen_after_clock_rollback().await?;
    let answer = stop(
        &mut socket,
        &target.organization_id,
        stop_request(&waiting)?,
    )
    .await?;
    assert_eq!(
        answer,
        TypedCallResult::Answered {
            reply: StopReply::Stands {
                ending: approval_request_protocol::StopReceiptEnding::TimedOut {}
            },
        }
    );
    assert_eq!(
        read_request(fixture, &waiting).await?,
        [text("ended"), text("closed_without_receipt")]
    );
    Ok(())
}

pub async fn corrupt_approval_charges_only_its_own_attempt(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let (target, mut socket, corrupt) = registered_waiting_request(fixture).await?;
    let mut healthy = Vec::new();
    for ordinal in 1..12 {
        let request = held_start(&target.router_run, ordinal)?;
        let receipt =
            require_receipt(raise(&mut socket, &target.organization_id, request.clone()).await?)?;
        healthy.push((request, receipt));
    }
    fixture
        .fixture_write(Statement {
            sql: "UPDATE approval_requests SET offer_json='{}' WHERE request_id=?",
            args: vec![text(corrupt.request_id().as_str())],
        })
        .await?;
    socket
        .close(reqwest_websocket::CloseCode::Normal, None)
        .await?;
    let (_, first_receipt) = healthy.first().ok_or("missing healthy approval fixture")?;
    let deadline = first_receipt.window.deadline().timestamp();
    let mark = deadline + (-deadline).rem_euclid(300);
    fixture
        .advance(u64::try_from(mark - fixture.now_seconds())?)
        .await?;
    fixture.observe_due_alarm().await?;
    for (request, _) in &healthy {
        assert_eq!(
            read_request(fixture, request).await?,
            [text("ended"), text("owed")]
        );
    }
    assert_eq!(
        read_request(fixture, &corrupt).await?,
        [text("waiting"), SqlValue::Null]
    );
    let failures = fixture.read(Statement {
        sql: "SELECT r.request_id,a.failure_count FROM engine_work_attempts a JOIN approval_requests r ON r.approval_row_id=a.subject_key WHERE a.machine_kind='approval_request' ORDER BY r.request_id",
        args: vec![],
    }).await?;
    assert_eq!(
        failures.rows,
        [vec![
            text(corrupt.request_id().as_str()),
            SqlValue::Integer(SafeInteger::try_from(1_i64)?)
        ]]
    );
    Ok(())
}

use approval_request_protocol::RegisterRunRequest;
use central_link_protocol::{LinkCallRefusal, MachineLinkCloseCode, approval_rpc::RegisterRunRpc};
use futures_util::StreamExt;
use reqwest_websocket::{CloseCode, Message, WebSocket};
use std::future::Future;
pub async fn approval_authority_refusals_leave_store_unchanged(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let mut socket = fixture.connect().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    // Replacing the socket and registering a distinct run retires the first.
    socket.close(CloseCode::Normal, None).await?;
    let mut socket = fixture.connect().await?;
    let replacement: RouterRunId = OTHER_RUN.parse()?;
    assert_eq!(
        register_run(&mut socket, organization, &replacement).await?,
        RegisterRunReply::Registered {}
    );
    socket.close(CloseCode::Normal, None).await?;
    let mut socket = fixture.connect().await?;
    let before = snapshot(fixture).await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::RunRetired {}
    );
    assert_eq!(
        raise(
            &mut socket,
            organization,
            held_start(&target.router_run, 0)?
        )
        .await?,
        RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunNotRegistered {}
        }
    );
    assert_eq!(snapshot(fixture).await?, before);

    assert_eq!(
        register_run(&mut socket, organization, &replacement).await?,
        RegisterRunReply::Registered {}
    );
    // End the stored enrollment while retaining the old socket to observe its refusal.
    fixture.end_enrollment().await?;
    let before = snapshot(fixture).await?;
    let result = central_call::<RegisterRunRpc>(
        &mut socket,
        organization,
        RegisterRunRequest {
            router_run: replacement,
        },
    )
    .await?;
    assert_eq!(
        result,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::AuthorityLapsed {
                cause: MachineLinkCloseCode::Revoked
            }
        }
    );
    assert!(
        matches!(socket.next().await, Some(Ok(Message::Close { code, .. })) if u16::from(code) == MachineLinkCloseCode::Revoked.code())
    );
    assert_eq!(snapshot(fixture).await?, before);
    Ok(())
}

pub async fn displaced_carrier_writes_nothing(
    fixture: &impl ApprovalLinkFixture,
    arm: impl Future<Output = Result<()>>,
    replace_held_carrier: impl Future<Output = Result<WebSocket>>,
) -> Result<()> {
    let (target, mut original, _) = registered_waiting_request(fixture).await?;
    let before = snapshot(fixture).await?;
    let new_request = held_start(&target.router_run, 12)?;
    arm.await?;
    let (old_reply, replacement) = futures_util::join!(
        raise(&mut original, &target.organization_id, new_request),
        replace_held_carrier,
    );
    let mut replacement = replacement?;
    if old_reply.is_ok() {
        return Err(format!("the displaced socket got an approval reply: {old_reply:?}").into());
    }
    heartbeat(&mut replacement).await?;
    assert_eq!(snapshot(fixture).await?, before);
    Ok(())
}
