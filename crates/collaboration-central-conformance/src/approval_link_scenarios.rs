//! Socket oracle shared by the two production-composition integration suites.
use crate::ConformanceResult as Result;
use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, Raise, RaiseReceipt, RaiseReply, RegisterRunReply,
    RegisterRunRequest, RouterRunId, StopReceiptEnding, StopReply, StopRequest,
};
use central_link_protocol::{
    CentralLinkRpc, CorrelationId, DataFrame, LinkCallRefusal, LinkFrame, MacCall, MacLinkRpc,
    TokenRenewalResult, TypedCallResult,
    approval_rpc::{
        ApprovalHolderTarget, ApprovalServiceTarget, DeliverEndingRpc, RaiseApprovalRpc,
        RegisterRunRpc, StopApprovalRpc,
    },
};
use central_storage::{ReadOutcome, SqlValue, Statement};
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::Envelope;
use reqwest_websocket::{CloseCode, WebSocket};
use serde_json::json;
use std::future::Future;

pub const ROUTER_RUN: &str = "019f0000-0000-7000-8000-000000005410";
pub const FIXTURE_DECIDER: &str = "019f0000-0000-7000-8000-000000005413";
pub const FIXTURE_DECISION_OPERATION: &str = "019f0000-0000-7000-8000-000000005414";
const CORRELATION: &str = "019f0000-0000-7000-8000-000000005412";

#[derive(Clone, Copy)]
pub enum EnrollmentLoginEnd {
    Revoke,
    Supersede,
    Idle,
}

pub trait ApprovalLinkFixture {
    fn target(&self) -> Result<ApprovalHolderTarget>;
    fn now_seconds(&self) -> i64;
    fn reopen_after_clock_rollback(&self) -> impl Future<Output = Result<()>> {
        async { Ok(()) }
    }
    fn set_wall_seconds(&self, seconds: i64) -> impl Future<Output = Result<()>>;
    fn set_wall_millis(&self, millis: i64) -> impl Future<Output = Result<()>>;
    fn connect_without_capabilities(&self) -> impl Future<Output = Result<WebSocket>>;
    fn connect(&self) -> impl Future<Output = Result<WebSocket>> {
        async {
            let mut socket = self.connect_without_capabilities().await?;
            announce_mac_calls(
                &mut socket,
                vec![
                    central_link_protocol::MacCallKind::DeliverEnding,
                    central_link_protocol::MacCallKind::ApprovalNotice,
                ],
            )
            .await?;
            Ok(socket)
        }
    }
    fn read(&self, statement: Statement) -> impl Future<Output = Result<ReadOutcome>>;
    fn advance(&self, seconds: u64) -> impl Future<Output = Result<()>>;
    fn refresh(&self) -> impl Future<Output = Result<String>>;
    fn observe_due_alarm(&self) -> impl Future<Output = Result<()>>;
    fn fail_next_applying_write(&self) -> impl Future<Output = Result<()>>;
    fn arm_notice_recovery(&self) -> impl Future<Output = Result<()>>;
    fn await_notice_recovery(&self) -> impl Future<Output = Result<()>>;
    fn fixture_write(&self, statement: Statement) -> impl Future<Output = Result<()>>;
    fn decide_provider_approval(
        &self,
        request_id: &workspace_ids::ApprovalRequestId,
    ) -> impl Future<Output = Result<()>>;
    fn hold_delivery_before_write(&self) -> impl Future<Output = Result<()>>;
    fn wait_delivery_before_write(&self) -> impl Future<Output = Result<()>>;
    fn release_delivery_before_write(&self) -> impl Future<Output = Result<()>>;
    fn end_enrollment(&self) -> impl Future<Output = Result<()>>;
    fn end_enrollment_through_login(
        &self,
        event: EnrollmentLoginEnd,
    ) -> impl Future<Output = Result<()>>;
    fn hold_delivery_answer(&self) -> impl Future<Output = Result<()>>;
    fn observe_delivery_answer(&self) -> impl Future<Output = Result<()>>;
}

pub async fn registered_stop_and_replay(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let target = fixture.target()?;
    let mut socket = fixture.connect().await?;
    let run = &target.router_run;
    assert_eq!(
        register_run(&mut socket, &target.organization_id, run).await?,
        RegisterRunReply::Registered {}
    );
    let request = held_start(run, 0)?;
    let received =
        require_receipt(raise(&mut socket, &target.organization_id, request.clone()).await?)?;
    let repeated =
        require_receipt(raise(&mut socket, &target.organization_id, request.clone()).await?)?;
    assert_eq!(received, repeated);
    assert_eq!(
        read_request(fixture, &request).await?,
        [text("waiting"), SqlValue::Null]
    );

    let stopped = stop(
        &mut socket,
        &target.organization_id,
        stop_request(&request)?,
    )
    .await?;
    assert!(matches!(
        stopped,
        TypedCallResult::Answered {
            reply: StopReply::Stands {
                ending: StopReceiptEnding::Cancelled { .. }
            }
        }
    ));
    assert_eq!(
        read_request(fixture, &request).await?,
        [text("ended"), text("closed_without_receipt")]
    );
    // The next frame must answer Ping. A Stop must never offer its own ending.
    heartbeat(&mut socket).await?;
    socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

#[derive(Clone, Copy)]
pub enum DeadlineDelivery {
    Connected,
    RecoverAfterReconnect,
    StopOwedAtCapacity,
}

pub async fn approval_deadline_delivery(
    fixture: &impl ApprovalLinkFixture,
    scenario: DeadlineDelivery,
) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let capacity = matches!(scenario, DeadlineDelivery::StopOwedAtCapacity);
    if capacity {
        let remaining = u64::try_from((-fixture.now_seconds()).rem_euclid(300))?;
        if remaining != 0 {
            fixture.advance(remaining).await?;
        }
    }
    let mut socket = fixture.connect().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let first = held_start(&target.router_run, 0)?;
    let receipt = require_receipt(raise(&mut socket, organization, first.clone()).await?)?;
    if capacity {
        fixture.fail_next_applying_write().await?;
        assert_eq!(
            stop(&mut socket, organization, stop_request(&first)?).await?,
            TypedCallResult::Refused {
                reason: LinkCallRefusal::CentralFault
            }
        );
        assert_eq!(
            read_request(fixture, &first).await?,
            [text("waiting"), SqlValue::Null]
        );
    }
    let deadline = receipt.window.deadline().timestamp();
    let mark = deadline + (-deadline).rem_euclid(300);
    if matches!(scenario, DeadlineDelivery::RecoverAfterReconnect) {
        socket.close(CloseCode::Normal, None).await?;
        fixture
            .advance(u64::try_from(mark - fixture.now_seconds())?)
            .await?;
        fixture.observe_due_alarm().await?;
        assert_eq!(
            read_request(fixture, &first).await?,
            [text("ended"), text("owed")]
        );
        socket = fixture.connect().await?;
        assert_eq!(
            register_run(&mut socket, organization, &target.router_run).await?,
            RegisterRunReply::Registered {}
        );
    } else {
        // Renew across the hour using real signed grants. Raise the later requests
        // one second later so only the first is due at this boundary.
        fixture.advance(1).await?;
        if capacity {
            for ordinal in 1..256 {
                require_receipt(
                    raise(
                        &mut socket,
                        organization,
                        held_start(&target.router_run, ordinal)?,
                    )
                    .await?,
                )?;
            }
            require_capacity_refusal(fixture, &mut socket, &target).await?;
        }
        for seconds in [499, 500, 500, 500, 500, 500, 500] {
            fixture.advance(seconds).await?;
            renew(&mut socket, fixture.refresh().await?).await?;
        }
        assert_eq!(
            read_request(fixture, &first).await?,
            [text("waiting"), SqlValue::Null]
        );
        if mark > deadline {
            fixture
                .advance(u64::try_from(deadline - fixture.now_seconds())?)
                .await?;
            assert_eq!(
                read_request(fixture, &first).await?,
                [text("waiting"), SqlValue::Null]
            );
            heartbeat(&mut socket).await?;
        }
        fixture
            .advance(u64::try_from(mark - fixture.now_seconds())?)
            .await?;
        fixture.observe_due_alarm().await?;
    }
    let (correlation, offered) = next_ending(&mut socket, &target).await?;
    assert_eq!(offered.request_id(), first.request_id());
    assert_eq!(offered.contents_digest(), &first.contents_digest()?);
    assert!(matches!(
        offered.clone().into_ending(),
        approval_request_protocol::Ending::TimedOut {}
    ));
    assert_eq!(
        read_request(fixture, &first).await?,
        [text("ended"), text("owed")]
    );

    if capacity {
        let before = snapshot(fixture).await?;
        fixture.hold_delivery_answer().await?;
        answer_ending(
            &mut socket,
            target.clone(),
            correlation,
            DeliverEndingReply::StopOwed {},
        )
        .await?;
        fixture.observe_delivery_answer().await?;
        assert_eq!(snapshot(fixture).await?, before);
        require_capacity_refusal(fixture, &mut socket, &target).await?;
        assert_eq!(
            stop(&mut socket, organization, stop_request(&first)?).await?,
            TypedCallResult::Answered {
                reply: StopReply::Stands {
                    ending: StopReceiptEnding::TimedOut {}
                }
            }
        );
        assert_eq!(
            read_request(fixture, &first).await?,
            [text("ended"), text("closed_without_receipt")]
        );
        require_receipt(
            raise(
                &mut socket,
                organization,
                held_start(&target.router_run, 256)?,
            )
            .await?,
        )?;
    } else {
        answer_ending(
            &mut socket,
            target.clone(),
            correlation,
            DeliverEndingReply::Received {},
        )
        .await?;
        observe_received(fixture, &first).await?;
    }
    socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

pub async fn received_frees_capacity_and_replacement_retires_run(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let remaining = u64::try_from((-fixture.now_seconds()).rem_euclid(300))?;
    if remaining != 0 {
        fixture.advance(remaining).await?;
    }
    let mut socket = fixture.connect().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let first = held_start(&target.router_run, 0)?;
    let receipt = require_receipt(raise(&mut socket, organization, first.clone()).await?)?;
    fixture.advance(1).await?;
    for ordinal in 1..256 {
        require_receipt(
            raise(
                &mut socket,
                organization,
                held_start(&target.router_run, ordinal)?,
            )
            .await?,
        )?;
    }
    for seconds in [499, 500, 500, 500, 500, 500, 500] {
        fixture.advance(seconds).await?;
        renew(&mut socket, fixture.refresh().await?).await?;
    }
    fixture
        .advance(u64::try_from(
            receipt.window.deadline().timestamp() - fixture.now_seconds(),
        )?)
        .await?;
    fixture.observe_due_alarm().await?;
    let (correlation, offer) = next_ending(&mut socket, &target).await?;
    assert_eq!(offer.request_id(), first.request_id());
    assert_eq!(offer.contents_digest(), &first.contents_digest()?);
    assert_eq!(
        offer.into_ending(),
        approval_request_protocol::Ending::TimedOut {}
    );
    require_owed(fixture, &first).await?;
    assert_eq!(
        run_outstanding(fixture, &target.router_run).await?,
        [255, 1]
    );
    require_joined_capacity_refusal(fixture, &mut socket, &target, 256).await?;

    fixture.hold_delivery_answer().await?;
    answer_ending(
        &mut socket,
        target.clone(),
        correlation,
        DeliverEndingReply::Received {},
    )
    .await?;
    fixture.observe_delivery_answer().await?;
    // Workerd evicts the object and reopens its store. Tokio has no link-fixture reopen.
    fixture.reopen_after_clock_rollback().await?;
    assert_eq!(
        read_request(fixture, &first).await?,
        [text("ended"), text("received_by_holder")]
    );
    assert_eq!(
        run_outstanding(fixture, &target.router_run).await?,
        [255, 0]
    );
    require_receipt(
        raise(
            &mut socket,
            organization,
            held_start(&target.router_run, 256)?,
        )
        .await?,
    )?;
    assert_eq!(
        run_outstanding(fixture, &target.router_run).await?,
        [256, 0]
    );
    require_joined_capacity_refusal(fixture, &mut socket, &target, 257).await?;
    socket.close(CloseCode::Normal, None).await?;

    let mut socket = fixture.connect_without_capabilities().await?;
    let replacement: RouterRunId = "019f0000-0000-7000-8000-000000005416".parse()?;
    let prior_requests = retirement_requests(fixture, &target.router_run).await?;
    let prior_destinations = retirement_destinations(fixture, &target.router_run).await?;
    assert_eq!(prior_requests.len(), 257);
    assert_eq!(prior_destinations.len(), 257);
    let before_retirement = retirement_snapshot(fixture).await?;
    fixture.fail_next_applying_write().await?;
    assert_eq!(
        central_call::<RegisterRunRpc>(
            &mut socket,
            organization,
            RegisterRunRequest {
                router_run: replacement.clone(),
            }
        )
        .await?,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::CentralFault
        },
    );
    assert_eq!(retirement_snapshot(fixture).await?, before_retirement);
    assert_eq!(
        register_run(&mut socket, organization, &replacement).await?,
        RegisterRunReply::Registered {}
    );
    fixture.reopen_after_clock_rollback().await?;
    assert_eq!(run_outstanding(fixture, &target.router_run).await?, [0, 0]);
    let retired_requests = retirement_requests(fixture, &target.router_run).await?;
    assert_eq!(
        retired_requests.len(),
        257,
        "retirement retains every old request"
    );
    let retired_at = require_replaced_run(fixture, &target.router_run, &replacement).await?;
    require_retired_requests(&prior_requests, &retired_requests, &first, &retired_at)?;
    assert_eq!(
        retirement_destinations(fixture, &target.router_run).await?,
        prior_destinations
    );
    socket.close(CloseCode::Normal, None).await?;
    Ok(())
}

async fn run_outstanding(
    fixture: &impl ApprovalLinkFixture,
    run: &RouterRunId,
) -> Result<[i64; 2]> {
    let observed = fixture.read(Statement {
        sql: "SELECT count(CASE WHEN record_state='waiting' THEN 1 END), count(CASE WHEN delivery_state='owed' THEN 1 END) FROM approval_requests WHERE router_run_id=?",
        args: vec![text(run.as_str())],
    }).await?;
    match observed.rows.as_slice() {
        [row] => match row.as_slice() {
            [SqlValue::Integer(waiting), SqlValue::Integer(owed)] => {
                Ok([waiting.get(), owed.get()])
            }
            _ => Err("expected the Waiting and Owed counts".into()),
        },
        _ => Err("expected one outstanding count row".into()),
    }
}

async fn retirement_requests(
    fixture: &impl ApprovalLinkFixture,
    run: &RouterRunId,
) -> Result<Vec<Vec<SqlValue>>> {
    Ok(fixture.read(Statement {
        sql: "SELECT request_id, record_state, delivery_state, ending_json, compact_ending_json, ended_at FROM approval_requests WHERE router_run_id=? ORDER BY request_id",
        args: vec![text(run.as_str())],
    }).await?.rows)
}

async fn retirement_destinations(
    fixture: &impl ApprovalLinkFixture,
    run: &RouterRunId,
) -> Result<Vec<Vec<SqlValue>>> {
    Ok(fixture.read(Statement {
        sql: "SELECT r.request_id, d.* FROM approval_request_destinations d JOIN approval_requests r ON r.approval_row_id=d.approval_row_id WHERE r.router_run_id=? ORDER BY d.approval_destination_id",
        args: vec![text(run.as_str())],
    }).await?.rows)
}

async fn require_replaced_run(
    fixture: &impl ApprovalLinkFixture,
    retired: &RouterRunId,
    replacement: &RouterRunId,
) -> Result<String> {
    let runs = fixture.read(Statement {
        sql: "SELECT router_run_id, retired_at FROM approval_router_runs WHERE router_run_id IN (?,?) ORDER BY registration_order",
        args: vec![text(retired.as_str()), text(replacement.as_str())],
    }).await?.rows;
    let [old_run, new_run] = runs.as_slice() else {
        return Err("expected the retained old run and its replacement".into());
    };
    let [SqlValue::Text(old_id), SqlValue::Text(retired_at)] = old_run.as_slice() else {
        return Err("the old run needs a retirement timestamp".into());
    };
    assert_eq!(old_id, retired.as_str());
    assert!(!retired_at.is_empty());
    assert_eq!(new_run, &[text(replacement.as_str()), SqlValue::Null]);
    Ok(retired_at.clone())
}

fn require_retired_requests(
    before: &[Vec<SqlValue>],
    after: &[Vec<SqlValue>],
    first: &Raise,
    retired_at: &str,
) -> Result<()> {
    let [received_before, waiting @ ..] = before else {
        return Err("expected the received request before retirement".into());
    };
    let [received_after, retired @ ..] = after else {
        return Err("expected the received request after retirement".into());
    };
    if received_before.first() != Some(&text(first.request_id().as_str()))
        || received_after != received_before
    {
        return Err(format!(
            "retirement must retain the exact paid request {}: before {received_before:?}, after {received_after:?}",
            first.request_id().as_str(),
        ).into());
    }
    for (prior, settled) in waiting.iter().zip(retired) {
        let [
            SqlValue::Text(id),
            SqlValue::Text(state),
            SqlValue::Text(delivery),
            SqlValue::Text(ending),
            SqlValue::Text(compact),
            SqlValue::Text(ended_at),
        ] = settled.as_slice()
        else {
            return Err(
                format!("expected a retained request with a complete ending: {settled:?}").into(),
            );
        };
        if prior.first() != Some(&text(id))
            || state != "ended"
            || delivery != "closed_without_receipt"
            || ended_at != retired_at
        {
            return Err(format!(
                "expected {:?} to end without receipt at {retired_at}, got {settled:?}",
                prior.first(),
            )
            .into());
        }
        let reason = approval_request_protocol::CancelReason::RequesterGone {};
        let ending: DeliverEndingRequest = serde_json::from_str(ending)?;
        if ending.request_id().as_str() != id
            || ending.into_ending() != (approval_request_protocol::Ending::Cancelled { reason })
            || serde_json::from_str::<StopReceiptEnding>(compact)?
                != (StopReceiptEnding::Cancelled { reason })
        {
            return Err(format!(
                "expected both endings for {id} to record RequesterGone: {settled:?}"
            )
            .into());
        }
    }
    Ok(())
}

async fn require_joined_capacity_refusal(
    fixture: &impl ApprovalLinkFixture,
    socket: &mut WebSocket,
    target: &ApprovalHolderTarget,
    ordinal: u16,
) -> Result<()> {
    let before = retirement_snapshot(fixture).await?;
    assert_eq!(
        raise(
            socket,
            &target.organization_id,
            held_start(&target.router_run, ordinal)?
        )
        .await?,
        RaiseReply::TooManyOutstanding {},
    );
    assert_eq!(retirement_snapshot(fixture).await?, before);
    Ok(())
}

async fn retirement_snapshot(
    fixture: &impl ApprovalLinkFixture,
) -> Result<Vec<Vec<Vec<SqlValue>>>> {
    let mut rows = vec![snapshot(fixture).await?];
    for sql in [
        "SELECT * FROM approval_router_runs ORDER BY router_run_id",
        "SELECT * FROM approval_request_destinations ORDER BY approval_destination_id",
        "SELECT * FROM engine_due_entries WHERE work_kind='approval_request' ORDER BY due_entry_id",
    ] {
        rows.push(fixture.read(Statement { sql, args: vec![] }).await?.rows);
    }
    Ok(rows)
}

async fn require_capacity_refusal(
    fixture: &impl ApprovalLinkFixture,
    socket: &mut WebSocket,
    target: &ApprovalHolderTarget,
) -> Result<()> {
    let before = snapshot(fixture).await?;
    let reply = raise(
        socket,
        &target.organization_id,
        held_start(&target.router_run, 256)?,
    )
    .await?;
    if reply != (RaiseReply::TooManyOutstanding {}) {
        return Err(format!("the 257th Raise returned {reply:?}").into());
    }
    if snapshot(fixture).await? != before {
        return Err("the refused 257th Raise changed stored approval requests".into());
    }
    Ok(())
}

pub async fn registered_waiting_request(
    fixture: &impl ApprovalLinkFixture,
) -> Result<(ApprovalHolderTarget, WebSocket, Raise)> {
    let target = fixture.target()?;
    let mut socket = fixture.connect().await?;
    assert_eq!(
        register_run(&mut socket, &target.organization_id, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let request = held_start(&target.router_run, 0)?;
    require_receipt(raise(&mut socket, &target.organization_id, request.clone()).await?)?;
    assert_eq!(
        read_request(fixture, &request).await?,
        [text("waiting"), SqlValue::Null]
    );
    Ok((target, socket, request))
}

pub async fn require_owed(fixture: &impl ApprovalLinkFixture, request: &Raise) -> Result<()> {
    assert_eq!(
        read_request(fixture, request).await?,
        [text("ended"), text("owed")]
    );
    Ok(())
}

pub async fn receive_fixture_decision(
    fixture: &impl ApprovalLinkFixture,
    socket: &mut WebSocket,
    target: ApprovalHolderTarget,
    request: &Raise,
) -> Result<()> {
    receive_decision(
        fixture,
        socket,
        target,
        request,
        approval_request_protocol::Ending::UnprovenCallDecided {
            decision: approval_request_protocol::SessionCallDecision::Allow,
            person_id: FIXTURE_DECIDER.parse()?,
        },
    )
    .await
}

pub async fn receive_decision(
    fixture: &impl ApprovalLinkFixture,
    socket: &mut WebSocket,
    target: ApprovalHolderTarget,
    request: &Raise,
    expected: approval_request_protocol::Ending,
) -> Result<()> {
    let offer = next_ending(socket, &target).await?;
    accept_fixture_decision(fixture, socket, target, request, expected, offer).await
}

pub async fn receive_decision_before(
    fixture: &impl ApprovalLinkFixture,
    socket: &mut WebSocket,
    target: ApprovalHolderTarget,
    request: &Raise,
    expected: approval_request_protocol::Ending,
    unavailable: impl Future<Output = Result<()>>,
) -> Result<()> {
    let offer = next_ending_before(socket, &target, unavailable).await?;
    accept_fixture_decision(fixture, socket, target, request, expected, offer).await
}

async fn accept_fixture_decision(
    fixture: &impl ApprovalLinkFixture,
    socket: &mut WebSocket,
    target: ApprovalHolderTarget,
    request: &Raise,
    expected: approval_request_protocol::Ending,
    (correlation, offered): (CorrelationId, DeliverEndingRequest),
) -> Result<()> {
    assert_eq!(offered.request_id(), request.request_id());
    assert_eq!(offered.contents_digest(), &request.contents_digest()?);
    assert_eq!(offered.into_ending(), expected);
    require_owed(fixture, request).await?;
    answer_ending(socket, target, correlation, DeliverEndingReply::Received {}).await?;
    observe_received(fixture, request).await
}

pub(crate) async fn observe_received(
    fixture: &impl ApprovalLinkFixture,
    request: &Raise,
) -> Result<()> {
    // The answer application runs outside the socket pump. Read from the owning
    // store while keeping the injected clock fixed.
    for _ in 0..128 {
        if read_request(fixture, request).await? == [text("ended"), text("received_by_holder")] {
            return Ok(());
        }
    }
    Err("the owning transaction did not apply Received".into())
}

pub fn held_start(run: &RouterRunId, ordinal: u16) -> Result<Raise> {
    Ok(serde_json::from_value(json!({
        "kind":"unprovenCall",
        "requestId":format!("019f0000-0000-7000-8000-{ordinal:012x}"),
        "holder":{"routerRun":run},
        "invocation": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "offer":{"operation":{"kind":"startSession","agentProgram":"claude","workingFolder":"/fixture","mode":"default","workFolders":[]}},
    }))?)
}

pub async fn register_run(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    run: &RouterRunId,
) -> Result<RegisterRunReply> {
    answered(
        central_call::<RegisterRunRpc>(
            socket,
            organization,
            RegisterRunRequest {
                router_run: run.clone(),
            },
        )
        .await?,
    )
}

pub async fn raise(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    raise: Raise,
) -> Result<RaiseReply> {
    answered(central_call::<RaiseApprovalRpc>(socket, organization, raise).await?)
}

pub fn require_receipt(reply: RaiseReply) -> Result<RaiseReceipt> {
    let RaiseReply::Receipt { receipt } = reply else {
        return Err(format!("Raise did not return a receipt: {reply:?}").into());
    };
    Ok(receipt)
}

pub(crate) async fn stop(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    request: StopRequest,
) -> Result<TypedCallResult<StopReply>> {
    central_call::<StopApprovalRpc>(socket, organization, request).await
}

pub(crate) fn stop_request(raise: &Raise) -> Result<StopRequest> {
    let request_id = raise.request_id().clone();
    let router_run = raise.router_run().clone();
    let contents_digest = raise.contents_digest()?;
    Ok(match raise {
        Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => StopRequest::Provider {
            request_id,
            router_run,
            contents_digest,
            cause: approval_request_protocol::ProviderStopCause::TurnStopped,
        },
        Raise::UnprovenCall { .. } => StopRequest::UnprovenCall {
            request_id,
            router_run,
            contents_digest,
            cause: approval_request_protocol::CallStopCause::CallWithdrawn,
        },
    })
}

pub async fn next_ending(
    socket: &mut WebSocket,
    expected: &ApprovalHolderTarget,
) -> Result<(CorrelationId, DeliverEndingRequest)> {
    let frame = receive_expected_text(socket, "holder ending offer").await?;
    ending_frame(frame, expected)
}

async fn next_ending_before(
    socket: &mut WebSocket,
    expected: &ApprovalHolderTarget,
    unavailable: impl Future<Output = Result<()>>,
) -> Result<(CorrelationId, DeliverEndingRequest)> {
    let frame =
        approval_socket_frames::receive_frame_before(socket, "holder ending offer", unavailable)
            .await?;
    ending_frame(frame, expected)
}

fn ending_frame(
    frame: LinkFrame,
    expected: &ApprovalHolderTarget,
) -> Result<(CorrelationId, DeliverEndingRequest)> {
    let LinkFrame::Data(DataFrame::CallMac {
        correlation_id,
        request,
    }) = frame
    else {
        return Err("expected a holder ending offer".into());
    };
    match *request {
        MacCall::DeliverEnding(envelope) if &envelope.target == expected => {
            Ok((correlation_id, envelope.body))
        }
        MacCall::DeliverEnding(_) => Err("ending did not target the admitted run".into()),
        MacCall::ApprovalNotice(_) => {
            Err("received a notice while expecting a holder ending".into())
        }
        MacCall::MacProbe(_) => Err("received a link probe while expecting a holder ending".into()),
    }
}

pub async fn answer_ending(
    socket: &mut WebSocket,
    target: ApprovalHolderTarget,
    correlation_id: CorrelationId,
    body: DeliverEndingReply,
) -> Result<()> {
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::MacCallReply {
            correlation_id,
            result: Box::new(TypedCallResult::Answered {
                reply: DeliverEndingRpc::reply(Envelope { target, body }),
            }),
        }),
    )
    .await
}

pub async fn heartbeat(socket: &mut WebSocket) -> Result<()> {
    send_frame(socket, LinkFrame::Ping).await?;
    if receive_frame(socket).await? != LinkFrame::Pong {
        return Err("expected heartbeat with no intervening frame".into());
    }
    Ok(())
}

pub(crate) async fn renew(socket: &mut WebSocket, token: String) -> Result<()> {
    let correlation: CorrelationId = CORRELATION.parse()?;
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::RenewMachineToken {
            correlation_id: correlation.clone(),
            token: token.try_into()?,
        }),
    )
    .await?;
    if !matches!(receive_frame(socket).await?, LinkFrame::Data(DataFrame::MachineTokenRenewal {
        correlation_id, result: TokenRenewalResult::Renewed { .. },
    }) if correlation_id == correlation)
    {
        return Err("fresh signed grant did not renew the fixture socket".into());
    }
    Ok(())
}

pub(crate) async fn central_call<R: CentralLinkRpc<Target = ApprovalServiceTarget>>(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    body: R::Request,
) -> Result<TypedCallResult<R::Reply>> {
    let target = ApprovalServiceTarget {
        organization_id: organization.clone(),
    };
    let correlation: CorrelationId = CORRELATION.parse()?;
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: correlation.clone(),
            request: Box::new(R::request(Envelope {
                target: target.clone(),
                body,
            })),
        }),
    )
    .await?;
    let LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result,
    }) = receive_frame(socket).await?
    else {
        return Err("expected correlated Central approval reply".into());
    };
    if correlation_id != correlation {
        return Err("Central approval correlation differs".into());
    }
    match *result {
        TypedCallResult::Refused { reason } => Ok(TypedCallResult::Refused { reason }),
        TypedCallResult::Answered { reply } => {
            let envelope = R::take_reply(reply)
                .filter(|reply| reply.target == target)
                .ok_or("Central approval marker or target differs")?;
            Ok(TypedCallResult::Answered {
                reply: envelope.body,
            })
        }
    }
}

fn answered<Reply>(result: TypedCallResult<Reply>) -> Result<Reply> {
    match result {
        TypedCallResult::Answered { reply } => Ok(reply),
        TypedCallResult::Refused { reason } => {
            Err(format!("expected service answer, got {reason:?}").into())
        }
    }
}

pub async fn read_request(
    fixture: &impl ApprovalLinkFixture,
    request: &Raise,
) -> Result<Vec<SqlValue>> {
    let records = fixture
        .read(Statement {
            sql: "SELECT record_state,delivery_state FROM approval_requests WHERE request_id=?",
            args: vec![text(request.request_id().as_str())],
        })
        .await?;
    match records.rows.as_slice() {
        [row] => Ok(row.clone()),
        _ => Err("expected exactly one durable approval row".into()),
    }
}

pub(crate) async fn snapshot(fixture: &impl ApprovalLinkFixture) -> Result<Vec<Vec<SqlValue>>> {
    Ok(fixture
        .read(Statement {
            sql: "SELECT * FROM approval_requests ORDER BY request_id",
            args: vec![],
        })
        .await?
        .rows)
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

#[path = "mac_capability_scenarios.rs"]
mod mac_capability_scenarios;
pub use mac_capability_scenarios::{
    HolderRecoveryOrder, announce_mac_calls, capability_ending_recovery, exchange_direct_mail,
    require_duplicate_capabilities_close,
};

#[path = "approval_socket_frames.rs"]
mod approval_socket_frames;
pub use approval_socket_frames::receive_frame;
pub(crate) use approval_socket_frames::send_frame;

pub(crate) use approval_socket_frames::receive_expected_text;
