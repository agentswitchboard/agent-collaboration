//! Socket oracle shared by the two production-composition integration suites.
use crate::ConformanceResult as Result;
use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, Raise, RaiseReceipt, RaiseReply, RegisterRunReply,
    RegisterRunRequest, RouterRunId, StopReceiptEnding, StopReply, StopRequest,
};
use central_link_protocol::{
    CentralLinkRpc, CorrelationId, DataFrame, FrameDirection, LinkCallRefusal, LinkFrame, MacCall,
    MacLinkRpc, TokenRenewalResult, TypedCallResult,
    approval_rpc::{
        ApprovalHolderTarget, ApprovalServiceTarget, DeliverEndingRpc, RaiseApprovalRpc,
        RegisterRunRpc, StopApprovalRpc,
    },
    decode_text, encode_text,
};
use central_storage::{ReadOutcome, SqlValue, Statement};
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::Envelope;
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{CloseCode, Message, WebSocket};
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
    fn connect(&self) -> impl Future<Output = Result<WebSocket>>;
    fn read(&self, statement: Statement) -> impl Future<Output = Result<ReadOutcome>>;
    fn advance(&self, seconds: u64) -> impl Future<Output = Result<()>>;
    fn refresh(&self) -> impl Future<Output = Result<String>>;
    fn observe_due_alarm(&self) -> impl Future<Output = Result<()>>;
    fn fail_next_applying_write(&self) -> impl Future<Output = Result<()>>;
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
    let (correlation, offered) = next_ending(socket, &target).await?;
    assert_eq!(offered.request_id(), request.request_id());
    assert_eq!(offered.contents_digest(), &request.contents_digest()?);
    assert_eq!(
        offered.into_ending(),
        approval_request_protocol::Ending::UnprovenCallDecided {
            decision: approval_request_protocol::SessionCallDecision::Allow,
            person_id: FIXTURE_DECIDER.parse()?,
        },
    );
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
    let LinkFrame::Data(DataFrame::CallMac {
        correlation_id,
        request,
    }) = receive_frame(socket).await?
    else {
        return Err("expected a holder ending offer".into());
    };
    match *request {
        MacCall::DeliverEnding(envelope) if &envelope.target == expected => {
            Ok((correlation_id, envelope.body))
        }
        _ => Err("ending did not target the admitted run".into()),
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

pub async fn receive_frame(socket: &mut WebSocket) -> Result<LinkFrame> {
    let Message::Text(text) = socket.next().await.ok_or("approval socket ended")?? else {
        return Err("expected application text on approval socket".into());
    };
    Ok(decode_text(&text, FrameDirection::ToRouter)?)
}

async fn send_frame(socket: &mut WebSocket, frame: LinkFrame) -> Result<()> {
    socket
        .send(Message::Text(encode_text(
            &frame,
            FrameDirection::ToCentral,
        )?))
        .await?;
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
