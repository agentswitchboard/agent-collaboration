//! Runs the approval deadline through the real machine link on both hosts.
use super::{ProviderAsk, create_bound, provider_raise};
use crate::{
    ConformanceResult as Result,
    approval_link_scenarios::{
        ApprovalLinkFixture, answer_ending, heartbeat, next_ending, observe_received, raise,
        read_request, receive_frame, register_run, renew, require_receipt, send_frame,
    },
};
use approval_request_protocol::{DeliverEndingReply, Ending, RegisterRunReply};
use central_link_protocol::{
    DataFrame, LinkFrame, LinkRequest, ReplyPayload, RequestPayload,
    approval_rpc::ApprovalHolderTarget,
};
use central_storage::{SqlValue, Statement};
use reqwest_websocket::WebSocket;
use workspace_model::{
    Caller,
    central_mail::{
        CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
        NoticeCadence,
    },
};

pub trait ApprovalAlarmRaceFixture: ApprovalLinkFixture {
    fn seed_alarm_race_probe(
        &self,
        ordinal: u16,
        due_at_ms: i64,
    ) -> impl Future<Output = Result<()>>;
    fn ordinary_request_at_retained_mark(
        &self,
        socket: &mut WebSocket,
        mark: i64,
    ) -> impl Future<Output = Result<Option<i64>>>;
    fn retained_alarm_ms(&self) -> impl Future<Output = Result<Option<i64>>>;
}

pub async fn publish_ordinary_alarm_request(
    socket: &mut WebSocket,
    target: &ApprovalHolderTarget,
    ordinal: u16,
) -> Result<()> {
    let request = LinkRequest::new(
        format!("019f0000-0000-7000-8000-00000002{ordinal:04x}").parse()?,
        Caller::Session(serde_json::from_value(serde_json::json!({
            "machineId": target.machine_id,
            "sessionId": "approval-alarm-request",
        }))?),
        RequestPayload::Mail(CentralMailRequest {
            request_id: format!("019f0000-0000-7000-8000-00000003{ordinal:04x}").try_into()?,
            operation: CentralMailOperation::SetNoticeCadence {
                cadence: NoticeCadence::M15,
            },
        }),
    )?;
    let expected = request.correlation_id().clone();
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::Request {
            request: Box::new(request),
        }),
    )
    .await?;
    if !matches!(
        receive_frame(socket).await?,
        LinkFrame::Data(DataFrame::Reply { correlation_id, result, .. })
            if correlation_id == expected
                && matches!(*result, ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::NoticeCadenceSet { .. })))
    ) {
        return Err("the ordinary mail request did not answer before the alarm drive".into());
    }
    Ok(())
}

pub async fn approval_alarm_mark_race(fixture: &impl ApprovalAlarmRaceFixture) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let mut socket = fixture.connect().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let (session, _) = create_bound(&mut socket, organization, 1, None).await?;
    let request = provider_raise(&target, &session, 7, ProviderAsk::Question)?;
    let receipt = require_receipt(raise(&mut socket, organization, request.clone()).await?)?;
    let deadline = receipt.window.deadline().timestamp();
    let mark = deadline + (-deadline).rem_euclid(300);
    for _ in 0..7 {
        fixture.advance(500).await?;
        renew(&mut socket, fixture.refresh().await?).await?;
    }
    for ordinal in 0..251 {
        // The 10-second-early request's look-ahead stops five seconds short of these
        // probes. The mark's own batch sees them and has an independent capped tail.
        fixture
            .seed_alarm_race_probe(ordinal, (mark + 45) * 1000)
            .await?;
    }
    fixture
        .advance(u64::try_from(mark - 10 - fixture.now_seconds())?)
        .await?;
    publish_ordinary_alarm_request(&mut socket, &target, 1).await?;
    assert_eq!(fixture.retained_alarm_ms().await?, Some(mark * 1000));
    assert_eq!(
        read_request(fixture, &request).await?,
        [SqlValue::Text("waiting".into()), SqlValue::Null]
    );
    let still_armed = fixture
        .ordinary_request_at_retained_mark(&mut socket, mark)
        .await?;
    assert_eq!(still_armed, Some(mark * 1000));
    fixture.observe_due_alarm().await?;
    let (correlation, offered) = next_ending(&mut socket, &target).await?;
    assert_eq!(offered.request_id(), request.request_id());
    assert_eq!(offered.contents_digest(), &request.contents_digest()?);
    assert_eq!(offered.into_ending(), Ending::TimedOut {});
    answer_ending(
        &mut socket,
        target,
        correlation,
        DeliverEndingReply::Received {},
    )
    .await?;
    observe_received(fixture, &request).await?;
    heartbeat(&mut socket).await?;

    let rows = fixture.read(Statement {
        sql: "SELECT COUNT(*) FROM engine_probe_items WHERE group_id='019f0000-0000-7000-8000-000000040000' AND state='waiting'",
        args: vec![],
    }).await?;
    assert_eq!(rows.rows, [vec![SqlValue::Integer(1.try_into()?)]]);
    assert_eq!(
        fixture.retained_alarm_ms().await?,
        Some((mark + 300) * 1000)
    );
    Ok(())
}
