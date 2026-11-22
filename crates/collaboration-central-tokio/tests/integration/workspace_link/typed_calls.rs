use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
use super::{Fixture, INSTALLATION, OTHER_MACHINE, receive, send, signed};
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, CorrelationId, DataFrame, LinkCallRefusal, LinkFrame,
    MacLinkRpc, MachineLinkCloseCode, TypedCallResult,
    test_calls::{CentralProbe, MacProbe, ProbeReply, ProbeRequest},
};
use central_storage::BatchOutcome;
use collaboration_central_conformance::ConformanceResult as Result;
use collaboration_central_core::TYPED_LINK_CALL_TIMEOUT;
use fleet_rpc::{CallFailure, Envelope};
use futures_util::{SinkExt, StreamExt};
use reqwest_websocket::{CloseCode, Message, WebSocket};
use router_clock::RuntimeClock;
use serde_json::json;
use std::time::Duration;
use tokio::task::JoinHandle;
use workspace_ids::MachineId;

const CENTRAL_CORRELATION: &str = "019f0000-0000-7000-8000-00000000cd01";
const UNKNOWN_CORRELATION: &str = "019f0000-0000-7000-8000-00000000cd02";
const WRONG_TARGET_CORRELATION: &str = "019f0000-0000-7000-8000-00000000cd03";

#[tokio::test]
async fn central_typed_calls_reply_with_their_correlation_and_refuse_unknown_and_wrong_target()
-> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = async {
        let mut socket = connect(&fixture).await?;
        let correlation: CorrelationId = CENTRAL_CORRELATION.parse()?;
        let target = INSTALLATION.parse()?;
        send(&mut socket, &central_probe(correlation.clone(), target)).await?;
        expect_central_reply(
            &mut socket,
            correlation.clone(),
            TypedCallResult::Answered {
                reply: CentralProbe::reply(Envelope {
                    target: INSTALLATION.parse()?,
                    body: ProbeReply { echoed: 42 },
                }),
            },
        )
        .await?;

        let wrong_target: CorrelationId = WRONG_TARGET_CORRELATION.parse()?;
        send(
            &mut socket,
            &central_probe(wrong_target.clone(), OTHER_MACHINE.parse()?),
        )
        .await?;
        expect_central_reply(
            &mut socket,
            wrong_target,
            TypedCallResult::Refused {
                reason: LinkCallRefusal::WrongTarget,
            },
        )
        .await?;

        send_unknown_probe(&mut socket).await?;
        expect_central_reply(
            &mut socket,
            UNKNOWN_CORRELATION.parse()?,
            TypedCallResult::Refused {
                reason: LinkCallRefusal::UnknownCall,
            },
        )
        .await?;
        socket_barrier(&mut socket).await
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn central_typed_calls_distinguish_grant_expiry_from_stored_endings() -> Result<()> {
    for state in [
        EnrollmentAtApply::StillEnrolled,
        EnrollmentAtApply::OwnerRevoked,
        EnrollmentAtApply::EndedForIdle,
    ] {
        let fixture = Fixture::new().await?;
        let observed = verify_lapsed_authority(&fixture, state).await;
        fixture.close().await?;
        observed?;
    }
    Ok(())
}

async fn verify_lapsed_authority(fixture: &Fixture, state: EnrollmentAtApply) -> Result<()> {
    let mut socket = connect(fixture).await?;
    socket_barrier(&mut socket).await?;
    fixture.clock.advance(Duration::from_secs(601))?;
    if let Some(reason) = state.stored_end_reason() {
        let central = fixture
            .host
            .central(&signed::ORGANIZATION.to_owned().try_into()?)
            .await?;
        // Keep the socket registered so restoration must read the stored cause.
        let _turn = central.runtime().take_central_turn().await;
        let ending = stored_ending_batch(INSTALLATION, fixture.enrollment_id.as_str(), reason)?;
        if !matches!(
            central.runtime().statement_host().run_batch(ending).await?,
            BatchOutcome::Applied { .. }
        ) {
            return Err("could not stage the stored ending".into());
        }
    }
    let correlation = if matches!(state, EnrollmentAtApply::StillEnrolled) {
        // A valid but unknown call must authenticate before its name is refused.
        send_unknown_probe(&mut socket).await?;
        UNKNOWN_CORRELATION.parse()?
    } else {
        let correlation: CorrelationId = CENTRAL_CORRELATION.parse()?;
        send(
            &mut socket,
            &central_probe(correlation.clone(), INSTALLATION.parse()?),
        )
        .await?;
        correlation
    };
    let cause = state.expected_close();
    expect_central_reply(
        &mut socket,
        correlation,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::AuthorityLapsed { cause },
        },
    )
    .await?;
    let close = socket.next().await;
    if !matches!(close, Some(Ok(Message::Close { code, .. })) if MachineLinkCloseCode::from_code(code.into()) == Some(cause))
    {
        return Err(format!("expected socket to finish with {cause:?}, got {close:?}").into());
    }
    Ok(())
}

#[tokio::test]
async fn mac_typed_call_waiters_follow_correlations_on_the_open_socket() -> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = async {
        let organization = signed::ORGANIZATION.to_owned().try_into()?;
        let machine = INSTALLATION.parse()?;
        let disconnected = fixture
            .host
            .call_machine::<MacProbe>(&organization, &machine, probe_request(machine.clone(), 0))
            .await;
        expect_call_failure(disconnected, CallFailure::NeverReached)?;
        let mut socket = connect(&fixture).await?;
        let first = start_mac_call(&fixture, &mut socket, 11).await?;
        let second = start_mac_call(&fixture, &mut socket, 22).await?;
        if first.correlation == second.correlation {
            return Err("two live calls shared a correlation".into());
        }

        answer_mac_call(&mut socket, &second.correlation, INSTALLATION, 22).await?;
        let duplicate = second.correlation.clone();
        expect_probe_reply(second, 22).await?;
        answer_mac_call(&mut socket, &duplicate, INSTALLATION, 99).await?;
        socket_barrier(&mut socket).await?;
        answer_mac_call(&mut socket, &first.correlation, INSTALLATION, 11).await?;
        expect_probe_reply(first, 11).await?;

        let wrong_target = start_mac_call(&fixture, &mut socket, 33).await?;
        answer_mac_call(&mut socket, &wrong_target.correlation, OTHER_MACHINE, 33).await?;
        expect_call_failure(wrong_target.result.await?, CallFailure::AnswerLost)
    }
    .await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn mac_typed_call_timeout_close_and_reconnect_retire_waiters() -> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = verify_outbound_retirement(&fixture).await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn mac_typed_call_never_dispatches_to_an_open_socket_with_lapsed_authority() -> Result<()> {
    let fixture = Fixture::new().await?;
    let observed = async {
        let mut socket = connect(&fixture).await?;
        socket_barrier(&mut socket).await?;
        fixture.clock.advance(Duration::from_secs(601))?;
        let organization = signed::ORGANIZATION.to_owned().try_into()?;
        let machine = INSTALLATION.parse::<MachineId>()?;
        expect_call_failure(
            fixture.host.call_machine::<MacProbe>(&organization, &machine, probe_request(machine.clone(), 42)).await,
            CallFailure::NeverReached,
        )?;
        let close = socket.next().await;
        if !matches!(close, Some(Ok(Message::Close { code, .. })) if MachineLinkCloseCode::from_code(code.into()) == Some(MachineLinkCloseCode::Expired)) {
            return Err(format!("expected Expired before any call went on the wire, got {close:?}").into());
        }
        Ok(())
    }.await;
    fixture.close().await?;
    observed
}

async fn verify_outbound_retirement(fixture: &Fixture) -> Result<()> {
    let mut socket = connect(fixture).await?;
    let deadline = fixture.clock.monotonic_now().after(TYPED_LINK_CALL_TIMEOUT);
    let expired = start_mac_call(fixture, &mut socket, 7).await?;
    fixture.clock.not_due_check_now(deadline).await;
    let tick = Duration::from_millis(1);
    fixture.clock.advance(TYPED_LINK_CALL_TIMEOUT - tick)?;
    fixture.clock.not_due_check_now(deadline).await;
    if expired.result.is_finished() {
        return Err("the pending call retired before its deadline".into());
    }
    fixture.clock.advance(tick)?;
    expect_call_failure(expired.result.await?, CallFailure::AnswerLost)?;

    let retry = start_mac_call(fixture, &mut socket, 7).await?;
    if retry.correlation == expired.correlation {
        return Err("retry reused the timed-out correlation".into());
    }
    answer_mac_call(&mut socket, &expired.correlation, INSTALLATION, 99).await?;
    socket_barrier(&mut socket).await?;
    answer_mac_call(&mut socket, &retry.correlation, INSTALLATION, 7).await?;
    expect_probe_reply(retry, 7).await?;

    let replaced = start_mac_call(fixture, &mut socket, 8).await?;
    let mut reconnect = connect(fixture).await?;
    expect_call_failure(replaced.result.await?, CallFailure::AnswerLost)?;
    let current = start_mac_call(fixture, &mut reconnect, 8).await?;
    if current.correlation == replaced.correlation {
        return Err("new connection reused the closed socket's correlation".into());
    }
    answer_mac_call(&mut reconnect, &replaced.correlation, INSTALLATION, 99).await?;
    socket_barrier(&mut reconnect).await?;
    answer_mac_call(&mut reconnect, &current.correlation, INSTALLATION, 8).await?;
    expect_probe_reply(current, 8).await?;

    let closed = start_mac_call(fixture, &mut reconnect, 9).await?;
    reconnect.close(CloseCode::Normal, None).await?;
    expect_call_failure(closed.result.await?, CallFailure::AnswerLost)
}

type ProbeResult = std::result::Result<Envelope<MachineId, ProbeReply>, CallFailure>;

struct MacAttempt {
    correlation: CorrelationId,
    result: JoinHandle<ProbeResult>,
}

async fn start_mac_call(
    fixture: &Fixture,
    socket: &mut WebSocket,
    sequence: u32,
) -> Result<MacAttempt> {
    let organization = signed::ORGANIZATION.to_owned().try_into()?;
    let machine: MachineId = INSTALLATION.parse()?;
    let request = probe_request(machine.clone(), sequence);
    let expected = MacProbe::request(request.clone());
    let host = fixture.host.clone();
    let mut result = tokio::spawn(async move {
        host.call_machine::<MacProbe>(&organization, &machine, request)
            .await
    });
    let frame = tokio::select! {
        frame = receive(socket) => frame?,
        early = &mut result => return Err(format!("call finished before the peer received it: {:?}", early?).into()),
    };
    match frame {
        LinkFrame::Data(DataFrame::CallMac {
            correlation_id,
            request,
        }) if *request == expected => Ok(MacAttempt {
            correlation: correlation_id,
            result,
        }),
        _ => {
            Err(format!("expected the outbound call for sequence {sequence}, got {frame:?}").into())
        }
    }
}

fn probe_request(target: MachineId, sequence: u32) -> Envelope<MachineId, ProbeRequest> {
    Envelope {
        target,
        body: ProbeRequest { sequence },
    }
}

async fn answer_mac_call(
    socket: &mut WebSocket,
    correlation_id: &CorrelationId,
    target: &str,
    echoed: u32,
) -> Result<()> {
    send(
        socket,
        &LinkFrame::Data(DataFrame::MacCallReply {
            correlation_id: correlation_id.clone(),
            result: Box::new(TypedCallResult::Answered {
                reply: MacProbe::reply(Envelope {
                    target: target.parse()?,
                    body: ProbeReply { echoed },
                }),
            }),
        }),
    )
    .await
}

async fn expect_probe_reply(attempt: MacAttempt, echoed: u32) -> Result<()> {
    let actual = attempt.result.await?;
    let expected = Ok(Envelope {
        target: INSTALLATION.parse()?,
        body: ProbeReply { echoed },
    });
    if actual != expected {
        return Err(format!(
            "{:#?} completed with {actual:?}, expected {expected:?}",
            attempt.correlation
        )
        .into());
    }
    Ok(())
}

fn expect_call_failure(result: ProbeResult, expected: CallFailure) -> Result<()> {
    if result != Err(expected) {
        return Err(format!("expected {expected:?}, got {result:?}").into());
    }
    Ok(())
}

fn central_probe(correlation_id: CorrelationId, target: MachineId) -> LinkFrame {
    LinkFrame::Data(DataFrame::CallCentral {
        correlation_id,
        request: Box::new(CentralProbe::request(probe_request(target, 42))),
    })
}

async fn send_unknown_probe(socket: &mut WebSocket) -> Result<()> {
    socket.send(Message::Text(json!({
        "kind": "callCentral", "correlationId": UNKNOWN_CORRELATION,
        "request": {"call": "futureProbe", "envelope": {"target": INSTALLATION, "body": {"sequence": 42}}},
    }).to_string())).await?;
    Ok(())
}

async fn expect_central_reply(
    socket: &mut WebSocket,
    correlation_id: CorrelationId,
    result: TypedCallResult<CentralCallReply>,
) -> Result<()> {
    let expected = LinkFrame::Data(DataFrame::CentralCallReply {
        correlation_id,
        result: Box::new(result),
    });
    let observed = receive(socket).await?;
    if observed != expected {
        return Err(format!("expected {expected:?}, got {observed:?}").into());
    }
    Ok(())
}

async fn connect(fixture: &Fixture) -> Result<WebSocket> {
    let mut socket = fixture.connect().await?;
    if !matches!(
        receive(&mut socket).await?,
        LinkFrame::Data(DataFrame::Welcome { .. })
    ) {
        return Err("expected a welcomed socket".into());
    }
    Ok(socket)
}

async fn socket_barrier(socket: &mut WebSocket) -> Result<()> {
    send(socket, &LinkFrame::Ping).await?;
    if receive(socket).await? != LinkFrame::Pong {
        return Err("expected the open socket to answer the heartbeat".into());
    }
    Ok(())
}
