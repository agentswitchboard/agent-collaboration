//! Typed exchanges use the real public WebSocket and private object invocation.

mod capacity;

use super::super::workerd_host::{
    c2_controls::{C2EvictionCause, C2EvictionError},
    workerd_control_http_client,
};
use super::fixture::{ConnectedLink, Fixture, MACHINE_ID, receive, send};
use super::receipt_frames::{EnrollmentAtApply, stored_ending_batch};
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, CorrelationId, DataFrame, LinkCallRefusal, MacCall,
    MacLinkRpc, MachineLinkCloseCode, TypedCallResult,
    test_calls::{CentralProbe, MacProbe, ProbeReply, ProbeRequest},
};
use central_storage::BatchOutcome;
use collaboration_central_conformance::ConformanceResult as Result;
use fleet_rpc::Envelope;
use futures_util::{SinkExt, StreamExt};
use reqwest::{Response, StatusCode};
use reqwest_websocket::{Message, WebSocket};
use serde_json::json;
use workspace_ids::MachineId;

const INBOUND_CORRELATION: &str = "019f0000-0000-7000-8000-00000000c721";
const WRONG_TARGET: &str = "019f0000-0000-7000-8000-00000000c722";

#[tokio::test]
async fn typed_calls_restore_authority_and_correlate_refusals() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = check_inbound_calls(&fixture).await;
    fixture.close().await?;
    result?;

    for state in [
        EnrollmentAtApply::StillEnrolled,
        EnrollmentAtApply::OwnerRevoked,
    ] {
        let fixture = Fixture::new().await?;
        let result = check_authority_lapse(&fixture, state).await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}

async fn check_inbound_calls(fixture: &Fixture) -> Result<()> {
    let mut link = ConnectedLink::open(fixture).await?;
    let target: MachineId = MACHINE_ID.parse()?;
    assert_eq!(
        central_probe(&mut link.socket, &target, 11).await?,
        TypedCallResult::Answered {
            reply: CentralProbe::reply(Envelope {
                target,
                body: ProbeReply { echoed: 11 }
            }),
        }
    );
    assert_eq!(
        central_probe(&mut link.socket, &WRONG_TARGET.parse()?, 12).await?,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::WrongTarget
        }
    );
    let correlation_id = INBOUND_CORRELATION.parse()?;
    let unknown = json!({
        "kind": "callCentral",
        "correlationId": correlation_id,
        "request": {"call": "unregisteredProbe", "envelope": {"target": MACHINE_ID, "body": {}}},
    });
    link.socket.send(Message::Text(unknown.to_string())).await?;
    assert_eq!(
        central_reply(&mut link.socket, &correlation_id).await?,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::UnknownCall
        }
    );
    link.close().await
}

async fn check_authority_lapse(fixture: &Fixture, state: EnrollmentAtApply) -> Result<()> {
    let mut link = ConnectedLink::open(fixture).await?;
    super::renewal::advance(fixture, 601).await?;
    if let Some(reason) = state.stored_end_reason() {
        let batch = stored_ending_batch(MACHINE_ID, fixture.enrollment.as_str(), reason)?;
        assert!(matches!(
            fixture.runtime().run_batch(batch).await?,
            BatchOutcome::Applied { .. }
        ));
    }
    let cause = state.expected_close();
    assert_eq!(
        central_probe(&mut link.socket, &MACHINE_ID.parse()?, 13).await?,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::AuthorityLapsed { cause }
        }
    );
    let closed = link.socket.next().await;
    assert!(
        matches!(closed, Some(Ok(Message::Close { code, .. }))
        if MachineLinkCloseCode::from_code(code.into()) == Some(cause)),
        "{closed:?}"
    );
    Ok(())
}

async fn central_probe(
    socket: &mut WebSocket,
    target: &MachineId,
    sequence: u32,
) -> Result<TypedCallResult<CentralCallReply>> {
    let correlation_id: CorrelationId =
        format!("019f0000-0000-7000-8000-{sequence:012x}").parse()?;
    send(
        socket,
        &DataFrame::CallCentral {
            correlation_id: correlation_id.clone(),
            request: Box::new(CentralProbe::request(Envelope {
                target: target.clone(),
                body: ProbeRequest { sequence },
            })),
        },
    )
    .await?;
    central_reply(socket, &correlation_id).await
}

async fn central_reply(
    socket: &mut WebSocket,
    expected: &CorrelationId,
) -> Result<TypedCallResult<CentralCallReply>> {
    let frame = receive(socket).await?;
    let DataFrame::CentralCallReply {
        correlation_id,
        result,
    } = frame
    else {
        return Err(format!("expected a typed Central reply, got {frame:?}").into());
    };
    assert_eq!(&correlation_id, expected);
    Ok(*result)
}

#[tokio::test]
async fn typed_calls_to_mac_settle_on_reply_deadline_and_physical_close() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = check_outbound_calls(&fixture).await;
    fixture.close().await?;
    result
}

async fn require_private_invocation_route(fixture: &Fixture) -> Result<()> {
    let client = workerd_control_http_client();
    let object_address = json!({"kind": "central", "value": fixture.organization}).to_string();
    let body = json!({
        "machineId": MACHINE_ID,
        "request": {"call": "macProbe", "envelope": {"target": MACHINE_ID, "body": {"sequence": 20}}},
    });
    for origin in [
        fixture.host.public_origin().to_owned(),
        fixture.host.signin_origin(),
    ] {
        let response = client
            .post(format!("{origin}/internal/machine-link/call"))
            .header("x-central-object-address", &object_address)
            .json(&body)
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    Ok(())
}

async fn check_outbound_calls(fixture: &Fixture) -> Result<()> {
    require_private_invocation_route(fixture).await?;
    assert_mac_failure(
        invoke_mac(fixture, 20).await?,
        StatusCode::CONFLICT,
        "neverReached",
    )
    .await?;
    let mut link = ConnectedLink::open(fixture).await?;
    let (first, answered) = tokio::try_join!(
        async {
            let correlation = mac_probe(&mut link.socket, 21).await?;
            reply_to_mac_probe(&mut link.socket, correlation.clone(), 21).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(correlation)
        },
        invoke_mac(fixture, 21),
    )?;
    assert_probe_answer(answered, 21).await?;

    let (timed_out, answer) = tokio::try_join!(
        async {
            let correlation = mac_probe(&mut link.socket, 22).await?;
            assert_ne!(correlation, first);
            reply_to_mac_probe(&mut link.socket, first, 900).await?;
            let receipt = central_probe(&mut link.socket, &MACHINE_ID.parse()?, 92).await?;
            assert!(matches!(receipt, TypedCallResult::Answered { .. }));
            let eviction = fixture.host.c2_evict(&fixture.organization).await;
            assert!(
                matches!(
                    eviction,
                    Err(C2EvictionError::Failed(C2EvictionCause::ActiveReferences))
                ),
                "{eviction:?}"
            );
            super::renewal::advance(fixture, 5).await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(correlation)
        },
        invoke_mac(fixture, 22),
    )?;
    assert_mac_failure(answer, StatusCode::SERVICE_UNAVAILABLE, "answerLost").await?;

    // This is an actual eviction of the idle instance with the original socket hibernating.
    fixture.host.c2_evict(&fixture.organization).await?;
    let (_, answered) = tokio::try_join!(
        async {
            let correlation = mac_probe(&mut link.socket, 23).await?;
            assert_ne!(correlation, timed_out);
            reply_to_mac_probe(&mut link.socket, timed_out, 901).await?;
            reply_to_mac_probe(&mut link.socket, correlation, 23).await
        },
        invoke_mac(fixture, 23),
    )?;
    assert_probe_answer(answered, 23).await?;

    let (_, closed) = tokio::try_join!(
        async {
            mac_probe(&mut link.socket, 24).await?;
            link.close().await
        },
        invoke_mac(fixture, 24),
    )?;
    assert_mac_failure(closed, StatusCode::SERVICE_UNAVAILABLE, "answerLost").await?;
    Ok(())
}

#[tokio::test]
async fn a_reset_of_the_owning_object_is_answer_lost_at_the_caller() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let mut link = ConnectedLink::open(&fixture).await?;
        let (_, answer) = tokio::try_join!(
            async {
                mac_probe(&mut link.socket, 25).await?;
                // This fixture checks for a real cold alarm on the reconstructed object.
                fixture.runtime().cold_alarm().await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            },
            invoke_mac(&fixture, 25),
        )?;
        assert_mac_failure(answer, StatusCode::SERVICE_UNAVAILABLE, "answerLost").await
    }
    .await;
    fixture.close().await?;
    result
}

async fn invoke_mac(fixture: &Fixture, sequence: u32) -> Result<Response> {
    Ok(workerd_control_http_client()
        .post(format!(
            "{}/test-support/workspace/{}/typed-mac-call",
            fixture.host.public_origin(),
            fixture.organization.as_str(),
        ))
        .json(&json!({"target": MACHINE_ID, "body": {"sequence": sequence}}))
        .send()
        .await?)
}

async fn mac_probe(socket: &mut WebSocket, expected: u32) -> Result<CorrelationId> {
    let (correlation_id, sequence) = mac_call(socket).await?;
    assert_eq!(sequence, expected);
    Ok(correlation_id)
}

async fn mac_call(socket: &mut WebSocket) -> Result<(CorrelationId, u32)> {
    let frame = receive(socket).await?;
    let DataFrame::CallMac {
        correlation_id,
        request,
    } = frame
    else {
        return Err(format!("expected typed Mac call, got {frame:?}").into());
    };
    let MacCall::MacProbe(envelope) = *request else {
        return Err("expected the registered Mac probe call".into());
    };
    assert_eq!(envelope.target.as_str(), MACHINE_ID);
    Ok((correlation_id, envelope.body.sequence))
}

async fn reply_to_mac_probe(
    socket: &mut WebSocket,
    correlation_id: CorrelationId,
    echoed: u32,
) -> Result<()> {
    send(
        socket,
        &DataFrame::MacCallReply {
            correlation_id,
            result: Box::new(TypedCallResult::Answered {
                reply: MacProbe::reply(Envelope {
                    target: MACHINE_ID.parse()?,
                    body: ProbeReply { echoed },
                }),
            }),
        },
    )
    .await
}

async fn assert_probe_answer(answer: Response, echoed: u32) -> Result<()> {
    let answer = answer
        .error_for_status()?
        .json::<Envelope<MachineId, ProbeReply>>()
        .await?;
    assert_eq!(answer.target.as_str(), MACHINE_ID);
    assert_eq!(answer.body.echoed, echoed);
    Ok(())
}

async fn assert_mac_failure(answer: Response, status: StatusCode, kind: &str) -> Result<()> {
    assert_eq!(answer.status(), status);
    assert_eq!(
        answer.json::<serde_json::Value>().await?,
        json!({"kind": kind})
    );
    Ok(())
}
