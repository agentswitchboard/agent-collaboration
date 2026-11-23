use super::typed_calls::{connect, expect_call_failure, expect_central_reply, socket_barrier};
use super::{
    Fixture, INSTALLATION, OTHER_MACHINE, OTHER_ORG, receive, send, signed, start_fixture,
};
use central_link_protocol::{
    CentralLinkRpc, DataFrame, LinkCallRefusal, LinkFrame, MacCallReply, MacLinkRpc,
    TypedCallResult,
    approval_rpc::{ApprovalHolderTarget, ApprovalServiceTarget, DeliverEndingRpc, RegisterRunRpc},
};
use collaboration_central_conformance::ConformanceResult as Result;
use fleet_contracts::{DeviceLoginDecision, MachineTokenDecision};
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::{CallFailure, Envelope, Rpc};
use reqwest_websocket::{RequestBuilderExt, WebSocket};
use router_clock::RuntimeClock;
use serde_json::json;

const REGISTER_OTHER_ORG: &str = "019f0000-0000-7000-8000-00000000cd10";
const REGISTER_FIRST: &str = "019f0000-0000-7000-8000-00000000cd11";
const REGISTER_REPLACEMENT: &str = "019f0000-0000-7000-8000-00000000cd12";
const ENDING_REQUEST: &str = "019f0000-0000-7000-8000-00000000cd13";
const ROUTER_RUN: &str = "019f0000-0000-7000-8000-00000000cd14";
const OTHER_RUN: &str = "019f0000-0000-7000-8000-00000000cd15";
const OTHER_ENROLLMENT: &str = "019f0000-0000-7000-8000-00000000cd16";

#[tokio::test]
async fn mac_typed_call_delivers_an_ending_only_to_its_bound_holder() -> Result<()> {
    let fixture = start_fixture().await?;
    let observed = verify_bound_holder(&fixture).await;
    fixture.close().await?;
    observed
}

async fn verify_bound_holder(fixture: &Fixture) -> Result<()> {
    let mut first = connect(fixture).await?;
    let holder = holder_target(fixture)?;
    expect_not_written(fixture, &mut first, holder.clone()).await?;

    register_run(
        &mut first,
        REGISTER_OTHER_ORG,
        OTHER_ORG.to_owned().try_into()?,
        false,
    )
    .await?;
    expect_not_written(fixture, &mut first, holder.clone()).await?;
    register_run(
        &mut first,
        REGISTER_FIRST,
        fixture.organization_id.clone(),
        true,
    )
    .await?;

    for wrong_target in [
        ApprovalHolderTarget {
            organization_id: OTHER_ORG.to_owned().try_into()?,
            ..holder.clone()
        },
        ApprovalHolderTarget {
            machine_id: OTHER_MACHINE.parse()?,
            ..holder.clone()
        },
        ApprovalHolderTarget {
            enrollment_id: OTHER_ENROLLMENT.to_owned().try_into()?,
            ..holder.clone()
        },
        ApprovalHolderTarget {
            router_run: OTHER_RUN.parse()?,
            ..holder.clone()
        },
    ] {
        expect_not_written(fixture, &mut first, wrong_target).await?;
    }
    let raw = fixture.host.call_machine::<DeliverEndingRpc>(
        &fixture.organization_id,
        &holder.machine_id,
        ending_request(holder.clone())?,
    );
    tokio::select! {
        result = raw => expect_call_failure(result, CallFailure::NeverReached)?,
        frame = receive(&mut first) => return Err(format!("raw ending reached the registered holder: {:?}", frame?).into()),
    }
    socket_barrier(&mut first).await?;
    expect_delivery(fixture, &mut first, holder.clone()).await?;

    let mut replacement = connect(fixture).await?;
    expect_not_written(fixture, &mut replacement, holder.clone()).await?;
    register_run(
        &mut replacement,
        REGISTER_REPLACEMENT,
        fixture.organization_id.clone(),
        true,
    )
    .await?;
    expect_delivery(fixture, &mut replacement, holder).await
}

pub(super) fn holder_target(fixture: &Fixture) -> Result<ApprovalHolderTarget> {
    Ok(ApprovalHolderTarget {
        organization_id: fixture.organization_id.clone(),
        machine_id: INSTALLATION.parse()?,
        enrollment_id: fixture.enrollment_id.clone(),
        router_run: ROUTER_RUN.parse()?,
    })
}

pub(super) fn ending_reply(target: ApprovalHolderTarget) -> Result<MacCallReply> {
    Ok(DeliverEndingRpc::reply(Envelope {
        target,
        body: serde_json::from_value(json!({"kind": "received"}))?,
    }))
}

type EndingRequest = Envelope<ApprovalHolderTarget, <DeliverEndingRpc as Rpc>::Request>;

fn ending_request(target: ApprovalHolderTarget) -> Result<EndingRequest> {
    Ok(Envelope {
        target,
        body: serde_json::from_value(json!({
            "requestId": ENDING_REQUEST,
            "contentsDigest": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "ending": {"kind":"providerApproval","ending":{"kind":"timedOut"}},
        }))?,
    })
}

async fn expect_not_written(
    fixture: &Fixture,
    peer: &mut WebSocket,
    target: ApprovalHolderTarget,
) -> Result<()> {
    let machine = INSTALLATION.parse()?;
    let request = ending_request(target)?;
    let call = fixture.host.fixture_call_machine::<DeliverEndingRpc>(
        &fixture.organization_id,
        &machine,
        request,
    );
    tokio::select! {
        result = call => expect_call_failure(result, CallFailure::NeverReached)?,
        frame = receive(peer) => return Err(format!("denied holder received a frame: {:?}", frame?).into()),
    }
    socket_barrier(peer).await
}

async fn expect_delivery(
    fixture: &Fixture,
    peer: &mut WebSocket,
    holder: ApprovalHolderTarget,
) -> Result<()> {
    let request = ending_request(holder.clone())?;
    let expected = DeliverEndingRpc::request(request.clone());
    let machine = holder.machine_id.clone();
    let call = fixture.host.fixture_call_machine::<DeliverEndingRpc>(
        &fixture.organization_id,
        &machine,
        request,
    );
    let mut call = std::pin::pin!(call);
    let frame = tokio::select! {
        result = &mut call => return Err(format!("delivery ended before a socket frame: {result:?}").into()),
        frame = receive(peer) => frame?,
    };
    let LinkFrame::Data(DataFrame::CallMac {
        correlation_id,
        request,
    }) = frame
    else {
        return Err(format!("expected the holder's call, got {frame:?}").into());
    };
    if *request != expected {
        return Err("the holder's envelope changed on the wire".into());
    }
    send(
        peer,
        &LinkFrame::Data(DataFrame::MacCallReply {
            correlation_id,
            result: Box::new(TypedCallResult::Answered {
                reply: ending_reply(holder.clone())?,
            }),
        }),
    )
    .await?;
    let result = call.await;
    let expected = Ok(TypedCallResult::Answered {
        reply: Envelope {
            target: holder,
            body: serde_json::from_value(json!({"kind": "received"}))?,
        },
    });
    if result != expected {
        return Err(format!("expected the holder's answer {expected:?}, got {result:?}").into());
    }
    Ok(())
}

async fn register_run(
    peer: &mut WebSocket,
    correlation: &str,
    organization_id: OrganizationId,
    accepted: bool,
) -> Result<()> {
    let correlation: central_link_protocol::CorrelationId = correlation.parse()?;
    let target = ApprovalServiceTarget { organization_id };
    let request = RegisterRunRpc::request(Envelope {
        target: target.clone(),
        body: serde_json::from_value(json!({"routerRun": ROUTER_RUN}))?,
    });
    send(
        peer,
        &LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: correlation.clone(),
            request: Box::new(request),
        }),
    )
    .await?;
    let result = if accepted {
        TypedCallResult::Answered {
            reply: RegisterRunRpc::reply(Envelope {
                target,
                body: serde_json::from_value(json!({"kind": "registered"}))?,
            }),
        }
    } else {
        TypedCallResult::Refused {
            reason: LinkCallRefusal::WrongTarget,
        }
    };
    expect_central_reply(peer, correlation, result).await
}

pub(super) async fn connect_other_machine(fixture: &Fixture) -> Result<WebSocket> {
    let runtime = fixture.host.central(&fixture.organization_id).await?;
    let now = fixture.clock.utc_now().timestamp();
    let request = serde_json::from_value(json!({
        "organizationId": fixture.organization_id, "approver": signed::OWNER,
        "permissionChecked": "machine:enroll", "requestId": "019f0000-0000-7000-8000-00000000cd17",
        "machineId": OTHER_MACHINE, "label": "Second socket fixture",
        "allowedWorkspaces": [], "approverSecondFactorAt": now - 60, "codeExpiresAt": now + 600,
    }))?;
    let DeviceLoginDecision::Approved { enrollment_id, .. } =
        runtime.decide_device_login(&request).await?
    else {
        return Err("second fixture machine approval refused".into());
    };
    let grant = serde_json::from_value(json!({
        "organizationId": fixture.organization_id, "enrollmentId": enrollment_id, "clientId": enrollment_id,
    }))?;
    let MachineTokenDecision::Grant {
        grant_expires_at, ..
    } = runtime.decide_machine_token(&grant).await?
    else {
        return Err("second fixture machine grant refused".into());
    };
    let bearer = signed::machine_token(
        signed::ORGANIZATION,
        OTHER_MACHINE,
        enrollment_id.as_str(),
        now,
        grant_expires_at.0,
    )?;
    let mut peer = fixture
        .client()?
        .get(fixture.url(signed::ORGANIZATION))
        .bearer_auth(bearer)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?;
    if !matches!(receive(&mut peer).await?, LinkFrame::Data(DataFrame::Welcome { machine_id, .. }) if machine_id.as_str() == OTHER_MACHINE)
    {
        return Err("second live socket lacks its own machine identity".into());
    }
    Ok(peer)
}
