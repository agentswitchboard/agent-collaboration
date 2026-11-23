//! The actual Workerd socket owns the destination and each pending response.

use super::super::{fixture, renewal};
use super::{
    ConnectedLink, Fixture, MACHINE_ID, Result, assert_mac_failure, assert_probe_answer,
    central_probe, central_reply, invoke_mac, mac_probe, reply_to_mac_probe,
    workerd_control_http_client,
};
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, CorrelationId, DataFrame, LinkCallRefusal, MacCall,
    MacLinkRpc, TokenRenewalResult, TypedCallResult,
    approval_rpc::{ApprovalHolderTarget, ApprovalServiceTarget, DeliverEndingRpc, RegisterRunRpc},
    test_calls::{MacProbe, ProbeReply},
};
use collaboration_central_conformance::signin_fixtures as signed;
use fleet_contracts::MachineTokenDecision;
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::{Envelope, Rpc};
use futures_util::StreamExt;
use reqwest::{Response, StatusCode};
use reqwest_websocket::{Message, RequestBuilderExt, WebSocket};
use serde_json::json;
use workspace_ids::RouterRunId;

const RUN: &str = "019f0000-0000-7000-8000-00000000d701";
const OTHER_RUN: &str = "019f0000-0000-7000-8000-00000000d702";
const OTHER_MACHINE: &str = "019f0000-0000-7000-8000-00000000d703";
const OTHER_ENROLLMENT: &str = "019f0000-0000-7000-8000-00000000d704";
const ENDING: &str = "019f0000-0000-7000-8000-00000000d705";
const ENDING_DIGEST: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

type EndingRequest = <DeliverEndingRpc as Rpc>::Request;
type EndingReply = <DeliverEndingRpc as Rpc>::Reply;

fn holder(fixture: &Fixture) -> Result<ApprovalHolderTarget> {
    Ok(ApprovalHolderTarget {
        organization_id: fixture.organization.clone(),
        machine_id: MACHINE_ID.parse()?,
        enrollment_id: fixture.enrollment.clone(),
        router_run: RUN.parse()?,
    })
}

fn ending() -> Result<EndingRequest> {
    Ok(serde_json::from_value(json!({
        "requestId": ENDING, "contentsDigest": ENDING_DIGEST,
        "ending": {"kind": "providerApproval", "ending": {"kind": "timedOut"}},
    }))?)
}

fn received() -> Result<EndingReply> {
    Ok(serde_json::from_value(json!({"kind": "received"}))?)
}

async fn register_run(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    run: &RouterRunId,
    correlation: &str,
) -> Result<TypedCallResult<CentralCallReply>> {
    let correlation_id: CorrelationId = correlation.parse()?;
    let body =
        serde_json::from_value::<<RegisterRunRpc as Rpc>::Request>(json!({"routerRun": run}))?;
    fixture::send(
        socket,
        &DataFrame::CallCentral {
            correlation_id: correlation_id.clone(),
            request: Box::new(RegisterRunRpc::request(Envelope {
                target: ApprovalServiceTarget {
                    organization_id: organization.clone(),
                },
                body,
            })),
        },
    )
    .await?;
    central_reply(socket, &correlation_id).await
}

async fn invoke_ending(fixture: &Fixture, target: &ApprovalHolderTarget) -> Result<Response> {
    invoke_ending_on(fixture, target, "typed-ending-call").await
}

async fn invoke_ending_on(
    fixture: &Fixture,
    target: &ApprovalHolderTarget,
    operation: &str,
) -> Result<Response> {
    Ok(workerd_control_http_client()
        .post(format!(
            "{}/test-support/workspace/{}/{operation}",
            fixture.host.public_origin(),
            fixture.organization.as_str(),
        ))
        .json(&json!({
            "machineId": MACHINE_ID,
            "request": Envelope { target: target.clone(), body: ending()? },
        }))
        .send()
        .await?)
}

async fn require_no_ending(
    fixture: &Fixture,
    socket: &mut WebSocket,
    target: &ApprovalHolderTarget,
    sequence: u32,
) -> Result<()> {
    assert_mac_failure(
        invoke_ending(fixture, target).await?,
        StatusCode::CONFLICT,
        "neverReached",
    )
    .await?;
    // A completed refusal is ordered before this round trip on the same socket.
    assert!(matches!(
        central_probe(socket, &MACHINE_ID.parse()?, sequence).await?,
        TypedCallResult::Answered { .. }
    ));
    Ok(())
}

async fn require_ending(
    fixture: &Fixture,
    socket: &mut WebSocket,
    target: &ApprovalHolderTarget,
) -> Result<()> {
    let drive = async {
        let frame = fixture::receive(socket).await?;
        let DataFrame::CallMac {
            correlation_id,
            request,
        } = frame
        else {
            return Err(format!("expected a private ending, got {frame:?}").into());
        };
        let MacCall::DeliverEnding(call) = *request else {
            return Err("expected the DeliverEnding marker".into());
        };
        assert_eq!(&call.target, target);
        assert_eq!(call.body, ending()?);
        fixture::send(
            socket,
            &DataFrame::MacCallReply {
                correlation_id,
                result: Box::new(TypedCallResult::Answered {
                    reply: DeliverEndingRpc::reply(Envelope {
                        target: target.clone(),
                        body: received()?,
                    }),
                }),
            },
        )
        .await
    };
    let (_, result) = tokio::try_join!(drive, invoke_ending(fixture, target))?;
    assert_eq!(
        result
            .error_for_status()?
            .json::<Envelope<ApprovalHolderTarget, EndingReply>>()
            .await?,
        Envelope {
            target: target.clone(),
            body: received()?
        },
    );
    Ok(())
}

#[tokio::test]
async fn the_exact_registered_holder_survives_renewal_and_idle_eviction_on_its_socket() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let result = exercise_run_binding(&fixture).await;
    fixture.close().await?;
    result
}

async fn exercise_run_binding(fixture: &Fixture) -> Result<()> {
    let target = holder(fixture)?;
    let mut owner = ConnectedLink::open(fixture).await?;
    require_no_ending(fixture, &mut owner.socket, &target, 201).await?;
    let different_org: OrganizationId = fixture::OTHER_ORGANIZATION.to_owned().try_into()?;
    assert_eq!(
        register_run(
            &mut owner.socket,
            &different_org,
            &target.router_run,
            "019f0000-0000-7000-8000-00000000d712"
        )
        .await?,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::WrongTarget
        },
    );
    require_no_ending(fixture, &mut owner.socket, &target, 202).await?;
    assert!(
        matches!(register_run(&mut owner.socket, &fixture.organization, &target.router_run,
        "019f0000-0000-7000-8000-00000000d713").await?,
        TypedCallResult::Answered { reply: CentralCallReply::RegisterRun(reply) }
            if serde_json::to_value(&reply.body)? == json!({"kind":"registered"}))
    );
    for (offset, wrong_target) in [
        ApprovalHolderTarget {
            organization_id: different_org,
            ..target.clone()
        },
        ApprovalHolderTarget {
            machine_id: OTHER_MACHINE.parse()?,
            ..target.clone()
        },
        ApprovalHolderTarget {
            enrollment_id: OTHER_ENROLLMENT.to_owned().try_into()?,
            ..target.clone()
        },
        ApprovalHolderTarget {
            router_run: OTHER_RUN.parse()?,
            ..target.clone()
        },
    ]
    .iter()
    .enumerate()
    {
        require_no_ending(
            fixture,
            &mut owner.socket,
            wrong_target,
            u32::try_from(offset)? + 203,
        )
        .await?;
    }
    assert_mac_failure(
        invoke_ending_on(fixture, &target, "typed-ending-production-ingress").await?,
        StatusCode::CONFLICT,
        "neverReached",
    )
    .await?;
    collaboration_central_conformance::approval_link_scenarios::heartbeat(&mut owner.socket)
        .await?;
    require_ending(fixture, &mut owner.socket, &target).await?;

    renewal::advance(fixture, 300).await?;
    let renewed_token = signed::machine_token(
        fixture::ORGANIZATION,
        MACHINE_ID,
        fixture.enrollment.as_str(),
        signed::NOW_SECONDS + 300,
        signed::NOW_SECONDS + 900,
    )?;
    let renewal: CorrelationId = "019f0000-0000-7000-8000-00000000d714".parse()?;
    fixture::send(
        &mut owner.socket,
        &DataFrame::RenewMachineToken {
            correlation_id: renewal.clone(),
            token: renewed_token.clone().try_into()?,
        },
    )
    .await?;
    assert!(
        matches!(fixture::receive(&mut owner.socket).await?, DataFrame::MachineTokenRenewal {
        correlation_id, result: TokenRenewalResult::Renewed { .. }
    } if correlation_id == renewal)
    );
    fixture.host.c2_evict(&fixture.organization).await?;
    renewal::advance(fixture, 301).await?;
    require_ending(fixture, &mut owner.socket, &target).await?;

    let mut replacement = open_socket(fixture, &renewed_token).await?;
    assert!(matches!(
        owner.socket.next().await,
        Some(Ok(Message::Close { .. }))
    ));
    require_no_ending(fixture, &mut replacement, &target, 207).await?;
    let replacement_target = ApprovalHolderTarget {
        router_run: OTHER_RUN.parse()?,
        ..target
    };
    assert!(matches!(
        register_run(
            &mut replacement,
            &fixture.organization,
            &replacement_target.router_run,
            "019f0000-0000-7000-8000-00000000d715"
        )
        .await?,
        TypedCallResult::Answered {
            reply: CentralCallReply::RegisterRun(_)
        }
    ));
    require_ending(fixture, &mut replacement, &replacement_target).await
}

async fn open_socket(fixture: &Fixture, token: &str) -> Result<WebSocket> {
    let mut socket = workerd_control_http_client()
        .get(fixture.link_url(fixture::ORGANIZATION))
        .bearer_auth(token)
        .upgrade()
        .send()
        .await?
        .into_websocket()
        .await?;
    assert!(matches!(
        fixture::receive(&mut socket).await?,
        DataFrame::Welcome { .. }
    ));
    Ok(socket)
}

async fn second_socket(fixture: &Fixture) -> Result<WebSocket> {
    let client = workerd_control_http_client();
    let enrollment = fixture::approve_machine(
        &fixture.host,
        &client,
        OTHER_MACHINE,
        "019f0000-0000-7000-8000-00000000d716",
        signed::NOW_SECONDS,
    )
    .await?;
    let MachineTokenDecision::Grant {
        machine_id,
        grant_expires_at,
        ..
    } = fixture::grant(&fixture.host, &client, &enrollment).await?
    else {
        return Err("second fixture machine did not receive its grant".into());
    };
    assert_eq!(machine_id.as_str(), OTHER_MACHINE);
    let token = signed::machine_token(
        fixture::ORGANIZATION,
        OTHER_MACHINE,
        enrollment.as_str(),
        signed::NOW_SECONDS,
        grant_expires_at.0,
    )?;
    open_socket(fixture, &token).await
}

#[tokio::test]
async fn a_pending_call_accepts_only_the_matching_answer_from_its_socket() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = exercise_reply_matching(&fixture).await;
    fixture.close().await?;
    result
}

async fn exercise_reply_matching(fixture: &Fixture) -> Result<()> {
    let mut owner = ConnectedLink::open(fixture).await?;
    let mut second = second_socket(fixture).await?;
    let ((), answer) = tokio::try_join!(
        async {
            let correlation_id = mac_probe(&mut owner.socket, 208).await?;
            fixture::send(
                &mut owner.socket,
                &DataFrame::MacCallReply {
                    correlation_id: correlation_id.clone(),
                    result: Box::new(TypedCallResult::Answered {
                        reply: DeliverEndingRpc::reply(Envelope {
                            target: holder(fixture)?,
                            body: received()?,
                        }),
                    }),
                },
            )
            .await?;
            fixture::send(
                &mut owner.socket,
                &DataFrame::MacCallReply {
                    correlation_id: correlation_id.clone(),
                    result: Box::new(TypedCallResult::Answered {
                        reply: MacProbe::reply(Envelope {
                            target: OTHER_MACHINE.parse()?,
                            body: ProbeReply { echoed: 100 },
                        }),
                    }),
                },
            )
            .await?;
            reply_to_mac_probe(&mut second, correlation_id.clone(), 101).await?;
            assert!(matches!(
                central_probe(&mut second, &OTHER_MACHINE.parse()?, 209).await?,
                TypedCallResult::Answered { .. }
            ));
            assert!(matches!(
                central_probe(&mut owner.socket, &MACHINE_ID.parse()?, 210).await?,
                TypedCallResult::Answered { .. }
            ));
            assert!(
                fixture
                    .runtime()
                    .control::<bool>("typed-mac-waiting", &correlation_id)
                    .await?
            );
            reply_to_mac_probe(&mut owner.socket, correlation_id, 208).await
        },
        invoke_mac(fixture, 208)
    )?;
    assert_probe_answer(answer, 208).await?;

    let ((), refused) = tokio::try_join!(
        async {
            let correlation_id = mac_probe(&mut owner.socket, 211).await?;
            fixture::send(
                &mut owner.socket,
                &DataFrame::MacCallReply {
                    correlation_id,
                    result: Box::new(TypedCallResult::Refused {
                        reason: LinkCallRefusal::WrongTarget,
                    }),
                },
            )
            .await
        },
        invoke_mac(fixture, 211)
    )?;
    assert_eq!(
        refused
            .error_for_status()?
            .json::<TypedCallResult<()>>()
            .await?,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::WrongTarget
        }
    );
    owner.close().await
}
