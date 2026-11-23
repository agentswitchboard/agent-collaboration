//! Forced C2 applying order on the real link.
use super::{
    ApprovalDestination, ApprovalLinkFixture, Chain, DecideApprovalAsSessionReply,
    DecideApprovalAsSessionRequest, DecideApprovalAsSessionRpc, ParentSetup,
    PassOnApprovalForSessionRpc, PassOnApprovalReply, PassOnApprovalRequest, Result, SqlValue,
    Statement, TypedCallResult, option, renew, typed_id,
};
use central_link_protocol::CorrelationId;

pub async fn pass_crosses_deadline(
    fixture: &impl ApprovalLinkFixture,
    arm: impl std::future::Future<Output = Result<()>>,
    cross_deadline: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    for _ in 0..7 {
        fixture.advance(500).await?;
        renew(&mut setup.socket, fixture.refresh().await?).await?;
    }
    fixture.advance(99).await?;
    let id = setup.approval.request_id().clone();
    arm.await?;
    let (reply, crossed) =
        futures_util::join!(setup.pass(id.clone(), typed_id(0x7128)?), cross_deadline);
    crossed?;
    assert_eq!(reply?, PassOnApprovalReply::TooLate {});
    let (_, offer) =
        crate::approval_link_scenarios::next_ending(&mut setup.socket, &setup.target).await?;
    assert_eq!(offer.request_id(), &id);
    assert_eq!(
        offer.into_ending(),
        approval_request_protocol::Ending::TimedOut {}
    );
    Ok(())
}

pub(super) async fn queued_decide_and_pass(
    fixture: &impl ApprovalLinkFixture,
    setup: &mut ParentSetup,
    id: approval_request_protocol::ApprovalRequestId,
    decide_operation: workspace_ids::RequestId,
    pass_operation: workspace_ids::RequestId,
) -> Result<()> {
    use central_link_protocol::{CentralLinkRpc, DataFrame, LinkFrame, MacCall, MacLinkRpc};
    use fleet_rpc::Envelope;
    let decide_correlation: CorrelationId = typed_id(0x7125)?.as_str().parse()?;
    let pass_correlation: CorrelationId = typed_id(0x7126)?.as_str().parse()?;
    let target = central_link_protocol::approval_rpc::ApprovalServiceTarget {
        organization_id: setup.target.organization_id.clone(),
    };
    let requests = [
        (
            decide_correlation.clone(),
            DecideApprovalAsSessionRpc::request(Envelope {
                target: target.clone(),
                body: setup.parent.request(DecideApprovalAsSessionRequest {
                    request_id: id.clone(),
                    operation_id: decide_operation.clone(),
                    value: option(1)?,
                }),
            }),
        ),
        (
            pass_correlation.clone(),
            PassOnApprovalForSessionRpc::request(Envelope {
                target,
                body: setup.parent.request(PassOnApprovalRequest {
                    request_id: id.clone(),
                    operation_id: pass_operation,
                }),
            }),
        ),
    ];
    for (correlation_id, request) in &requests {
        super::super::send_frame(
            &mut setup.socket,
            LinkFrame::Data(DataFrame::CallCentral {
                correlation_id: correlation_id.clone(),
                request: Box::new(request.clone()),
            }),
        )
        .await?;
    }
    let mut saw_decision = false;
    let mut saw_pass = false;
    let mut saw_ending = false;
    for _ in 0..3 {
        match super::super::receive_frame(&mut setup.socket).await? {
            LinkFrame::Data(DataFrame::CentralCallReply {
                correlation_id,
                result,
            }) => {
                let TypedCallResult::Answered { reply } = *result else {
                    return Err("queued C2 call was refused".into());
                };
                match reply {
                    central_link_protocol::CentralCallReply::DecideApprovalAsSession(result)
                        if correlation_id == decide_correlation && !saw_decision =>
                    {
                        assert!(matches!(
                            result.body,
                            DecideApprovalAsSessionReply::SavedDecision { .. }
                        ));
                        saw_decision = true;
                    }
                    central_link_protocol::CentralCallReply::PassOnApproval(result)
                        if correlation_id == pass_correlation && !saw_pass =>
                    {
                        assert!(matches!(
                            result.body,
                            PassOnApprovalReply::AlreadyDecided { .. }
                                | PassOnApprovalReply::PassedOn {
                                    to: ApprovalDestination::People {}
                                }
                        ));
                        saw_pass = true;
                    }
                    _ => return Err("queued C2 call returned a wrong or duplicate reply".into()),
                }
            }
            LinkFrame::Data(DataFrame::CallMac {
                correlation_id,
                request,
            }) if !saw_ending => {
                let MacCall::DeliverEnding(ending) = *request else {
                    return Err("queued C2 transition did not deliver its ending".into());
                };
                assert_eq!(ending.body.request_id(), &id);
                super::super::send_frame(&mut setup.socket, LinkFrame::Data(DataFrame::MacCallReply {
                    correlation_id, result: Box::new(TypedCallResult::Answered {
                        reply: central_link_protocol::approval_rpc::DeliverEndingRpc::reply(Envelope {
                            target: ending.target, body: approval_request_protocol::DeliverEndingReply::Received {},
                        }),
                    }),
                })).await?;
                saw_ending = true;
            }
            _ => return Err("queued C2 calls returned an unexpected frame".into()),
        }
    }
    assert!(saw_decision && saw_pass && saw_ending);
    let stored = fixture
        .read(Statement {
            sql: "SELECT decided_operation_id FROM approval_requests WHERE request_id=?",
            args: vec![SqlValue::Text(id.as_str().to_owned())],
        })
        .await?;
    assert_eq!(
        stored.rows,
        [[SqlValue::Text(decide_operation.as_str().to_owned())]]
    );
    Ok(())
}

use super::super::{
    ParentApprovalPause, PauseControl, SessionProof, acknowledge_notice, answered,
    seed_delegate_edge, text,
};
use crate::approval_link_scenarios::{
    EnrollmentLoginEnd, answer_ending, heartbeat, next_ending, observe_received, raise,
    register_run, require_receipt, stop, stop_request,
};
use crate::approval_provider_scenarios::{ProviderAsk, create_bound, provider_raise};
use crate::session_link_scenarios::{answer as session_answer, call as session_call, end};
use approval_request_protocol::{
    Answer, ApprovalNoticeReply, CentralTime, DecidedBy, DeliverEndingReply, Ending, Raise,
    StopReceiptEnding, StopReply,
};
use central_link_protocol::session_lifecycle_rpc::EndFleetSessionRpc;
use reqwest_websocket::WebSocket;

pub trait CompetingApprovalFixture: ApprovalLinkFixture {
    fn connect_competing_machine(
        &self,
    ) -> impl std::future::Future<
        Output = Result<(
            central_link_protocol::approval_rpc::ApprovalHolderTarget,
            WebSocket,
        )>,
    >;
    fn refresh_competing_machine(
        &self,
        target: &central_link_protocol::approval_rpc::ApprovalHolderTarget,
    ) -> impl std::future::Future<Output = Result<String>>;
    fn redeliver_approval_alarm(&self) -> impl std::future::Future<Output = Result<()>>;
}

#[derive(Clone, Copy)]
pub enum DecisionCompetitor {
    PassOn,
    Stop,
    Deadline,
    EnrollmentEnd,
    SessionEnd,
}
#[derive(Clone, Copy)]
pub enum DecisionCommitOrder {
    SessionFirst,
    CompetitorFirst,
}

pub async fn decide_against<F: std::future::Future<Output = Result<()>>>(
    fixture: &impl CompetingApprovalFixture,
    control: impl Fn(ParentApprovalPause, PauseControl) -> F,
    competitor: DecisionCompetitor,
    order: DecisionCommitOrder,
) -> Result<()> {
    let holder = fixture.target()?;
    let mut holder_socket = fixture.connect().await?;
    let (other_machine, mut parent_socket) = fixture.connect_competing_machine().await?;
    let organization = &holder.organization_id;
    register_run(&mut holder_socket, organization, &holder.router_run).await?;
    let (parent_id, parent_secret) =
        create_bound(&mut parent_socket, organization, 0x28, None).await?;
    let parent = SessionProof {
        session_id: parent_id,
        secret: parent_secret,
    };
    let (child_id, _) = create_bound(&mut holder_socket, organization, 0x29, None).await?;
    seed_delegate_edge(fixture, &child_id, &parent.session_id).await?;
    let next = if matches!(competitor, DecisionCompetitor::PassOn) {
        let (session_id, secret) =
            create_bound(&mut holder_socket, organization, 0x2a, None).await?;
        seed_delegate_edge(fixture, &parent.session_id, &session_id).await?;
        Some(SessionProof { session_id, secret })
    } else {
        None
    };
    let request = provider_raise(&holder, &child_id, 0x2b, ProviderAsk::Approval)?;
    let id = request.request_id();
    let window = require_receipt(raise(&mut holder_socket, organization, request.clone()).await?)?;
    acknowledge_notice(
        &mut parent_socket,
        &other_machine,
        &parent.session_id,
        id,
        ApprovalNoticeReply::Received {},
    )
    .await?;
    if let Some(next) = &next {
        let pass = answered(
            super::call::<PassOnApprovalForSessionRpc>(
                &mut parent_socket,
                organization,
                parent.request(PassOnApprovalRequest {
                    request_id: id.clone(),
                    operation_id: typed_id(0x7291)?,
                }),
            )
            .await?,
        )?;
        assert_eq!(
            pass,
            PassOnApprovalReply::PassedOn {
                to: ApprovalDestination::Session {
                    session_id: next.session_id.clone()
                }
            }
        );
        acknowledge_notice(
            &mut holder_socket,
            &holder,
            &next.session_id,
            id,
            ApprovalNoticeReply::Received {},
        )
        .await?;
    }

    if matches!(competitor, DecisionCompetitor::Deadline) {
        for _ in 0..7 {
            fixture.advance(500).await?;
            renew(&mut holder_socket, fixture.refresh().await?).await?;
            renew(
                &mut parent_socket,
                fixture.refresh_competing_machine(&other_machine).await?,
            )
            .await?;
        }
        fixture.advance(99).await?;
    }
    let applying_time =
        CentralTime::from_timestamp(fixture.now_seconds(), 0).ok_or("fixture time")?;
    let session_first = matches!(order, DecisionCommitOrder::SessionFirst);
    let pause = if session_first {
        ParentApprovalPause::BeforeReply
    } else {
        ParentApprovalPause::BeforeApply
    };
    control(pause, PauseControl::Arm).await?;
    let operation = typed_id(0x7292)?;
    let decision = super::call::<DecideApprovalAsSessionRpc>(
        &mut parent_socket,
        organization,
        parent.request(DecideApprovalAsSessionRequest {
            request_id: id.clone(),
            operation_id: operation.clone(),
            value: option(1)?,
        }),
    );
    let mut decision = std::pin::pin!(decision);
    let reached = control(pause, PauseControl::Wait);
    match futures_util::future::select(decision.as_mut(), std::pin::pin!(reached)).await {
        futures_util::future::Either::Left((answer, _)) => {
            return Err(format!("decision returned before applying hold: {:?}", answer?).into());
        }
        futures_util::future::Either::Right((held, _)) => held?,
    }
    let decider = DecidedBy::Session {
        session: parent.session_id.clone(),
    };
    let winner = if session_first || matches!(competitor, DecisionCompetitor::PassOn) {
        let Answer::Approval { option_id } = option(1)? else {
            return Err("expected fixture option".into());
        };
        Ending::Decided {
            answer: approval_request_protocol::ProviderAnswer::Approval { option_id },
            by: decider.clone(),
        }
    } else {
        match competitor {
            DecisionCompetitor::Stop => Ending::Cancelled {
                reason: approval_request_protocol::CancelReason::Stopped {
                    cause: approval_request_protocol::HolderStopCause::TurnStopped,
                },
            },
            DecisionCompetitor::Deadline => Ending::TimedOut {},
            DecisionCompetitor::EnrollmentEnd => Ending::Cancelled {
                reason: approval_request_protocol::CancelReason::EnrollmentEnded {},
            },
            DecisionCompetitor::SessionEnd => Ending::Cancelled {
                reason: approval_request_protocol::CancelReason::SessionEnded {},
            },
            DecisionCompetitor::PassOn => {
                return Err("pass-on retains the parent as a decider".into());
            }
        }
    };
    if session_first {
        receive_only_ending(fixture, &holder, &mut holder_socket, &request, &winner).await?;
    }
    match competitor {
        DecisionCompetitor::PassOn => {
            let session = next.as_ref().ok_or("missing pass-on competitor")?;
            let pass = answered(
                super::call::<PassOnApprovalForSessionRpc>(
                    &mut holder_socket,
                    organization,
                    session.request(PassOnApprovalRequest {
                        request_id: id.clone(),
                        operation_id: typed_id(0x7293)?,
                    }),
                )
                .await?,
            )?;
            assert_eq!(
                pass,
                if session_first {
                    PassOnApprovalReply::AlreadyDecided {
                        by: decider,
                        at: applying_time,
                    }
                } else {
                    PassOnApprovalReply::PassedOn {
                        to: ApprovalDestination::People {},
                    }
                }
            );
        }
        DecisionCompetitor::Stop => {
            assert_eq!(
                stop(&mut holder_socket, organization, stop_request(&request)?).await?,
                TypedCallResult::Answered {
                    reply: StopReply::Stands {
                        ending: if session_first {
                            StopReceiptEnding::DecidedEarlier {}
                        } else {
                            StopReceiptEnding::Cancelled {
                                reason: approval_request_protocol::CancelReason::Stopped {
                                    cause: approval_request_protocol::HolderStopCause::TurnStopped,
                                },
                            }
                        }
                    }
                }
            );
        }
        DecisionCompetitor::Deadline => {
            fixture.advance(1).await?;
            let remaining = u64::try_from((-fixture.now_seconds()).rem_euclid(300))?;
            if remaining > 0 {
                fixture.advance(remaining).await?;
            }
            if session_first {
                fixture.redeliver_approval_alarm().await?;
            } else {
                fixture.observe_due_alarm().await?;
            }
        }
        DecisionCompetitor::EnrollmentEnd => {
            fixture
                .end_enrollment_through_login(EnrollmentLoginEnd::Revoke)
                .await?
        }
        DecisionCompetitor::SessionEnd => {
            let ended =
                session_call::<EndFleetSessionRpc>(&mut holder_socket, organization, end(child_id))
                    .await?;
            assert!(matches!(
                session_answer::<EndFleetSessionRpc>(ended, organization)?,
                fleet_identity_protocol::EndFleetSessionReply::Settled {
                    settlement: fleet_identity_protocol::NewEndSettlement::Ended { .. }
                }
            ));
        }
    }
    control(pause, PauseControl::Release).await?;
    let reply = decision.await?;
    let decision_won = matches!(&winner, Ending::Decided { .. });
    let Answer::Approval { option_id } = option(1)? else {
        return Err("expected fixture option".into());
    };
    assert_eq!(
        reply,
        TypedCallResult::Answered {
            reply: if decision_won {
                DecideApprovalAsSessionReply::SavedDecision {
                    decision: approval_request_protocol::ApprovalDecision {
                        answer: approval_request_protocol::ProviderAnswer::Approval { option_id },
                        by: approval_request_protocol::SessionDecider::Session {
                            session: parent.session_id.clone(),
                        },
                    },
                }
            } else {
                DecideApprovalAsSessionReply::TooLate {}
            }
        }
    );
    if !session_first
        && !matches!(
            competitor,
            DecisionCompetitor::Stop | DecisionCompetitor::EnrollmentEnd
        )
    {
        receive_only_ending(fixture, &holder, &mut holder_socket, &request, &winner).await?;
    }
    let stored = fixture.read(Statement {
        sql: "SELECT record_state,ending_json,decided_operation_id,deadline_ms,delivery_state FROM approval_requests WHERE request_id=?",
        args: vec![text(id.as_str())],
    }).await?;
    let [row] = stored.rows.as_slice() else {
        return Err("expected one committed approval record".into());
    };
    let [
        SqlValue::Text(state),
        SqlValue::Text(delivery),
        decided,
        deadline,
        SqlValue::Text(delivery_state),
    ] = row.as_slice()
    else {
        return Err("expected complete terminal approval".into());
    };
    assert_eq!(state, "ended");
    let stored_ending: approval_request_protocol::DeliverEndingRequest =
        serde_json::from_str(delivery)?;
    assert_eq!(stored_ending.into_ending(), winner);
    assert_eq!(
        decided,
        &if decision_won {
            text(operation.as_str())
        } else {
            SqlValue::Null
        }
    );
    assert_eq!(
        deadline,
        &SqlValue::Integer(central_storage::SafeInteger::new(
            window.window.deadline().timestamp_millis()
        )?)
    );
    if matches!(competitor, DecisionCompetitor::EnrollmentEnd) && !session_first {
        assert_eq!(delivery_state, "closed_without_receipt");
    } else if matches!(competitor, DecisionCompetitor::Stop) && !session_first {
        assert_eq!(delivery_state, "closed_without_receipt");
        heartbeat(&mut holder_socket).await?;
    } else {
        assert_eq!(delivery_state, "received_by_holder");
        if !matches!(competitor, DecisionCompetitor::EnrollmentEnd) {
            heartbeat(&mut holder_socket).await?;
        }
    }
    Ok(())
}

async fn receive_only_ending(
    fixture: &impl ApprovalLinkFixture,
    holder: &central_link_protocol::approval_rpc::ApprovalHolderTarget,
    socket: &mut WebSocket,
    request: &Raise,
    winner: &Ending,
) -> Result<()> {
    let (correlation, delivery) = next_ending(socket, holder).await?;
    assert_eq!(delivery.request_id(), request.request_id());
    assert_eq!(&delivery.into_ending(), winner);
    fixture.hold_delivery_answer().await?;
    answer_ending(
        socket,
        holder.clone(),
        correlation,
        DeliverEndingReply::Received {},
    )
    .await?;
    fixture.observe_delivery_answer().await?;
    observe_received(fixture, request).await?;
    heartbeat(socket).await
}
