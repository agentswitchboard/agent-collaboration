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
