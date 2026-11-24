#[path = "parent_approval_races.rs"]
mod races;
pub use races::pass_crosses_deadline;
pub use races::{
    CompetingApprovalFixture, DecisionCommitOrder, DecisionCompetitor, decide_against,
};

use super::{
    ABSENT_REQUEST, Answer, ApprovalLinkFixture, ApprovalNoticeReply, Chain,
    DecideApprovalAsSessionReply, DecideApprovalAsSessionRequest, DecideApprovalAsSessionRpc,
    LinkCallRefusal, ListApprovalsForSessionReply, ListApprovalsForSessionRequest,
    ListApprovalsForSessionRpc, ParentSetup, PassOnApprovalForSessionRpc, PassOnApprovalReply,
    PassOnApprovalRequest, ReadApprovalForSessionReply, ReadApprovalForSessionRequest,
    ReadApprovalForSessionRpc, Result, SessionProof, SqlValue, Statement, TypedCallResult,
    acknowledge_notice, call, first_read, heartbeat, option, typed_id,
};
use crate::{
    approval_link_scenarios::{raise, renew, require_receipt},
    approval_provider_scenarios::{ProviderAsk, provider_raise},
};
use approval_request_protocol::{ApprovalDestination, CentralTime, DecidedBy, SessionCallDecision};
use reqwest_websocket::CloseCode;

#[derive(Clone, Copy)]
pub enum Case {
    ListPage,
    BadListProof,
    ReadRequest,
    ReadNotVisible,
    BadReadProof,
    DecideSaved,
    DecideAlreadyDecided,
    DecideTooLate,
    DecidePersonRequired,
    DecideHeldPersonRequired,
    DecideInvalid,
    DecideNotVisible,
    DecideConflict,
    BadDecideProof,
    PassToSession,
    PassToPeople,
    PassAlreadyDecided,
    PassTooLate,
    PassNotVisible,
    PassConflict,
    BadPassProof,
    EarlierDestinationStillDecides,
    ThreeLevels,
    EndedParentRetainsRequest,
    NoticeReconnect,
    NoticeEnds,
    NoticeNotHere,
    DecideAndPass,
}

pub async fn observe_case(fixture: &impl ApprovalLinkFixture, case: Case) -> Result<()> {
    let chain = match case {
        Case::ThreeLevels | Case::PassToSession | Case::EarlierDestinationStillDecides => {
            Chain::ThreeLevels
        }
        Case::EndedParentRetainsRequest => Chain::EndedParent,
        Case::DecidePersonRequired => Chain::KeptOption,
        _ => Chain::Direct,
    };
    let mut setup = ParentSetup::open(fixture, chain).await?;
    let id = setup.approval.request_id().clone();
    let absent = ABSENT_REQUEST.parse()?;
    let operation = typed_id(0x7103)?;
    let second = typed_id(0x7104)?;
    match case {
        Case::ListPage => {
            let ListApprovalsForSessionReply::Page { items, next } = setup.list().await?;
            assert!(matches!(items.as_slice(), [summary] if summary.request_id == id));
            assert_eq!(next, None);
            assert_eq!(
                first_read(fixture, &setup.parent.session_id, &id).await?,
                [[SqlValue::Null]]
            );
        }
        Case::ReadRequest => {
            let ReadApprovalForSessionReply::Request { view } = setup.read(id.clone()).await?
            else {
                return Err("visible C2 request was not returned".into());
            };
            assert_eq!(view.request_id(), &id);
            assert!(
                matches!(first_read(fixture, &setup.parent.session_id, &id).await?.as_slice(),
                [row] if matches!(row.as_slice(), [SqlValue::Text(_)]))
            );
        }
        Case::ReadNotVisible => {
            use std::mem::swap;
            swap(&mut setup.parent, &mut setup.child);
            assert_eq!(
                setup.read(id).await?,
                ReadApprovalForSessionReply::NotVisible {}
            );
            assert_eq!(
                setup.read(absent).await?,
                ReadApprovalForSessionReply::NotVisible {}
            );
        }
        Case::BadListProof | Case::BadReadProof | Case::BadDecideProof | Case::BadPassProof => {
            bad_proof(fixture, &mut setup, case, id, operation).await?;
        }
        Case::DecideSaved => {
            let reply = setup
                .decide(id.clone(), operation.clone(), option(1)?)
                .await?;
            assert!(matches!(
                reply,
                DecideApprovalAsSessionReply::SavedDecision { .. }
            ));
            acknowledge_ending(&mut setup).await?;
            assert_eq!(setup.decide(id, operation, option(1)?).await?, reply);
        }
        Case::DecideAlreadyDecided | Case::PassAlreadyDecided => {
            setup.decide(id.clone(), operation, option(1)?).await?;
            acknowledge_ending(&mut setup).await?;
            let by = DecidedBy::Session {
                session: setup.parent.session_id.clone(),
            };
            let at = CentralTime::from_timestamp_millis(fixture.now_seconds() * 1000)
                .ok_or("case time")?;
            if matches!(case, Case::DecideAlreadyDecided) {
                assert_eq!(
                    setup.decide(id, second, option(1)?).await?,
                    DecideApprovalAsSessionReply::AlreadyDecided { by, at }
                );
            } else {
                assert_eq!(
                    setup.pass(id, second).await?,
                    PassOnApprovalReply::AlreadyDecided { by, at }
                );
            }
        }
        Case::DecideTooLate | Case::PassTooLate => {
            reach_hour(fixture, &mut setup).await?;
            if matches!(case, Case::DecideTooLate) {
                assert_eq!(
                    setup.decide(id, operation, option(1)?).await?,
                    DecideApprovalAsSessionReply::TooLate {}
                );
            } else {
                assert_eq!(
                    setup.pass(id, operation).await?,
                    PassOnApprovalReply::TooLate {}
                );
            }
        }
        Case::DecidePersonRequired | Case::DecideHeldPersonRequired => {
            let before = floor_and_log(fixture).await?;
            fixture.advance(1).await?;
            let value = if matches!(case, Case::DecideHeldPersonRequired) {
                Answer::UnprovenCall {
                    decision: SessionCallDecision::Allow,
                }
            } else {
                option(1)?
            };
            assert_eq!(
                setup.decide(id, operation, value).await?,
                DecideApprovalAsSessionReply::PersonRequired {}
            );
            assert_eq!(floor_and_log(fixture).await?, before);
        }
        Case::DecideInvalid => {
            let Answer::Approval { option_id } = option(2)? else {
                return Err("expected option fixture".into());
            };
            assert_eq!(
                setup.decide(id, operation, option(2)?).await?,
                DecideApprovalAsSessionReply::Invalid {
                    reason: approval_request_protocol::InvalidAnswer::UnofferedOption { option_id },
                }
            );
        }
        Case::DecideNotVisible => {
            std::mem::swap(&mut setup.parent, &mut setup.child);
            assert_eq!(
                setup.decide(id, operation.clone(), option(1)?).await?,
                DecideApprovalAsSessionReply::NotVisible {}
            );
            assert_eq!(
                setup.decide(absent, operation, option(1)?).await?,
                DecideApprovalAsSessionReply::NotVisible {}
            );
        }
        Case::DecideConflict => {
            setup
                .decide(id.clone(), operation.clone(), option(1)?)
                .await?;
            acknowledge_ending(&mut setup).await?;
            assert_eq!(
                setup.decide(id, operation, option(2)?).await?,
                DecideApprovalAsSessionReply::OperationConflict {}
            );
        }
        Case::PassToPeople => {
            let passed = PassOnApprovalReply::PassedOn {
                to: ApprovalDestination::People {},
            };
            assert_eq!(setup.pass(id.clone(), operation.clone()).await?, passed);
            assert_eq!(setup.pass(id, operation).await?, passed);
        }
        Case::PassToSession | Case::ThreeLevels => {
            let next = setup
                .grandparent
                .as_ref()
                .ok_or("missing grandparent")?
                .session_id
                .clone();
            assert_eq!(
                setup.pass(id.clone(), operation).await?,
                PassOnApprovalReply::PassedOn {
                    to: ApprovalDestination::Session {
                        session_id: next.clone()
                    }
                }
            );
            acknowledge_notice(
                &mut setup.socket,
                &setup.target,
                &next,
                &id,
                ApprovalNoticeReply::Received {},
            )
            .await?;
            if matches!(case, Case::ThreeLevels) {
                setup.parent = setup.grandparent.take().ok_or("missing grandparent")?;
                assert_eq!(
                    setup.pass(id, second).await?,
                    PassOnApprovalReply::PassedOn {
                        to: ApprovalDestination::People {}
                    }
                );
            }
        }
        Case::PassNotVisible => {
            std::mem::swap(&mut setup.parent, &mut setup.child);
            assert_eq!(
                setup.pass(id, operation.clone()).await?,
                PassOnApprovalReply::NotVisible {}
            );
            assert_eq!(
                setup.pass(absent, operation).await?,
                PassOnApprovalReply::NotVisible {}
            );
        }
        Case::PassConflict => {
            setup.pass(id, operation.clone()).await?;
            let ask = provider_raise(
                &setup.target,
                &setup.child.session_id,
                7,
                ProviderAsk::Approval,
            )?;
            require_receipt(
                raise(
                    &mut setup.socket,
                    &setup.target.organization_id,
                    ask.clone(),
                )
                .await?,
            )?;
            acknowledge_notice(
                &mut setup.socket,
                &setup.target,
                &setup.parent.session_id,
                ask.request_id(),
                ApprovalNoticeReply::Received {},
            )
            .await?;
            assert_eq!(
                setup.pass(ask.request_id().clone(), operation).await?,
                PassOnApprovalReply::OperationConflict {}
            );
        }
        Case::EarlierDestinationStillDecides => {
            let next = setup
                .grandparent
                .as_ref()
                .ok_or("missing grandparent")?
                .session_id
                .clone();
            assert_eq!(
                setup.pass(id.clone(), operation).await?,
                PassOnApprovalReply::PassedOn {
                    to: ApprovalDestination::Session {
                        session_id: next.clone()
                    }
                }
            );
            acknowledge_notice(
                &mut setup.socket,
                &setup.target,
                &next,
                &id,
                ApprovalNoticeReply::Received {},
            )
            .await?;
            assert!(matches!(
                setup.read(id.clone()).await?,
                ReadApprovalForSessionReply::Request { .. }
            ));
            assert!(matches!(
                setup.decide(id, second, option(1)?).await?,
                DecideApprovalAsSessionReply::SavedDecision { .. }
            ));
        }
        Case::EndedParentRetainsRequest => {
            observe_ended_parent_timeout(fixture, &mut setup, &id).await?
        }
        Case::NoticeReconnect | Case::NoticeNotHere | Case::NoticeEnds => {
            observe_notice(fixture, setup, case, id, operation).await?;
        }
        Case::DecideAndPass => {
            races::queued_decide_and_pass(fixture, &mut setup, id, operation, second).await?
        }
    }
    Ok(())
}

async fn bad_proof(
    fixture: &impl ApprovalLinkFixture,
    setup: &mut ParentSetup,
    case: Case,
    request_id: approval_request_protocol::ApprovalRequestId,
    operation_id: workspace_ids::RequestId,
) -> Result<()> {
    let bad = SessionProof {
        session_id: setup.parent.session_id.clone(),
        secret: setup.child.secret.clone(),
    };
    let before = floor_and_log(fixture).await?;
    fixture.advance(1).await?;
    let socket = &mut setup.socket;
    let organization = &setup.target.organization_id;
    let refusal = match case {
        Case::BadListProof => refusal(
            call::<ListApprovalsForSessionRpc>(
                socket,
                organization,
                bad.request(ListApprovalsForSessionRequest { cursor: None }),
            )
            .await?,
        )?,
        Case::BadReadProof => refusal(
            call::<ReadApprovalForSessionRpc>(
                socket,
                organization,
                bad.request(ReadApprovalForSessionRequest { request_id }),
            )
            .await?,
        )?,
        Case::BadDecideProof => refusal(
            call::<DecideApprovalAsSessionRpc>(
                socket,
                organization,
                bad.request(DecideApprovalAsSessionRequest {
                    request_id,
                    operation_id,
                    value: option(1)?,
                }),
            )
            .await?,
        )?,
        Case::BadPassProof => refusal(
            call::<PassOnApprovalForSessionRpc>(
                socket,
                organization,
                bad.request(PassOnApprovalRequest {
                    request_id,
                    operation_id,
                }),
            )
            .await?,
        )?,
        _ => return Err("bad proof case selected with a different purpose".into()),
    };
    assert_eq!(refusal, LinkCallRefusal::SessionProofNotAccepted);
    heartbeat(socket).await?;
    assert_eq!(floor_and_log(fixture).await?, before);
    Ok(())
}

fn refusal<T>(outcome: TypedCallResult<T>) -> Result<LinkCallRefusal> {
    match outcome {
        TypedCallResult::Refused { reason } => Ok(reason),
        TypedCallResult::Answered { .. } => Err("bad proof received an approval reply".into()),
    }
}

async fn floor_and_log(fixture: &impl ApprovalLinkFixture) -> Result<Vec<Vec<SqlValue>>> {
    Ok(fixture.read(Statement {
        sql: "SELECT observed_ms,(SELECT COUNT(*) FROM runtime_request_log) AS log_count FROM approval_applying_time",
        args: vec![],
    }).await?.rows)
}

async fn reach_hour(fixture: &impl ApprovalLinkFixture, setup: &mut ParentSetup) -> Result<()> {
    for _ in 0..7 {
        fixture.advance(500).await?;
        renew(&mut setup.socket, fixture.refresh().await?).await?;
    }
    fixture.advance(100).await?;
    Ok(())
}

async fn acknowledge_ending(setup: &mut ParentSetup) -> Result<()> {
    use crate::approval_link_scenarios::{answer_ending, next_ending};
    let (correlation, _) = next_ending(&mut setup.socket, &setup.target).await?;
    answer_ending(
        &mut setup.socket,
        setup.target.clone(),
        correlation,
        approval_request_protocol::DeliverEndingReply::Received {},
    )
    .await
}

async fn observe_notice(
    fixture: &impl ApprovalLinkFixture,
    mut setup: ParentSetup,
    case: Case,
    id: approval_request_protocol::ApprovalRequestId,
    operation: workspace_ids::RequestId,
) -> Result<()> {
    assert_eq!(
        first_read(fixture, &setup.parent.session_id, &id).await?,
        [[SqlValue::Null]]
    );
    if matches!(case, Case::NoticeEnds) {
        setup.decide(id, operation, option(1)?).await?;
        acknowledge_ending(&mut setup).await?;
        setup.socket.close(CloseCode::Normal, None).await?;
        return reconnect_without_notice(fixture).await;
    }
    setup.socket.close(CloseCode::Normal, None).await?;
    setup.socket = fixture.connect().await?;
    let acknowledgement = if matches!(case, Case::NoticeNotHere) {
        ApprovalNoticeReply::NotHere {}
    } else {
        ApprovalNoticeReply::Received {}
    };
    acknowledge_notice(
        &mut setup.socket,
        &setup.target,
        &setup.parent.session_id,
        &id,
        acknowledgement,
    )
    .await?;
    if matches!(case, Case::NoticeNotHere) {
        setup.socket.close(CloseCode::Normal, None).await?;
        setup.socket = fixture.connect().await?;
        acknowledge_notice(
            &mut setup.socket,
            &setup.target,
            &setup.parent.session_id,
            &id,
            ApprovalNoticeReply::Received {},
        )
        .await?;
        return Ok(());
    }
    assert!(matches!(
        setup.read(id.clone()).await?,
        ReadApprovalForSessionReply::Request { .. }
    ));
    setup.socket.close(CloseCode::Normal, None).await?;
    reconnect_without_notice(fixture).await
}

async fn reconnect_without_notice(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    fixture.arm_notice_recovery().await?;
    let mut socket = fixture.connect().await?;
    {
        let recovered = std::pin::pin!(fixture.await_notice_recovery());
        let received = std::pin::pin!(super::receive_frame(&mut socket));
        match futures_util::future::select(recovered, received).await {
            futures_util::future::Either::Left((result, _)) => result?,
            futures_util::future::Either::Right((frame, _)) => {
                return Err(format!("closed notice was sent again: {:?}", frame?).into());
            }
        }
    }
    heartbeat(&mut socket).await
}

async fn observe_ended_parent_timeout(
    fixture: &impl ApprovalLinkFixture,
    setup: &mut ParentSetup,
    id: &approval_request_protocol::ApprovalRequestId,
) -> Result<()> {
    let request = setup.parent.request(ReadApprovalForSessionRequest {
        request_id: id.clone(),
    });
    let rejected = call::<ReadApprovalForSessionRpc>(
        &mut setup.socket,
        &setup.target.organization_id,
        request,
    )
    .await?;
    assert_eq!(refusal(rejected)?, LinkCallRefusal::SessionProofNotAccepted);

    let grandparent = setup.grandparent.as_ref().ok_or("missing grandparent")?;
    let request = grandparent.request(ReadApprovalForSessionRequest {
        request_id: id.clone(),
    });
    assert_eq!(
        super::answered(
            call::<ReadApprovalForSessionRpc>(
                &mut setup.socket,
                &setup.target.organization_id,
                request,
            )
            .await?
        )?,
        ReadApprovalForSessionReply::NotVisible {}
    );

    let destinations = fixture.read(Statement {
        sql: "SELECT d.session_id FROM approval_request_destinations d JOIN approval_requests r ON r.approval_row_id=d.approval_row_id WHERE r.request_id=? ORDER BY d.position",
        args: vec![SqlValue::Text(id.as_str().to_owned())],
    }).await?;
    assert_eq!(
        destinations.rows,
        [[SqlValue::Text(setup.parent.session_id.as_str().to_owned())]]
    );
    reach_hour(fixture, setup).await?;
    let remaining = u64::try_from((-fixture.now_seconds()).rem_euclid(300))?;
    if remaining > 0 {
        fixture.advance(remaining).await?;
    }
    fixture.observe_due_alarm().await?;
    let (_, ending) =
        crate::approval_link_scenarios::next_ending(&mut setup.socket, &setup.target).await?;
    assert_eq!(ending.request_id(), id);
    assert_eq!(
        ending.into_ending(),
        approval_request_protocol::Ending::TimedOut {}
    );
    Ok(())
}
