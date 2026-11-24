//! Fences for C2 reply loss and approval-notice writes.
use crate::fixture_link_reader::read_fixture_frame;
use std::future::Future;

use approval_request_protocol::{
    ApprovalNoticeReply, ApprovalRequestId, ListApprovalsForSessionReply, PassOnApprovalReply,
    PassOnApprovalRequest, ReadApprovalForSessionReply, ReadApprovalForSessionRequest,
};
use central_link_protocol::{
    CentralLinkRpc, DataFrame, LinkFrame, MacCall, MacLinkRpc, TypedCallResult,
    approval_rpc::{
        ApprovalNoticeRpc, ApprovalServiceTarget, PassOnApprovalForSessionRpc,
        ReadApprovalForSessionRpc,
    },
};
use central_storage::{SqlValue, Statement};
use fleet_identity_protocol::OrganizationId;
use fleet_rpc::Envelope;
use reqwest_websocket::{CloseCode, WebSocket};

use super::{Chain, ParentSetup, Result, acknowledge_notice, option, typed_id};
use crate::{
    approval_link_scenarios::{
        ApprovalLinkFixture, heartbeat, raise, receive_frame, require_receipt, send_frame,
    },
    approval_provider_scenarios::{ProviderAsk, provider_raise},
};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum ParentApprovalPause {
    BeforeApply,
    BeforeReply,
    BeforeNoticeWrite,
    AfterNoticeAttempt,
}

#[derive(Clone, Copy)]
pub enum PauseControl {
    Arm,
    Wait,
    Release,
}

async fn start_call<R: CentralLinkRpc<Target = ApprovalServiceTarget>>(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    body: R::Request,
) -> Result<()> {
    send_frame(
        socket,
        LinkFrame::Data(DataFrame::CallCentral {
            correlation_id: super::CORRELATION.parse()?,
            request: Box::new(R::request(Envelope {
                target: ApprovalServiceTarget {
                    organization_id: organization.clone(),
                },
                body,
            })),
        }),
    )
    .await
}

async fn read_state(
    fixture: &impl ApprovalLinkFixture,
    id: &ApprovalRequestId,
) -> Result<Vec<Vec<SqlValue>>> {
    Ok(fixture.read(Statement {
        sql: "SELECT d.first_read_at,d.updated_at,r.record_state,r.deadline_ms,(SELECT observed_ms FROM approval_applying_time) FROM approval_request_destinations d JOIN approval_requests r ON r.approval_row_id=d.approval_row_id WHERE r.request_id=? ORDER BY d.position",
        args: vec![SqlValue::Text(id.as_str().to_owned())],
    }).await?.rows)
}

pub async fn read_survives_lost_reply<F: Future<Output = Result<()>>>(
    fixture: &impl ApprovalLinkFixture,
    control: impl Fn(ParentApprovalPause, PauseControl) -> F,
) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let id = setup.approval.request_id().clone();
    assert!(
        matches!(read_state(fixture, &id).await?.as_slice(), [row] if matches!(row.first(), Some(SqlValue::Null)))
    );

    control(ParentApprovalPause::BeforeReply, PauseControl::Arm).await?;
    start_call::<ReadApprovalForSessionRpc>(
        &mut setup.socket,
        &setup.target.organization_id,
        setup.parent.request(ReadApprovalForSessionRequest {
            request_id: id.clone(),
        }),
    )
    .await?;
    control(ParentApprovalPause::BeforeReply, PauseControl::Wait).await?;
    let committed = read_state(fixture, &id).await?;
    assert!(
        matches!(committed.as_slice(), [row] if matches!(row.first(), Some(SqlValue::Text(_))))
    );
    fixture.arm_notice_recovery().await?;
    let mut replacement = fixture.connect().await?;
    drop(setup.socket);
    control(ParentApprovalPause::BeforeReply, PauseControl::Release).await?;
    observe_no_recovered_notice(fixture, &mut replacement).await?;
    setup.socket = replacement;

    fixture.advance(1).await?;
    let first_reply = setup.read(id.clone()).await?;
    assert!(matches!(
        &first_reply,
        ReadApprovalForSessionReply::Request { .. }
    ));
    assert_eq!(read_state(fixture, &id).await?, committed);
    assert_eq!(setup.read(id.clone()).await?, first_reply);
    assert_eq!(read_state(fixture, &id).await?, committed);

    let ListApprovalsForSessionReply::Page { items, next } = setup.list().await?;
    assert_eq!(
        items
            .iter()
            .map(|item| &item.request_id)
            .collect::<Vec<_>>(),
        [&id]
    );
    assert_eq!(next, None);
    assert_eq!(
        read_state(fixture, &id)
            .await?
            .first()
            .and_then(|row| row.first()),
        committed.first().and_then(|row| row.first())
    );
    Ok(())
}

pub async fn pass_notice_survives_origin_loss<F: Future<Output = Result<()>>>(
    fixture: &impl ApprovalLinkFixture,
    control: impl Fn(ParentApprovalPause, PauseControl) -> F,
) -> Result<()> {
    use ParentApprovalPause::{BeforeApply, BeforeReply};
    let mut setup = ParentSetup::open(fixture, Chain::ThreeLevels).await?;
    let id = setup.approval.request_id().clone();
    assert!(matches!(
        setup.read(id.clone()).await?,
        ReadApprovalForSessionReply::Request { .. }
    ));
    let grandparent = setup.grandparent.take().ok_or("missing grandparent")?;
    control(BeforeApply, PauseControl::Arm).await?;
    control(BeforeReply, PauseControl::Arm).await?;
    let operation_id = typed_id(0x7180)?;
    start_call::<PassOnApprovalForSessionRpc>(
        &mut setup.socket,
        &setup.target.organization_id,
        setup.parent.request(PassOnApprovalRequest {
            request_id: id.clone(),
            operation_id: operation_id.clone(),
        }),
    )
    .await?;
    control(BeforeApply, PauseControl::Wait).await?;
    fixture.arm_notice_recovery().await?;
    let mut replacement = fixture.connect().await?;
    observe_no_recovered_notice(fixture, &mut replacement).await?;

    control(BeforeApply, PauseControl::Release).await?;
    control(BeforeReply, PauseControl::Wait).await?;
    drop(setup.socket);
    acknowledge_notice(
        &mut replacement,
        &setup.target,
        &grandparent.session_id,
        &id,
        ApprovalNoticeReply::Received {},
    )
    .await?;
    control(BeforeReply, PauseControl::Release).await?;
    setup.socket = replacement;
    assert_eq!(
        setup.pass(id.clone(), operation_id).await?,
        PassOnApprovalReply::PassedOn {
            to: approval_request_protocol::ApprovalDestination::Session {
                session_id: grandparent.session_id.clone()
            },
        }
    );
    setup.parent = grandparent;
    assert!(matches!(
        setup.read(id.clone()).await?,
        ReadApprovalForSessionReply::Request { .. }
    ));
    setup.socket.close(CloseCode::Normal, None).await?;
    fixture.arm_notice_recovery().await?;
    let mut replacement = fixture.connect().await?;
    observe_no_recovered_notice(fixture, &mut replacement).await
}

pub async fn queued_notice_stops_at_deadline<F: Future<Output = Result<()>>>(
    fixture: &impl ApprovalLinkFixture,
    control: impl Fn(ParentApprovalPause, PauseControl) -> F,
) -> Result<()> {
    use ParentApprovalPause::{AfterNoticeAttempt, BeforeNoticeWrite};
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    for _ in 0..7 {
        fixture.advance(500).await?;
        crate::approval_link_scenarios::renew(&mut setup.socket, fixture.refresh().await?).await?;
    }
    fixture.advance(99).await?;
    setup.socket.close(CloseCode::Normal, None).await?;
    control(BeforeNoticeWrite, PauseControl::Arm).await?;
    control(AfterNoticeAttempt, PauseControl::Arm).await?;
    let mut socket = fixture.connect().await?;
    control(BeforeNoticeWrite, PauseControl::Wait).await?;
    fixture.advance(1).await?;
    control(BeforeNoticeWrite, PauseControl::Release).await?;
    control(AfterNoticeAttempt, PauseControl::Wait).await?;
    control(AfterNoticeAttempt, PauseControl::Release).await?;
    heartbeat(&mut socket).await
}

#[derive(Clone, Copy)]
pub enum SettlesRecoveredNotice {
    Read,
    Ending,
}

pub async fn stale_recovery_page(
    fixture: &impl ApprovalLinkFixture,
    settlement: SettlesRecoveredNotice,
) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let earlier = setup.approval.request_id().clone();
    let second = provider_raise(
        &setup.target,
        &setup.child.session_id,
        0x190,
        ProviderAsk::Approval,
    )?;
    let later = second.request_id().clone();
    require_receipt(raise(&mut setup.socket, &setup.target.organization_id, second).await?)?;
    acknowledge_notice(
        &mut setup.socket,
        &setup.target,
        &setup.parent.session_id,
        &later,
        ApprovalNoticeReply::Received {},
    )
    .await?;
    setup.socket.close(CloseCode::Normal, None).await?;
    fixture.arm_notice_recovery().await?;
    setup.socket = fixture.connect().await?;
    let LinkFrame::Data(DataFrame::CallMac {
        correlation_id,
        request,
    }) = receive_frame(&mut setup.socket).await?
    else {
        return Err("expected first owed notice".into());
    };
    let MacCall::ApprovalNotice(notice) = *request else {
        return Err("expected notice".into());
    };
    let pending = match notice.body.request_id {
        ref id if id == &earlier => later,
        ref id if id == &later => earlier,
        _ => return Err("recovery offered an unrelated notice".into()),
    };
    match settlement {
        SettlesRecoveredNotice::Read => assert!(matches!(
            setup.read(pending).await?,
            ReadApprovalForSessionReply::Request { .. }
        )),
        SettlesRecoveredNotice::Ending => assert!(matches!(
            setup.decide(pending, typed_id(0x7191)?, option(1)?).await?,
            super::DecideApprovalAsSessionReply::SavedDecision { .. }
        )),
    }
    send_frame(
        &mut setup.socket,
        LinkFrame::Data(DataFrame::MacCallReply {
            correlation_id,
            result: Box::new(TypedCallResult::Answered {
                reply: ApprovalNoticeRpc::reply(Envelope {
                    target: notice.target,
                    body: ApprovalNoticeReply::Received {},
                }),
            }),
        }),
    )
    .await?;
    observe_no_recovered_notice(fixture, &mut setup.socket).await
}

pub(super) async fn observe_no_recovered_notice(
    fixture: &impl ApprovalLinkFixture,
    socket: &mut WebSocket,
) -> Result<()> {
    use futures_util::future::{Either, select};
    {
        let recovered = std::pin::pin!(fixture.await_notice_recovery());
        let received = std::pin::pin!(receive_frame(socket));
        match select(received, recovered).await {
            Either::Right((result, _)) => result?,
            Either::Left((frame, _)) => {
                return Err(
                    format!("unexpected notice before recovery settled: {:?}", frame?).into(),
                );
            }
        }
    }
    heartbeat(socket).await
}

pub async fn pass_notice_reaches_a_separate_destination<F: Future<Output = Result<()>>>(
    fixture: &impl super::CompetingApprovalFixture,
    control: impl Fn(ParentApprovalPause, PauseControl) -> F,
) -> Result<()> {
    use super::{SessionProof, seed_delegate_edge};
    use crate::approval_link_scenarios::register_run;
    use crate::approval_provider_scenarios::create_bound;

    let holder = fixture.target()?;
    let organization = &holder.organization_id;
    let mut destination_socket = fixture.connect().await?;
    register_run(&mut destination_socket, organization, &holder.router_run).await?;
    let (origin, mut origin_socket) = fixture.connect_competing_machine().await?;
    let (parent_id, parent_secret) =
        create_bound(&mut origin_socket, organization, 0x31, None).await?;
    let parent = SessionProof {
        session_id: parent_id,
        secret: parent_secret,
    };
    let (destination_id, destination_secret) =
        create_bound(&mut destination_socket, organization, 0x32, None).await?;
    let destination = SessionProof {
        session_id: destination_id,
        secret: destination_secret,
    };
    let (child, _) = create_bound(&mut destination_socket, organization, 0x33, None).await?;
    seed_delegate_edge(fixture, &child, &parent.session_id).await?;
    seed_delegate_edge(fixture, &parent.session_id, &destination.session_id).await?;
    let request = provider_raise(&holder, &child, 0x34, ProviderAsk::Approval)?;
    let id = request.request_id().clone();
    require_receipt(raise(&mut destination_socket, organization, request).await?)?;
    acknowledge_notice(
        &mut origin_socket,
        &origin,
        &parent.session_id,
        &id,
        ApprovalNoticeReply::Received {},
    )
    .await?;

    control(ParentApprovalPause::BeforeReply, PauseControl::Arm).await?;
    start_call::<PassOnApprovalForSessionRpc>(
        &mut origin_socket,
        organization,
        parent.request(PassOnApprovalRequest {
            request_id: id.clone(),
            operation_id: typed_id(0x72ff)?,
        }),
    )
    .await?;
    control(ParentApprovalPause::BeforeReply, PauseControl::Wait).await?;
    drop(origin_socket);
    acknowledge_notice(
        &mut destination_socket,
        &holder,
        &destination.session_id,
        &id,
        ApprovalNoticeReply::Received {},
    )
    .await?;
    control(ParentApprovalPause::BeforeReply, PauseControl::Release).await?;
    assert!(matches!(
        super::answered(
            super::call::<ReadApprovalForSessionRpc>(
                &mut destination_socket,
                organization,
                destination.request(ReadApprovalForSessionRequest {
                    request_id: id.clone()
                }),
            )
            .await?
        )?,
        ReadApprovalForSessionReply::Request { .. }
    ));
    assert!(
        matches!(super::first_read(fixture, &destination.session_id, &id).await?.as_slice(), [row] if matches!(row.as_slice(), [SqlValue::Text(_)]))
    );
    heartbeat(&mut destination_socket).await
}

pub async fn hidden_approval_matches_an_absent_one_on_wire(
    fixture: &impl ApprovalLinkFixture,
) -> Result<()> {
    use approval_request_protocol::{
        DecideApprovalAsSessionReply, DecideApprovalAsSessionRequest, ReadApprovalForSessionRequest,
    };
    use central_link_protocol::{
        CentralCallReply, FrameDirection, approval_rpc::DecideApprovalAsSessionRpc, decode_text,
    };
    use reqwest_websocket::Message;

    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let hidden = setup.approval.request_id().clone();
    let absent = super::ABSENT_REQUEST.parse::<ApprovalRequestId>()?;
    fixture
        .fixture_write(Statement {
            sql: "UPDATE approval_requests SET offer_json='malformed' WHERE request_id=?",
            args: vec![SqlValue::Text(hidden.as_str().to_owned())],
        })
        .await?;
    let organization = &setup.target.organization_id;
    let session = &setup.child;
    for (hidden, absent) in [
        (
            ReadApprovalForSessionRpc::request(Envelope {
                target: ApprovalServiceTarget {
                    organization_id: organization.clone(),
                },
                body: session.request(ReadApprovalForSessionRequest {
                    request_id: hidden.clone(),
                }),
            }),
            ReadApprovalForSessionRpc::request(Envelope {
                target: ApprovalServiceTarget {
                    organization_id: organization.clone(),
                },
                body: session.request(ReadApprovalForSessionRequest {
                    request_id: absent.clone(),
                }),
            }),
        ),
        (
            DecideApprovalAsSessionRpc::request(Envelope {
                target: ApprovalServiceTarget {
                    organization_id: organization.clone(),
                },
                body: session.request(DecideApprovalAsSessionRequest {
                    request_id: hidden.clone(),
                    operation_id: typed_id(0xb290)?,
                    value: option(1)?,
                }),
            }),
            DecideApprovalAsSessionRpc::request(Envelope {
                target: ApprovalServiceTarget {
                    organization_id: organization.clone(),
                },
                body: session.request(DecideApprovalAsSessionRequest {
                    request_id: absent.clone(),
                    operation_id: typed_id(0xb290)?,
                    value: option(1)?,
                }),
            }),
        ),
        (
            PassOnApprovalForSessionRpc::request(Envelope {
                target: ApprovalServiceTarget {
                    organization_id: organization.clone(),
                },
                body: session.request(PassOnApprovalRequest {
                    request_id: hidden,
                    operation_id: typed_id(0xb291)?,
                }),
            }),
            PassOnApprovalForSessionRpc::request(Envelope {
                target: ApprovalServiceTarget {
                    organization_id: organization.clone(),
                },
                body: session.request(PassOnApprovalRequest {
                    request_id: absent,
                    operation_id: typed_id(0xb291)?,
                }),
            }),
        ),
    ] {
        let mut responses = Vec::new();
        for request in [hidden, absent] {
            let correlation = super::CORRELATION.parse()?;
            send_frame(
                &mut setup.socket,
                LinkFrame::Data(DataFrame::CallCentral {
                    correlation_id: correlation,
                    request: Box::new(request),
                }),
            )
            .await?;
            let Some(Ok(Message::Text(text))) =
                read_fixture_frame(&mut setup.socket, "approval notice").await?
            else {
                return Err("expected textual Central approval reply".into());
            };
            responses.push(text);
        }
        let [hidden_reply, absent_reply] = responses.as_slice() else {
            return Err("expected both replies".into());
        };
        assert_eq!(hidden_reply, absent_reply);
        let LinkFrame::Data(DataFrame::CentralCallReply { result, .. }) =
            decode_text(hidden_reply, FrameDirection::ToRouter)?
        else {
            return Err("expected a typed Central call reply".into());
        };
        let TypedCallResult::Answered { reply } = *result else {
            return Err("hidden or absent request received a link refusal".into());
        };
        assert!(match reply {
            CentralCallReply::ReadApprovalForSession(reply) =>
                matches!(reply.body, ReadApprovalForSessionReply::NotVisible {}),
            CentralCallReply::DecideApprovalAsSession(reply) =>
                matches!(reply.body, DecideApprovalAsSessionReply::NotVisible {}),
            CentralCallReply::PassOnApproval(reply) =>
                matches!(reply.body, PassOnApprovalReply::NotVisible {}),
            _ => false,
        });
    }
    heartbeat(&mut setup.socket).await
}
