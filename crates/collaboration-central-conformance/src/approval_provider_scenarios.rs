//! Real socket checks for Central's provider approvals and questions.
use crate::{
    ConformanceResult as Result,
    approval_link_scenarios::{
        ApprovalLinkFixture, answer_ending, heartbeat, next_ending, observe_received, raise,
        read_request, register_run, renew, require_owed, require_receipt, stop, stop_request,
    },
    session_link_scenarios::{answer, call, end},
};
use approval_request_protocol::{
    ApprovalOffer, CancelReason, DeliverEndingReply, Description, Ending, Label, OfferedOption,
    OfferedOptionId, OfferedOptions, OptionEffect, OptionLifetime, ProviderHolder, QuestionField,
    QuestionFieldId, QuestionFields, QuestionOffer, Raise, RaiseReply, RegisterRunReply,
    StopReceiptEnding, StopReply, Title,
};
use central_link_protocol::{
    TypedCallResult,
    approval_rpc::ApprovalHolderTarget,
    session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc},
};
use central_storage::{SqlValue, Statement};
use fleet_identity_protocol::{
    BindFleetSession, BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply,
    CreateSessionKind, EndFleetSessionReply, OrganizationId, PresentedSessionSecret,
    SessionCreator, SessionSecret,
};
use reqwest_websocket::WebSocket;
use workspace_ids::CentralSessionId;

#[derive(Clone, Copy)]
pub enum ProviderAsk {
    Approval,
    Question,
}

pub async fn provider_session_end_and_refusals(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let mut socket = fixture.connect().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let (parent, parent_secret) = create_bound(&mut socket, organization, 1, None).await?;
    let (delegate, _) =
        create_bound(&mut socket, organization, 2, Some((&parent, parent_secret))).await?;
    let stopped = [
        provider_raise(&target, &parent, 3, ProviderAsk::Approval)?,
        provider_raise(&target, &parent, 8, ProviderAsk::Question)?,
    ];
    let question = provider_raise(&target, &delegate, 4, ProviderAsk::Question)?;
    let waiting = provider_raise(&target, &delegate, 5, ProviderAsk::Approval)?;
    for request in stopped.iter().chain([&question, &waiting]) {
        require_receipt(raise(&mut socket, organization, request.clone()).await?)?;
        if request.requesting_session() == Some(&delegate) {
            crate::parent_approval_scenarios::acknowledge_notice(
                &mut socket,
                &target,
                &parent,
                request.request_id(),
                approval_request_protocol::ApprovalNoticeReply::Received {},
            )
            .await?;
        }
        assert_eq!(
            read_request(fixture, request).await?,
            [text("waiting"), SqlValue::Null]
        );
    }

    for request in &stopped {
        let actual = stop(&mut socket, organization, stop_request(request)?).await?;
        assert!(matches!(
            actual,
            TypedCallResult::Answered {
                reply: StopReply::Stands {
                    ending: StopReceiptEnding::Cancelled { .. }
                }
            }
        ));
        assert_eq!(
            read_request(fixture, request).await?,
            [text("ended"), text("closed_without_receipt")]
        );
    }
    heartbeat(&mut socket).await?;

    for request in [&question, &waiting] {
        let routes = fixture.read(Statement {
            sql: "SELECT d.destination_kind,d.session_machine_id,d.session_enrollment_id,d.session_id FROM approval_request_destinations d JOIN approval_requests r ON r.approval_row_id=d.approval_row_id WHERE r.request_id=? ORDER BY d.destination_kind",
            args: vec![text(request.request_id().as_str())],
        }).await?;
        assert_eq!(
            routes.rows,
            [vec![
                text("parent_session"),
                text(target.machine_id.as_str()),
                text(target.enrollment_id.as_str()),
                text(parent.as_str()),
            ]]
        );
    }
    let result = answer::<EndFleetSessionRpc>(
        call::<EndFleetSessionRpc>(&mut socket, organization, end(delegate.clone())).await?,
        organization,
    )?;
    assert!(matches!(result, EndFleetSessionReply::Settled { .. }));
    for offered_request in [&question, &waiting] {
        require_owed(fixture, offered_request).await?;
    }
    let mut remaining = vec![&question, &waiting];
    for _ in 0..2 {
        let (correlation, offered) = next_ending(&mut socket, &target).await?;
        let index = remaining
            .iter()
            .position(|request| request.request_id() == offered.request_id())
            .ok_or("session end offered the wrong approval")?;
        let request = remaining.remove(index);
        assert_eq!(offered.contents_digest(), &request.contents_digest()?);
        assert_eq!(
            offered.into_ending(),
            Ending::Cancelled {
                reason: CancelReason::SessionEnded {}
            }
        );
        answer_ending(
            &mut socket,
            target.clone(),
            correlation,
            DeliverEndingReply::Received {},
        )
        .await?;
        observe_received(fixture, request).await?;
    }
    let after_end = provider_raise(&target, &delegate, 6, ProviderAsk::Question)?;
    assert_eq!(
        raise(&mut socket, organization, after_end.clone()).await?,
        RaiseReply::SessionEnded {}
    );
    let row = fixture
        .read(Statement {
            sql: "SELECT request_id FROM approval_requests WHERE request_id=?",
            args: vec![text(after_end.request_id().as_str())],
        })
        .await?;
    assert!(row.rows.is_empty());
    Ok(())
}

pub async fn provider_deadline(
    fixture: &impl ApprovalLinkFixture,
    ask: ProviderAsk,
    trigger: ProviderExpiryTrigger,
) -> Result<()> {
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let mut socket = fixture.connect().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let (session, _) = create_bound(&mut socket, organization, 1, None).await?;
    let request = provider_raise(&target, &session, 7, ask)?;
    let receipt = require_receipt(raise(&mut socket, organization, request.clone()).await?)?;
    for _ in 0..7 {
        fixture.advance(500).await?;
        renew(&mut socket, fixture.refresh().await?).await?;
    }
    let start = fixture.now_seconds();
    let deadline = receipt.window.deadline().timestamp();
    let mark = deadline + (-deadline).rem_euclid(300);
    if deadline + 1 >= mark {
        return Err("fixture must have time between deadline and alarm mark".into());
    }
    fixture
        .advance(u64::try_from(deadline + 1 - start)?)
        .await?;
    assert_eq!(
        read_request(fixture, &request).await?,
        [text("waiting"), SqlValue::Null]
    );
    heartbeat(&mut socket).await?;
    match trigger {
        ProviderExpiryTrigger::Stop => {
            assert_eq!(
                stop(&mut socket, organization, stop_request(&request)?).await?,
                TypedCallResult::Answered {
                    reply: StopReply::Stands {
                        ending: StopReceiptEnding::TimedOut {}
                    }
                }
            );
            assert_eq!(
                read_request(fixture, &request).await?,
                [text("ended"), text("closed_without_receipt")]
            );
            heartbeat(&mut socket).await?;
        }
        ProviderExpiryTrigger::Engine | ProviderExpiryTrigger::Registration => {
            if matches!(trigger, ProviderExpiryTrigger::Engine) {
                fixture
                    .advance(u64::try_from(mark - fixture.now_seconds())?)
                    .await?;
                fixture.observe_due_alarm().await?;
            } else {
                socket = fixture.connect().await?;
                assert_eq!(
                    register_run(&mut socket, organization, &target.router_run).await?,
                    RegisterRunReply::Registered {}
                );
            }
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
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub enum ProviderExpiryTrigger {
    Stop,
    Engine,
    Registration,
}

pub async fn create_bound(
    socket: &mut WebSocket,
    organization: &OrganizationId,
    ordinal: u16,
    parent: Option<(&CentralSessionId, SessionSecret)>,
) -> Result<(CentralSessionId, SessionSecret)> {
    let (creator, session_kind) = match parent {
        Some((session_id, secret)) => (
            SessionCreator::Session {
                session_id: session_id.clone(),
                presented: PresentedSessionSecret::Secret { secret },
            },
            CreateSessionKind::Delegate {
                parent: session_id.clone(),
            },
        ),
        None => (
            SessionCreator::RouterOperator {},
            CreateSessionKind::Ordinary {},
        ),
    };
    let created = call::<CreateFleetSessionRpc>(
        socket,
        organization,
        CreateFleetSession {
            operation_id: fixture_id(ordinal).parse()?,
            attempt: 1_u64.try_into()?,
            creator,
            agent_program: fleet_identity_protocol::HarnessKind::Codex,
            session_kind,
        },
    )
    .await?;
    let CreateFleetSessionReply::Reserved {
        session_id, secret, ..
    } = answer::<CreateFleetSessionRpc>(created, organization)?
    else {
        return Err("provider's Central session was not reserved".into());
    };
    let bound = call::<BindFleetSessionRpc>(
        socket,
        organization,
        BindFleetSession {
            session_id: session_id.clone(),
            harness_session_id: format!("approval-provider-{ordinal}").parse()?,
            secret: secret.clone(),
        },
    )
    .await?;
    if !matches!(
        answer::<BindFleetSessionRpc>(bound, organization)?,
        BindFleetSessionReply::Active { .. }
    ) {
        return Err("provider's Central session was not bound".into());
    }
    Ok((session_id, secret))
}

pub fn provider_raise(
    target: &ApprovalHolderTarget,
    session: &CentralSessionId,
    ordinal: u16,
    ask: ProviderAsk,
) -> Result<Raise> {
    let request_id = fixture_id(ordinal).parse()?;
    let holder = ProviderHolder {
        session_id: session.clone(),
        router_run: target.router_run.clone(),
    };
    match ask {
        ProviderAsk::Approval => Ok(Raise::ProviderApproval {
            request_id,
            holder,
            offer: ApprovalOffer {
                title: Title::new("Approve the provider request")?,
                description: None,
                subject: None,
                options: OfferedOptions::new(vec![OfferedOption {
                    id: OfferedOptionId::new(1)?,
                    label: Label::new("Allow")?,
                    effect: OptionEffect::Allow,
                    lasts: OptionLifetime::ThisRequest {},
                }])?,
            },
        }),
        ProviderAsk::Question => Ok(Raise::ProviderQuestion {
            request_id,
            holder,
            offer: QuestionOffer {
                prompt: Description::new("Continue the provider request?")?,
                fields: QuestionFields::new(vec![QuestionField::Boolean {
                    field_id: QuestionFieldId::new(1)?,
                    label: Label::new("Continue")?,
                    description: None,
                    required: true,
                }])?,
            },
        }),
    }
}

fn fixture_id(ordinal: u16) -> String {
    format!("019f0000-0000-7000-8000-000000006{ordinal:03x}")
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

pub async fn same_socket_renewal_keeps_delivery(
    fixture: &impl ApprovalLinkFixture,
    timing: RenewalDuringDelivery,
) -> Result<()> {
    use approval_request_protocol::{DecidedBy, ProviderAnswer};
    let target = fixture.target()?;
    let organization = &target.organization_id;
    let mut socket = fixture.connect().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let (session, _) = create_bound(&mut socket, organization, 1, None).await?;
    let request = provider_raise(&target, &session, 7, ProviderAsk::Approval)?;
    require_receipt(raise(&mut socket, organization, request.clone()).await?)?;

    fixture.advance(598).await?;
    if matches!(timing, RenewalDuringDelivery::BeforeWrite) {
        fixture.hold_delivery_before_write().await?;
    }
    fixture
        .decide_provider_approval(request.request_id())
        .await?;
    if matches!(timing, RenewalDuringDelivery::BeforeWrite) {
        fixture.wait_delivery_before_write().await?;
        renew(&mut socket, fixture.refresh().await?).await?;
        fixture.advance(3).await?;
        fixture.release_delivery_before_write().await?;
    }
    let (correlation, offered) = next_ending(&mut socket, &target).await?;
    assert_eq!(offered.request_id(), request.request_id());
    assert_eq!(offered.contents_digest(), &request.contents_digest()?);
    assert_eq!(
        offered.into_ending(),
        Ending::Decided {
            answer: ProviderAnswer::Approval {
                option_id: OfferedOptionId::new(1)?
            },
            by: DecidedBy::Person {
                person_id: crate::approval_link_scenarios::FIXTURE_DECIDER.parse()?
            },
        }
    );
    if matches!(timing, RenewalDuringDelivery::BeforeAnswer) {
        renew(&mut socket, fixture.refresh().await?).await?;
        fixture.advance(3).await?;
    }
    answer_ending(
        &mut socket,
        target,
        correlation,
        DeliverEndingReply::Received {},
    )
    .await?;
    observe_received(fixture, &request).await
}

#[derive(Clone, Copy)]
pub enum RenewalDuringDelivery {
    BeforeWrite,
    BeforeAnswer,
}
