//! C2 guards for offered answers, recorded parents, and paging.
use approval_request_protocol::{
    Answer, ApprovalDestination, ApprovalNoticeReply, ApprovalRequestId, CallOffer,
    DecideApprovalAsSessionReply, ListApprovalsForSessionReply, ListApprovalsForSessionRequest,
    OptionLifetime, PassOnApprovalReply, PassOnApprovalRequest, ProviderAnswer, QuestionAnswer,
    QuestionAnswerValue, QuestionFieldAnswer, QuestionFieldId, Raise, ReadApprovalForSessionReply,
    RetainedOptionStorage, SessionCallDecision, SessionCallOperation,
};
use central_link_protocol::{
    LinkCallRefusal, TypedCallResult,
    approval_rpc::{ListApprovalsForSessionRpc, PassOnApprovalForSessionRpc, RaiseApprovalRpc},
};
use central_storage::{SqlValue, Statement};

use super::{
    Chain, ParentSetup, Result, acknowledge_notice, answered, call, option, text, typed_id,
};
use crate::{
    approval_link_scenarios::{
        ApprovalLinkFixture, central_call, heartbeat, held_start, next_ending, raise,
        require_receipt,
    },
    approval_provider_scenarios::{ProviderAsk, provider_raise},
    session_link_scenarios::{answer as session_answer, call as session_call, end},
};

#[derive(Clone, Copy)]
pub enum ParentApprovalProof {
    HeldOperations,
    RestrictedAnswers,
    ProviderQuestion,
    ListContinuation,
    SessionReplay,
    EndedNextParent,
    MissingParentAtRaise,
    CorruptParentAtRaise,
    MissingParentAtPass,
    CorruptParentAtPass,
    InvalidReadFact,
    PositionGap,
    ForwardAtDeadline,
    BackwardsDestination,
    ReadAfterEnding,
}

pub async fn observe_parent_proof(
    fixture: &impl ApprovalLinkFixture,
    proof: ParentApprovalProof,
) -> Result<()> {
    use ParentApprovalProof as Proof;
    match proof {
        Proof::HeldOperations => held_operations(fixture).await,
        Proof::RestrictedAnswers => restricted_answers(fixture).await,
        Proof::ProviderQuestion => provider_question(fixture).await,
        Proof::ListContinuation => list_continuation(fixture).await,
        Proof::SessionReplay | Proof::EndedNextParent => {
            session_hop(fixture, matches!(proof, Proof::EndedNextParent)).await
        }
        Proof::MissingParentAtRaise => {
            bad_parent(fixture, RoutePhase::Raise, ParentDamage::MissingEdge).await
        }
        Proof::CorruptParentAtRaise => {
            bad_parent(fixture, RoutePhase::Raise, ParentDamage::MalformedParent).await
        }
        Proof::MissingParentAtPass => {
            bad_parent(fixture, RoutePhase::Pass, ParentDamage::MissingEdge).await
        }
        Proof::InvalidReadFact
        | Proof::PositionGap
        | Proof::ForwardAtDeadline
        | Proof::BackwardsDestination
        | Proof::ReadAfterEnding => corrupt_destination(fixture, proof).await,
        Proof::CorruptParentAtPass => {
            bad_parent(fixture, RoutePhase::Pass, ParentDamage::MalformedParent).await
        }
    }
}

async fn held_operations(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let held_start = held_start(&setup.target.router_run, 0x7210)?;
    let mut held_end =
        crate::approval_link_scenarios::held_start(&setup.target.router_run, 0x7211)?;
    let Raise::UnprovenCall { offer, .. } = &mut held_end else {
        return Err("expected a parked call".into());
    };
    *offer = CallOffer {
        operation: SessionCallOperation::EndSessionForGood {
            session_id: setup.child.session_id.clone(),
        },
    };
    for request in [held_start, held_end] {
        let id = request.request_id().clone();
        require_receipt(raise(&mut setup.socket, &setup.target.organization_id, request).await?)?;
        let before = approval_state(fixture).await?;
        fixture.advance(1).await?;
        assert_eq!(
            setup
                .decide(
                    id,
                    typed_id(0x7212)?,
                    Answer::UnprovenCall {
                        decision: SessionCallDecision::Allow
                    }
                )
                .await?,
            DecideApprovalAsSessionReply::PersonRequired {}
        );
        assert_eq!(approval_state(fixture).await?, before);
    }
    Ok(())
}

async fn restricted_answers(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let conflicting_operation = typed_id(0x7220)?;
    let initial = setup.approval.request_id().clone();
    assert_eq!(
        setup.pass(initial, conflicting_operation.clone()).await?,
        PassOnApprovalReply::PassedOn {
            to: ApprovalDestination::People {}
        }
    );
    for (ordinal, lasts) in [
        (
            0x221,
            OptionLifetime::KeptUntilChanged {
                kept: RetainedOptionStorage::AgentSettingsFile,
            },
        ),
        (0x222, OptionLifetime::NotVerified {}),
    ] {
        let mut request = provider_raise(
            &setup.target,
            &setup.child.session_id,
            ordinal,
            ProviderAsk::Approval,
        )?;
        let Raise::ProviderApproval { offer, .. } = &mut request else {
            return Err("expected provider approval".into());
        };
        let mut options = offer.options.as_slice().to_vec();
        options.first_mut().ok_or("approval lacks an option")?.lasts = lasts;
        offer.options = approval_request_protocol::OfferedOptions::new(options)?;
        let id = request.request_id().clone();
        require_receipt(raise(&mut setup.socket, &setup.target.organization_id, request).await?)?;
        acknowledge_notice(
            &mut setup.socket,
            &setup.target,
            &setup.parent.session_id,
            &id,
            ApprovalNoticeReply::Received {},
        )
        .await?;
        let before = approval_state(fixture).await?;
        fixture.advance(1).await?;
        for operation in [typed_id(ordinal)?, conflicting_operation.clone()] {
            assert_eq!(
                setup.decide(id.clone(), operation, option(1)?).await?,
                DecideApprovalAsSessionReply::PersonRequired {}
            );
            assert_eq!(approval_state(fixture).await?, before);
        }
    }
    Ok(())
}

async fn provider_question(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let request = provider_raise(
        &setup.target,
        &setup.child.session_id,
        0x230,
        ProviderAsk::Question,
    )?;
    let id = request.request_id().clone();
    require_receipt(raise(&mut setup.socket, &setup.target.organization_id, request).await?)?;
    acknowledge_notice(
        &mut setup.socket,
        &setup.target,
        &setup.parent.session_id,
        &id,
        ApprovalNoticeReply::Received {},
    )
    .await?;
    let answer = QuestionAnswer::Answered {
        fields: vec![QuestionFieldAnswer {
            field_id: QuestionFieldId::new(1)?,
            value: QuestionAnswerValue::Boolean { value: true },
        }],
    };
    let by = setup.parent.session_id.clone();
    assert_eq!(
        setup
            .decide(
                id.clone(),
                typed_id(0x7231)?,
                Answer::Question {
                    answer: answer.clone()
                }
            )
            .await?,
        DecideApprovalAsSessionReply::SavedDecision {
            decision: approval_request_protocol::ApprovalDecision {
                answer: ProviderAnswer::Question {
                    answer: answer.clone()
                },
                by: approval_request_protocol::SessionDecider::Session {
                    session: by.clone()
                },
            }
        }
    );
    let (_, delivery) = next_ending(&mut setup.socket, &setup.target).await?;
    assert_eq!(delivery.request_id(), &id);
    assert_eq!(
        delivery.into_ending(),
        approval_request_protocol::Ending::Decided {
            answer: ProviderAnswer::Question { answer },
            by: approval_request_protocol::DecidedBy::Session { session: by },
        }
    );
    Ok(())
}

async fn list_continuation(fixture: &impl ApprovalLinkFixture) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::Direct).await?;
    let mut ids = vec![setup.approval.request_id().clone()];
    for ordinal in 0x240..0x2a4 {
        let request = provider_raise(
            &setup.target,
            &setup.child.session_id,
            ordinal,
            ProviderAsk::Approval,
        )?;
        let id = request.request_id().clone();
        require_receipt(raise(&mut setup.socket, &setup.target.organization_id, request).await?)?;
        acknowledge_notice(
            &mut setup.socket,
            &setup.target,
            &setup.parent.session_id,
            &id,
            ApprovalNoticeReply::Received {},
        )
        .await?;
        // Read the notice without ending the approval so setup can exceed one
        // socket ledger while every waiting row still contributes to the page.
        assert!(matches!(
            setup.read(id.clone()).await?,
            ReadApprovalForSessionReply::Request { .. }
        ));
        ids.push(id);
    }
    ids.sort_by(|left, right| right.cmp(left));
    let ListApprovalsForSessionReply::Page { items, next } = setup.list().await?;
    assert_eq!(items.len(), 100);
    assert_eq!(
        items
            .iter()
            .map(|item| &item.request_id)
            .collect::<Vec<_>>(),
        ids.iter().take(100).collect::<Vec<_>>()
    );
    assert_eq!(next.as_ref(), ids.get(99));
    let ListApprovalsForSessionReply::Page { items, next } = answered(
        call::<ListApprovalsForSessionRpc>(
            &mut setup.socket,
            &setup.target.organization_id,
            setup
                .parent
                .request(ListApprovalsForSessionRequest { cursor: next }),
        )
        .await?,
    )?;
    assert_eq!(
        items
            .iter()
            .map(|item| &item.request_id)
            .collect::<Vec<_>>(),
        ids.iter().skip(100).collect::<Vec<_>>()
    );
    assert_eq!(next, None);
    Ok(())
}

async fn session_hop(fixture: &impl ApprovalLinkFixture, ended: bool) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::ThreeLevels).await?;
    let parent = setup.grandparent.take().ok_or("missing grandparent")?;
    let id = setup.approval.request_id().clone();
    let first_operation = typed_id(0x72b0)?;
    if ended {
        use central_link_protocol::session_lifecycle_rpc::EndFleetSessionRpc;
        let reply = session_call::<EndFleetSessionRpc>(
            &mut setup.socket,
            &setup.target.organization_id,
            end(parent.session_id.clone()),
        )
        .await?;
        assert!(matches!(
            session_answer::<EndFleetSessionRpc>(reply, &setup.target.organization_id)?,
            fleet_identity_protocol::EndFleetSessionReply::Settled { .. }
        ));
    }
    let expected = PassOnApprovalReply::PassedOn {
        to: ApprovalDestination::Session {
            session_id: parent.session_id.clone(),
        },
    };
    let previous_rows = route_rows(fixture, &id).await?;
    assert_eq!(
        setup.pass(id.clone(), first_operation.clone()).await?,
        expected
    );
    acknowledge_notice(
        &mut setup.socket,
        &setup.target,
        &parent.session_id,
        &id,
        ApprovalNoticeReply::Received {},
    )
    .await?;
    let current_rows = route_rows(fixture, &id).await?;
    let [first, next] = current_rows.as_slice() else {
        return Err("expected two durable destinations".into());
    };
    let [only] = previous_rows.as_slice() else {
        return Err("expected the original destination".into());
    };
    assert_eq!(first, only);
    let [_, _, original_created, original_deadline] = first.as_slice() else {
        return Err("invalid original route row".into());
    };
    let [session, position, created, deadline] = next.as_slice() else {
        return Err("invalid next route row".into());
    };
    assert_eq!(session, &text(parent.session_id.as_str()));
    assert_eq!(created, original_created);
    assert_eq!(deadline, original_deadline);
    assert_eq!(
        position,
        &SqlValue::Integer(central_storage::SafeInteger::new(1)?)
    );
    assert_eq!(setup.pass(id.clone(), first_operation).await?, expected);
    assert_eq!(
        setup.pass(id.clone(), typed_id(0x72b1)?).await?,
        PassOnApprovalReply::NotVisible {}
    );
    assert_eq!(route_rows(fixture, &id).await?, current_rows);
    if !ended {
        let original = std::mem::replace(&mut setup.parent, parent);
        assert_eq!(
            setup.pass(id.clone(), typed_id(0x72b2)?).await?,
            PassOnApprovalReply::PassedOn {
                to: ApprovalDestination::People {}
            }
        );
        setup.parent = original;
        assert_eq!(
            setup.pass(id, typed_id(0x72b3)?).await?,
            PassOnApprovalReply::NotVisible {}
        );
    }
    Ok(())
}

async fn route_rows(
    fixture: &impl ApprovalLinkFixture,
    request: &ApprovalRequestId,
) -> Result<Vec<Vec<SqlValue>>> {
    Ok(fixture.read(Statement {
        sql: "SELECT d.session_id,d.position,d.created_at,r.deadline_ms FROM approval_request_destinations d JOIN approval_requests r ON r.approval_row_id=d.approval_row_id WHERE r.request_id=? ORDER BY d.position",
        args: vec![text(request.as_str())],
    }).await?.rows)
}

enum RoutePhase {
    Raise,
    Pass,
}
enum ParentDamage {
    MissingEdge,
    MalformedParent,
}

async fn bad_parent(
    fixture: &impl ApprovalLinkFixture,
    phase: RoutePhase,
    damage: ParentDamage,
) -> Result<()> {
    let mut setup = ParentSetup::open(fixture, Chain::ThreeLevels).await?;
    let grandparent = setup.grandparent.as_ref().ok_or("missing grandparent")?;
    let (child, parent) = match phase {
        RoutePhase::Raise => (&setup.child, &setup.parent),
        RoutePhase::Pass => (&setup.parent, grandparent),
    };
    let corrupt = match damage {
        ParentDamage::MissingEdge => Statement {
            sql: "DELETE FROM actor_delegates WHERE actor_id=?",
            args: vec![text(child.session_id.as_str())],
        },
        ParentDamage::MalformedParent => Statement {
            sql: "UPDATE actor_sessions SET session_state='invalid' WHERE actor_id=?",
            args: vec![text(parent.session_id.as_str())],
        },
    };
    fixture.fixture_write(corrupt).await?;
    let before = approval_state(fixture).await?;
    match phase {
        RoutePhase::Raise => {
            let request = provider_raise(
                &setup.target,
                &setup.child.session_id,
                0x2c0,
                ProviderAsk::Approval,
            )?;
            assert_eq!(
                central_call::<RaiseApprovalRpc>(
                    &mut setup.socket,
                    &setup.target.organization_id,
                    request
                )
                .await?,
                TypedCallResult::Refused {
                    reason: LinkCallRefusal::CentralFault
                }
            );
        }
        RoutePhase::Pass => {
            let id = setup.approval.request_id().clone();
            assert_eq!(
                call::<PassOnApprovalForSessionRpc>(
                    &mut setup.socket,
                    &setup.target.organization_id,
                    setup.parent.request(PassOnApprovalRequest {
                        request_id: id,
                        operation_id: typed_id(0x72c1)?
                    })
                )
                .await?,
                TypedCallResult::Refused {
                    reason: LinkCallRefusal::CentralFault
                }
            );
        }
    }
    heartbeat(&mut setup.socket).await?;
    assert_eq!(approval_state(fixture).await?, before);
    Ok(())
}

async fn approval_state(fixture: &impl ApprovalLinkFixture) -> Result<Vec<Vec<Vec<SqlValue>>>> {
    let mut state = Vec::new();
    for sql in [
        "SELECT * FROM approval_requests ORDER BY approval_row_id",
        "SELECT * FROM approval_request_destinations ORDER BY approval_destination_id",
        "SELECT * FROM engine_due_entries ORDER BY subject_key",
        "SELECT * FROM runtime_request_log ORDER BY request_id",
        "SELECT * FROM approval_applying_time ORDER BY organization_id",
    ] {
        state.push(fixture.read(Statement { sql, args: vec![] }).await?.rows);
    }
    Ok(state)
}

async fn corrupt_destination(
    fixture: &impl ApprovalLinkFixture,
    proof: ParentApprovalProof,
) -> Result<()> {
    use super::delivery_cases::observe_no_recovered_notice;
    use approval_request_protocol::ReadApprovalForSessionRequest;
    use central_link_protocol::approval_rpc::ReadApprovalForSessionRpc;
    use reqwest_websocket::CloseCode;

    let has_chain = matches!(
        proof,
        ParentApprovalProof::ForwardAtDeadline | ParentApprovalProof::BackwardsDestination
    );
    let mut setup = ParentSetup::open(
        fixture,
        if has_chain {
            Chain::ThreeLevels
        } else {
            Chain::Direct
        },
    )
    .await?;
    let id = setup.approval.request_id().clone();
    if has_chain {
        fixture.advance(1).await?;
        let grandparent = setup
            .grandparent
            .as_ref()
            .ok_or("missing grandparent")?
            .session_id
            .clone();
        let reply = setup.pass(id.clone(), typed_id(0x72e1)?).await?;
        assert_eq!(
            reply,
            PassOnApprovalReply::PassedOn {
                to: ApprovalDestination::Session {
                    session_id: grandparent.clone()
                },
            }
        );
        acknowledge_notice(
            &mut setup.socket,
            &setup.target,
            &grandparent,
            &id,
            ApprovalNoticeReply::Received {},
        )
        .await?;
        if matches!(proof, ParentApprovalProof::BackwardsDestination) {
            fixture.advance(1).await?;
            let next = setup.grandparent.take().ok_or("missing grandparent")?;
            let prior = std::mem::replace(&mut setup.parent, next);
            assert_eq!(
                setup.pass(id.clone(), typed_id(0x72e2)?).await?,
                PassOnApprovalReply::PassedOn {
                    to: ApprovalDestination::People {}
                }
            );
            setup.parent = prior;
        }
    }
    if matches!(proof, ParentApprovalProof::ReadAfterEnding) {
        assert!(matches!(
            setup
                .decide(id.clone(), typed_id(0x72e3)?, option(1)?)
                .await?,
            DecideApprovalAsSessionReply::SavedDecision { .. }
        ));
        let (correlation, _) = next_ending(&mut setup.socket, &setup.target).await?;
        crate::approval_link_scenarios::answer_ending(
            &mut setup.socket,
            setup.target.clone(),
            correlation,
            approval_request_protocol::DeliverEndingReply::Received {},
        )
        .await?;
        crate::approval_link_scenarios::observe_received(fixture, &setup.approval).await?;
    }
    let receipt = fixture
        .read(Statement {
            sql: "SELECT first_saved_at, deadline_ms FROM approval_requests WHERE request_id=?",
            args: vec![text(id.as_str())],
        })
        .await?;
    let [record] = receipt.rows.as_slice() else {
        return Err("missing approval record".into());
    };
    let [SqlValue::Text(first_save), SqlValue::Integer(deadline)] = record.as_slice() else {
        return Err("invalid saved window".into());
    };
    let deadline = approval_request_protocol::CentralTime::from_timestamp_millis(deadline.get())
        .ok_or("invalid deadline")?;
    let following = (deadline - chrono::Duration::seconds(10))
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let edge = deadline.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let (sql, args) = match proof {
        ParentApprovalProof::PositionGap => (
            "UPDATE approval_request_destinations SET position=2 WHERE position=0 AND approval_row_id=(SELECT approval_row_id FROM approval_requests WHERE request_id=?)",
            vec![text(id.as_str())],
        ),
        ParentApprovalProof::InvalidReadFact => (
            "UPDATE approval_request_destinations SET first_read_at='invalid' WHERE position=0 AND approval_row_id=(SELECT approval_row_id FROM approval_requests WHERE request_id=?)",
            vec![text(id.as_str())],
        ),
        ParentApprovalProof::ForwardAtDeadline => (
            "UPDATE approval_request_destinations SET created_at=?1,raised_notice_at=?1,updated_at=?1 WHERE position=1 AND approval_row_id=(SELECT approval_row_id FROM approval_requests WHERE request_id=?2)",
            vec![text(&edge), text(id.as_str())],
        ),
        ParentApprovalProof::BackwardsDestination => (
            "UPDATE approval_request_destinations SET created_at=?1 WHERE position=2 AND approval_row_id=(SELECT approval_row_id FROM approval_requests WHERE request_id=?2)",
            vec![text(first_save), text(id.as_str())],
        ),
        ParentApprovalProof::ReadAfterEnding => (
            "UPDATE approval_request_destinations SET first_read_at=?1,updated_at=?1 WHERE position=0 AND approval_row_id=(SELECT approval_row_id FROM approval_requests WHERE request_id=?2)",
            vec![text(&following), text(id.as_str())],
        ),
        _ => return Err("unknown destination corruption fixture".into()),
    };
    fixture.fixture_write(Statement { sql, args }).await?;
    let before = approval_state(fixture).await?;
    assert_eq!(
        call::<ReadApprovalForSessionRpc>(
            &mut setup.socket,
            &setup.target.organization_id,
            setup
                .parent
                .request(ReadApprovalForSessionRequest { request_id: id }),
        )
        .await?,
        TypedCallResult::Refused {
            reason: LinkCallRefusal::CentralFault
        }
    );
    assert_eq!(approval_state(fixture).await?, before);
    setup.socket.close(CloseCode::Normal, None).await?;
    fixture.arm_notice_recovery().await?;
    let mut socket = fixture.connect().await?;
    observe_no_recovered_notice(fixture, &mut socket).await?;
    assert_eq!(approval_state(fixture).await?, before);
    Ok(())
}
