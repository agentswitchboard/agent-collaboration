use approval_request_protocol::{CentralTime, Ending, RegisterRunReply};
use central_storage::{SqlValue, Statement};
use serde_json::{Value, json};

use super::{
    DECIDE, DecisionRaceOrder, OWNER, Person, PersonApprovalPause, PersonApprovalRaceFixture,
    RacePauseStep, Result, expect_ok, fresh_request_id, publish_person_floors, read_winner, text,
};
use crate::approval_link_scenarios::{
    ApprovalLinkFixture, heartbeat, raise, receive_decision, register_run, require_receipt,
};

/// A signed person and an authenticated destination session decide the same passed request.
pub async fn check_person_session_race(
    fixture: &impl PersonApprovalRaceFixture,
    order: DecisionRaceOrder,
) -> Result<()> {
    use crate::approval_provider_scenarios::{ProviderAsk, create_bound, provider_raise};
    use crate::parent_approval_scenarios::{
        SessionProof, acknowledge_notice, answered, call, option, typed_id,
    };
    use approval_request_protocol::{
        Answer, ApprovalDestination, ApprovalNoticeReply, DecideApprovalAsSessionReply,
        DecideApprovalAsSessionRequest, DecidedBy, PassOnApprovalReply, PassOnApprovalRequest,
        ProviderAnswer, SessionDecider,
    };
    use central_link_protocol::approval_rpc::{
        DecideApprovalAsSessionRpc, PassOnApprovalForSessionRpc,
    };

    let link = fixture.link();
    let target = link.target()?;
    let organization = &target.organization_id;
    let mut socket = link.connect().await?;
    assert_eq!(
        register_run(&mut socket, organization, &target.router_run).await?,
        RegisterRunReply::Registered {}
    );
    let (parent_id, parent_secret) = create_bound(&mut socket, organization, 0x48, None).await?;
    let parent = SessionProof {
        session_id: parent_id,
        secret: parent_secret,
    };
    let (child_id, _) = create_bound(
        &mut socket,
        organization,
        0x49,
        Some((&parent.session_id, parent.secret.clone())),
    )
    .await?;
    let request = provider_raise(&target, &child_id, 0x4a, ProviderAsk::Approval)?;
    let id = request.request_id();
    let receipt = require_receipt(raise(&mut socket, organization, request.clone()).await?)?;
    acknowledge_notice(
        &mut socket,
        &target,
        &parent.session_id,
        id,
        ApprovalNoticeReply::Received {},
    )
    .await?;
    assert_eq!(
        answered(
            call::<PassOnApprovalForSessionRpc>(
                &mut socket,
                organization,
                parent.request(PassOnApprovalRequest {
                    request_id: id.clone(),
                    operation_id: typed_id(0x7350)?
                })
            )
            .await?
        )?,
        PassOnApprovalReply::PassedOn {
            to: ApprovalDestination::People {}
        }
    );
    publish_person_floors(fixture, organization.as_str()).await?;

    let sampled = link.now_seconds();
    let owner = Person::owner(organization.as_str(), sampled)?;
    let person_operation = fresh_request_id(sampled)?;
    let session_operation = typed_id(0x7351)?;
    let person_value = json!({"kind": "approval", "optionId": 1});
    let body = json!({"organization": organization, "requestId": id, "operationId": person_operation, "answer": person_value});
    let intent = json!({"v":1, "operation":"ApprovalDecide", "organization":organization,
        "target":id, "operationId":person_operation, "params":{"answer":person_value}});
    let headers = owner.headers(DECIDE, &intent, &fresh_request_id(sampled)?, sampled)?;
    let pause = match order {
        DecisionRaceOrder::CompetitorFirst => PersonApprovalPause::BeforeTurn,
        DecisionRaceOrder::PersonFirst => PersonApprovalPause::AfterCommit,
    };
    fixture.control_pause(pause, RacePauseStep::Arm).await?;
    let mut deciding = std::pin::pin!(fixture.post_private(DECIDE, &body, headers));
    let reached = tokio::select! {
        reached = fixture.control_pause(pause, RacePauseStep::Wait) => reached,
        reply = &mut deciding => Err(format!("signed Decide returned before its hold: {reply:?}").into()),
    };
    let person_first = matches!(order, DecisionRaceOrder::PersonFirst);
    let winner = if person_first {
        DecidedBy::Person {
            person_id: OWNER.parse()?,
        }
    } else {
        DecidedBy::Session {
            session: parent.session_id.clone(),
        }
    };
    let Answer::Approval { option_id } = option(1)? else {
        return Err("expected fixture option".into());
    };
    let ending = Ending::Decided {
        answer: ProviderAnswer::Approval {
            option_id: option_id.clone(),
        },
        by: winner.clone(),
    };
    let at = CentralTime::from_timestamp(sampled, 0).ok_or("invalid case time")?;
    let winner_operation = if person_first {
        person_operation.as_str()
    } else {
        session_operation.as_str()
    };

    let observed = async {
        reached?;
        if person_first {
            receive_decision(link, &mut socket, target.clone(), &request, ending.clone()).await?;
        }
        let session_reply = answered(
            call::<DecideApprovalAsSessionRpc>(
                &mut socket,
                organization,
                parent.request(DecideApprovalAsSessionRequest {
                    request_id: id.clone(),
                    operation_id: session_operation.clone(),
                    value: option(1)?,
                }),
            )
            .await?,
        )?;
        assert_eq!(
            session_reply,
            if person_first {
                DecideApprovalAsSessionReply::AlreadyDecided {
                    by: winner.clone(),
                    at,
                }
            } else {
                DecideApprovalAsSessionReply::SavedDecision {
                    decision: approval_request_protocol::ApprovalDecision {
                        answer: ProviderAnswer::Approval { option_id },
                        by: SessionDecider::Session {
                            session: parent.session_id,
                        },
                    },
                }
            }
        );
        if !person_first {
            receive_decision(link, &mut socket, target.clone(), &request, ending.clone()).await?;
        }
        let stored = read_winner(link, &request).await?;
        stored.require(
            &request,
            ending,
            sampled,
            Some(winner_operation),
            "received_by_holder",
        )?;
        heartbeat(&mut socket).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(stored)
    }
    .await;
    let released = fixture.control_pause(pause, RacePauseStep::Release).await;
    let after_first = observed?;
    released?;

    let actual: Value = serde_json::from_slice(&expect_ok(deciding.await?)?)?;
    let expected = if person_first {
        json!({"kind":"savedDecision","decision":{"by":winner,"answer":person_value}})
    } else {
        json!({"kind":"alreadyDecided","by":winner,"at":at})
    };
    assert_eq!(actual, expected);
    assert_eq!(read_winner(link, &request).await?, after_first);
    let deadline = link
        .read(Statement {
            sql: "SELECT deadline_ms FROM approval_requests WHERE request_id=?",
            args: vec![text(id.as_str())],
        })
        .await?;
    assert_eq!(
        deadline.rows,
        [[SqlValue::Integer(
            receipt.window.deadline().timestamp_millis().try_into()?
        )]]
    );
    heartbeat(&mut socket).await
}
