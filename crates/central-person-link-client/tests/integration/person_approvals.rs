use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use approval_request_protocol::{
    Answer, ApprovalDecideReply, ApprovalListFilter, ApprovalListReply, ApprovalReadReply,
    ApprovalRequestId, ApprovalSummaryState, DecidedBy, DeliverEndingReply, Ending, GiverDecision,
    OfferedOptionId, ProviderAnswer, Raise, SessionCallDecision,
};
use central_link_protocol::{
    CentralCall, DataFrame, LinkFrame, MacCallKind, PersonLinkCloseCode,
    approval_rpc::{ApprovalHolderTarget, RegisterRunReply},
};
use central_person_link_client::{
    ApprovalDecisionOutcome, PersonLinkError,
    test_support::{
        fixture_authority::INITIAL_SECONDS,
        fixture_signin::{ApprovalRelayFault, ApprovalSignIn},
    },
};
use central_storage::{SqlValue, Statement};
use collaboration_central_conformance::{
    approval_link_scenarios::{
        ROUTER_RUN, announce_mac_calls, answer_ending, heartbeat, held_start, next_ending, raise,
        receive_frame, register_run, require_receipt,
    },
    approval_provider_scenarios::{ProviderAsk, create_bound, provider_raise},
    signin_fixtures::{
        ORGANIZATION, OWNER, OWNER_SUBJECT, POLICY, Person,
        approval_socket::{self, PersonApprovalEndpoint, SocketApprovalReply},
        fresh_request_id,
        person_link::person_link_bearer,
    },
};
use collaboration_central_tokio::test_support::{
    MACHINE_LINK_FIXTURE_MACHINE, MachineLinkFixture, start_session_link_fixture,
};
use fleet_runtime::PausePoint;
use reqwest_websocket::WebSocket;
use router_clock::{ManualClock, RuntimeClock};
use serde_json::{Value, json};
use workspace_ids::RequestId;

use super::PERSON;

#[path = "person_approval_variants.rs"]
mod variants;

#[path = "person_approval_fixture.rs"]
mod fixture;

use fixture::ApprovalFixture;

fn operation(fixture: &ApprovalFixture) -> RequestId {
    fresh_request_id(fixture.central.clock.utc_now().timestamp())
        .unwrap()
        .parse()
        .unwrap()
}

async fn seed(holder: &mut WebSocket, target: &ApprovalHolderTarget, request: Raise) -> Raise {
    require_receipt(
        raise(holder, &target.organization_id, request.clone())
            .await
            .unwrap(),
    )
    .unwrap();
    request
}

fn allow() -> Answer {
    Answer::UnprovenCall {
        decision: SessionCallDecision::Allow,
    }
}

fn person() -> DecidedBy {
    DecidedBy::Person {
        person_id: OWNER.parse().unwrap(),
    }
}

fn saved(answer: Answer) -> ApprovalDecisionOutcome {
    ApprovalDecisionOutcome::Reply(ApprovalDecideReply::SavedDecision {
        decision: GiverDecision {
            by: person(),
            answer,
        },
    })
}

fn held_ending() -> Ending {
    Ending::UnprovenCallDecided {
        decision: SessionCallDecision::Allow,
        person_id: OWNER.parse().unwrap(),
    }
}

#[tokio::test]
async fn lists_every_waiting_page_in_order_and_rejects_a_repeated_cursor() {
    let fixture = ApprovalFixture::start().await;
    let (target, mut holder) = fixture.holder().await;
    let mut ids = Vec::new();
    for ordinal in (1..=101).rev() {
        let request = seed(
            &mut holder,
            &target,
            held_start(&target.router_run, ordinal).unwrap(),
        )
        .await;
        ids.push(request.request_id().clone());
    }
    let ended = seed(
        &mut holder,
        &target,
        held_start(&target.router_run, 102).unwrap(),
    )
    .await;
    let mut client = fixture.signin.client();
    assert_eq!(
        client
            .decide_approval(ended.request_id().clone(), operation(&fixture), allow())
            .await
            .unwrap(),
        saved(allow())
    );
    fixture
        .ending(&mut holder, &target, &ended, held_ending())
        .await;

    let ApprovalListReply::Page { items, next } = client
        .list_approvals(ApprovalListFilter::Waiting)
        .await
        .unwrap();
    assert_eq!(next, None);
    assert!(
        items
            .iter()
            .all(|item| matches!(item.state, ApprovalSummaryState::Waiting {}))
    );
    assert_eq!(
        items
            .into_iter()
            .map(|item| item.request_id)
            .collect::<Vec<_>>(),
        ids
    );
    {
        let evidence = fixture.signin.relay.evidence.lock().unwrap();
        let [_, first, second] = evidence.approval_calls.as_slice() else {
            panic!("expected decide and exactly two waiting-list calls");
        };
        assert_eq!((first.socket, second.socket), (1, 1));
        assert!(
            matches!(&first.request, CentralCall::ListPersonApprovals(call) if call.body.cursor.is_none())
        );
        assert!(
            matches!(&second.request, CentralCall::ListPersonApprovals(call) if call.body.cursor.as_ref() == ids.get(99))
        );
    }

    let ApprovalListReply::Page { items, next } = client
        .list_approvals(ApprovalListFilter::All)
        .await
        .unwrap();
    assert_eq!(next, None);
    assert_eq!(items.len(), 102);
    assert!(
        items
            .iter()
            .any(|item| &item.request_id == ended.request_id()
                && matches!(item.state, ApprovalSummaryState::Ended { .. }))
    );

    fixture
        .signin
        .relay
        .script_once(ApprovalRelayFault::RepeatListCursor);
    assert_eq!(
        client.list_approvals(ApprovalListFilter::Waiting).await,
        Err(PersonLinkError::ApprovalCursor)
    );
    drop(client);
    drop(holder);
    fixture.close().await;
}

#[tokio::test]
async fn reads_provider_options_and_decides_both_kinds_as_the_person() {
    let fixture = ApprovalFixture::start().await;
    let (target, mut holder) = fixture.holder().await;
    let (provider, _) = create_bound(&mut holder, &target.organization_id, 1, None)
        .await
        .unwrap();
    let request = provider_raise(&target, &provider, 2, ProviderAsk::Approval).unwrap();
    let request = seed(&mut holder, &target, request).await;
    let held = seed(
        &mut holder,
        &target,
        held_start(&target.router_run, 3).unwrap(),
    )
    .await;
    let mut client = fixture.signin.client();
    let ApprovalReadReply::Request { view } = client
        .read_approval(request.request_id().clone())
        .await
        .unwrap()
    else {
        panic!("expected visible request");
    };
    assert_eq!(view.request_id(), request.request_id());
    let view = serde_json::to_value(view).unwrap();
    assert_eq!(view.pointer("/state/kind"), Some(&json!("waiting")));
    assert_eq!(view.pointer("/offer/options/0/id"), Some(&json!(1)));

    let wrong_option = Answer::Approval {
        option_id: OfferedOptionId::new(2).unwrap(),
    };
    assert!(matches!(
        client
            .decide_approval(
                request.request_id().clone(),
                operation(&fixture),
                wrong_option
            )
            .await
            .unwrap(),
        ApprovalDecisionOutcome::Reply(ApprovalDecideReply::Invalid { .. })
    ));
    let option_id = OfferedOptionId::new(1).unwrap();
    let answer = Answer::Approval {
        option_id: option_id.clone(),
    };
    assert_eq!(
        client
            .decide_approval(
                request.request_id().clone(),
                operation(&fixture),
                answer.clone()
            )
            .await
            .unwrap(),
        saved(answer)
    );
    fixture
        .ending(
            &mut holder,
            &target,
            &request,
            Ending::Decided {
                answer: ProviderAnswer::Approval { option_id },
                by: person(),
            },
        )
        .await;

    assert_eq!(
        client
            .decide_approval(held.request_id().clone(), operation(&fixture), allow())
            .await
            .unwrap(),
        saved(allow())
    );
    fixture
        .ending(&mut holder, &target, &held, held_ending())
        .await;

    let unfamiliar: ApprovalRequestId = "019f0000-0000-7000-8000-00000000f001".parse().unwrap();
    assert_eq!(
        client.read_approval(unfamiliar.clone()).await.unwrap(),
        ApprovalReadReply::NotVisible {}
    );
    assert_eq!(
        client
            .decide_approval(unfamiliar, operation(&fixture), allow())
            .await
            .unwrap(),
        ApprovalDecisionOutcome::Reply(ApprovalDecideReply::NotVisible {})
    );
    drop(client);
    drop(holder);
    fixture.close().await;
}

#[tokio::test]
async fn revision_after_commit_reconnects_and_replays_the_exact_decide() {
    let fixture = ApprovalFixture::start().await;
    let (target, mut holder) = fixture.holder().await;
    let request = seed(
        &mut holder,
        &target,
        held_start(&target.router_run, 1).unwrap(),
    )
    .await;
    let operation_id = operation(&fixture);
    let runtime = fixture
        .central
        .host
        .central(&fixture.central.organization_id)
        .await
        .unwrap();
    let mut pause = runtime
        .runtime()
        .pause_points()
        .hold_central(PausePoint::AfterPersonApprovalCommit);
    let mut client = fixture.signin.client();

    let decide =
        client.decide_approval(request.request_id().clone(), operation_id.clone(), allow());
    let revise = async {
        pause.until_held().await;
        assert_eq!(
            fixture.operation_rows().await,
            [vec![SqlValue::Text(operation_id.to_string())]]
        );
        fixture.publish_person_revision(5).await;
        pause.release();
    };
    let (outcome, ()) = tokio::join!(decide, revise);

    assert_eq!(outcome.unwrap(), saved(allow()));
    fixture
        .ending(&mut holder, &target, &request, held_ending())
        .await;
    assert_eq!(
        fixture.operation_rows().await,
        [vec![SqlValue::Text(operation_id.to_string())]]
    );
    {
        let evidence = fixture.signin.relay.evidence.lock().unwrap();
        let [first, replay] = evidence.approval_calls.as_slice() else {
            panic!("expected exactly two decide attempts");
        };
        assert_eq!(evidence.closes, [PersonLinkCloseCode::Reconnect.code()]);
        assert_eq!((first.socket, replay.socket), (1, 2));
        assert_ne!(first.correlation, replay.correlation);
        assert_eq!(first.request, replay.request);
        assert!(
            matches!(&first.request, CentralCall::DecidePersonApproval(call)
            if &call.body.request_id == request.request_id() && call.body.operation_id == operation_id && call.body.answer == allow())
        );
    }
    drop(client);
    drop(holder);
    fixture.close().await;
}

#[tokio::test]
async fn lost_committed_reply_is_unknown_and_the_operation_id_recovers_it() {
    let fixture = ApprovalFixture::start().await;
    let (target, mut holder) = fixture.holder().await;
    let request = seed(
        &mut holder,
        &target,
        held_start(&target.router_run, 1).unwrap(),
    )
    .await;
    let request_id = request.request_id().clone();
    let operation_id = operation(&fixture);
    let runtime = fixture
        .central
        .host
        .central(&fixture.central.organization_id)
        .await
        .unwrap();
    runtime
        .runtime()
        .lose_reply_after_apply(operation_id.clone());
    let mut client = fixture.signin.client();

    let outcome = client
        .decide_approval(request_id.clone(), operation_id.clone(), allow())
        .await
        .unwrap();

    assert_eq!(
        outcome,
        ApprovalDecisionOutcome::Unknown {
            operation_id: operation_id.clone()
        }
    );
    assert_eq!(
        fixture.operation_rows().await,
        [vec![SqlValue::Text(operation_id.to_string())]]
    );
    assert_eq!(fixture.signin.relay.evidence.lock().unwrap().closes, [1011]);
    fixture
        .ending(&mut holder, &target, &request, held_ending())
        .await;

    let mut client = fixture.signin.client();
    assert_eq!(
        client
            .decide_approval(request_id.clone(), operation_id.clone(), allow())
            .await
            .unwrap(),
        saved(allow())
    );
    assert_eq!(
        client
            .decide_approval(
                request_id.clone(),
                operation_id,
                Answer::UnprovenCall {
                    decision: SessionCallDecision::Decline
                }
            )
            .await
            .unwrap(),
        ApprovalDecisionOutcome::Reply(ApprovalDecideReply::OperationConflict {})
    );
    assert!(
        matches!(client.decide_approval(request_id, operation(&fixture), allow()).await.unwrap(), ApprovalDecisionOutcome::Reply(ApprovalDecideReply::AlreadyDecided { by, .. }) if by == person())
    );
    heartbeat(&mut holder).await.unwrap();
    drop(client);
    drop(holder);
    fixture.close().await;
}

#[tokio::test]
async fn expired_request_is_too_late_and_foreign_target_is_a_typed_refusal() {
    let fixture = ApprovalFixture::start().await;
    let (target, mut holder) = fixture.holder().await;
    let request = seed(
        &mut holder,
        &target,
        held_start(&target.router_run, 1).unwrap(),
    )
    .await;
    let foreign = "019f0000-0000-7000-8000-00000000f002";
    let raw_client = fixture.central.client().unwrap();
    let origin = fixture.central.origin();
    let now = fixture.central.clock.utc_now().timestamp();
    let mut owner = Person::owner(ORGANIZATION, now).unwrap();
    owner.second_factor_at = None;
    let mut foreign_call = PersonApprovalEndpoint {
        client: &raw_client,
        origin: &origin,
        now_seconds: now,
    }
    .connect(&owner)
    .await
    .unwrap();
    let wrong_target = approval_socket::exchange(
        &mut foreign_call.socket,
        &fresh_request_id(now).unwrap(),
        approval_socket::READ,
        foreign,
        &json!({"requestId": request.request_id()}),
    )
    .await
    .unwrap();
    assert_eq!(
        wrong_target,
        SocketApprovalReply::Refused {
            reason: "wrongTarget".into()
        }
    );
    drop(foreign_call);
    fixture
        .central
        .clock
        .advance(Duration::from_secs(3601))
        .unwrap();

    let mut client = fixture.signin.client();
    let outcome = client
        .decide_approval(request.request_id().clone(), operation(&fixture), allow())
        .await
        .unwrap();

    assert_eq!(
        outcome,
        ApprovalDecisionOutcome::Reply(ApprovalDecideReply::TooLate {})
    );
    drop(client);
    drop(holder);
    fixture.close().await;
}

#[tokio::test]
async fn mismatched_correlated_reply_fails_the_decide_protocol() {
    let fixture = ApprovalFixture::start().await;
    let (target, mut holder) = fixture.holder().await;
    let request = seed(
        &mut holder,
        &target,
        held_start(&target.router_run, 1).unwrap(),
    )
    .await;
    let mut client = fixture.signin.client();
    fixture
        .signin
        .relay
        .script_once(ApprovalRelayFault::WrongDecideReply);

    let outcome = client
        .decide_approval(request.request_id().clone(), operation(&fixture), allow())
        .await;

    assert_eq!(outcome, Err(PersonLinkError::Protocol));
    assert!(fixture.operation_rows().await.is_empty());
    drop(client);
    drop(holder);
    fixture.close().await;
}
