use super::*;
use crate::external_provider_supervisor::{AdmissionTestPause, PromptBusyPolicy};
use collaboration_protocol::{MessageDelivery as ProviderMessageDelivery, ProviderKind};
use provider_session_machines::session_turn_machine::SessionTurn;
use runtime_primitives::Clock as _;
use state_machine_core::StateMachine as _;
use switchboard_machine_core_store::{
    DirectHandoff, DirectHandoffChange, HandoffCommit, InboxDecision, InboxOfferExpiry, InboxStore,
    MachineLocalStore, SeenReceiptState,
};
use switchboard_message_custody::{
    flush_marks::next_mark_after,
    handover::{DirectHandoffMachine, Handoff, HandoffEffect, HandoffEvent, NoticePreTakeRefusal},
};

const NOTICE_MESSAGE_ID: &str = "019f0000-0000-7000-8000-000000000017";

#[tokio::test]
async fn pre_owner_notice_custody_yields_to_the_human_prompt() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::HumanThenNoticeSteer,
        ProviderKind::ClaudeCode,
    )
    .await;
    let (adapter, recipient, notice) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));
    let attempt = notice.attempt();
    let notice_operation = OperationId::try_from(String::from(attempt)).unwrap();
    let (notice_held, release_notice) = pause_before_owner(&rig, notice_operation);
    let ready = adapter.ready(&recipient).await.unwrap();
    let offer = ready.offer(notice);
    tokio::pin!(offer);
    tokio::select! {
        reached = notice_held => reached.unwrap(),
        result = &mut offer => panic!("the notice passed its pre-owner pause: {result:?}"),
    }

    let operation = OperationId::generate();
    let started = rig
        .supervisor
        .prompt_contents(human_prompt(&rig, operation, PromptBusyPolicy::RefuseBusy))
        .await;
    let _ = release_notice.send(());
    assert!(
        started.is_ok(),
        "the held notice blocked the human: {started:?}"
    );
    let outcome = offer.await;
    let refused = rig
        .supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id: OperationId::try_from(String::from(attempt)).unwrap(),
            timeout_seconds: PositiveSeconds::try_from(1).unwrap(),
        })
        .await
        .expect_err("the original idle offer cannot take the human's turn");
    assert_eq!(refused.effect, ProviderOperationEffect::None);
    assert_waiting_then_steered(&rig, &clock, &adapter, recipient, attempt, outcome).await;
    finish_notice_turn(&rig).await;
    rig.supervisor.shutdown().await.unwrap();
    assert_wire_script_completed(&rig, true);
}

#[tokio::test]
async fn a_reserved_human_prompt_defers_notice_until_handoff_releases() {
    for policy in [
        PromptBusyPolicy::RefuseBusy,
        PromptBusyPolicy::Send(ProviderMessageDelivery::Auto),
    ] {
        let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
        let rig = managed_notice_fixture(
            clock.clone(),
            NoticeScript::HumanThenNoticeSteer,
            ProviderKind::ClaudeCode,
        )
        .await;
        let (adapter, recipient, notice) =
            prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));
        let attempt = notice.attempt();
        let notice_operation = OperationId::try_from(String::from(attempt)).unwrap();
        let operation = OperationId::generate();
        let (human_held, release_human) = pause_before_owner(&rig, operation.clone());
        let human = rig
            .supervisor
            .prompt_contents(human_prompt(&rig, operation, policy));
        tokio::pin!(human);
        tokio::select! {
            reached = human_held => reached.unwrap(),
            result = &mut human => panic!("the human passed its pre-owner pause: {result:?}"),
        }
        assert_eq!(
            rig.supervisor.owner_session_activity(&rig.target),
            crate::ProviderSessionActivity::Idle
        );

        let (notice_reached_owner, release_notice) = pause_before_owner(&rig, notice_operation);
        let offer = adapter.ready(&recipient).await.unwrap().offer(notice);
        tokio::pin!(offer);
        let outcome = tokio::select! {
            result = &mut offer => result,
            reached = notice_reached_owner => {
                reached.unwrap();
                drop(release_notice);
                panic!("a notice passed admission while a human reservation was held");
            }
        };
        release_human.send(()).unwrap();
        human.await.expect("the earlier human request runs");
        assert_waiting_then_steered(&rig, &clock, &adapter, recipient, attempt, outcome).await;
        finish_notice_turn(&rig).await;
        rig.supervisor.shutdown().await.unwrap();
        assert_wire_script_completed(&rig, true);
    }
}

#[tokio::test]
async fn a_taken_notice_turn_gets_the_ordinary_running_refusal_for_a_human_prompt() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::Direct,
        ProviderKind::ClaudeCode,
    )
    .await;
    let (adapter, recipient, notice) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));
    assert!(matches!(
        adapter.ready(&recipient).await.unwrap().offer(notice).await,
        NoticeWriteOutcome::Written { .. }
    ));
    assert_eq!(
        rig.supervisor.owner_session_activity(&rig.target),
        crate::ProviderSessionActivity::Running
    );
    let refused = rig
        .supervisor
        .prompt_contents(human_prompt(
            &rig,
            OperationId::generate(),
            PromptBusyPolicy::RefuseBusy,
        ))
        .await
        .expect_err("the notice is an actual running turn");
    assert_eq!(refused.kind, ConversationOperationFailureKind::Busy);
    assert_eq!(refused.effect, ProviderOperationEffect::None);
    finish_notice_turn(&rig).await;
    rig.supervisor.shutdown().await.unwrap();
    assert_wire_script_completed(&rig, false);
}

fn pause_before_owner(
    rig: &SteeringSession,
    operation: OperationId,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (entered, wait_for_entry) = tokio::sync::oneshot::channel();
    let (release_owner, release) = tokio::sync::oneshot::channel();
    *rig.supervisor
        .inner
        .before_owner_prompt_test_pause
        .lock()
        .unwrap() = Some((operation, AdmissionTestPause { entered, release }));
    (wait_for_entry, release_owner)
}

fn human_prompt(
    rig: &SteeringSession,
    operation_id: OperationId,
    busy: PromptBusyPolicy,
) -> ProviderPromptContentsRequest {
    ProviderPromptContentsRequest {
        operation_id,
        input_id: session_event_model::InputId::generate(),
        target: rig.target.clone(),
        requested_by: rig.target.clone().into(),
        approver: rig.target.clone().into(),
        input: acp_client_runtime::ProviderTurnInput::Prompt(
            session_event_model::ProviderSubmission::plain(vec![
                session_event_model::PromptContent::text(HUMAN_TEXT.to_owned()).unwrap(),
            ]),
        ),
        busy,
    }
}

async fn assert_waiting_then_steered(
    rig: &SteeringSession,
    clock: &ManualClock,
    adapter: &ManagedAcpNoticeAdapter,
    recipient: MailSessionRef,
    attempt: NoticeAttemptId,
    outcome: NoticeWriteOutcome,
) {
    use workspace_model::{
        PostText,
        central_mail::{MessageBody, MessageDelivery, MessageRecord},
    };

    assert_eq!(
        outcome,
        NoticeWriteOutcome::DefinitelyNotWritten(NoticePreTakeRefusal::RejectedBeforeTake)
    );
    let now = clock.utc_now();
    let expiry = now + chrono::TimeDelta::minutes(15);
    let message = MessageRecord::queued_central(
        NOTICE_MESSAGE_ID.parse().unwrap(),
        recipient.clone(),
        MessageBody::Direct {
            body: PostText::try_from("inbox fixture".to_owned()).unwrap(),
            delivery: MessageDelivery::Auto,
            reply_to: None,
        },
        now,
    );
    let mut inbox = InboxStore::new(
        MachineLocalStore::open(&rig._root.path().join("notice-inbox.sqlite"))
            .await
            .unwrap(),
    );
    inbox
        .record_delivery(
            &message,
            InboxDecision::Accepted,
            now.timestamp_millis(),
            InboxOfferExpiry::DirectWithExpiry(expiry),
        )
        .await
        .unwrap();
    let waiting = Handoff {
        expires_at: expiry,
        phase: DirectHandoff::Waiting,
    };
    let (writing, effects) =
        DirectHandoffMachine::transition(&waiting, HandoffEvent::EligibleAtMark { attempt, now })
            .unwrap();
    assert_eq!(effects, [HandoffEffect::OfferClaimedNotice]);
    commit_notice_state(&mut inbox, &recipient, waiting.phase, writing.phase, now).await;

    let (waiting, effects) = DirectHandoffMachine::transition(
        &writing,
        HandoffEvent::WriteFinished {
            attempt,
            outcome,
            now,
        },
    )
    .unwrap();
    assert!(effects.is_empty());
    commit_notice_state(&mut inbox, &recipient, writing.phase, waiting.phase, now).await;
    let row = inbox
        .scan_direct_handoffs()
        .await
        .unwrap()
        .healthy
        .remove(0);
    assert_eq!(row.handoff, DirectHandoff::Waiting);

    clock.advance_to(next_mark_after(clock.now())).unwrap();
    let now = clock.utc_now();
    let next_attempt = NoticeAttemptId::mint(now);
    let (retry, effects) = DirectHandoffMachine::transition(
        &Handoff::from(&row),
        HandoffEvent::EligibleAtMark {
            attempt: next_attempt,
            now,
        },
    )
    .unwrap();
    assert_eq!(effects, [HandoffEffect::OfferClaimedNotice]);
    assert_eq!(
        retry.phase,
        DirectHandoff::Writing {
            attempt: next_attempt
        }
    );
    commit_notice_state(&mut inbox, &recipient, row.handoff, retry.phase, now).await;
    assert_eq!(
        inbox.scan_direct_handoffs().await.unwrap().healthy[0].handoff,
        retry.phase
    );

    let foreground = rig
        .supervisor
        .owner_session_snapshot(&rig.target)
        .unwrap()
        .turn;
    assert!(matches!(foreground, SessionTurn::Running { .. }));
    assert!(!rig.receipt.with_extension("steer").exists());
    let retry_notice = ClaimedNotice::new(
        recipient.clone(),
        next_attempt,
        vec![NOTICE_MESSAGE_ID.parse().unwrap()],
        clock
            .monotonic_now()
            .after(std::time::Duration::from_secs(1)),
        now + chrono::TimeDelta::seconds(1),
        Arc::new(NoticeAuthority::new()),
    )
    .unwrap();

    let written = adapter
        .ready(&recipient)
        .await
        .expect("the live foreground turn can take the retry")
        .offer(retry_notice)
        .await;
    assert_eq!(written, NoticeWriteOutcome::Written { taken_at: now });
    assert!(rig.receipt.with_extension("steer").is_file());

    let (settled, effects) = DirectHandoffMachine::transition(
        &retry,
        HandoffEvent::WriteFinished {
            attempt: next_attempt,
            outcome: written,
            now,
        },
    )
    .unwrap();
    assert_eq!(effects, [HandoffEffect::WakeReceiptPump]);
    let seen = DirectHandoff::Seen {
        completed_at: now,
        receipt: SeenReceiptState::Owed,
    };
    assert_eq!(settled.phase, seen);
    commit_notice_state(&mut inbox, &recipient, retry.phase, settled.phase, now).await;
    drop(inbox);
    let mut reopened = InboxStore::new(
        MachineLocalStore::open(&rig._root.path().join("notice-inbox.sqlite"))
            .await
            .unwrap(),
    );
    let stored = reopened
        .scan_direct_handoffs()
        .await
        .unwrap()
        .healthy
        .remove(0);
    assert_eq!(stored.handoff, seen);
    assert_eq!(
        rig.supervisor
            .owner_session_snapshot(&rig.target)
            .unwrap()
            .turn,
        foreground
    );
}

async fn commit_notice_state(
    inbox: &mut InboxStore,
    recipient: &MailSessionRef,
    expected: DirectHandoff,
    next: DirectHandoff,
    now: workspace_model::Timestamp,
) {
    assert_eq!(
        inbox
            .compare_and_set_direct_handoffs(
                recipient,
                &[DirectHandoffChange {
                    message_id: NOTICE_MESSAGE_ID.parse().unwrap(),
                    expected,
                    next
                }],
                now,
            )
            .await
            .unwrap(),
        HandoffCommit::Committed,
    );
}
