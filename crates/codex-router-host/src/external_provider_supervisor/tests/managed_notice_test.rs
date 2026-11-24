use super::*;
use crate::ManagedClaudeNoticeAdapter;
use router_clock::{ManualClock, RuntimeClock};
use switchboard_message_custody::{
    handover::NoticeWriteOutcome,
    handover_notice::{
        ClaimedNotice, MachineId, ManagedClaudeNoticePort, NoticeAttemptId, NoticeAuthority,
        SessionRef as MailSessionRef,
    },
};

async fn idle_claude_fixture(clock: Arc<ManualClock>) -> SteeringSession {
    let mut identity = binding();
    identity.runtime.provider = collaboration_protocol::ProviderKind::ClaudeCode;
    identity.endpoint.endpoint_id =
        collaboration_protocol::EndpointId::try_from("claude-local".to_owned()).unwrap();
    steering_session_for_binding(
        "cursor-settings-agent.ts",
        "reactive-session-1",
        |receipt| {
            vec![
                "reactive".into(),
                receipt.display().to_string(),
                "hold-first-prompt-open".into(),
            ]
        },
        clock,
        identity,
    )
    .await
}

fn prepare_notice(
    rig: &SteeringSession,
    clock: &ManualClock,
    authority: Arc<NoticeAuthority>,
) -> (ManagedClaudeNoticeAdapter, MailSessionRef, ClaimedNotice) {
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let recipient = MailSessionRef {
        machine_id: machine.clone(),
        session_id: String::from(rig.target.session_id.clone()).parse().unwrap(),
    };
    let notice = ClaimedNotice::new(
        recipient.clone(),
        NoticeAttemptId::try_from("019f0000-0000-7000-8000-000000000015".to_owned()).unwrap(),
        vec!["019f0000-0000-7000-8000-000000000017".parse().unwrap()],
        clock
            .monotonic_now()
            .after(std::time::Duration::from_secs(1)),
        authority,
    )
    .unwrap();
    (
        ManagedClaudeNoticeAdapter::new(
            rig.supervisor.clone(),
            machine,
            rig.target.endpoint.clone(),
        ),
        recipient,
        notice,
    )
}

#[tokio::test]
async fn managed_claude_notice_is_written_on_take_before_the_provider_turn_ends() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = idle_claude_fixture(clock.clone()).await;
    let (adapter, recipient, notice) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));

    let ready = adapter.ready(&recipient).await.unwrap();
    assert_eq!(
        ready.offer(notice).await,
        NoticeWriteOutcome::Written {
            taken_at: clock.utc_now()
        }
    );
    assert_eq!(
        rig.supervisor.owner_session_activity(&rig.target),
        crate::ProviderSessionActivity::Running
    );
    assert!(matches!(
        adapter.ready(&recipient).await,
        Err(switchboard_message_custody::handover_notice::NoticeDeferred::Busy)
    ));
    rig.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn retired_custody_cannot_take_a_managed_claude_notice() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = idle_claude_fixture(clock.clone()).await;
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, notice) = prepare_notice(&rig, &clock, authority.clone());
    let ready = adapter.ready(&recipient).await.unwrap();
    authority.close();

    assert!(matches!(
        ready.offer(notice).await,
        NoticeWriteOutcome::DefinitelyNotWritten(_)
    ));
    assert_eq!(
        rig.supervisor.owner_session_activity(&rig.target),
        crate::ProviderSessionActivity::Idle
    );
    rig.supervisor.shutdown().await.unwrap();
}
