use super::*;
use acp_client_runtime::handover_test_control::{HandoverPause, HandoverPausePoint};
use provider_session_machines::session_owner::AuthorizationTarget;
use session_event_model::InputId;
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug)]
enum NoticePath {
    Direct,
    ExactSteer,
}

impl NoticePath {
    fn script(self, writes_notice: bool) -> NoticeScript {
        match (self, writes_notice) {
            (Self::Direct, false) => NoticeScript::NoWrite,
            (Self::Direct, true) => NoticeScript::Direct,
            (Self::ExactSteer, false) => NoticeScript::DirectWithSteeringAvailable,
            (Self::ExactSteer, true) => NoticeScript::DirectThenSteer,
        }
    }

    async fn claim(
        self,
        rig: &SteeringSession,
        clock: &ManualClock,
        authority: Arc<NoticeAuthority>,
    ) -> (ManagedAcpNoticeAdapter, MailSessionRef, ClaimedNotice) {
        let (adapter, recipient, direct) = prepare_notice(rig, clock, Arc::clone(&authority));
        let notice = match self {
            Self::Direct => direct,
            Self::ExactSteer => {
                let foreground = adapter.ready(&recipient).await.expect("idle foreground");
                assert!(matches!(
                    foreground.offer(direct).await,
                    NoticeWriteOutcome::Written { .. }
                ));
                second_notice(&recipient, clock, authority)
            }
        };
        (adapter, recipient, notice)
    }

    fn assert_authorization(self, target: AuthorizationTarget) {
        match self {
            Self::Direct => assert_eq!(target, AuthorizationTarget::NewTurn),
            Self::ExactSteer => assert!(matches!(target, AuthorizationTarget::ExistingTurn { .. })),
        }
    }

    async fn finish(self, rig: &SteeringSession, wrote_notice: bool) {
        match (self, wrote_notice) {
            (Self::Direct, false) => {
                rig.supervisor.shutdown().await.expect("shutdown");
                assert!(!rig.receipt.with_extension("prompt").is_file());
                assert!(!rig.receipt.exists(), "unexpected notice reached the peer");
            }
            _ => {
                finish_notice_turn(rig).await;
                rig.supervisor.shutdown().await.expect("shutdown");
                assert_wire_script_completed(rig, matches!(self, Self::ExactSteer) && wrote_notice);
            }
        }
    }
}

fn pause(
    rig: &SteeringSession,
    notice: &ClaimedNotice,
    point: HandoverPausePoint,
) -> HandoverPause {
    let input = InputId::new(String::from(notice.attempt())).expect("notice input id");
    rig.supervisor
        .runtime_for(&rig.target.endpoint)
        .expect("fixture provider")
        .pause_work_handover_for_test(point, input)
}

async fn notice_cutoff_case(path: NoticePath) {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        Arc::clone(&clock),
        path.script(false),
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let (adapter, recipient, notice) = path
        .claim(&rig, &clock, Arc::new(NoticeAuthority::new()))
        .await;
    let writer = adapter.ready(&recipient).await.expect("ready notice");
    let cutoff = notice.not_after();
    let pause = pause(&rig, &notice, HandoverPausePoint::BeforeQuestion);
    let offer = tokio::spawn(writer.offer(notice));

    let authorization = pause
        .reached
        .await
        .expect("offer reached the writer question");
    path.assert_authorization(authorization.target);
    clock
        .advance(Duration::from_secs(1))
        .expect("step to claim cutoff");
    assert_eq!(clock.monotonic_now(), cutoff);
    pause.release.send(()).expect("release ordered writer");

    let outcome = offer.await.expect("offer settled");
    assert!(
        matches!(outcome, NoticeWriteOutcome::DefinitelyNotWritten(_)),
        "{path:?}: {outcome:?}"
    );
    path.finish(&rig, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn direct_notice_at_its_exact_cutoff_is_refused_inside_real_acp_handover() {
    notice_cutoff_case(NoticePath::Direct).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn steered_notice_at_its_exact_cutoff_is_refused_inside_real_acp_handover() {
    notice_cutoff_case(NoticePath::ExactSteer).await;
}

async fn withdrawn_notice_after_offer_case(path: NoticePath) {
    use switchboard_message_custody::handover::NoticePreTakeRefusal;

    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        Arc::clone(&clock),
        path.script(false),
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, notice) = path.claim(&rig, &clock, Arc::clone(&authority)).await;
    let writer = adapter.ready(&recipient).await.expect("ready notice");
    let pause = pause(&rig, &notice, HandoverPausePoint::AfterOffer);
    let offer = tokio::spawn(writer.offer(notice));

    let authorization = pause.reached.await.expect("ACP recorded the offer");
    path.assert_authorization(authorization.target);
    assert!(
        !authority.read_guard_held_for_test(),
        "the claim guard must be free for withdrawal to win"
    );
    authority.close();
    pause.release.send(()).expect("resume after withdrawal");

    // The notice adapter reports RejectedBeforeTake only if its take timestamp is absent.
    assert_eq!(
        offer.await.expect("offer settled"),
        NoticeWriteOutcome::DefinitelyNotWritten(NoticePreTakeRefusal::RejectedBeforeTake),
        "{path:?}: a notice withdrawn after offer cannot be taken"
    );
    path.finish(&rig, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn direct_notice_withdrawn_after_offer_is_not_taken() {
    withdrawn_notice_after_offer_case(NoticePath::Direct).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn steered_notice_withdrawn_after_offer_is_not_taken() {
    withdrawn_notice_after_offer_case(NoticePath::ExactSteer).await;
}

async fn notice_withdrawal_case(path: NoticePath) {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        Arc::clone(&clock),
        path.script(true),
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, notice) = path.claim(&rig, &clock, Arc::clone(&authority)).await;
    let writer = adapter.ready(&recipient).await.expect("ready notice");
    let checked_at = clock.utc_now();
    let pause = pause(&rig, &notice, HandoverPausePoint::BeforePublication);
    let offer = tokio::spawn(writer.offer(notice));

    let authorization = pause.reached.await.expect("last question accepted");
    path.assert_authorization(authorization.target);
    let claim_guard_held = authority.read_guard_held_for_test();
    let (send_attempt, attempted) = oneshot::channel();
    let (send_complete, mut completed) = oneshot::channel();
    let withdrawing_authority = Arc::clone(&authority);
    let withdrawal = tokio::task::spawn_blocking(move || {
        let _test_running = send_attempt.send(());
        withdrawing_authority.close();
        let _test_running = send_complete.send(());
    });
    attempted.await.expect("withdrawal requested");
    let closed_before_publication = completed.try_recv().is_ok();
    clock
        .advance(Duration::from_secs(1))
        .expect("advance after the question");
    pause.release.send(()).expect("release ordered publication");

    let outcome = offer.await.expect("offer settled");
    withdrawal.await.expect("withdrawal thread joined");
    assert!(
        claim_guard_held,
        "{path:?}: the real claim guard was released before publication"
    );
    assert!(
        !closed_before_publication,
        "{path:?}: the claim closed while publication was held"
    );
    completed
        .await
        .expect("withdrawal completed after publication");
    assert!(
        !authority.read_guard_held_for_test(),
        "take releases its claim guard"
    );
    assert_eq!(
        outcome,
        NoticeWriteOutcome::Written {
            taken_at: checked_at
        }
    );
    path.finish(&rig, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn direct_notice_withdrawal_after_the_positive_question_waits_for_publication() {
    notice_withdrawal_case(NoticePath::Direct).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn steered_notice_withdrawal_after_the_positive_question_waits_for_publication() {
    notice_withdrawal_case(NoticePath::ExactSteer).await;
}
