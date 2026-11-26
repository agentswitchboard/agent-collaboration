use super::*;
use crate::session_owner_calls::InputAcceptance;
use acp_client_runtime::handover_test_control::HandoverPausePoint;
use provider_session_machines::session_owner::RevokeReason;
use session_event_model::{InputId, PromptContent, ProviderSubmission};
use std::time::Duration;

fn prompt_request(rig: &SteeringSession) -> ProviderPromptContentsRequest {
    ProviderPromptContentsRequest {
        operation_id: OperationId::generate(),
        input_id: InputId::generate(),
        target: rig.target.clone(),
        requested_by: requester().into(),
        approver: requester().into(),
        input: acp_client_runtime::ProviderTurnInput::Prompt(ProviderSubmission::plain(vec![
            PromptContent::text(NOTICE_TEXT.into()).expect("fixture prompt"),
        ])),
        busy: PromptBusyPolicy::RefuseBusy,
    }
}

#[derive(Clone, Copy, Debug)]
enum OfferPath {
    Refused,
    Taken,
    FullWriter,
}

async fn check_approval_lease_at_offer(path: OfferPath) {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let script = if matches!(path, OfferPath::Taken) {
        NoticeScript::Direct
    } else {
        NoticeScript::NoWrite
    };
    let rig = managed_notice_fixture(
        Arc::clone(&clock),
        script,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("stored owner");
    let held_writer = matches!(path, OfferPath::FullWriter).then(|| {
        owner
            .hold_provider_writer_capacity()
            .expect("real provider writer hold")
    });
    let runtime = rig
        .supervisor
        .runtime_for(&rig.target.endpoint)
        .expect("provider runtime");
    let provider_session = String::from(rig.target.session_id.clone());
    let request = prompt_request(&rig);
    let pause = runtime
        .pause_work_handover_for_test(HandoverPausePoint::BeforeOffer, request.input_id.clone());
    let supervisor = Arc::clone(&rig.supervisor);
    let prompt =
        tokio::spawn(async move { supervisor.submit_owned_direct_delivery(request).await });

    let authorization = pause
        .reached
        .await
        .expect("Host reached the actual ACP offer");
    let turn = runtime
        .held_work_approval_for_test(&provider_session)
        .expect("approval installed before the offer");
    if matches!(path, OfferPath::Refused) {
        runtime.retire_work_route_for_test(&authorization);
    }
    pause.release.send(()).expect("release offer");
    if matches!(path, OfferPath::FullWriter) {
        let deadline = clock.monotonic_now().after(Duration::from_secs(10));
        clock.not_due_check_now(deadline).await;
        clock
            .advance(Duration::from_millis(9999))
            .expect("one millisecond before cutoff");
        clock.not_due_check_now(deadline).await;
        assert!(
            !prompt.is_finished(),
            "a full writer produces no early terminal result"
        );
        assert_eq!(
            runtime.held_work_approval_for_test(&provider_session),
            Some(turn)
        );
        clock
            .advance(Duration::from_millis(1))
            .expect("reach the exact enqueue cutoff");
    }

    let outcome = prompt
        .await
        .expect("prompt task joined")
        .expect("typed admission");
    match path {
        OfferPath::Taken => {
            assert_eq!(outcome, InputAcceptance::Started);
            assert_eq!(
                runtime.held_work_approval_for_test(&provider_session),
                Some(turn)
            );
            finish_notice_turn(&rig).await;
            assert_wire_script_completed(&rig, false);
        }
        OfferPath::Refused => {
            assert!(
                matches!(outcome, InputAcceptance::Unsent { .. }),
                "{outcome:?}"
            );
            assert_eq!(runtime.held_work_approval_for_test(&provider_session), None);
        }
        OfferPath::FullWriter => {
            assert_eq!(
                outcome,
                InputAcceptance::Unsent {
                    reason: Some(RevokeReason::EnqueueExpired)
                }
            );
            assert_eq!(runtime.held_work_approval_for_test(&provider_session), None);
        }
    }
    drop(held_writer);
    rig.supervisor.shutdown().await.expect("fixture shutdown");
    if !matches!(path, OfferPath::Taken) {
        assert!(!rig.receipt.with_extension("prompt").is_file());
        assert!(!rig.receipt.exists(), "{path:?} reached the peer");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_refused_offer_releases_the_preinstalled_approval_lease() {
    check_approval_lease_at_offer(OfferPath::Refused).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_taken_offer_keeps_the_preinstalled_approval_lease_with_the_run() {
    check_approval_lease_at_offer(OfferPath::Taken).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_full_writer_keeps_the_approval_lease_until_the_exact_enqueue_cutoff() {
    check_approval_lease_at_offer(OfferPath::FullWriter).await;
}
