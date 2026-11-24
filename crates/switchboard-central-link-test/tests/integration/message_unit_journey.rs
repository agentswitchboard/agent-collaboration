use std::{os::unix::fs::DirBuilderExt as _, sync::Arc};

use central_link_client::WelcomeClockAnchor;
use central_link_protocol::ReplyPayload;
use collaboration_central_tokio::test_support::start_machine_link_fixture;
use router_clock::{ManualClock, RuntimeClock};
use switchboard_central_link::{
    DialAddress,
    link_driver::{DriverSettings, LinkDriver},
    link_policy::LinkStatus,
};
use switchboard_machine_core_store::load_or_create_machine_id;
use switchboard_machine_enrollment::CredentialStore as _;
use switchboard_message_custody::{
    custody_submission::{Submission, SubmissionPayload},
    custody_task::CustodyTaskEvent,
    flush_clock::{FlushJitter, MachineFlushJitter, wake_for_mark},
};
use switchboard_message_unit::{
    FrontRefusal, MessageUnitEvents, PieceStop,
    test_support::{LinkFixture, start_with_link_fixture},
};
use tokio::sync::{mpsc, oneshot, watch};
use workspace_ids::{RequestId, SessionRef};
use workspace_model::{
    Caller, PostText,
    central_mail::{CentralMailAnswer, CentralMailResult, MessageDelivery, SendMessageRequest},
};

use super::message_unit_fixture::{
    EPOCH, FixtureDiscovery, FixtureRefresh, KnownRecipient, RecordCustody, enroll,
    grant_at_central, open_store,
};

#[tokio::test]
async fn enrolled_unit_sends_to_another_session_through_central_and_joins_in_order() {
    let clock = Arc::new(ManualClock::new(EPOCH.parse().unwrap()));
    let central = start_machine_link_fixture(clock.clone()).await.unwrap();
    let root = central.root_path().join("message-unit");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .unwrap();
    let machine_id = load_or_create_machine_id(&root, clock.utc_now().timestamp_millis())
        .await
        .unwrap();
    let store = open_store(&root, machine_id.clone(), clock.clone()).await;
    let (enrollment_id, bearer) = grant_at_central(&central, &machine_id).await;
    enroll(
        &store,
        machine_id.clone(),
        central.organization_id.as_str().parse().unwrap(),
        enrollment_id,
    )
    .await;
    let sender = SessionRef {
        machine_id: machine_id.clone(),
        session_id: "fixture-sender".parse().unwrap(),
    };
    let recipient = SessionRef {
        machine_id: machine_id.clone(),
        session_id: "fixture-recipient".parse().unwrap(),
    };
    let (events, mut observed_events) = mpsc::unbounded_channel();
    let (link_status, mut observed_status) = watch::channel(LinkStatus::NotEnrolled);
    let (discovery, mut observed_discovery) = mpsc::unbounded_channel();
    let (driver_finished, observed_finished) = oneshot::channel();
    let (release_driver, wait_for_release) = oneshot::channel();
    let address = central
        .url(central.organization_id.as_str())
        .replacen("http://", "ws://", 1);

    let mut runtime = start_with_link_fixture(
        &root,
        store.clone(),
        clock.clone(),
        Arc::new(KnownRecipient(recipient.session_id.clone())),
        MessageUnitEvents {
            custody: Arc::new(RecordCustody(events)),
            link_status,
        },
        LinkFixture {
            discovery: FixtureDiscovery(discovery),
            refresh: FixtureRefresh {
                enrollment_id,
                bearer,
            },
            run_driver: |mut driver: LinkDriver<_, _>| async move {
                let settings = {
                    let published = driver
                        .settings
                        .wait_for(|found| found.address.is_ok())
                        .await
                        .unwrap();
                    DriverSettings {
                        binding: published.binding.clone(),
                        address: DialAddress::test_loopback(&address),
                    }
                };
                let (_publication, updates) = watch::channel(settings);
                driver.settings = updates;
                driver.run().await;
                driver_finished.send(()).unwrap();
                wait_for_release.await.unwrap();
            },
        },
    )
    .await;
    assert_eq!(observed_discovery.recv().await, Some(()));
    let status = observed_status
        .wait_for(|status| {
            matches!(
                status,
                LinkStatus::Welcomed { .. } | LinkStatus::Fault { .. }
            )
        })
        .await
        .unwrap()
        .clone();
    assert_eq!(
        status,
        LinkStatus::Welcomed {
            machine_id: machine_id.clone()
        }
    );
    let anchor = WelcomeClockAnchor {
        central_time: EPOCH.parse().unwrap(),
        observed_at: clock.monotonic_now(),
    };
    let front = runtime.front();
    let handle = front.custody().unwrap();
    let request: RequestId = "019f0000-0000-7000-8000-000000000052".parse().unwrap();
    let submission = handle
        .reserve()
        .await
        .unwrap()
        .submit(
            Caller::Session(sender),
            SubmissionPayload::SendMessage {
                request: request.clone(),
                message: SendMessageRequest {
                    to: recipient,
                    body: PostText::try_from("fixture message for another session".to_owned())
                        .unwrap(),
                    delivery: MessageDelivery::Auto,
                    reply_to: None,
                },
            },
        )
        .resolve()
        .await;
    let Submission::InOutbox(receipt) = submission else {
        assert!(
            matches!(submission, Submission::InOutbox(_)),
            "expected an outbox receipt: {submission:?}"
        );
        return;
    };
    assert_eq!(receipt.id, request);
    let jitter = MachineFlushJitter::new(&machine_id);
    let first_wake = wake_for_mark(
        anchor,
        receipt.flush_at,
        jitter.jitter_millis(receipt.flush_at),
    );
    clock.not_due_check_now(first_wake).await;
    clock
        .advance(first_wake.saturating_duration_since(clock.monotonic_now()))
        .unwrap();
    let event = observed_events.recv().await.unwrap();
    assert!(matches!(event, CustodyTaskEvent::Flush(report)
        if report.issues.is_empty() && matches!(report.answered.as_slice(), [answer]
        if answer.request == request && matches!(answer.reply, ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { .. }))))));

    let stopping = tokio::spawn(async move { runtime.stop().await });
    observed_finished.await.unwrap();
    assert!(matches!(front.custody(), Err(FrontRefusal::ShuttingDown)));
    assert!(
        handle.reserve().await.is_ok(),
        "custody must remain open until the driver joins"
    );
    assert!(!stopping.is_finished());
    release_driver.send(()).unwrap();
    let stopped = stopping.await.unwrap();
    assert!(stopped.enrollment_store.unwrap().unwrap().outcome.is_ok());
    let stopped = stopped.unit.unwrap();
    assert_eq!(stopped.driver, PieceStop::Joined(Ok(())));
    assert_eq!(stopped.custody, PieceStop::Joined(Ok(())));
    assert_eq!(stopped.settings, PieceStop::Joined(Ok(())));
    assert!(
        matches!(stopped.lease, PieceStop::Joined(receipt) if receipt.worker.is_ok() && receipt.unsettled_retirements.is_empty())
    );
    assert!(handle.reserve().await.is_err());
    assert!(store.read_metadata().await.is_err());
    central.close().await.unwrap();
}
