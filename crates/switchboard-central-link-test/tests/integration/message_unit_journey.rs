use std::{
    future::ready, os::unix::fs::DirBuilderExt as _, path::Path, pin::pin, sync::Arc,
    time::Duration,
};

use central_link_client::WelcomeClockAnchor;
use central_link_protocol::ReplyPayload;
use collaboration_central_tokio::test_support::start_machine_link_fixture;
use router_clock::{ManualClock, RuntimeClock};
use switchboard_central_link::{
    DialAddress,
    link_driver::{DriverSettings, LinkDriver},
};
use switchboard_machine_core_store::{MACHINE_LOCAL_DATABASE, load_or_create_machine_id};
use switchboard_machine_enrollment::{
    ConditionalWrite, CredentialStore as _, EnrollmentEnd, EnrollmentMetadata,
};
use switchboard_message_custody::{
    custody_task::CustodyTaskEvent,
    flush_clock::{FlushJitter, MachineFlushJitter, wake_for_mark},
};
use switchboard_message_system::{
    CloseReport, FrontRefusal, MessageRoots, MessageSystem, MessageSystemEvents,
    MessageSystemObservation, MessageSystemReason, MessageSystemStatus as Status,
    test_support::{
        AdmittedLocalSession, DirectMessageWrite, LinkFixture, MessageDeliveryObservation,
        MessageSendOutcome, MessageStatus, MessageUnitPort, MessageWriteStatus,
        start_with_link_fixture,
    },
};
use tokio::sync::{mpsc, oneshot, watch};
use workspace_ids::{RequestId, SessionRef};
use workspace_model::{
    PostText,
    central_mail::{CentralMailAnswer, CentralMailResult},
};

use super::message_unit_fixture::{
    EPOCH, FixtureDiscovery, FixtureRefresh, KnownRecipient, RecordCustody, enroll,
    grant_at_central, open_store,
};

#[tokio::test]
async fn live_enrollment_sends_through_central_restarts_and_drains_in_order() {
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
    let writer = open_store(&root, machine_id.clone(), clock.clone()).await;
    let (enrollment_id, bearer) = grant_at_central(
        &central,
        &machine_id,
        "019f0000-0000-7000-8000-00000000cd11".parse().unwrap(),
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
    let (discovery, mut observed_discovery) = mpsc::unbounded_channel();
    let (driver_controls, mut observed_driver) = mpsc::unbounded_channel();
    let address = central
        .url(central.organization_id.as_str())
        .replacen("http://", "ws://", 1);

    let system = start_with_link_fixture(
        MessageRoots::new(root.clone(), root.join("secrets")),
        machine_id.clone(),
        store.clone(),
        clock.clone(),
        Arc::new(KnownRecipient(recipient.session_id.clone())),
        MessageSystemEvents::new(Arc::new(RecordCustody(events))),
        LinkFixture {
            discovery: FixtureDiscovery(discovery),
            refresh: FixtureRefresh {
                enrollment_id,
                bearer,
            },
            run_driver: move |driver: LinkDriver<_, _>| {
                run_driver(driver, address.clone(), driver_controls.clone())
            },
        },
    );
    let front = system.front();
    let mut status = system.status();
    observe_status(&mut status, Status::NotEnrolled).await;
    assert!(matches!(front.custody(), Err(FrontRefusal::NotEnrolled)));
    assert!(front.available_machine().is_err());
    let poll = Duration::from_secs(15);
    clock
        .not_due_check_now(clock.monotonic_now().after(poll))
        .await;
    enroll(
        &writer,
        machine_id.clone(),
        central.organization_id.as_str().parse().unwrap(),
        enrollment_id,
    )
    .await;
    assert!(
        observed_driver.try_recv().is_err(),
        "the outside writer does not wake the Host's store"
    );
    clock.advance(poll).unwrap();
    assert_eq!(observed_discovery.recv().await, Some(()));
    let first_driver = observed_driver.recv().await.unwrap();
    observe_status(&mut status, Status::Connected).await;
    let anchor = WelcomeClockAnchor {
        central_time: clock.utc_now(),
        observed_at: clock.monotonic_now(),
    };
    let handle = front
        .custody()
        .expect("connected message system publishes custody");
    let request: RequestId = "019f0000-0000-7000-8000-000000000052".parse().unwrap();
    let sender = AdmittedLocalSession::admitted_by_local_session_directory(sender);
    assert_eq!(front.available_machine(), Ok(machine_id.clone()));
    let submission = front
        .submit(
            &sender,
            DirectMessageWrite {
                request_id: request.clone(),
                to: recipient,
                body: PostText::try_from("fixture message for another session".to_owned()).unwrap(),
            },
        )
        .await
        .unwrap();
    let MessageSendOutcome::InOutbox {
        request_id,
        flush_at,
    } = submission
    else {
        assert!(
            matches!(submission, MessageSendOutcome::InOutbox { .. }),
            "expected an outbox receipt: {submission:?}"
        );
        return;
    };
    assert_eq!(request_id, request);
    assert_eq!(
        front
            .request_status(&sender, request.clone())
            .await
            .unwrap(),
        MessageWriteStatus::InOutbox { flush_at }
    );
    let mark = runtime_primitives::SyncTime::from_unix_millis(flush_at.timestamp_millis());
    let jitter = MachineFlushJitter::new(&machine_id);
    let first_wake = wake_for_mark(anchor, mark, jitter.jitter_millis(mark));
    clock.not_due_check_now(first_wake).await;
    clock
        .advance(first_wake.saturating_duration_since(clock.monotonic_now()))
        .unwrap();
    let event = observed_events.recv().await.unwrap();
    assert!(matches!(event, CustodyTaskEvent::Flush(report)
        if report.issues.is_empty() && matches!(report.answered.as_slice(), [answer]
        if answer.request == request && matches!(answer.reply, ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { .. }))))));
    let status_after_flush = front.request_status(&sender, request).await.unwrap();
    assert!(
        matches!(
            status_after_flush,
            MessageWriteStatus::Answered {
                delivery: MessageDeliveryObservation::Checked(
                    MessageStatus::Sent { .. } | MessageStatus::Delivered { .. }
                ),
                ..
            }
        ),
        "expected Central acceptance and a separate delivery observation, got {status_after_flush:?}"
    );

    first_driver.exit.send(()).unwrap();
    observe_status(&mut status, Status::Restarting).await;
    assert!(matches!(front.custody(), Err(FrontRefusal::Restarting)));
    assert!(handle.reserve().await.is_err());
    let cooldown = Duration::from_secs(5);
    clock
        .not_due_check_now(clock.monotonic_now().after(cooldown))
        .await;
    clock.advance(cooldown).unwrap();
    let second_driver = observed_driver.recv().await.unwrap();
    observe_status(&mut status, Status::Connected).await;
    let handle = front.custody().unwrap();

    let EnrollmentMetadata::Active { generation, .. } = writer.read_metadata().await.unwrap()
    else {
        panic!("active enrollment before ending");
    };
    assert_eq!(
        writer
            .remove(generation, EnrollmentEnd::Revoked)
            .await
            .unwrap(),
        ConditionalWrite::Applied
    );
    clock.advance(poll).unwrap();
    observe_status(&mut status, Status::Stopping).await;
    assert_eq!(
        status.borrow().reason,
        MessageSystemReason::EnrollmentRevoked
    );
    second_driver.finished.await.unwrap();
    assert!(matches!(
        front.custody(),
        Err(FrontRefusal::EnrollmentEnded)
    ));
    assert!(
        handle.reserve().await.is_ok(),
        "custody must remain open until the driver joins"
    );
    second_driver.release.send(()).unwrap();
    observe_status(&mut status, Status::EnrollmentEnded).await;
    assert_eq!(
        status.borrow().reason,
        MessageSystemReason::EnrollmentRevoked
    );
    assert!(handle.reserve().await.is_err());
    assert!(matches!(
        front.custody(),
        Err(FrontRefusal::EnrollmentEnded)
    ));
    assert_eq!(system.close().await, CloseReport::Closed);
    assert_eq!(status.borrow().status, Status::Stopped);
    assert!(matches!(front.custody(), Err(FrontRefusal::ShuttingDown)));
    assert!(store.read_metadata().await.is_err());
    writer.close().await.unwrap();
    central.close().await.unwrap();
}

struct DriverControls {
    exit: oneshot::Sender<()>,
    finished: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

async fn run_driver(
    mut driver: LinkDriver<
        switchboard_machine_enrollment::EnrollmentKey,
        switchboard_link_lease::EnrollmentLeaseSource,
    >,
    address: String,
    controls: mpsc::UnboundedSender<DriverControls>,
) {
    let (exit, exit_requested) = oneshot::channel();
    let (finished, finish_observed) = oneshot::channel();
    let (release, released) = oneshot::channel();
    assert!(
        controls
            .send(DriverControls {
                exit,
                finished: finish_observed,
                release,
            })
            .is_ok(),
        "driver controls receiver disappeared"
    );
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
    tokio::select! {
        Ok(()) = exit_requested => return,
        () = Box::pin(driver.run()) => {}
    }
    finished.send(()).unwrap();
    released.await.unwrap();
}

async fn observe_status(status: &mut watch::Receiver<MessageSystemObservation>, expected: Status) {
    status
        .wait_for(|found| found.status == expected)
        .await
        .unwrap();
}

fn start_at(root: &Path, clock: Arc<ManualClock>) -> MessageSystem {
    let (events, _unused) = mpsc::unbounded_channel();
    MessageSystem::start(
        MessageRoots::new(root.to_owned(), root.join("secrets")),
        clock,
        Arc::new(KnownRecipient("fixture-recipient".parse().unwrap())),
        MessageSystemEvents::new(Arc::new(RecordCustody(events))),
    )
}

#[tokio::test]
async fn close_before_first_read_does_not_open_the_store() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("router-root");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .unwrap();
    let clock = Arc::new(ManualClock::new(EPOCH.parse().unwrap()));
    let system = start_at(&root, clock);
    let status = system.status();
    assert_eq!(status.borrow().status, Status::Starting);

    assert_eq!(system.close().await, CloseReport::Closed);

    assert_eq!(status.borrow().status, Status::Stopped);
    assert!(!root.join(MACHINE_LOCAL_DATABASE).try_exists().unwrap());
}

#[tokio::test]
async fn close_with_retry_due_after_open_failure_does_not_open_the_store() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("router-root");
    std::fs::write(&root, b"not a directory").unwrap();
    let clock = Arc::new(ManualClock::new(EPOCH.parse().unwrap()));
    let system = start_at(&root, clock.clone());
    let mut status = system.status();
    observe_status(&mut status, Status::EnrollmentUnreadable).await;
    let retry = Duration::from_secs(5);
    clock
        .not_due_check_now(clock.monotonic_now().after(retry))
        .await;
    std::fs::remove_file(&root).unwrap();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .unwrap();

    let mut close = pin!(system.close());
    tokio::select! {
        biased;
        report = &mut close => panic!("close unexpectedly completed on its first poll: {report:?}"),
        () = ready(()) => {}
    }
    clock.advance(retry).unwrap();
    assert_eq!(close.await, CloseReport::Closed);

    assert_eq!(status.borrow().status, Status::Stopped);
    assert!(!root.join(MACHINE_LOCAL_DATABASE).try_exists().unwrap());
}
