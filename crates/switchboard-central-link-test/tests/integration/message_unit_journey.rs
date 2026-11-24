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
    custody_submission::{Submission, SubmissionPayload},
    custody_task::CustodyTaskEvent,
    flush_clock::{FlushJitter, MachineFlushJitter, wake_for_mark},
};
use switchboard_message_system::{
    CloseReport, FrontRefusal, MessageRoots, MessageSystem, MessageSystemEvents,
    MessageSystemStatus as Status,
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
    let (enrollment_id, bearer) = grant_at_central(&central, &machine_id).await;
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
    assert!(handle.reserve().await.is_err());
    assert!(matches!(
        front.custody(),
        Err(FrontRefusal::EnrollmentEnded)
    ));
    assert_eq!(system.close().await, CloseReport::Closed);
    assert_eq!(*status.borrow(), Status::Stopped);
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

async fn observe_status(status: &mut watch::Receiver<Status>, expected: Status) {
    status.wait_for(|found| *found == expected).await.unwrap();
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
    assert_eq!(*status.borrow(), Status::Starting);

    assert_eq!(system.close().await, CloseReport::Closed);

    assert_eq!(*status.borrow(), Status::Stopped);
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

    assert_eq!(*status.borrow(), Status::Stopped);
    assert!(!root.join(MACHINE_LOCAL_DATABASE).try_exists().unwrap());
}
