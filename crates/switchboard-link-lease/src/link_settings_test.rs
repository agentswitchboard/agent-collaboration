use switchboard_machine_enrollment::{StoredEnrollment, decode_discovery};
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::tests::{Fixture, stop_lease};

struct Script(mpsc::UnboundedSender<oneshot::Sender<Result<DiscoveryAnswer, SignInFailure>>>);

impl DiscoverySource for Script {
    async fn discover(&self) -> Result<DiscoveryAnswer, SignInFailure> {
        let (reply, answer) = oneshot::channel();
        self.0.send(reply).unwrap();
        answer.await.unwrap()
    }
}

fn discovery(path: &str) -> DiscoveryAnswer {
    decode_discovery(format!(r#"{{"client_id":"019a0000-0000-7000-8000-000000000011","link_url":"https://central.example:8443/{{organization_id}}/{path}"}}"#).as_bytes()).unwrap()
}

#[tokio::test]
async fn an_external_replacement_supersedes_a_held_discovery_before_it_answers() {
    let fixture = Fixture::new();
    let (requests, mut discovery_calls) = mpsc::unbounded_channel();
    let (_changed, changes) = watch::channel(0);
    let (stop, shutdown) = watch::channel(false);
    let (source, mut settings) = LinkSettingsSource::with_discovery(
        fixture.store.clone(),
        Script(requests),
        fixture.binding.context.clone(),
        fixture.clock.clone(),
        changes,
        Arc::new(BTreeSet::new()),
    );
    let work = tokio::spawn(source.run(shutdown));
    let obsolete = discovery_calls.recv().await.unwrap();
    assert!(
        fixture
            .clock
            .has_not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(15)))
    );

    let mut replacement = fixture.binding.clone();
    replacement.generation = replacement.generation.successor().unwrap();
    replacement.credential.enrollment_id = "019a0000-0000-7000-8000-000000000008".parse().unwrap();
    let key = replacement.key();
    fixture
        .store
        .row
        .send_replace(StoredEnrollment::Active(replacement));
    fixture.clock.advance(Duration::from_secs(15)).unwrap();

    let current = discovery_calls.recv().await.unwrap();
    assert!(obsolete.is_closed());
    current.send(Ok(discovery("current"))).unwrap();
    let published = settings
        .wait_for(|value| value.binding.as_ref() == Some(&key) && value.address.is_ok())
        .await
        .unwrap()
        .clone();
    assert_eq!(
        published.address,
        DialAddress::secure(&format!(
            "wss://central.example:8443/{}/current",
            key.organization_id
        ))
    );
    stop.send_replace(true);
    work.await.unwrap();
    assert_eq!(
        stop_lease(&fixture.source, fixture.clock.as_ref())
            .await
            .worker,
        Ok(())
    );
}

#[tokio::test]
async fn a_local_change_publishes_an_incompatible_key_without_contacting_discovery() {
    let fixture = Fixture::new();
    let (requests, mut discovery_calls) = mpsc::unbounded_channel();
    let (changed, changes) = watch::channel(0);
    let (stop, shutdown) = watch::channel(false);
    let (source, mut settings) = LinkSettingsSource::with_discovery(
        fixture.store.clone(),
        Script(requests),
        fixture.binding.context.clone(),
        fixture.clock.clone(),
        changes,
        Arc::new(BTreeSet::new()),
    );
    let work = tokio::spawn(source.run(shutdown));
    let older = discovery_calls.recv().await.unwrap();
    let mut replacement = fixture.binding.clone();
    replacement.context.deployment =
        SignInDeployment::new("https://other.example/".parse().unwrap()).unwrap();
    replacement.context.organization_id = "019a0000-0000-7000-8000-000000000009".parse().unwrap();
    let key = replacement.key();
    fixture
        .store
        .row
        .send_replace(StoredEnrollment::Active(replacement));
    changed.send_replace(1);

    let published = settings
        .wait_for(|value| value.binding.as_ref() == Some(&key))
        .await
        .unwrap()
        .clone();
    assert_eq!(published.address, Err(AddressError::DiscoveryPending));
    assert!(older.is_closed());
    assert!(discovery_calls.try_recv().is_err());
    stop.send_replace(true);
    work.await.unwrap();
    assert_eq!(
        stop_lease(&fixture.source, fixture.clock.as_ref())
            .await
            .worker,
        Ok(())
    );
}

#[test]
fn discovery_failure_uses_cached_address_only_for_unavailable_and_pins_retry_delay() {
    use switchboard_machine_enrollment::InvalidReason;

    let fixture_deployment =
        SignInDeployment::new("https://sign-in.example/".parse().unwrap()).unwrap();
    let target = DiscoveryTarget {
        key: EnrollmentKey {
            machine_id: "019a0000-0000-7000-8000-000000000002".parse().unwrap(),
            organization_id: "019a0000-0000-7000-8000-000000000001".parse().unwrap(),
            enrollment_id: "019a0000-0000-7000-8000-000000000003".parse().unwrap(),
        },
        deployment: fixture_deployment,
    };
    let addr =
        DialAddress::secure("wss://central.example:8443/019a0000-0000-7000-8000-000000000001/link")
            .unwrap();
    let invalid = || Err(AddressError::InvalidLocation);
    let mut saved = None;
    assert_eq!(
        project_discovery(&target, Ok(discovery("link")), &mut saved),
        (Ok(addr.clone()), None)
    );
    for (retry_after, expected) in [
        (None, 30),
        (Some(Duration::ZERO), 1),
        (Some(Duration::from_secs(45)), 45),
    ] {
        assert_eq!(
            project_discovery(
                &target,
                Err(SignInFailure::Unavailable { retry_after }),
                &mut saved
            ),
            (Ok(addr.clone()), Some(Duration::from_secs(expected)))
        );
    }
    assert_eq!(
        project_discovery(&target, Ok(discovery("link?route=public")), &mut saved),
        (invalid(), Some(Duration::from_secs(30)))
    );
    assert!(saved.is_none());
    assert_eq!(
        project_discovery(
            &target,
            Err(SignInFailure::Unavailable { retry_after: None }),
            &mut saved
        ),
        (
            Err(AddressError::DiscoveryUnavailable),
            Some(Duration::from_secs(30))
        )
    );

    for reason in [
        InvalidReason::MalformedAnswer,
        InvalidReason::OversizedAnswer,
        InvalidReason::InvalidClient,
    ] {
        saved = Some(addr.clone());
        assert_eq!(
            project_discovery(&target, Err(SignInFailure::Invalid { reason }), &mut saved),
            (invalid(), Some(Duration::from_secs(30)))
        );
        assert!(saved.is_none());
    }
}

#[tokio::test]
async fn a_metadata_error_starts_unbound_then_preserves_last_publication_after_start() {
    let fixture = Fixture::new();
    fixture.store.fail_metadata.send_replace(true);
    let (requests, mut discovery_calls) = mpsc::unbounded_channel();
    let (changed, changes) = watch::channel(0);
    let (stop, shutdown) = watch::channel(false);
    let (source, mut settings) = LinkSettingsSource::with_discovery(
        fixture.store.clone(),
        Script(requests),
        fixture.binding.context.clone(),
        fixture.clock.clone(),
        changes,
        Arc::new(BTreeSet::new()),
    );
    let task = tokio::spawn(source.run(shutdown));
    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5)))
        .await;
    assert_eq!(*settings.borrow(), no_settings());
    assert!(discovery_calls.try_recv().is_err());

    fixture.store.fail_metadata.send_replace(false);
    fixture.clock.advance(Duration::from_secs(5)).unwrap();
    discovery_calls
        .recv()
        .await
        .unwrap()
        .send(Ok(discovery("link")))
        .unwrap();
    let published = settings
        .wait_for(|current| current.address.is_ok())
        .await
        .unwrap()
        .clone();
    fixture.store.fail_metadata.send_replace(true);
    changed.send_replace(1);
    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(10)))
        .await;
    assert_eq!(*settings.borrow(), published);
    assert!(discovery_calls.try_recv().is_err());

    stop.send_replace(true);
    task.await.unwrap();
    assert_eq!(
        stop_lease(&fixture.source, fixture.clock.as_ref())
            .await
            .worker,
        Ok(())
    );
}

struct SettingsFixture {
    changes: watch::Sender<u64>,
    published: watch::Receiver<DriverSettings<EnrollmentKey>>,
    requests: mpsc::UnboundedReceiver<oneshot::Sender<Result<DiscoveryAnswer, SignInFailure>>>,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl SettingsFixture {
    fn start(fixture: &Fixture) -> Self {
        let (discovery, requests) = mpsc::unbounded_channel();
        let (changes, revisions) = watch::channel(0);
        let (stop, shutdown) = watch::channel(false);
        let (source, published) = LinkSettingsSource::with_discovery(
            fixture.store.clone(),
            Script(discovery),
            fixture.binding.context.clone(),
            fixture.clock.clone(),
            revisions,
            Arc::new(BTreeSet::new()),
        );
        Self {
            changes,
            published,
            requests,
            stop,
            task: tokio::spawn(source.run(shutdown)),
        }
    }

    async fn close(self) {
        self.stop.send_replace(true);
        self.task.await.unwrap();
    }
}

fn install_replacement(fixture: &Fixture) -> EnrollmentKey {
    let mut replacement = fixture.binding.clone();
    replacement.generation = replacement.generation.successor().unwrap();
    replacement.credential.enrollment_id = "019a0000-0000-7000-8000-000000000008".parse().unwrap();
    let key = replacement.key();
    fixture
        .store
        .row
        .send_replace(StoredEnrollment::Active(replacement));
    key
}

#[tokio::test]
async fn observing_replacement_unpublishes_the_old_address_while_discovery_is_held() {
    let fixture = Fixture::new();
    let mut settings = SettingsFixture::start(&fixture);
    settings
        .requests
        .recv()
        .await
        .unwrap()
        .send(Ok(discovery("original")))
        .unwrap();
    let original = settings
        .published
        .wait_for(|value| value.address.is_ok())
        .await
        .unwrap()
        .clone();
    assert_eq!(original.binding, Some(fixture.binding.key()));

    let replacement = install_replacement(&fixture);
    settings.changes.send_replace(1);
    let held_discovery = settings.requests.recv().await.unwrap();

    assert_eq!(*settings.published.borrow(), unresolved(replacement));
    settings.close().await;
    assert!(held_discovery.is_closed());
    assert_eq!(
        stop_lease(&fixture.source, fixture.clock.as_ref())
            .await
            .worker,
        Ok(())
    );
}

#[tokio::test]
async fn a_local_change_during_the_final_read_blocks_the_obsolete_address() {
    let fixture = Fixture::new();
    let mut settings = SettingsFixture::start(&fixture);
    let old_discovery = settings.requests.recv().await.unwrap();
    let (reading_final_snapshot, return_final_snapshot) = fixture.store.metadata_read.hold_next();
    old_discovery.send(Ok(discovery("obsolete"))).unwrap();
    reading_final_snapshot.await.unwrap();

    let replacement = install_replacement(&fixture);
    settings.changes.send_replace(1);
    let (reading_next_snapshot, return_next_snapshot) = fixture.store.metadata_read.hold_next();
    assert!(return_final_snapshot.send(Box::new(|| {})).is_ok());
    reading_next_snapshot.await.unwrap();

    assert_eq!(
        settings.published.borrow().address,
        Err(AddressError::DiscoveryPending)
    );
    assert!(return_next_snapshot.send(Box::new(|| {})).is_ok());
    let held_discovery = settings.requests.recv().await.unwrap();
    assert_eq!(*settings.published.borrow(), unresolved(replacement));
    settings.close().await;
    assert!(held_discovery.is_closed());
    assert_eq!(
        stop_lease(&fixture.source, fixture.clock.as_ref())
            .await
            .worker,
        Ok(())
    );
}

#[tokio::test]
async fn a_replacement_seen_on_final_read_is_used_without_a_second_read() {
    let fixture = Fixture::new();
    let mut settings = SettingsFixture::start(&fixture);
    let old_discovery = settings.requests.recv().await.unwrap();
    let replacement = install_replacement(&fixture);
    let (reading_replacement, return_replacement) = fixture.store.metadata_read.hold_next();
    old_discovery.send(Ok(discovery("obsolete"))).unwrap();
    reading_replacement.await.unwrap();

    fixture.store.fail_metadata.send_replace(true);
    assert!(return_replacement.send(Box::new(|| {})).is_ok());
    let held_discovery = tokio::select! {
        request = settings.requests.recv() => request,
        () = fixture.clock.not_due_check_now(ClockInstant::ZERO.after(METADATA_RETRY)) => None,
    };

    assert_eq!(*settings.published.borrow(), unresolved(replacement));
    settings.close().await;
    assert!(held_discovery.unwrap().is_closed());
    assert_eq!(
        stop_lease(&fixture.source, fixture.clock.as_ref())
            .await
            .worker,
        Ok(())
    );
}

#[tokio::test]
async fn a_metadata_retry_keeps_the_current_discovery_deadline_but_a_replacement_starts_fresh() {
    let fixture = Fixture::new();
    let mut settings = SettingsFixture::start(&fixture);
    let retry_after = Duration::from_secs(60);
    let original_due = ClockInstant::ZERO.after(retry_after);
    settings
        .requests
        .recv()
        .await
        .unwrap()
        .send(Err(SignInFailure::Unavailable {
            retry_after: Some(retry_after),
        }))
        .unwrap();
    fixture.clock.not_due_check_now(original_due).await;

    fixture.store.fail_metadata.send_replace(true);
    fixture.clock.advance(METADATA_POLL).unwrap();
    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(METADATA_POLL + METADATA_RETRY))
        .await;
    fixture.store.fail_metadata.send_replace(false);
    fixture.clock.advance(METADATA_RETRY).unwrap();

    tokio::select! {
        biased;
        request = settings.requests.recv() => panic!("metadata recovery started discovery early: {}", request.is_some()),
        () = fixture.clock.not_due_check_now(original_due) => {}
    }
    assert!(settings.requests.try_recv().is_err());

    fixture
        .clock
        .advance(retry_after - METADATA_POLL - METADATA_RETRY)
        .unwrap();
    settings
        .requests
        .recv()
        .await
        .unwrap()
        .send(Err(SignInFailure::Unavailable {
            retry_after: Some(retry_after),
        }))
        .unwrap();
    fixture
        .clock
        .not_due_check_now(original_due.after(retry_after))
        .await;

    let replacement = install_replacement(&fixture);
    settings.changes.send_replace(1);
    let replacement_discovery = settings.requests.recv().await.unwrap();
    assert_eq!(*settings.published.borrow(), unresolved(replacement));
    assert_eq!(fixture.clock.monotonic_now(), original_due);

    settings.close().await;
    assert!(replacement_discovery.is_closed());
    assert_eq!(
        stop_lease(&fixture.source, fixture.clock.as_ref())
            .await
            .worker,
        Ok(())
    );
}
