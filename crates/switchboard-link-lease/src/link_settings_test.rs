use switchboard_machine_enrollment::{StoredEnrollment, decode_discovery};
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::tests::Fixture;

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
        .wait_for(|value| value.binding.as_ref() == Some(&key))
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
    fixture.source.close().await.unwrap();
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
    assert_eq!(published.address, Err(AddressError::InvalidLocation));
    assert!(older.is_closed());
    assert!(discovery_calls.try_recv().is_err());
    stop.send_replace(true);
    work.await.unwrap();
    fixture.source.close().await.unwrap();
}
