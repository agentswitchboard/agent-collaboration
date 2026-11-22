use std::time::Duration;

use switchboard_machine_enrollment::{
    BindingGeneration, CentralEndpointIds, ConditionalWrite, CredentialStore, EnrollmentBinding,
    EnrollmentContext, EnrollmentEnd, EnrollmentFailure, EnrollmentReceipt, RefreshCredential,
    SecretValue, StoreDurability, StoredEnrollment,
};
use tokio::{sync::oneshot, task::JoinHandle};

use super::scripted_sign_in::{
    ENROLLMENT_ID, INITIAL_ACCESS, INITIAL_REFRESH, RECEIVER, RELAY, ScriptStore, SignInScript, at,
    enrollment_request, machine_id, organization_id, token_reply,
};

const OLD_ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000007";
const OTHER_ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000008";

fn generation(value: u64) -> BindingGeneration {
    BindingGeneration::from_persisted(value)
}

fn previous_binding(script: &SignInScript, version: u64, id: &str) -> EnrollmentBinding {
    EnrollmentBinding {
        generation: generation(version),
        context: EnrollmentContext {
            deployment: script.client.deployment().clone(),
            machine_id: machine_id(),
            organization_id: organization_id(),
        },
        credential: RefreshCredential {
            enrollment_id: id.parse().expect("preexisting enrollment"),
            central: CentralEndpointIds {
                receiver: RECEIVER.parse().expect("receiver"),
                relay: RELAY.parse().expect("relay"),
            },
            refresh_token: SecretValue::new("test-preexisting-refresh".to_owned())
                .expect("fictional secret"),
        },
    }
}

async fn held_last_poll() -> (SignInScript, oneshot::Sender<()>) {
    let (held, release) = token_reply(INITIAL_ACCESS, INITIAL_REFRESH).hold();
    (SignInScript::start(6, [held]).await, release)
}

async fn start_enrollment(
    script: &mut SignInScript,
    store: &ScriptStore,
) -> JoinHandle<Result<EnrollmentReceipt, EnrollmentFailure>> {
    let task = tokio::spawn({
        let store = store.clone();
        let client = script.client.clone();
        async move {
            client
                .enroll(enrollment_request(&machine_id()), &store, |_| Ok(()))
                .await
        }
    });
    script.next_request().await.assert_discovery();
    script.next_request().await.assert_start();
    script.wait_for_poll(5, 5).await;
    let poll = script.next_request().await;
    poll.assert_at(5);
    poll.assert_poll();
    task
}

#[tokio::test]
async fn final_in_flight_success_installs_after_the_old_enrollment_rotates() {
    let (mut script, release) = held_last_poll().await;
    let store = ScriptStore::default();
    let prior = previous_binding(&script, 1, OLD_ENROLLMENT);
    assert_eq!(
        store.replace(generation(0), &prior).await,
        Ok(ConditionalWrite::Applied)
    );
    let mut enrollment = start_enrollment(&mut script, &store).await;
    script
        .clock
        .advance(Duration::from_secs(1))
        .expect("reach code expiry");
    let remained_current = tokio::select! {
        () = script.clock.not_due_check_now(at(8)) => true,
        _ = &mut enrollment => false,
    };
    assert!(
        remained_current,
        "an already started poll has a short response window"
    );

    let rotated_old = previous_binding(&script, 2, OLD_ENROLLMENT);
    assert_eq!(
        store.replace(generation(1), &rotated_old).await,
        Ok(ConditionalWrite::Applied)
    );
    release.send(()).expect("deliver approval");
    let receipt = enrollment
        .await
        .expect("enrollment task")
        .expect("new enrollment receipt");

    assert_eq!(receipt.generation, generation(3));
    assert_eq!(receipt.enrollment_id.to_string(), ENROLLMENT_ID);
    let committed = store.current().expect("current binding");
    assert_eq!(committed.credential.enrollment_id, receipt.enrollment_id);
    assert_eq!(committed.context, prior.context);
    script.assert_exhausted();
}

#[tokio::test]
async fn a_late_approval_cannot_overwrite_a_different_enrollment() {
    let (mut script, release) = held_last_poll().await;
    let store = ScriptStore::default();
    let prior = previous_binding(&script, 1, OLD_ENROLLMENT);
    assert_eq!(
        store.replace(generation(0), &prior).await,
        Ok(ConditionalWrite::Applied)
    );
    let enrollment = start_enrollment(&mut script, &store).await;
    script
        .clock
        .advance(Duration::from_secs(1))
        .expect("reach code expiry");
    let winner = previous_binding(&script, 2, OTHER_ENROLLMENT);
    assert_eq!(
        store.replace(generation(1), &winner).await,
        Ok(ConditionalWrite::Applied)
    );

    release.send(()).expect("deliver approval");
    assert_eq!(
        enrollment.await.expect("enrollment task"),
        Err(EnrollmentFailure::ReplacementConflict)
    );
    assert_eq!(store.snapshot(), StoredEnrollment::Active(winner));
    script.assert_exhausted();
}

#[tokio::test]
async fn an_unanswered_last_poll_expires_at_its_short_bound() {
    let (mut script, _keep_reply_pending) = held_last_poll().await;
    let store = ScriptStore::default();
    let enrollment = start_enrollment(&mut script, &store).await;
    script
        .clock
        .advance(Duration::from_secs(3))
        .expect("pass final response deadline");

    let result = tokio::select! {
        result = enrollment => Some(result.expect("enrollment task")),
        () = script.clock.not_due_check_now(at(9)) => None,
    };
    assert_eq!(
        result,
        Some(Err(EnrollmentFailure::Ended(EnrollmentEnd::Expired)))
    );
    assert_eq!(store.snapshot(), StoredEnrollment::Empty);
    script.assert_exhausted();
}

#[tokio::test]
async fn the_command_reports_memory_only_before_starting_device_login() {
    let mut script = SignInScript::start(20, []).await;
    let store = ScriptStore::new(StoreDurability::MemoryOnly);
    let machine = machine_id();
    let enrollment = script
        .client
        .enroll(enrollment_request(&machine), &store, |_| Ok(()));
    let status = tokio::select! {
        biased;
        _ = script.observations.recv() => None,
        result = enrollment => Some(result),
    };
    assert_eq!(status, Some(Err(EnrollmentFailure::NoDurableStore)));
    assert_eq!(store.snapshot(), StoredEnrollment::Empty);
}
