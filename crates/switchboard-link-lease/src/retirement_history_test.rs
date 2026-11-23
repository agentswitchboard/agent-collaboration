use hegel::generators as gs;

use super::*;

const RETRY: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug)]
enum History {
    DroppedReceiver,
    DelayedCallback,
    RetainedHandle,
    ReplacedBeforeRead,
    WaitingDefinitive,
}

#[derive(Clone, Copy, Debug)]
enum WhileFenced {
    Acquire,
    ReportDefinitive,
    Retry { healthy: bool },
}

#[hegel::test(test_cases = 100)]
fn generated_retirement_histories_preserve_generation_and_custody(case: hegel::TestCase) {
    let history = case.draw(gs::sampled_from(vec![
        History::DroppedReceiver,
        History::DelayedCallback,
        History::RetainedHandle,
        History::ReplacedBeforeRead,
        History::WaitingDefinitive,
    ]));
    let events = case.draw(
        gs::vecs(gs::sampled_from(vec![
            WhileFenced::Acquire,
            WhileFenced::ReportDefinitive,
            WhileFenced::Retry { healthy: false },
            WhileFenced::Retry { healthy: true },
        ]))
        .max_size(6),
    );
    let close_healthy = case.draw(gs::booleans());
    let before_start = case.draw(gs::booleans());
    let other_key = case.draw(gs::booleans());
    let readback_probes = case.draw(gs::sampled_from(vec![0, 1, 2]));
    let (link_cause, stored_cause) = case.draw(gs::sampled_from(vec![
        (EnrollmentEnd::Revoked, StoredEnding::Revoked),
        (EnrollmentEnd::Inactive, StoredEnding::Inactive),
        (
            EnrollmentEnd::ConnectionEnrollmentEnded,
            StoredEnding::EndedByCentral,
        ),
    ]));

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            match history {
                History::DroppedReceiver => {
                    dropped_receiver(events, close_healthy, link_cause).await;
                }
                History::DelayedCallback => delayed_callback(readback_probes).await,
                History::RetainedHandle => {
                    retained_handle(before_start, other_key, close_healthy, stored_cause).await;
                }
                History::ReplacedBeforeRead => replaced_before_read(close_healthy).await,
                History::WaitingDefinitive => waiting_definitive(link_cause, stored_cause).await,
            }
        });
}

async fn dropped_receiver(
    events: Vec<WhileFenced>,
    close_healthy: bool,
    link_cause: EnrollmentEnd,
) {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    fixture.store.fail_end.send_replace(true);
    let mut attempts = fixture.store.end_attempts.subscribe();
    let acquire = start_acquire(&fixture.source, &key).await;
    let response = fixture.refreshes.recv().await.unwrap();
    drop(acquire);
    response.send(refresh_refused()).unwrap();
    attempts.wait_for(|count| *count >= 2).await.unwrap();

    let mut durable = false;
    assert_blocked(&mut fixture, &key).await;
    for event in events {
        match event {
            WhileFenced::Acquire => assert_blocked(&mut fixture, &key).await,
            WhileFenced::ReportDefinitive => fixture.source.end(&key, link_cause),
            WhileFenced::Retry { healthy } if !durable => {
                let previous_attempts = *attempts.borrow();
                wait_for_retry(&fixture).await;
                fixture.store.fail_end.send_replace(!healthy);
                fixture.clock.advance(RETRY).unwrap();
                if healthy {
                    fixture
                        .store
                        .row
                        .subscribe()
                        .wait_for(|row| matches!(row, StoredEnrollment::Ended { .. }))
                        .await
                        .unwrap();
                    durable = true;
                } else {
                    attempts
                        .wait_for(|count| *count >= previous_attempts + 2)
                        .await
                        .unwrap();
                }
            }
            WhileFenced::Retry { .. } => fixture.clock.advance(RETRY).unwrap(),
        }
        assert!(fixture.retirements.is_fenced(&key));
    }

    fixture.store.fail_end.send_replace(!close_healthy);
    let report = fixture.source.close().await.unwrap();
    if durable || close_healthy {
        assert_eq!(report, UnfinishedLeaseWork::default());
        assert_ended(&fixture, &key, 2, StoredEnding::RefreshRefused);
    } else {
        assert_eq!(report.retirements, vec![key.clone()]);
        assert!(report.acquisitions.is_empty());
        assert_eq!(
            *fixture.store.row.borrow(),
            StoredEnrollment::Active(fixture.binding.clone())
        );
    }
    assert!(fixture.retirements.is_fenced(&key));
    assert!(fixture.refreshes.try_recv().is_err());
}

async fn delayed_callback(probes: usize) {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    first_refusal_with_failed_write(&mut fixture, &key).await;
    let successor = advance_binding(&fixture, false).await;
    fixture.store.fail_end.send_replace(false);
    fixture.clock.advance(RETRY).unwrap();
    let mut saved = fixture.store.row.subscribe();
    tokio::select! {
        biased;
        _ = saved.wait_for(|row| matches!(row, StoredEnrollment::Ended { .. })) => {
            panic!("a refusal ended a generation above its ceiling");
        }
        () = async {
            while fixture.retirements.is_fenced(&key) {
                tokio::task::yield_now().await;
            }
        } => {}
    }
    assert_eq!(*saved.borrow(), StoredEnrollment::Active(successor));

    let second = start_acquire(&fixture.source, &key).await;
    let reply = fixture.refreshes.recv().await.unwrap();
    fixture.source.end(&key, EnrollmentEnd::LeaseRefused);
    // Hold a wrong build's unverified cleanup while Sign-in gives real evidence.
    let unverified = if fixture.retirements.is_fenced(&key) {
        let (reading, release) = fixture.store.metadata_read.hold_next();
        reading.await.unwrap();
        Some(release)
    } else {
        None
    };
    reply.send(refresh_refused()).unwrap();
    assert_eq!(second.await, Err(LeaseFailure::EnrollmentEnded));
    if let Some(release) = unverified {
        assert!(release.send(Box::new(|| {})).is_ok());
    }
    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
    assert_ended(&fixture, &key, 3, StoredEnding::RefreshRefused);
    for _ in 0..probes {
        assert_blocked(&mut fixture, &key).await;
    }
}

async fn retained_handle(
    before_start: bool,
    other_key: bool,
    close_healthy: bool,
    why: StoredEnding,
) {
    let mut fixture = Fixture::with_fence(|fence, active| {
        if before_start {
            fence.request_definitive(&target_key(active, other_key), why);
        }
    });
    let key = target_key(&fixture.binding.key(), other_key);
    fixture.store.fail_end.send_replace(!close_healthy);
    if !before_start && !other_key {
        let in_flight = start_acquire(&fixture.source, &key).await;
        let response = fixture.refreshes.recv().await.unwrap();
        fixture.retirements.request_definitive(&key, why);
        response.send(fixture.rotated()).unwrap();
        assert_eq!(in_flight.await, Err(LeaseFailure::EnrollmentEnded));
    } else if !before_start {
        fixture.retirements.request_definitive(&key, why);
    }

    assert_blocked(&mut fixture, &key).await;
    let report = fixture.source.close().await.unwrap();
    assert!(report.acquisitions.is_empty());
    if other_key || close_healthy {
        assert!(report.retirements.is_empty());
    } else {
        assert_eq!(report.retirements, vec![key.clone()]);
    }
    if !other_key && close_healthy {
        assert_ended(&fixture, &key, 2, why);
    } else {
        assert_eq!(
            *fixture.store.row.borrow(),
            StoredEnrollment::Active(fixture.binding.clone())
        );
    }
    assert!(fixture.retirements.is_fenced(&key));
    assert!(fixture.refreshes.try_recv().is_err());
}

async fn replaced_before_read(credential_unavailable: bool) {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    let (reading, release) = fixture.store.before_credential_sample.hold_next();
    let acquire = start_acquire(&fixture.source, &key).await;
    reading.await.unwrap();
    let replacement = advance_binding(&fixture, true).await;
    fixture
        .store
        .fail_credential
        .send_replace(credential_unavailable);
    assert!(release.send(Box::new(|| {})).is_ok());
    assert_eq!(
        acquire.await,
        Err(LeaseFailure::Unavailable {
            retry_after: Duration::from_secs(1)
        })
    );
    assert_eq!(*fixture.store.credential_opens.borrow(), 0);
    assert_eq!(
        *fixture.store.row.borrow(),
        StoredEnrollment::Active(replacement)
    );
    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
    assert!(!fixture.retirements.is_fenced(&key));
    assert!(fixture.refreshes.try_recv().is_err());
}

async fn waiting_definitive(link_cause: EnrollmentEnd, stored_cause: StoredEnding) {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    first_refusal_with_failed_write(&mut fixture, &key).await;
    advance_binding(&fixture, false).await;
    let (reading, release) = fixture.store.metadata_read.hold_next();
    fixture.store.fail_end.send_replace(false);
    fixture.clock.advance(RETRY).unwrap();
    reading.await.unwrap();
    fixture.source.end(&key, link_cause);
    assert_blocked(&mut fixture, &key).await;
    assert!(release.send(Box::new(|| {})).is_ok());

    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
    assert_ended(&fixture, &key, 3, stored_cause);
    assert!(fixture.retirements.is_fenced(&key));
    assert!(fixture.refreshes.try_recv().is_err());
}

async fn first_refusal_with_failed_write(fixture: &mut Fixture, key: &EnrollmentKey) {
    fixture.store.fail_end.send_replace(true);
    let acquire = start_acquire(&fixture.source, key).await;
    fixture
        .refreshes
        .recv()
        .await
        .unwrap()
        .send(refresh_refused())
        .unwrap();
    assert_eq!(acquire.await, Err(LeaseFailure::EnrollmentEnded));
    // Ask the old version to schedule its first retry as well.
    fixture.source.end(key, EnrollmentEnd::LeaseRefused);
    wait_for_retry(fixture).await;
}

async fn wait_for_retry(fixture: &Fixture) {
    let due = fixture.clock.monotonic_now().after(RETRY);
    fixture.clock.not_due_check_now(due).await;
}

async fn assert_blocked(fixture: &mut Fixture, key: &EnrollmentKey) {
    tokio::select! {
        biased;
        result = fixture.source.acquire(key, None) => assert_eq!(result, Err(LeaseFailure::EnrollmentEnded)),
        request = fixture.refreshes.recv() => panic!("a fenced key reached Sign-in: {}", request.is_some()),
    }
}

async fn advance_binding(fixture: &Fixture, other_key: bool) -> EnrollmentBinding {
    let mut successor = fixture.binding.clone();
    successor.generation = successor.generation.successor().unwrap();
    successor.credential.enrollment_id =
        target_key(&fixture.binding.key(), other_key).enrollment_id;
    assert_eq!(
        fixture
            .store
            .replace(fixture.binding.generation, &successor)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    successor
}

fn target_key(key: &EnrollmentKey, other_key: bool) -> EnrollmentKey {
    let mut key = key.clone();
    if other_key {
        key.enrollment_id = "019a0000-0000-7000-8000-000000000008".parse().unwrap();
    }
    key
}

fn assert_ended(fixture: &Fixture, key: &EnrollmentKey, generation: u64, why: StoredEnding) {
    assert_eq!(
        *fixture.store.row.borrow(),
        StoredEnrollment::Ended {
            key: key.clone(),
            generation: BindingGeneration::from_persisted(generation).unwrap(),
            why,
        }
    );
}
