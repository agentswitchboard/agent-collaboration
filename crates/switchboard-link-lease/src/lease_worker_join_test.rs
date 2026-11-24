use std::{sync::mpsc as blocking, task::Poll};

use router_clock::ManualClock;
use switchboard_central_link::lease_source::LeaseSource;
use switchboard_machine_enrollment::{EnrollmentEnd, RetirementFence};

use crate::{
    Admission, Completion, EnrollmentLeaseSource, LeaseJoin, LeaseWorkerExit, tests::Fixture,
};

use super::*;

fn exit_publication() -> (
    WorkerExitPublication,
    watch::Receiver<Option<LeaseWorkerExit>>,
) {
    let (observed, exit) = watch::channel(None);
    let admission = Arc::new(Mutex::new(Admission::Open));
    (
        WorkerExitPublication {
            admission,
            observed,
        },
        exit,
    )
}

#[tokio::test]
async fn worker_exit_seals_admission_before_held_children_are_joined() {
    let fixture = Fixture::new();
    let key = fixture.binding.key();
    let clock = fixture.clock.clone();
    let fence = RetirementFence::default();
    let worker_fence = fence.clone();
    let worker_key = key.clone();
    let (commands, _unused) = mpsc::unbounded_channel();
    let (closing, stopped) = watch::channel(None);
    let (publication, worker_exit) = exit_publication();
    let admission = publication.admission.clone();
    let children = ChildTasks::new();
    let (ready, waiting) = oneshot::channel();
    let (release, held) = oneshot::channel::<()>();
    children.spawn(async move {
        ready.send(()).unwrap();
        let _ = held.await;
        FinishedWork::Acquired
    });
    waiting.await.unwrap();
    let worker = tokio::spawn(async move {
        worker_fence.request_definitive(&worker_key, EnrollmentEnd::Revoked);
        panic!("fixture forces the lease worker to exit");
    });
    while !worker.is_finished() {
        tokio::task::yield_now().await;
    }
    let (sampled_reaper, sampling) = oneshot::channel();
    let reaper = tokio::spawn(async move {
        let mut reaping = std::pin::pin!(reap_worker(
            worker,
            &children,
            clock.as_ref(),
            stopped,
            &publication,
        ));
        let sampled = poll_fn(|cx| Poll::Ready(reaping.as_mut().poll(cx))).await;
        assert!(sampled.is_pending());
        sampled_reaper.send(()).unwrap();
        reaping.await
    });
    let source = EnrollmentLeaseSource {
        commands,
        retirements: fence.clone(),
        closing,
        admission,
        worker_exit,
        excluded: Arc::new(BTreeSet::new()),
        completion: tokio::sync::Mutex::new(Completion::Running(reaper)),
    };
    sampling.await.unwrap();

    let expected_exit = Poll::Ready(LeaseWorkerExit::Failed(LeaseWorkerFailure::Panicked));
    let mut exit = std::pin::pin!(source.worker_exit());
    assert_eq!(
        poll_fn(|cx| Poll::Ready(exit.as_mut().poll(cx))).await,
        expected_exit
    );
    assert_eq!(
        source.worker_exit().await,
        LeaseWorkerExit::Failed(LeaseWorkerFailure::Panicked)
    );
    assert!(!release.is_closed());
    let mut late_key = key.clone();
    late_key.enrollment_id = "019a0000-0000-7000-8000-000000000008".parse().unwrap();
    source.end(&late_key, central_link_client::EnrollmentEnd::Revoked);
    assert!(!fence.is_fenced(&late_key));
    let mut late_acquire = source.acquire(&late_key, None);
    assert_eq!(
        poll_fn(|cx| Poll::Ready(late_acquire.as_mut().poll(cx))).await,
        Poll::Ready(Err(LeaseFailure::Fault)),
    );

    source.request_stop_by(fixture.clock.monotonic_now());
    let expected = Ok(LeaseJoin {
        unsettled_retirements: vec![key.clone()],
        worker: Err(LeaseWorkerFailure::Panicked),
    });
    assert_eq!(source.join().await, expected);
    assert!(release.is_closed());
    source.end(&late_key, central_link_client::EnrollmentEnd::Revoked);
    assert_eq!(fence.pending_keys(), vec![key]);
    assert_eq!(source.join().await, expected);
    fixture
        .source
        .request_stop_by(fixture.clock.monotonic_now());
    fixture.source.join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reaping_waits_for_the_physically_held_child_after_abort() {
    let children = ChildTasks::new();
    let clock = ManualClock::new(Default::default());
    let (_stop, closing) = watch::channel(Some(ClockInstant::ZERO));
    let (running, entered) = oneshot::channel();
    let (release, held) = blocking::channel();
    children.spawn(async move {
        running.send(()).unwrap();
        let _released = held.recv();
        FinishedWork::Acquired
    });
    entered.await.unwrap();

    let mut reaping = std::pin::pin!(children.reap(&clock, closing));
    let before_release = poll_fn(|cx| Poll::Ready(reaping.as_mut().poll(cx))).await;
    assert!(before_release.is_pending());
    release.send(()).unwrap();
    reaping.await;
    assert!(children.is_empty());
}

#[tokio::test]
async fn reaping_after_worker_failure_lets_children_finish_before_cutoff() {
    let children = ChildTasks::new();
    let clock = ManualClock::new(Default::default());
    let (_stop, closing) = watch::channel(Some(ClockInstant::ZERO.after(Duration::from_secs(5))));
    let (running, entered) = oneshot::channel();
    let (release, held) = oneshot::channel();
    let (finished, observed) = oneshot::channel();
    children.spawn(async move {
        running.send(()).unwrap();
        held.await.unwrap();
        finished.send(()).unwrap();
        FinishedWork::Acquired
    });
    entered.await.unwrap();
    let worker = tokio::spawn(async { panic!("fixture forces the lease worker to exit") });
    let (publication, _exit) = exit_publication();
    let mut reaper = std::pin::pin!(reap_worker(
        worker,
        &children,
        &clock,
        closing,
        &publication
    ));
    tokio::select! {
        outcome = &mut reaper => panic!("reaper finished before the child: {outcome:?}"),
        () = clock.not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5))) => {}
    }
    release.send(()).unwrap();
    assert_eq!(reaper.await, Err(LeaseWorkerFailure::Panicked));
    observed.await.unwrap();
}

struct UnexpectedRefresh;

impl RefreshSource for UnexpectedRefresh {
    async fn refresh(
        &self,
        _: &switchboard_machine_enrollment::RefreshCredential,
    ) -> switchboard_machine_enrollment::RefreshAnswer {
        panic!("cutoff acquisition reached Sign-in");
    }
}

async fn retry_with_cutoff(remaining: Duration) -> LeaseJoin {
    let fixture = Fixture::new();
    let key = fixture.binding.key();
    let owner = RefreshOwner::new(
        fixture.store.clone(),
        UnexpectedRefresh,
        fixture.clock.clone(),
        fixture.binding.context.clone(),
    );
    let fence = owner.retirement_fence();
    fence.request_definitive(&key, EnrollmentEnd::Revoked);
    fixture.store.fail_end.send_replace(true);
    let (stop, closing) = watch::channel(None);

    let result = {
        let mut retirement = std::pin::pin!(retire(&owner, fixture.clock.as_ref(), &key, closing));
        let waiting = poll_fn(|cx| Poll::Ready(retirement.as_mut().poll(cx))).await;
        assert!(waiting.is_pending());
        let attempts_at_retry = *fixture.store.end_attempts.borrow();
        let reads_at_retry = *fixture.store.metadata_reads.borrow();
        assert!(attempts_at_retry > 0);
        fixture.store.fail_end.send_replace(false);
        let now = fixture.clock.monotonic_now();
        stop.send_replace(Some(now.after(Duration::from_secs(5))));
        stop.send_replace(Some(now.after(remaining)));
        let result = retirement.await;
        if remaining.is_zero() {
            assert!(matches!(result, FinishedWork::StillPending));
            assert_eq!(*fixture.store.end_attempts.borrow(), attempts_at_retry);
            assert_eq!(*fixture.store.metadata_reads.borrow(), reads_at_retry);
        } else {
            assert!(matches!(result, FinishedWork::Retired));
            assert_eq!(*fixture.store.end_attempts.borrow(), attempts_at_retry + 1);
            assert!(*fixture.store.metadata_reads.borrow() > reads_at_retry);
        }
        result
    };
    let source =
        EnrollmentLeaseSource::start(owner, fixture.clock.clone(), Arc::new(BTreeSet::new()));
    source.request_stop_by(fixture.clock.monotonic_now());
    let joined = source.join().await.unwrap();
    if matches!(result, FinishedWork::StillPending) {
        assert_eq!(joined.unsettled_retirements, vec![key]);
    } else {
        assert!(joined.unsettled_retirements.is_empty());
    }
    fixture
        .source
        .request_stop_by(fixture.clock.monotonic_now());
    fixture.source.join().await.unwrap();
    joined
}

#[tokio::test]
async fn a_retry_released_at_the_tightened_cutoff_enters_no_store_attempt() {
    assert_eq!(retry_with_cutoff(Duration::ZERO).await.worker, Ok(()));
}

#[tokio::test]
async fn a_retry_released_before_cutoff_can_settle_the_fence() {
    assert_eq!(
        retry_with_cutoff(Duration::from_secs(1)).await.worker,
        Ok(())
    );
}

#[tokio::test]
async fn an_acquire_first_polled_at_cutoff_does_not_enter_the_owner() {
    let fixture = Fixture::new();
    let key = fixture.binding.key();
    let owner = RefreshOwner::new(
        fixture.store.clone(),
        UnexpectedRefresh,
        fixture.clock.clone(),
        fixture.binding.context.clone(),
    );
    let (stop, closing) = watch::channel(None);
    let acquisition = acquire_unless_due(&owner, fixture.clock.as_ref(), closing, &key, None);
    stop.send_replace(Some(fixture.clock.monotonic_now()));
    assert_eq!(acquisition.await, Err(LeaseFailure::Fault));
    assert_eq!(*fixture.store.credential_opens.borrow(), 0);
    fixture
        .source
        .request_stop_by(fixture.clock.monotonic_now());
    fixture.source.join().await.unwrap();
}
