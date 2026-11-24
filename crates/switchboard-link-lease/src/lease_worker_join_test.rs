use std::{sync::mpsc as blocking, task::Poll};

use router_clock::ManualClock;
use switchboard_machine_enrollment::{EnrollmentEnd, RetirementFence};

use crate::{Completion, EnrollmentLeaseSource, LeaseJoin, tests::Fixture};

use super::*;

#[tokio::test]
async fn a_failed_worker_joins_with_the_independent_pending_fence() {
    let fixture = Fixture::new();
    let key = fixture.binding.key();
    let clock = fixture.clock.clone();
    let fence = RetirementFence::default();
    let worker_fence = fence.clone();
    let worker_key = key.clone();
    let (commands, _unused) = mpsc::unbounded_channel();
    let (closing, stopped) = watch::channel(None);
    let worker = tokio::spawn(async move {
        worker_fence.request_definitive(&worker_key, EnrollmentEnd::Revoked);
        panic!("fixture forces the lease worker to exit");
    });
    let reaper = tokio::spawn(async move {
        reap_worker(worker, &ChildTasks::new(), clock.as_ref(), stopped).await
    });
    let source = EnrollmentLeaseSource {
        commands,
        retirements: fence,
        closing,
        excluded: Arc::new(BTreeSet::new()),
        completion: tokio::sync::Mutex::new(Completion::Running(reaper)),
    };

    let expected = LeaseJoin {
        unsettled_retirements: vec![key],
        worker: Err(LeaseWorkerFailure::Panicked),
    };
    assert_eq!(source.join().await, expected);
    assert_eq!(source.join().await, expected);
    fixture
        .source
        .request_stop_by(fixture.clock.monotonic_now());
    fixture.source.join().await;
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
    let mut reaper = std::pin::pin!(reap_worker(worker, &children, &clock, closing));
    tokio::select! {
        outcome = &mut reaper => panic!("reaper finished before the child: {outcome:?}"),
        () = clock.not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5))) => {}
    }
    release.send(()).unwrap();
    assert_eq!(reaper.await, Err(LeaseWorkerFailure::Panicked));
    observed.await.unwrap();
}
