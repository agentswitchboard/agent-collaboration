use std::{
    future::{Future, pending, poll_fn},
    pin::pin,
    sync::mpsc,
    task::Poll,
};

use tokio::{sync::oneshot, task::JoinHandle};

use super::{
    Arc, CustodyRunFailure, CustodyStop, JoinSet, Mutex, StopOnExit, retain_group, wait_for_stop,
};

async fn finished_member() -> JoinHandle<()> {
    let (exiting, exited) = oneshot::channel();
    let member = tokio::spawn(async move { exiting.send(()).unwrap() });
    exited.await.unwrap();
    // These tests run on one thread. The task returns in the poll that sent
    // the signal, before this test resumes.
    assert!(member.is_finished());
    member
}

#[tokio::test]
async fn cancelling_a_member_still_requires_joining_its_sibling() {
    let stop = CustodyStop::new();
    let mut stopped = stop.0.subscribe();
    let exit = StopOnExit::new(&stop, CustodyRunFailure::WorkerPanicked);
    let worker = tokio::spawn(async move {
        let _exit = exit;
        pending().await
    });
    let (release, held) = oneshot::channel();
    let offer_taker = tokio::spawn(async move {
        assert_eq!(held.await, Ok(()));
    });
    worker.abort();
    let group = retain_group(
        stop,
        worker,
        offer_taker,
        Arc::new(Mutex::new(JoinSet::new())),
    );

    wait_for_stop(&mut stopped).await;
    let mut join = pin!(group.join());
    assert!(
        poll_fn(|context| Poll::Ready(join.as_mut().poll(context)))
            .await
            .is_pending()
    );
    assert_eq!(release.send(()), Ok(()));
    assert_eq!(join.await, Err(CustodyRunFailure::Cancelled));
    assert_eq!(group.join().await, Err(CustodyRunFailure::Cancelled));
}

struct SignalMemberExit(Option<oneshot::Sender<()>>);

impl Drop for SignalMemberExit {
    fn drop(&mut self) {
        if let Some(exited) = self.0.take() {
            let _ = exited.send(());
        }
    }
}

async fn first_failure(cancel_offer_first: bool) -> Result<(), CustodyRunFailure> {
    let stop = CustodyStop::new();
    let worker_exit = StopOnExit::new(&stop, CustodyRunFailure::WorkerPanicked);
    let offer_exit = StopOnExit::new(&stop, CustodyRunFailure::OfferTakerPanicked);
    let (worker_done, worker_exited) = oneshot::channel();
    let (offer_done, offer_exited) = oneshot::channel();
    // The signals drop after the production guards, even if a spawned
    // future is cancelled before its first poll.
    let worker_lifetime = (worker_exit, SignalMemberExit(Some(worker_done)));
    let offer_lifetime = (offer_exit, SignalMemberExit(Some(offer_done)));
    let (panic_worker, held_worker) = oneshot::channel::<()>();
    let worker = tokio::spawn(async move {
        let _lifetime = worker_lifetime;
        assert_eq!(held_worker.await, Ok(()));
        panic!("scripted custody worker panic");
    });
    let offer_taker = tokio::spawn(async move {
        let _lifetime = offer_lifetime;
        pending().await
    });
    let cancel_offer = offer_taker.abort_handle();
    let group = retain_group(
        stop,
        worker,
        offer_taker,
        Arc::new(Mutex::new(JoinSet::new())),
    );

    if cancel_offer_first {
        cancel_offer.abort();
        assert_eq!(offer_exited.await, Ok(()));
        assert_eq!(panic_worker.send(()), Ok(()));
        assert_eq!(worker_exited.await, Ok(()));
    } else {
        assert_eq!(panic_worker.send(()), Ok(()));
        assert_eq!(worker_exited.await, Ok(()));
        cancel_offer.abort();
        assert_eq!(offer_exited.await, Ok(()));
    }
    group.join().await
}

#[tokio::test]
async fn offer_cancellation_before_worker_panic_is_the_first_failure() {
    assert_eq!(first_failure(true).await, Err(CustodyRunFailure::Cancelled));
}

#[tokio::test]
async fn worker_panic_before_offer_cancellation_is_the_first_failure() {
    assert_eq!(
        first_failure(false).await,
        Err(CustodyRunFailure::WorkerPanicked)
    );
}

#[tokio::test]
async fn collector_runtime_shutdown_leaves_the_running_member_available_to_join() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .unwrap();
    let stop = CustodyStop::new();
    let exit = StopOnExit::new(&stop, CustodyRunFailure::WorkerPanicked);
    let (release, held) = mpsc::channel();
    let (entered, running) = oneshot::channel();
    let worker = runtime.spawn(async move {
        let _exit = exit;
        entered.send(()).unwrap();
        let _ = held.recv();
        pending().await
    });
    running.await.unwrap();
    let offer_taker = finished_member().await;
    let group = Arc::new(retain_group(
        stop,
        worker,
        offer_taker,
        Arc::new(Mutex::new(JoinSet::new())),
    ));
    let inner_group = group.clone();
    let (polled, join_pending) = oneshot::channel();
    let collector = runtime.spawn(async move {
        let mut polled = Some(polled);
        let mut join = pin!(inner_group.join());
        poll_fn(|context| {
            let result = join.as_mut().poll(context);
            if let Some(first_poll) = polled.take() {
                first_poll.send(result.is_pending()).unwrap();
            }
            result
        })
        .await
    });
    assert!(join_pending.await.unwrap());

    runtime.shutdown_background();
    let collector_result = collector.await;
    let mut outer_join = pin!(group.join());
    let while_member_running =
        poll_fn(|context| Poll::Ready(outer_join.as_mut().poll(context))).await;
    release.send(()).unwrap();

    assert!(collector_result.unwrap_err().is_cancelled());
    assert!(while_member_running.is_pending());
    assert_eq!(outer_join.await, Err(CustodyRunFailure::Cancelled));
}

struct HeldRemoteDrop {
    entered: Option<oneshot::Sender<()>>,
    released: mpsc::Receiver<()>,
}

impl Drop for HeldRemoteDrop {
    fn drop(&mut self) {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        let _ = self.released.recv();
    }
}

#[tokio::test]
async fn cancelling_a_join_wait_keeps_the_remote_destructor_owned() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    let (blocked, dropping) = oneshot::channel();
    let (release, released) = mpsc::channel();
    let held = HeldRemoteDrop {
        entered: Some(blocked),
        released,
    };
    let (started, ready) = oneshot::channel();
    let mut remote = JoinSet::new();
    remote.spawn_on(
        async move {
            let _held = held;
            started.send(()).unwrap();
            pending().await
        },
        runtime.handle(),
    );
    ready.await.unwrap();
    let group = retain_group(
        CustodyStop::new(),
        finished_member().await,
        finished_member().await,
        Arc::new(Mutex::new(remote)),
    );

    {
        let mut first_wait = pin!(group.join());
        assert!(
            poll_fn(|context| Poll::Ready(first_wait.as_mut().poll(context)))
                .await
                .is_pending()
        );
        dropping.await.unwrap();
    }
    let mut second_wait = pin!(group.join());
    let still_held = poll_fn(|context| Poll::Ready(second_wait.as_mut().poll(context))).await;
    release.send(()).unwrap();

    assert!(still_held.is_pending());
    assert_eq!(second_wait.await, Ok(()));
    runtime.shutdown_background();
}
