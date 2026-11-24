use std::{
    future::{Future, pending, poll_fn},
    pin::pin,
    task::Poll,
};

use tokio::sync::oneshot;

use super::{Arc, CustodyRunFailure, CustodyStop, JoinSet, Mutex, retain_group, wait_for_stop};

#[tokio::test]
async fn cancelling_a_member_still_requires_joining_its_sibling() {
    let stop = CustodyStop::new();
    let mut stopped = stop.0.subscribe();
    let worker = tokio::spawn(pending());
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
