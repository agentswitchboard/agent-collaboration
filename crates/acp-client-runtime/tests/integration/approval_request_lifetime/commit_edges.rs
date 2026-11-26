//! Check policy re-entry and peer cancel at the real callback reply boundary.

use super::*;
use agent_client_protocol::schema::v1::RequestId;
use futures_util::task::{ArcWake, waker};
use std::{
    sync::Mutex,
    task::{Context, Poll, Waker},
};
use tokio::sync::oneshot;

struct ScopeWaker {
    probe: Arc<dyn Fn() -> bool + Send + Sync>,
    observed: Mutex<Option<oneshot::Sender<bool>>>,
}

impl ArcWake for ScopeWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let locks_free = (arc_self.probe)();
        let observer = arc_self
            .observed
            .lock()
            .ok()
            .and_then(|mut observer| observer.take());
        if let Some(observer) = observer {
            let _test_gone = observer.send(locks_free);
        }
    }
}

fn scope_waker(probe: Arc<dyn Fn() -> bool + Send + Sync>) -> (Waker, oneshot::Receiver<bool>) {
    let (observed, witness) = oneshot::channel();
    (
        waker(Arc::new(ScopeWaker {
            probe,
            observed: Mutex::new(Some(observed)),
        })),
        witness,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_scope_wakes_and_reenters_without_holding_the_scope_locks() {
    let policy = Arc::new(Policy::default());
    let (factory, release, reentered) = policy.hold_factory();
    let (client, mut peer) = connect(policy.clone()).await;
    peer.create_session(&client, SESSION).await;
    let (_prompt, joined) = peer.start_prompt(&client, SESSION, 1).await;
    let probe = Arc::new(
        client
            .approval_scope_lock_probe_for_test(SESSION)
            .expect("real approval scope"),
    );
    peer.send(Callback::Permission.frame(SESSION, 91)).await;
    let (turn, scope_signal) = factory.await.expect("the real policy factory is paused");
    assert!(
        probe(),
        "policy factory must run with both approval locks free"
    );

    let (pending, drained) = client
        .observe_approval_scope_drain_for_test(SESSION)
        .expect("tracked responder");
    assert_eq!(pending.len(), 1);
    tokio::pin!(drained);
    let mut cancelled = scope_signal.cancelled();
    let (cancel_waker, cancel_witness) = scope_waker(probe.clone());
    let (drain_waker, drain_witness) = scope_waker(probe);
    assert_eq!(
        cancelled
            .as_mut()
            .poll(&mut Context::from_waker(&cancel_waker)),
        Poll::Pending
    );
    assert_eq!(
        drained
            .as_mut()
            .poll(&mut Context::from_waker(&drain_waker)),
        Poll::Pending
    );

    client.retire_approval_run_for_test(SESSION, turn.run);
    assert!(
        cancel_witness
            .await
            .expect("retirement wakes the actual scope token"),
        "scope cancellation woke while an approval lock was held"
    );
    assert!(
        scope_signal.is_cancelled(),
        "the exact retirement reached the close checkpoint"
    );
    release
        .send(())
        .expect("allow policy re-entry after the exact scope closes");
    assert!(
        reentered
            .await
            .expect("the factory re-enters the existing bindings"),
        "retired scope no longer accepts begin_cancel"
    );
    assert_reply(&peer.read().await, 91, Callback::Permission.cancelled());
    assert!(
        drain_witness
            .await
            .expect("the real guard wakes its drain waiter"),
        "responder drainage woke while an approval lock was held"
    );
    drained.await;
    peer.sync().await;
    drop(peer);
    client.shutdown().await;
    assert!(joined.await.expect("prompt task").is_err());
    assert_one_policy_stop(&policy, 1, turn, StopRequestsCause::GenerationRetired);
}

async fn send_cancel(peer: &mut Peer, id: u64) {
    peer.send(json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":id}}))
        .await;
}

#[expect(
    clippy::expect_used,
    reason = "The raw peer must reach and release the actual enqueue checkpoint."
)]
async fn cancel_before_enqueue(policy: Arc<Policy>, managed: bool) {
    let sibling = policy.hold_answer(92);
    let (client, mut peer) = connect(policy.clone()).await;
    peer.create_session(&client, SESSION).await;
    let (prompt, joined) = peer.start_prompt(&client, SESSION, 1).await;
    let (at_edge, release, queued) = client.hold_permission_reply_for_test(RequestId::Number(91));
    peer.send(Callback::Permission.frame(SESSION, 91)).await;
    let marker = at_edge
        .await
        .expect("response is assembled but not enqueued");
    assert!(!marker.is_cancelled());
    if !managed {
        assert!(policy.events().contains(&PolicyEvent::Finished(91)));
    }
    peer.send(Callback::Permission.frame(SESSION, 92)).await;
    wait_started(&policy, 92, 1).await;

    send_cancel(&mut peer, 91).await;
    marker.cancelled().await;
    release
        .send(())
        .expect("allow enqueue only after the SDK observes peer cancel");
    queued.await.expect("reply was enqueued");
    assert_reply(&peer.read().await, 91, Callback::Permission.cancelled());
    sibling
        .send(())
        .expect("the unrelated request remains pending");
    assert_reply(&peer.read().await, 92, selected_permission());
    peer.sync().await;
    assert!(policy.events().iter().all(|event| !matches!(
        event,
        PolicyEvent::ManagedCancel { .. } | PolicyEvent::Stopped { .. }
    )));
    peer.respond(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    let outcome = joined
        .await
        .expect("prompt task")
        .expect("cancelled request did not end its turn");
    assert_eq!(
        outcome.stop_reason,
        session_event_model::StopReason::EndTurn
    );
    assert_eq!(
        outcome.permission_refusal_reason, None,
        "an ordinary decline at the cancelled reply gate must not taint the prompt outcome"
    );
    client.shutdown().await;
}

#[tokio::test]
async fn peer_cancel_observed_between_selection_and_enqueue_declines_the_reply() {
    cancel_before_enqueue(Arc::new(Policy::default()), false).await;
}

#[tokio::test]
async fn peer_cancel_at_managed_reply_commit_never_cancels_the_turn() {
    cancel_before_enqueue(Arc::new(Policy::managed_first()), true).await;
}

#[tokio::test]
async fn peer_cancel_after_enqueue_keeps_the_selection_in_a_held_batch() {
    let policy = Arc::new(Policy::default());
    let sibling = policy.hold_answer(92);
    let (client, mut peer) = connect(policy.clone()).await;
    peer.create_session(&client, SESSION).await;
    let (prompt, joined) = peer.start_prompt(&client, SESSION, 1).await;
    let (at_edge, release, queued) = client.hold_permission_reply_for_test(RequestId::Number(91));
    peer.send(json!([
        Callback::Permission.frame(SESSION, 91),
        Callback::Permission.frame(SESSION, 92),
    ]))
    .await;
    let marker = at_edge
        .await
        .expect("the selected reply reached the commit edge");
    assert!(!marker.is_cancelled());
    wait_started(&policy, 92, 1).await;
    release.send(()).expect("release selected reply");
    queued
        .await
        .expect("the SDK accepted the reply before cancellation");

    send_cancel(&mut peer, 91).await;
    // The completed responder has unregistered its marker. Wait for real
    // inbound progress, while the sibling still holds the original batch.
    peer.sync().await;
    sibling.send(()).expect("release the separate slot");
    assert_eq!(
        peer.read().await,
        json!([
            {"jsonrpc":"2.0","id":91,"result":selected_permission()},
            {"jsonrpc":"2.0","id":92,"result":selected_permission()},
        ])
    );
    peer.respond(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    joined
        .await
        .expect("prompt task")
        .expect("the original turn remains live");
    peer.sync().await;
    client.shutdown().await;
}
