use super::DueBatchState;
use std::sync::Arc;

#[test]
fn competing_event_requires_fresh_discovery_before_release() {
    let state = Arc::new(DueBatchState::default());
    let mut active_claim = state.claim().unwrap();
    active_claim.start_discovery();

    assert!(state.claim().is_none());
    assert!(!active_claim.release_if_current());
    active_claim.start_discovery();
    assert!(active_claim.release_if_current());

    let next_claim = state.claim().unwrap();
    drop(active_claim);
    assert!(
        state.claim().is_none(),
        "dropping the old holder released a newer claim"
    );
    drop(next_claim);
    assert!(state.claim().is_some());
}

#[test]
fn dropping_the_drain_releases_even_a_dirty_claim() {
    let state = Arc::new(DueBatchState::default());
    let claim = state.claim().unwrap();
    assert!(state.claim().is_none());
    drop(claim);
    assert!(state.claim().is_some());
}
