use super::*;

const SAME_MILLISECOND: i64 = 1_795_420_000_000;

fn fresh() -> (FreshCorrelationId, CorrelationId) {
    let fresh = FreshCorrelationId::mint_at(SAME_MILLISECOND);
    let wire = fresh.wire_id().clone();
    (fresh, wire)
}

#[test]
fn only_a_matching_reply_on_the_owning_connection_can_retire_a_waiter() {
    let (first, wire) = fresh();
    let mut connection = TypedCallCorrelations::default();
    let mut other_connection = TypedCallCorrelations::<i32>::default();
    assert_eq!(connection.register(first, 1), Ok(()));
    assert_eq!(other_connection.take_if(&wire, |_| true), None);
    assert_eq!(connection.take_if(&wire, |expected| *expected == 2), None);
    assert_eq!(
        connection.take_if(&wire, |expected| *expected == 1),
        Some(1)
    );
    assert_eq!(connection.take_if(&wire, |_| true), None);
}

#[test]
fn pending_correlations_stop_at_capacity_and_a_reply_frees_a_slot() {
    let mut connection = TypedCallCorrelations::default();
    let mut wires = Vec::new();
    for ordinal in 0..64 {
        let (fresh, wire) = fresh();
        assert_eq!(connection.register(fresh, ordinal), Ok(()));
        wires.push(wire);
    }

    let (overflow, unused) = fresh();
    assert_eq!(connection.register(overflow, 64), Err(64));
    assert_eq!(connection.retire(&unused), None);
    assert_eq!(connection.retire(&wires[0]), Some(0));
    let (replacement, replacement_wire) = fresh();
    assert_eq!(connection.register(replacement, 64), Ok(()));
    assert_eq!(connection.retire(&replacement_wire), Some(64));
}

#[test]
fn an_evicted_wire_id_cannot_capture_a_new_waiter_even_at_the_same_clock_instant() {
    let (first, old_wire) = fresh();
    let mut connection = TypedCallCorrelations::default();
    assert_eq!(connection.register(first, 0), Ok(()));
    assert_eq!(connection.retire(&old_wire), Some(0));
    for ordinal in 1..=128 {
        let (fresh, wire) = fresh();
        assert_eq!(connection.register(fresh, ordinal), Ok(()));
        assert_eq!(connection.retire(&wire), Some(ordinal));
    }

    assert_eq!(connection.resolved.len(), 128);
    assert!(!connection.resolved.contains(&old_wire));
    let (retry, retry_wire) = fresh();
    assert_ne!(old_wire, retry_wire);
    assert_eq!(connection.register(retry, 129), Ok(()));
    assert_eq!(connection.take_if(&old_wire, |_| true), None);
    assert_eq!(connection.take_if(&retry_wire, |_| true), Some(129));

    let (in_flight, _) = fresh();
    assert_eq!(connection.register(in_flight, 130), Ok(()));
    assert_eq!(connection.drain_pending().collect::<Vec<_>>(), vec![130]);
    assert!(connection.resolved.is_empty());
}
