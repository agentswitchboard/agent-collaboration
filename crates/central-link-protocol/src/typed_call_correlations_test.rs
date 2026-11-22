use super::*;

#[test]
fn a_connection_ignores_duplicates_and_retired_calls_and_requires_a_fresh_retry() {
    let first: CorrelationId = "019f0000-0000-7000-8000-000000000001".parse().unwrap();
    let second: CorrelationId = "019f0000-0000-7000-8000-000000000002".parse().unwrap();
    let mut connection = TypedCallCorrelations::default();
    let mut other_connection = TypedCallCorrelations::<i32>::default();
    assert_eq!(connection.register(first.clone(), 1), Ok(()));
    assert_eq!(other_connection.retire(&first), None);
    assert_eq!(connection.retire(&first), Some(1));
    assert_eq!(connection.retire(&first), None);
    assert_eq!(connection.register(first, 2), Err(2));
    assert_eq!(connection.register(second.clone(), 2), Ok(()));
    assert_eq!(connection.drain_pending().collect::<Vec<_>>(), vec![2]);
    assert_eq!(connection.retire(&second), None);
}

fn correlation(ordinal: usize) -> CorrelationId {
    format!("019f0000-0000-7000-8000-{ordinal:012x}")
        .parse()
        .unwrap()
}

#[test]
fn pending_correlations_stop_at_capacity_and_a_reply_frees_a_slot() {
    let mut connection = TypedCallCorrelations::default();
    for ordinal in 0..64 {
        assert_eq!(connection.register(correlation(ordinal), ordinal), Ok(()));
    }

    assert_eq!(connection.register(correlation(64), 64), Err(64));
    assert_eq!(connection.retire(&correlation(64)), None);
    assert_eq!(connection.retire(&correlation(0)), Some(0));
    assert_eq!(connection.register(correlation(64), 64), Ok(()));
    assert_eq!(connection.retire(&correlation(64)), Some(64));
}

#[test]
fn resolved_correlations_keep_a_recent_window_and_are_forgotten_on_close() {
    let mut connection = TypedCallCorrelations::default();
    for ordinal in 0..129 {
        assert_eq!(connection.register(correlation(ordinal), ordinal), Ok(()));
        assert_eq!(connection.retire(&correlation(ordinal)), Some(ordinal));
    }

    assert_eq!(connection.register(correlation(128), 128), Err(128));
    assert_eq!(connection.retire(&correlation(0)), None);
    assert_eq!(connection.register(correlation(0), 0), Ok(()));
    assert_eq!(connection.drain_pending().collect::<Vec<_>>(), vec![0]);
    assert_eq!(connection.register(correlation(128), 128), Ok(()));
    assert_eq!(connection.register(correlation(0), 0), Ok(()));
}
