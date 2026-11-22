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
