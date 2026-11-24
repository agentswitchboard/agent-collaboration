use super::expires_at_ms;

#[test]
fn every_outbox_write_has_15_minutes() {
    assert_eq!(expires_at_ms(1_000), 1_000 + 900_000);
    assert_eq!(expires_at_ms(i64::MAX), i64::MAX);
}
