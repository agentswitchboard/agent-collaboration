use super::*;
use router_clock::ManualClock;

#[tokio::test]
async fn retries_and_other_ids_share_the_scan_and_follow_up_starts_fresh() {
    let root = tempfile::tempdir().unwrap();
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let registry = Arc::new(ClaudeCodeSessionRegistry::new(root.path().to_owned()));
    let inventory = ClaudeTerminalSessionInventory::new(registry.clone(), clock.clone());
    let first = PeerSessionId::try_from("first".to_owned()).unwrap();
    let other = PeerSessionId::try_from("other".to_owned()).unwrap();
    let mut scan = None;
    let abandoned = inventory.query_with(&first, |running| scan = Some(running));
    drop(abandoned);
    let retry = inventory.query_with(&first, |_| panic!("retry started a second scan"));
    let other = inventory.query_with(&other, |_| panic!("other ID started a second scan"));
    let running = scan.unwrap();
    tokio::task::spawn_blocking(move || {
        running.resolve(&PeerSessionCensus::scan(&registry), clock.as_ref())
    })
    .await
    .unwrap();
    let empty = SessionInventoryAnswer::Complete(0);
    assert_eq!(retry.await.unwrap(), empty);
    assert_eq!(other.await.unwrap(), empty);

    // A fresh registry observation sees this new unattributed live file.
    std::fs::write(
        root.path().join(format!("{}.json", std::process::id())),
        b"{}",
    )
    .unwrap();
    assert_eq!(inventory.query(&first).await.unwrap(), UNAVAILABLE);
}
