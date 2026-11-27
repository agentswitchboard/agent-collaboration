use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn backoff_advances_on_paused_time() {
    tokio::time::sleep(Duration::from_secs(5)).await;
}
