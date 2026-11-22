use super::{acp_connection_channels, run_acp_transport};
use serde_json::json;

#[tokio::test]
async fn local_output_overflow_does_not_report_peer_loss() {
    let (wire, router) = acp_connection_channels();
    let peer_disconnected = wire.peer_disconnected.clone();
    let (open_peer, transport) = tokio::io::duplex(64);

    let mut overflowed = false;
    for _ in 0..=1024 {
        if router
            .output
            .send(json!({"method": "session/update"}))
            .await
            .is_err()
        {
            overflowed = true;
            break;
        }
    }
    assert!(overflowed);
    assert!(run_acp_transport(transport, wire).await.is_ok());

    assert!(!peer_disconnected.is_cancelled());
    drop(open_peer);
}
