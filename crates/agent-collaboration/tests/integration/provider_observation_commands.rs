//! Marker projection through the executable's observe and listen commands.
use collaboration_contracts::{
    HubEvent, HubFuture, HubSessionSummary, SessionEventAttachment, SessionEventHub,
    SessionEventHubError, SkillViewAttachment,
};
use collaboration_mcp::test_support::ServedCollaborationApi;
use collaboration_service::ApplicationFixture;
use message_board::{SessionEndpointRef, SessionRef};
use serde_json::{Value, json};
use session_event_model::{SessionEvent, SessionState};
use std::{os::unix::fs::PermissionsExt as _, path::PathBuf, sync::Arc};
use tokio::sync::broadcast;

const SERVICE: &str = "00000000-0000-7000-8000-000000000101";
const RUN: &str = "00000000-0000-7000-8000-000000000102";

struct CutThenGap;

impl SessionEventHub for CutThenGap {
    fn attach(&self, _: SessionRef) -> HubFuture<'_, SessionEventAttachment> {
        Box::pin(async {
            let (sender, receiver) = broadcast::channel(1);
            sender
                .send(HubEvent {
                    sequence: 1,
                    event: SessionEvent::ResyncRequired { replay_epoch: 1 },
                })
                .unwrap();
            Ok(SessionEventAttachment {
                snapshot: vec![],
                receiver,
                epoch: 0,
                latest_issued_sequence: 0,
            })
        })
    }

    fn skill_view(&self, _: SessionRef, _: PathBuf) -> HubFuture<'_, SkillViewAttachment> {
        Box::pin(async { Err(SessionEventHubError::Unavailable) })
    }
    fn state(&self, _: SessionRef) -> HubFuture<'_, SessionState> {
        Box::pin(async { Err(SessionEventHubError::Unavailable) })
    }
    fn sessions(&self, _: SessionEndpointRef) -> HubFuture<'_, Vec<HubSessionSummary>> {
        Box::pin(async { Err(SessionEventHubError::Unavailable) })
    }
}

fn marker() -> Value {
    json!({"kind":"snapshotEnd","cursor":{"epoch":0,"afterSequence":0}})
}

async fn run_command(command: &str, extra: &[&str]) -> (Option<i32>, Vec<Value>) {
    let root = tempfile::tempdir_in("/tmp").unwrap();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let app = ApplicationFixture::new(SERVICE, RUN)
        .unwrap()
        .with_clock(Arc::new(router_clock::ManualClock::new(Default::default())))
        .with_provider_session_hub(Arc::new(CutThenGap))
        .with_endpoints(vec![serde_json::from_value(json!({
            "endpoint":{"serviceId":SERVICE,"endpointId":"claude-local"},
            "label":"Claude fixture",
            "availability":{"state":"available","observedAt":"2026-11-21T00:00:00Z"},
            "channels":[{"kind":"externalProvider","transport":"stdioAcp",
                "bindingId":"fixture","bindingGeneration":1,
                "runtime":{"provider":"claudeCode","runtimeName":"fixture"},
                "capabilities":[{"name":"create","status":"supported","evidence":"advertised"}]}]
        })).unwrap()])
        .unwrap()
        .build();
    let served = ServedCollaborationApi::start(root.path(), app).unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_agent-collaboration"))
        .args([
            "events",
            command,
            "--endpoint",
            "claude-local",
            "--session",
            "fixture-session",
            "--attach",
            "--service-directory",
        ])
        .arg(root.path())
        .args(extra)
        .output()
        .await
        .unwrap();
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    served.stop().await.unwrap();
    (output.status.code(), records)
}

// Rejects an observe command that drops the marker from either streamed output or the result.
#[tokio::test]
async fn observe_prints_snapshot_end_before_the_final_result() {
    let (exit, records) = run_command("observe", &["--stream"]).await;
    assert_eq!(exit, Some(0));
    let [snapshot, gap, result] = records.as_slice() else {
        panic!("wrong observation output: {records:?}")
    };
    assert_eq!(snapshot["event"], marker());
    assert_eq!(snapshot["cursor"], marker()["cursor"]);
    assert_eq!(gap["event"]["event"]["kind"], "resyncRequired");
    assert_eq!(result["events"], json!([marker(), gap["event"]]));
}

// Rejects a listen command that skips the marker or treats it as the end of the attachment.
#[tokio::test]
async fn listen_prints_snapshot_end_and_stops_at_the_gap() {
    let (exit, records) = run_command("listen", &[]).await;
    assert_eq!(exit, Some(3));
    let [ready, snapshot, gap] = records.as_slice() else {
        panic!("wrong listen output: {records:?}")
    };
    assert_eq!(ready["kind"], "listenerReady");
    assert_eq!(snapshot["kind"], "providerSessionEvent");
    assert_eq!(snapshot["message"], marker());
    assert_eq!(gap["message"]["event"]["kind"], "resyncRequired");
}
