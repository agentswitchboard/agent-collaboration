use super::*;
use rustix::io::Errno;
use serde_json::json;
use std::{future::ready, path::Path};

fn session(id: &str) -> SessionId {
    SessionId::try_from(id.to_owned()).unwrap()
}

fn write_record(root: &Path, pid: u32, body: &str) -> PathBuf {
    let file = root.join(format!("{pid}.json"));
    fs::write(&file, body).unwrap();
    file
}

fn identified(root: &Path, pid: u32, id: &str, time: i64) -> PathBuf {
    write_record(
        root,
        pid,
        &json!({"pid": pid, "sessionId": id, "startedAt": time}).to_string(),
    )
}

#[tokio::test]
async fn valid_json_without_a_usable_id_is_incomplete_when_live() {
    let root = tempfile::tempdir().unwrap();
    let entries = [
        Ok(write_record(
            root.path(),
            100,
            r#"{"pid":100,"startedAt":1500}"#,
        )),
        Ok(write_record(
            root.path(),
            101,
            r#"{"pid":101,"sessionId":""}"#,
        )),
    ];
    let census = PeerSessionCensus::read(entries, |_| Ok(true));
    let answer = census
        .answer_with(&session("target"), |_| Ok(true), |_| ready(Some(1_000)))
        .await;
    assert_eq!(
        answer,
        PeerCensusAnswer {
            at_least: 0,
            complete: false
        }
    );
}

#[tokio::test]
async fn each_readable_match_survives_an_unreadable_live_record() {
    let root = tempfile::tempdir().unwrap();
    let matches = [
        identified(root.path(), 100, "target", 1_500),
        identified(root.path(), 102, "target", 1_500),
    ];
    let unreadable = write_record(root.path(), 101, "not JSON");
    for count in 1..=2 {
        let entries = matches[..count].iter().cloned().chain([unreadable.clone()]);
        let census = PeerSessionCensus::read(entries.map(Ok), |_| Ok(true));
        let answer = census
            .answer_with(&session("target"), |_| Ok(true), |_| ready(Some(1_000)))
            .await;
        assert_eq!(
            answer,
            PeerCensusAnswer {
                at_least: count,
                complete: false
            }
        );
    }
}

#[tokio::test]
async fn unidentified_probe_failure_and_directory_iteration_failure_are_each_incomplete() {
    let root = tempfile::tempdir().unwrap();
    let unidentified = write_record(root.path(), 101, r#"{"pid":101}"#);
    let known = identified(root.path(), 102, "target", 1_500);
    for uncertain in [
        Ok(unidentified),
        Err(io::Error::other("fixture iteration error")),
    ] {
        let census = PeerSessionCensus::read([Ok(known.clone()), uncertain], |pid| match pid {
            102 => Ok(true),
            _ => Err(PeerRegistryError::LivenessProbe {
                process_id: pid,
                source: Errno::IO,
            }),
        });
        let answer = census
            .answer_with(&session("target"), |_| Ok(true), |_| ready(Some(1_000)))
            .await;
        assert_eq!(
            answer,
            PeerCensusAnswer {
                at_least: 1,
                complete: false
            }
        );
    }
}

#[tokio::test]
async fn different_id_and_proved_dead_records_do_not_pollute_the_target() {
    let root = tempfile::tempdir().unwrap();
    let entries = [
        Ok(write_record(root.path(), 100, "not JSON")),
        Ok(write_record(
            root.path(),
            101,
            r#"{"pid":200,"sessionId":"target","startedAt":1500}"#,
        )),
    ];
    let census = PeerSessionCensus::read(entries, |pid| Ok(pid == 101));
    let unexpected_probe = |_| -> std::future::Ready<Option<i64>> {
        panic!("a different ID or mismatched PID cannot start a process probe")
    };
    let answer = census
        .answer_with(&session("other"), |_| Ok(true), unexpected_probe)
        .await;
    assert_eq!(
        answer,
        PeerCensusAnswer {
            at_least: 0,
            complete: true
        }
    );
    let answer = census
        .answer_with(&session("target"), |_| Ok(true), unexpected_probe)
        .await;
    assert_eq!(
        answer,
        PeerCensusAnswer {
            at_least: 0,
            complete: false
        }
    );
}

#[tokio::test]
async fn excludes_reused_pid_and_keeps_unresolved_incarnation_incomplete() {
    let root = tempfile::tempdir().unwrap();
    let entries = [
        Ok(identified(root.path(), 100, "target", 2_500)),
        Ok(identified(root.path(), 101, "target", 1_500)),
    ];
    let census = PeerSessionCensus::read(entries, |_| Ok(true));
    for (later_process, complete) in [(Some(2_501), true), (Some(2_500), false), (None, false)] {
        let answer = census
            .answer_with(
                &session("target"),
                |_| Ok(true),
                |pid| {
                    ready(if pid == 100 {
                        Some(2_500)
                    } else {
                        later_process
                    })
                },
            )
            .await;
        assert_eq!(
            answer,
            PeerCensusAnswer {
                at_least: 1,
                complete
            }
        );
    }
}

#[tokio::test]
async fn real_child_is_current_only_for_record_written_after_its_start() {
    let root = tempfile::tempdir().unwrap();
    let child = tokio::process::Command::new("/bin/cat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let pid = child.id().unwrap();
    let started_at = process_started_at(pid, std::future::pending())
        .await
        .expect("ps for test-owned child");
    let registry = ClaudeCodeSessionRegistry::new(root.path().to_owned());

    for (recorded_at, expected) in [(started_at - 1_001, 0), (started_at + 1_000, 1)] {
        identified(root.path(), pid, "target", recorded_at);
        let answer = registry
            .scan_recipient_sessions()
            .answer(&session("target"), std::future::pending)
            .await;
        assert_eq!(
            answer,
            PeerCensusAnswer {
                at_least: expected,
                complete: true
            }
        );
    }
    drop(child);
}
