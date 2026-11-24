use super::*;
use rustix::io::Errno;
use serde_json::json;
use std::{
    future::{pending, ready},
    path::Path,
};

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

const fn answer(at_least: usize, complete: bool) -> PeerCensusOutcome {
    PeerCensusOutcome::Answer(PeerCensusAnswer {
        at_least,
        complete,
        managed_claude_alias: false,
    })
}

fn read_target(entries: impl IntoIterator<Item = io::Result<PathBuf>>) -> PeerSessionCensus {
    PeerSessionCensus::read(entries, &session("target"), |_| Ok(true), || true)
}

#[test]
fn checked_in_darwin_placeholder_uses_the_registration_timestamp() {
    let mut record: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/claude_code_session_record.json"
    ))
    .unwrap();
    record["pid"] = json!(42);
    let RegistryClaim::Identified {
        recorded: RecordedProcessStart::RegistrationTimestamp(recorded),
        ..
    } = registry_claim(42, &record)
    else {
        panic!("the placeholder is not a process observation");
    };
    assert_eq!(recorded, record["startedAt"].as_i64().unwrap());
}

#[test]
fn parseable_proc_start_compares_displayed_seconds_without_using_registration_stamp() {
    let record = json!({"pid":42, "startedAt":59_900,
        "procStart":"Thu Jan  1 00:00:59 1970"});
    let RegistryClaim::Identified { recorded, .. } = registry_claim(42, &record) else {
        panic!("the process start is usable");
    };
    assert_eq!(
        compare_incarnation(recorded, Some(ProcessStart(59_000))),
        Incarnation::Verified(ProcessStart(59_000))
    );
    assert_eq!(
        compare_incarnation(recorded, Some(ProcessStart(58_000))),
        Incarnation::Stale
    );
    assert_eq!(
        compare_incarnation(recorded, Some(ProcessStart(60_000))),
        Incarnation::Stale
    );
    assert_eq!(compare_incarnation(recorded, None), Incarnation::Unverified);
}

#[test]
fn known_limit_pid_reuse_inside_the_same_displayed_second_is_indistinguishable() {
    // The original process can start at .050 and its replacement at .700.
    // Both report the same displayed second after a record stamped at .100.
    let displayed_second = 59_000;
    for recorded in [
        RecordedProcessStart::DisplayedSecond(ProcessStart(displayed_second)),
        RecordedProcessStart::RegistrationTimestamp(displayed_second + 100),
    ] {
        assert_eq!(
            compare_incarnation(recorded, Some(ProcessStart(displayed_second))),
            Incarnation::Verified(ProcessStart(displayed_second))
        );
    }
}

#[tokio::test]
async fn valid_json_without_a_usable_id_is_incomplete_when_live() {
    let root = tempfile::tempdir().unwrap();
    let census = read_target([
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
    ]);
    let observed = census
        .answer_with(
            |_| Ok(true),
            |_| ready(ProcessStartObservation::Observed(ProcessStart(1_000))),
            || true,
            |_| {},
        )
        .await;
    assert_eq!(observed, answer(0, false));
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
        let census = read_target(entries.map(Ok));
        let observed = census
            .answer_with(
                |_| Ok(true),
                |_| ready(ProcessStartObservation::Observed(ProcessStart(1_000))),
                || true,
                |_| {},
            )
            .await;
        assert_eq!(observed, answer(count, false));
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
        let census = PeerSessionCensus::read(
            [Ok(known.clone()), uncertain],
            &session("target"),
            |pid| match pid {
                102 => Ok(true),
                _ => Err(PeerRegistryError::LivenessProbe {
                    process_id: pid,
                    source: Errno::IO,
                }),
            },
            || true,
        );
        let observed = census
            .answer_with(
                |_| Ok(true),
                |_| ready(ProcessStartObservation::Observed(ProcessStart(1_000))),
                || true,
                |_| {},
            )
            .await;
        assert_eq!(observed, answer(1, false));
    }
}

#[tokio::test]
async fn different_id_and_proved_dead_records_do_not_pollute_the_target() {
    let root = tempfile::tempdir().unwrap();
    let entries = [
        write_record(root.path(), 100, "not JSON"),
        write_record(
            root.path(),
            101,
            r#"{"pid":200,"sessionId":"target","startedAt":1500}"#,
        ),
    ];
    for (target, complete) in [("target", false), ("other", true)] {
        let census = PeerSessionCensus::read(
            entries.iter().cloned().map(Ok),
            &session(target),
            |pid| Ok(pid == 101),
            || true,
        );
        let unexpected_probe = |_| -> std::future::Ready<ProcessStartObservation> {
            panic!("a different ID or mismatched PID cannot start a process probe")
        };
        let observed = census
            .answer_with(|_| Ok(true), unexpected_probe, || true, |_| {})
            .await;
        assert_eq!(observed, answer(0, complete));
    }
}

#[tokio::test]
async fn excludes_reused_pid_and_keeps_unresolved_incarnation_incomplete() {
    let root = tempfile::tempdir().unwrap();
    let census = read_target([
        Ok(identified(root.path(), 100, "target", 2_500)),
        Ok(identified(root.path(), 101, "target", 1_500)),
    ]);
    for (later_process, complete) in [(Some(2_501), true), (Some(2_500), false), (None, false)] {
        let observed = census
            .answer_with(
                |_| Ok(true),
                |pid| {
                    let time = if pid == 100 {
                        Some(2_500)
                    } else {
                        later_process
                    };
                    ready(
                        time.map_or(ProcessStartObservation::Unavailable, |started| {
                            ProcessStartObservation::Observed(ProcessStart(started))
                        }),
                    )
                },
                || true,
                |_| {},
            )
            .await;
        assert_eq!(observed, answer(1, complete));
    }
}

#[tokio::test]
async fn two_distinct_proved_claims_return_before_the_next_held_probe() {
    let root = tempfile::tempdir().unwrap();
    let first = identified(root.path(), 100, "target", 2_500);
    let census = read_target(
        [
            first.clone(),
            first,
            identified(root.path(), 101, "target", 2_500),
            identified(root.path(), 102, "target", 2_500),
        ]
        .into_iter()
        .map(Ok),
    );
    let mut probes = Vec::new();
    let mut progress = Vec::new();
    let observed = {
        let mut lookup = std::pin::pin!(census.answer_with(
            |_| Ok(true),
            |pid| {
                probes.push(pid);
                std::future::poll_fn(move |_| {
                    if pid == 102 {
                        std::task::Poll::Pending
                    } else {
                        std::task::Poll::Ready(ProcessStartObservation::Observed(ProcessStart(
                            1_000,
                        )))
                    }
                })
            },
            || true,
            |count| progress.push(count),
        ));
        std::future::poll_fn(|cx| {
            let std::task::Poll::Ready(answer) = lookup.as_mut().poll(cx) else {
                panic!("two distinct claims must return before the next probe");
            };
            std::task::Poll::Ready(answer)
        })
        .await
    };
    assert_eq!(observed, answer(2, false));
    assert_eq!(probes, [100, 101]);
    assert_eq!(progress, [1, 2]);
}

#[tokio::test]
async fn interruption_preserves_the_already_proved_lower_bound() {
    let root = tempfile::tempdir().unwrap();
    let census = read_target([
        Ok(identified(root.path(), 100, "target", 2_500)),
        Ok(identified(root.path(), 101, "target", 2_500)),
        Ok(identified(root.path(), 102, "target", 2_500)),
    ]);
    let mut calls = Vec::new();
    let observed = census
        .answer_with(
            |_| Ok(true),
            |pid| {
                calls.push(pid);
                ready(if pid == 100 {
                    ProcessStartObservation::Observed(ProcessStart(1_000))
                } else {
                    ProcessStartObservation::Interrupted
                })
            },
            || true,
            |_| {},
        )
        .await;
    assert_eq!(observed, answer(1, false));
    assert_eq!(calls, [100, 101]);
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
    let displayed = tokio::process::Command::new("/bin/ps")
        .env("LC_ALL", "C")
        .env("TZ", "UTC")
        .args(["-p", &pid.to_string(), "-o", "lstart="])
        .output()
        .await
        .unwrap();
    assert!(displayed.status.success());
    let displayed = std::str::from_utf8(&displayed.stdout).unwrap().trim();
    let started_at = parse_ps_start(displayed).expect("ps for test-owned child");
    let registry = ClaudeCodeSessionRegistry::new(root.path().to_owned());
    let target = session("target");

    for (recorded_at, expected) in [(started_at - 1_001, 0), (started_at + 1_000, 1)] {
        identified(root.path(), pid, "target", recorded_at);
        let census = PeerSessionCensus::scan(&registry, &target, || true);
        assert_eq!(
            census.answer(|| true, pending, pending, |_| {}).await,
            answer(expected, true)
        );
    }
    write_record(
        root.path(),
        pid,
        &json!({
            "pid":pid, "sessionId":"target", "procStart":displayed, "startedAt":0
        })
        .to_string(),
    );
    let census = PeerSessionCensus::scan(&registry, &target, || true);
    assert_eq!(
        census.answer(|| true, pending, pending, |_| {}).await,
        answer(1, true)
    );
    drop(child);
}

#[tokio::test]
async fn denied_ancestry_keeps_the_verified_registry_claim() {
    struct LiveAdapter(ProcessIncarnation);
    impl ManagedClaudeLaunches for LiveAdapter {
        fn live_adapter(&self, _: &SessionId) -> Option<ProcessIncarnation> {
            Some(self.0)
        }
    }

    let root = tempfile::tempdir().unwrap();
    let census = read_target([Ok(write_record(
        root.path(),
        100,
        r#"{"pid":100,"sessionId":"target","startedAt":1500}"#,
    ))]);
    let launch = LiveAdapter(ProcessIncarnation {
        pid: NonZeroU32::new(42).unwrap(),
        started: ProcessStart(0),
    });
    let managed = ManagedClaudeClaim {
        launches: &launch,
        adapter: launch.0,
    };
    let observed = census
        .count_verified_claims(
            |_| Ok(true),
            |_| ready(ProcessStartObservation::Observed(ProcessStart(1_000))),
            || true,
            |_| {
                ready(managed_claim_disposition(
                    &managed,
                    &session("target"),
                    ProcessObservation::Unavailable,
                ))
            },
            |_| {},
        )
        .await;

    assert_eq!(
        observed,
        PeerCensusOutcome::Answer(PeerCensusAnswer {
            at_least: 1,
            complete: true,
            managed_claude_alias: false,
        }),
    );
}
