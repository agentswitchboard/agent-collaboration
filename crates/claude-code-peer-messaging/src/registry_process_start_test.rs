use super::{
    ProcessStartObservation, parse_ps_start, read_process_start, spawn_process_start, wait_for_reap,
};
use std::{fs, os::unix::fs::PermissionsExt};

#[test]
fn parses_macos_and_linux_ps_spacing_as_utc() {
    for (output, expected) in [
        ("Mon Nov 23 13:17:09 2026    \n", Some(1_795_439_829_000)),
        ("Thu Jan  1 00:00:00 1970\n", Some(0)),
        ("Thu Feb 29 00:00:00 2024\n", Some(1_709_164_800_000)),
        ("Mon Nov 23 25:00:00 2026\n", None),
        ("Fri Feb 29 00:00:00 2100\n", None),
        ("Mon Jan  1 00:00:00 1970\n", None),
        ("STARTED\n", None),
    ] {
        assert_eq!(parse_ps_start(output), expected, "{output:?}");
    }
}

#[tokio::test]
async fn an_expired_probe_kills_and_reaps_the_owned_child() {
    let temporary = tempfile::tempdir().unwrap();
    let command = temporary.path().join("held-ps");
    fs::write(&command, "#!/bin/sh\nexec /bin/sleep 3600\n").unwrap();
    fs::set_permissions(&command, fs::Permissions::from_mode(0o700)).unwrap();
    let mut child = spawn_process_start(&command, 1).expect("test-owned probe");

    assert_eq!(
        read_process_start(&mut child, std::future::ready(()), std::future::pending).await,
        ProcessStartObservation::Interrupted
    );
    let exit = child
        .try_wait()
        .unwrap()
        .expect("probe must be reaped before the answer");
    assert!(!exit.success());
}

#[tokio::test]
async fn an_unfinished_reap_hands_off_at_its_injected_deadline() {
    let (expire, expired) = tokio::sync::oneshot::channel();
    let mut wait = std::pin::pin!(wait_for_reap::<super::ProcessStart>(
        std::future::pending(),
        async {
            expired.await.unwrap();
        }
    ));
    std::future::poll_fn(|cx| {
        use std::{future::Future as _, task::Poll};
        assert!(wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    expire.send(()).unwrap();
    std::future::poll_fn(|cx| {
        use std::{future::Future as _, task::Poll};
        let Poll::Ready(answer) = wait.as_mut().poll(cx) else {
            panic!("reap must hand off when its injected deadline is ready");
        };
        assert_eq!(answer, ProcessStartObservation::ReapDeferred);
        Poll::Ready(())
    })
    .await;
}
