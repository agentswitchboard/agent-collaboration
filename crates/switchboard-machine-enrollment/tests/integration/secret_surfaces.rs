use std::time::Duration;
use tokio::process::Command;

use axum::http::StatusCode;
use switchboard_machine_enrollment::{AccessRequest, EnrollmentEnd, LeaseUnavailable};

use super::scripted_sign_in::{
    DEVICE_SECRET, INITIAL_ACCESS, INITIAL_REFRESH, ROTATED_ACCESS, ROTATED_REFRESH, Reply,
    ScriptStore, SignInScript, USER_CODE, enrollment_request, machine_id, organization_id,
    token_reply,
};

const CHILD_MARKER: &str = "DEL13A_SECRET_SURFACES_CHILD";
const CHILD_DONE: &str = "scripted enrollment and refresh captured";

async fn observe_enrollment_and_refresh() {
    let mut script = SignInScript::start(
        100,
        [
            token_reply(INITIAL_ACCESS, INITIAL_REFRESH),
            token_reply(ROTATED_ACCESS, ROTATED_REFRESH),
            Reply::new(
                StatusCode::BAD_REQUEST,
                serde_json::json!({
                    "error": "invalid_grant",
                    "reason": ROTATED_REFRESH,
                })
                .to_string(),
            ),
        ],
    )
    .await;
    let store = ScriptStore::default();
    let enrollment = script.spawn_attempt({
        let client = script.client.clone();
        let store = store.clone();
        async move {
            client
                .enroll(enrollment_request(&machine_id()), &store, |instructions| {
                    println!(
                        "open {} and enter {}",
                        instructions.verification_link, instructions.user_code
                    );
                    println!("{instructions:?}");
                    Ok(())
                })
                .await
        }
    });
    let discover = script.next_request().await;
    discover.assert_discovery();
    let start = script.next_request().await;
    start.assert_start();
    script.wait_for_poll(5, 5).await;
    let poll = script.next_request().await;
    poll.assert_poll();
    let receipt = enrollment
        .await
        .expect("enrollment task")
        .expect("approved enrollment");
    println!("{receipt:?} {:?}", store.current());
    let owner = script
        .client
        .refresh_owner(store.clone(), machine_id(), organization_id());
    let bearer = owner
        .lease(AccessRequest::Usable)
        .await
        .expect("rotated credential");
    assert!(bearer.token.expose_secret() == ROTATED_ACCESS);
    assert!(
        store
            .current()
            .expect("durable rotation")
            .credential
            .refresh_token
            .expose_secret()
            == ROTATED_REFRESH
    );
    println!("{bearer:?} {:?}", store.current());
    let rotation = script.next_request().await;
    rotation.assert_refresh(INITIAL_REFRESH);

    script
        .clock
        .advance(Duration::from_secs(60))
        .expect("advance expiry");
    let ending = owner
        .lease(AccessRequest::Usable)
        .await
        .expect_err("refused refresh");
    assert_eq!(
        ending,
        LeaseUnavailable::Ended {
            why: EnrollmentEnd::RefreshRefused,
        }
    );
    assert!(store.current().is_none());
    println!("{ending:?} {ending}");
    let refusal = script.next_request().await;
    refusal.assert_refresh(ROTATED_REFRESH);
    script.assert_exhausted();

    for request in [discover, start, poll, rotation, refusal] {
        println!("{}", request.visible_wire_fields());
    }
    for argument in std::env::args_os() {
        println!("child arg {argument:?}");
    }
    println!("{CHILD_DONE}");
}

#[tokio::test]
async fn enrollment_and_refresh_emit_no_secrets() {
    if std::env::var_os(CHILD_MARKER).is_some() {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_max_level(tracing::Level::TRACE)
            .try_init()
            .expect("observe tracing in this process");
        observe_enrollment_and_refresh().await;
        return;
    }

    // Nextest already signed this binary. Re-exec to capture its complete stdout and stderr.
    let case = format!(
        "{}::enrollment_and_refresh_emit_no_secrets",
        module_path!()
            .split_once("::")
            .expect("integration module path")
            .1
    );
    let child = Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", &case, "--nocapture"])
        .env(CHILD_MARKER, "1")
        .kill_on_drop(true)
        .output()
        .await
        .expect("captured enrollment child");
    let stdout = String::from_utf8_lossy(&child.stdout);
    let stderr = String::from_utf8_lossy(&child.stderr);

    for (name, value) in [
        ("device code", DEVICE_SECRET),
        ("initial access", INITIAL_ACCESS),
        ("initial refresh", INITIAL_REFRESH),
        ("rotated access", ROTATED_ACCESS),
        ("rotated refresh", ROTATED_REFRESH),
    ] {
        assert!(
            !stdout.contains(value) && !stderr.contains(value),
            "{name} was emitted outside a form body"
        );
    }
    assert!(
        child.status.success() && stdout.contains(CHILD_DONE),
        "child did not prove the complete enrollment and rotation"
    );
    assert!(
        stdout.contains(USER_CODE),
        "the public user code must still be shown"
    );
}
