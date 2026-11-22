use super::*;
use std::{
    cell::{Cell, RefCell},
    process::{Command, Stdio},
};

#[test]
fn reaped_root_skips_lookup_and_still_checks_descendants_and_final_inventory() {
    let mut child = Command::new("/usr/bin/true")
        .spawn()
        .expect("spawn disposable child");
    let root = i32::try_from(child.id()).expect("root PID");
    assert!(child.wait().expect("reap disposable child").success());
    let descendant = i32::MAX;
    let identity = |pid| ProcessIdentity {
        pid,
        parent: root,
        uid: i64::from(rustix::process::getuid().as_raw()),
        group: root,
        birth: "fixture".to_owned(),
        executable: "fixture".to_owned(),
    };
    let mut known = BTreeMap::from([(root, identity(root)), (descendant, identity(descendant))]);
    let probes = RefCell::new(Vec::new());
    let inventories = Cell::new(0);
    let cleanup = stop_owned_observing(
        &mut child,
        &mut known,
        &|| {
            inventories.set(inventories.get() + 1);
            Ok(BTreeMap::new())
        },
        &|pid| {
            probes.borrow_mut().push(pid);
            if pid == root {
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into())
            } else {
                Ok(None)
            }
        },
    )
    .expect("observe reaped process");
    assert_eq!(
        *probes.borrow(),
        [descendant],
        "reaped root must never reach per-PID lookup"
    );
    assert_eq!(
        inventories.get(),
        3,
        "pre-signal, settlement, and final inventory"
    );
    assert!(cleanup.remaining.is_empty());
    assert!(cleanup.errors.is_empty());
    assert_eq!(cleanup.exit, Some(0));
}

#[test]
fn initial_signal_identity_failure_survives_successful_fallback_reap() {
    let mut child = Command::new("/bin/cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn disposable child");
    let evidence = tempfile::tempdir().expect("private evidence");
    let mut known = BTreeMap::new();
    let root = i32::try_from(child.id()).expect("exact child PID");
    let lookups = Cell::new(0);
    let outcome = cleanup_owned(&mut child, &mut known, evidence.path(), &|pid| {
        lookups.set(lookups.get() + 1);
        if pid == root && lookups.get() == 1 {
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into())
        } else {
            lookup_identity(pid)
        }
    });
    if child.try_wait().expect("fixture status").is_none() {
        child.kill().expect("kill disposable fixture");
        let _ =
            wait_for_child(&mut child, Duration::from_secs(5)).expect("reap disposable fixture");
    }
    let receipt: serde_json::Value = serde_json::from_slice(
        &std::fs::read(evidence.path().join("host-settlement.json")).expect("cleanup receipt"),
    )
    .expect("valid receipt");
    assert!(
        matches!(
            outcome,
            Err(DriverError::Runtime(
                "owned cleanup incomplete; inspect settlement"
            ))
        ),
        "{outcome:?}, {receipt}"
    );
    assert_eq!(
        receipt["errors"],
        serde_json::json!([{"stage":"initialSignal","pid":root,"error":"PermissionError"}])
    );
    assert_eq!(
        receipt["signals"],
        serde_json::json!([{"pid":root,"signal":15}])
    );
    assert_eq!(receipt["remaining"], serde_json::json!({}));
}
