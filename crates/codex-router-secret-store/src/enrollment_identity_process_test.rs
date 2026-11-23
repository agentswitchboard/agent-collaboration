use std::{
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Child, ChildStdout, Command, Stdio},
};

use tempfile::TempDir;

use crate::file_backend::FileSecretStore;

const CHILD: &str = "enrollment_row_sealer::tests::identity_process::store_identity_child";
const ROOT: &str = "ROUTER_TEST_ENROLLMENT_MARKER_ROOT";
const ID: &str = "ROUTER_TEST_ENROLLMENT_MARKER_CANDIDATE";
const CANDIDATES: [&str; 2] = [
    "019a0000-0000-7000-8000-000000000011",
    "019a0000-0000-7000-8000-000000000022",
];

#[test]
#[ignore = "launched by the parent against its private temporary root"]
fn store_identity_child() {
    let root = std::env::var_os(ROOT).unwrap();
    let store = FileSecretStore::open(Path::new(&root)).unwrap();
    let candidate = std::env::var(ID).unwrap();
    assert_eq!(store.read_store_id_file().unwrap(), None);
    println!("ENROLLMENT_MARKER_READY");
    std::io::stdout().flush().unwrap();
    let mut byte = [0; 1];
    std::io::stdin().read_exact(&mut byte).unwrap();
    assert_eq!(byte, [1]);

    store.publish_enrollment_store_id(&candidate).unwrap();

    println!(
        "ENROLLMENT_MARKER_SEEN={}",
        store.read_store_id_file().unwrap().unwrap()
    );
}

struct MarkerPublisher {
    child: Child,
    stdout: BufReader<ChildStdout>,
}

impl MarkerPublisher {
    fn start(root: &Path, candidate: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD, "--ignored", "--nocapture"])
            .env(ROOT, root)
            .env(ID, candidate)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self { child, stdout }
    }

    fn read_receipt(&mut self, prefix: &str) -> String {
        loop {
            let mut line = String::new();
            assert_ne!(
                self.stdout.read_line(&mut line).unwrap(),
                0,
                "marker child ended before {prefix}"
            );
            if let Some(value) = line.trim().strip_prefix(prefix) {
                return value.to_owned();
            }
        }
    }

    fn publish(&mut self) {
        self.child.stdin.take().unwrap().write_all(&[1]).unwrap();
    }
}

impl Drop for MarkerPublisher {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn concurrent_first_identity_publications_converge_on_one_marker() {
    let root = TempDir::new().unwrap();
    let mut first = MarkerPublisher::start(root.path(), CANDIDATES[0]);
    let mut second = MarkerPublisher::start(root.path(), CANDIDATES[1]);
    first.read_receipt("ENROLLMENT_MARKER_READY");
    second.read_receipt("ENROLLMENT_MARKER_READY");
    first.publish();
    let recorded = first.read_receipt("ENROLLMENT_MARKER_SEEN=");
    second.publish();
    let competing = second.read_receipt("ENROLLMENT_MARKER_SEEN=");

    assert_eq!(
        competing, recorded,
        "two publishers must converge on the winning identity"
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("store-id")).unwrap(),
        recorded
    );
    assert!(first.child.wait().unwrap().success());
    assert!(second.child.wait().unwrap().success());
}
