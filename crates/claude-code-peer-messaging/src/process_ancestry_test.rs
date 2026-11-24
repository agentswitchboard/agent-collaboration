use super::{NonZeroU32, ProcessAncestry, ProcessIncarnation, ProcessStart};

fn process(pid: u32, second: i64) -> ProcessIncarnation {
    ProcessIncarnation {
        pid: NonZeroU32::new(pid).unwrap(),
        started: ProcessStart(second * 1_000),
    }
}

#[test]
fn requires_a_strict_descendant_and_matching_endpoint_incarnations() {
    let snapshot = ProcessAncestry::parse(concat!(
        " 10    1 Thu Jan  1 00:00:01 1970\n",
        " 20   10 Thu Jan  1 00:00:02 1970\n",
        " 30   20 Thu Jan  1 00:00:03 1970\n",
        " 40    1 Thu Jan  1 00:00:04 1970\n",
    ))
    .unwrap();
    let adapter = process(10, 1);
    let worker = process(30, 3);
    assert!(snapshot.proves(worker, adapter));
    assert!(!snapshot.proves(process(40, 4), adapter));
    assert!(!snapshot.proves(worker, process(10, 0)));
    assert!(!snapshot.proves(process(30, 2), adapter));
    assert!(!snapshot.proves(adapter, adapter));
}
