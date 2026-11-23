# Message custody tests

Sibling `*_test.rs` modules exercise the pure flush and custody policies, including generated event orders checked against independent models.

`integration` exercises message custody through its public API, including committed inbox decisions, receipt settlement, reopening the owning SQLite file, and a store-fault campaign that compares the caller's write view with a scripted Central peer.
