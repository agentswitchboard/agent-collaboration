# Enrollment store tests

`integration/main.rs` exercises the public enrollment scope contract through the real SQLite worker and a fixture Keychain. It covers empty, active, and ended records, including the row and watch revision after a refusal.

Run the debug integration target with `cargo nextest run --locked -p switchboard-machine-enrollment-store --features test-support --test integration`. The workspace CI build enables the feature through the link integration suite's dev dependency.
