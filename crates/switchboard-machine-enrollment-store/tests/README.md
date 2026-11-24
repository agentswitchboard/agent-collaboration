# Enrollment store tests

`integration/main.rs` exercises the public enrollment scope contract through the real SQLite worker and a fixture Keychain. It covers empty, active, and ended records, including the row and watch revision after a refusal.

`integration/enrollment_preflight.rs` runs the Sign-in client against a local HTTPS peer. It checks preflight refusals and a first-grant race against a competing install.

`integration/store_lifecycle.rs` checks synchronous request refusal, requested worker exit, and join through the public store API. It also covers lost observation and a retained receipt after a dedicated runtime shuts down.

Run the debug integration target with `cargo nextest run --locked -p switchboard-machine-enrollment-store --features test-support --test integration`. The workspace CI build enables the feature through the link integration suite's dev dependency.
