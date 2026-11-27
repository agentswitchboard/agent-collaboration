# Reviewing a codex-router change

This file is for any agent or person reviewing a change to this repository. It
says how to review one and states the review rules. The rules are also the
rulebook for the automated checks in [`review/`](review/README.md): each rule
statement below carries a hidden marker that binds it to its check.

## What review is here

Review checks a change's quality, its fit with the design it claims to follow,
and the repository's rules. Verification, which proves behaviour on the real
target, is a different role and is not covered here.

## How to review a change

1. Read the change, and the design it claims to follow.
2. Run the checks (see [Running the checks](#running-the-checks)).
3. Read each nudge at its line, with the code around it.
4. Decide whether the rule is broken, and say why.
5. Report each finding with its file and line.

Check output names absolute paths on the machine that ran it. Don't paste it
into a public pull request, issue or thread; quote the repository-relative path
and line instead.

## The rules

### Time

Code takes time from one injected clock, created once and passed down (the
`RuntimeClock` in `router-clock`). New code takes every clock read, sleep and
timeout from that clock, including `router_clock::timeout` for a bounded wait;
that is the Time rule in [AGENTS.md](AGENTS.md#time). These checks flag only the
subset a reviewer most needs to see: tests that need real time to pass, and
production code that makes its own clock or times its own state from the system
clock. An example marked "Not flagged by this check" is outside what the check
looks for; it is not an exception to the Time rule.

#### Test files

Check: `review.time.test_file_waits_on_real_time` (R001).

A test file never needs real time to pass: it drives the injected clock or waits on an event instead. <!-- R001 -->

A test that waits for time to move is slow and flaky, and it hides the same
reach for the system clock in the code it tests.

Bad: a test sleeps until an expiry it is checking arrives.

```rust
#[tokio::test]
async fn invite_expires() {
    let invite = registry.issue_invite(Duration::from_secs(2));
    std::thread::sleep(Duration::from_secs(3));
    assert!(registry.redeem(invite).is_err());
}
```

Not flagged by this check: a deadline that only bounds a wait for an event, and
fires only when the test is already failing.

```rust
#[tokio::test]
async fn delivery_is_announced() {
    let mut events = hub.subscribe();
    hub.deliver(message());
    let event = router_clock::timeout(&*clock, WAIT_LIMIT, events.recv()).await;
    assert!(matches!(event, Ok(Some(Event::Delivered { .. }))));
}
```

Not flagged by this check: the test runs on tokio's paused time, so the sleep
advances a virtual clock and takes no real time.

```rust
#[tokio::test(start_paused = true)]
async fn retry_waits_its_backoff() {
    let task = tokio::spawn(retrying_send(peer.clone()));
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(peer.attempts(), 2);
    task.abort();
}
```

#### Tests inside production files

Check: `review.time.inline_test_waits_on_real_time` (R002).

A test inside a production file never needs real time to pass: it drives the injected clock or waits on an event instead. <!-- R002 -->

The rule is R001's, for a `#[test]` function or `#[cfg(test)]` module that lives
in a production source file.

Bad: an inline test sleeps until a lease runs out.

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn lease_lapses() {
        let lease = Lease::grant(Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(80));
        assert!(lease.has_lapsed());
    }
}
```

Not flagged by this check: the time is a fixed fixture value. It builds data, and
nothing waits for it to move.

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn old_records_are_listed_as_stale() {
        let reference = Utc.with_ymd_and_hms(2026, 1, 15, 12, 0, 0).unwrap();
        let record = Record::new_seen_at(reference - chrono::Duration::days(40));
        assert!(record.is_stale_against(reference));
    }
}
```

#### Production code

Check: `review.time.production_decides_on_system_clock` (R003).

Production code never makes its own clock source, and never drives the system's own expiry, lifetime or schedule state from the system clock. <!-- R003 -->

A second clock source splits time in two, and state timed by the system clock
cannot be tested without waiting for it.

Bad: a function in production hands out the current time to its callers.

```rust
pub fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}
```

Bad: an expiry is computed from the system clock.

```rust
let cursor = Cursor { expires_at: Instant::now() + CURSOR_LIFETIME, ..cursor };
```

Not flagged by this check: a timeout that only bounds how long to wait on a
socket. New code takes it from the injected clock with `router_clock::timeout`.

```rust
let reply = tokio::time::timeout(REPLY_WAIT, socket.next()).await;
```

Good: a timestamp from the injected clock that records when something happened.

```rust
row.recorded_at = clock.utc_now();
```

### Host crate

Check: `review.crate.file_within_stated_job` (R004).

The `codex-router-host` crate only builds components and injects them: no session logic, state machine, stored or wire type, or SQL lives in it, and a module is added to it only if it constructs or wires something. <!-- R004 -->

Logic that settles in the Host cannot be reused or tested on its own, and the
crate grows into a second owner of what other crates own. A Host file may pass
configuration through, map errors, apply defaults while assembling, inject ports
and adapters, and start services. A policy, a stored or wire format, a retry or
supervision loop, a migration, a cache or a state machine belongs in the crate
that owns that job.

The check reads the Host crate's source files except `lib.rs` and test files;
read those yourself.

## What a nudge means

A nudge is a reason to look, not a verdict. It points at a line and names the
rule that may be broken there; the reviewer reads the code and decides.

- **Finding:** the check thinks the rule is broken here. Confirm or dismiss it,
  with a reason.
- **Uncertain:** the check could not tell. Read the code and decide.
- **Unavailable:** the check could not evaluate that piece; the script lists each
  one with its place. Read it yourself, or run the command-line equivalent below.

Every rule here has severity `warn`: no finding or uncertain result changes the
script's exit code. Exit 1 means the checks could not run or did not finish,
including a run with pieces it could not evaluate.

## Running the checks

From the repository root, after `pnpm --dir review install --frozen-lockfile`,
with the change's head checked out:

```sh
node review/src/review-checks.ts --base origin/main --head HEAD
```

With no arguments it checks the changes between `git merge-base origin/main HEAD`
and `HEAD`. It checks only the Rust files under `crates/` that the change adds,
copies, modifies or renames, and stops if the checked-out commit or any of those
files differs from the requested head. [`review/README.md`](review/README.md)
describes its output and exit codes.

The script runs the checks through `@sunstack/sdk`. Until the SDK can reach the
model credential from Node, judged checks come back unavailable, and the script
prints the equivalent `sunstack` command for the same files:

```sh
sunstack lint --config review/sunstack.lint.json <the changed files>
```

Judged checks make paid model requests. The cost per pull request is small.

## Adding a rule

A new rule is measured before it is trusted:

1. Write its cases first: code that breaks the rule and code that keeps it.
2. Check the rule against those cases by hand, before any paid run.
3. Have the rule reviewed independently.
4. Run it on a development set, adjust it, then freeze it.
5. Run the frozen rule on a held-out set it has never seen.

[`review/README.md`](review/README.md#adding-a-rule) lists the files a new rule
touches and the order to add them in.
