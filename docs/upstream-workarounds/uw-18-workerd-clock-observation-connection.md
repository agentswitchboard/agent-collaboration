# UW-18 · Workerd resets a reused clock-observation connection

The bridge IPC test sometimes fails with `read ECONNRESET` on Linux. A packet trace captured a successful clock-observation POST and its complete JSON response. Undici then sent the next `raw-signin-observe` POST on that socket. The live Workerd listener sent a server-side TCP reset on the reused connection. The bridge was ready and no restart was outstanding. Why the listener resets the reused connection remains unverified. [PR #335 Workerd CI failure on Linux x64](https://github.com/agentswitchboard/agent-collaboration/actions/runs/44972510969/job/137055158337)

The Verifier ran the selected test 200 times per pin on Linux ARM64 with Node 26.11.1 and the pinned verification dependencies.

| Pin | Pass | ECONNRESET | Socket EOF | Timeout |
| --- | ---: | ---: | ---: | ---: |
| Main `68bcff9ec` | 141 | 54 | 2 | 3 |
| C3a `a2a389366` | 142 | 57 | 0 | 1 |

The comparison's main and candidate socket traces both show the previous response completing before the reset. The failed request was the first observation for the fixture object ending in `d215`, immediately after the clock-value probe for `d211`. The single-header control passed 20/20 on each pin. The C3a Verifier's receipt set retains `passive-baseline-2.ndjson`, `passive-head-1.ndjson`, and the server RST in `packet-probe-retry/attempt-1.tcp`.

The test helper in [workerd-storage.test.ts](../../verification/targets/workerd-storage.test.ts) sends `Connection: close` on its clock-observation POSTs so two such requests cannot reuse the same HTTP connection. The timeout, assertions and number of requests stay the same.

## Recreate and check removal

In an isolated Linux checkout, omit `Connection: close` from the clock-observation POST helper in `assertBridgeControls`. Use the repository's pinned Node and lockfile. From the repository root, run the exact case in a fresh Node process for each attempt.

```sh
pnpm --dir verification install --frozen-lockfile
node --test --test-name-pattern='^bridge IPC controls use the shared clock and external fresh-store restart$' verification/targets/workerd-storage.test.ts
```

Remove the workaround when the same test passes 200 of 200 attempts on Linux with keep-alive restored, using the existing assertions and timeout.
