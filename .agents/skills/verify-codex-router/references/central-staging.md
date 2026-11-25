# Central staging: G4 / OB7

For central-storage-reset D1, use [D1 central mail](d1-central-mail.md). O5 reset and O6 Cloudflare capacity are staging-only declarations awaiting the Lead's live target/deploy grant. The D1 preparation entry cannot execute them.

## Current entry

The staging notice journey uses the existing CLI and session event stream on a declared, already-running signed debug Host. It supports a local sender and a declared separate-Mac sender handoff through staging Central. The legacy staging entry still refuses with exit 2. Independent D1 routes retain their own scope. No staging access, enrollment, sign-in or deployment authority follows from this recipe.

```sh
node verification/run.ts --staging-message /absolute/private/staging-message.json
```

Create the declaration outside the checkout, mode `0600`, with a fresh `evidence` directory under an existing owner-private directory. Supply:

- `journey` as `idle` or `running`, and the 40-character `source` pin. For `running`, optional `answerLines` chooses the bounded no-tools answer length from 4,000 to 20,000. The default is 8,000.
- `router` and `collaboration`, each with canonical absolute `path`, `sha256`, signing `identifier` and `team`. Use the signed debug artifacts. Pin the approved `root`, loopback `port`, enrolled `machineId` and `hostPid` of that Host.
- `claudeAdapter` with absolute `path` and `sha256`, and its actual `claudeAdapterPid` under that Host. The driver compares only the actual process's expected local Anthropic route and keeps no process arguments or environment in evidence. Use the normal Host launch path with its already authenticated debug-only account. Stop on a login or permission request.
- `sender` and `recipient`, distinct exact SessionRef objects of the form `{endpoint: {serviceId, endpointId}, sessionId}`. Use the real current sender session's identity and an isolated idle managed Claude recipient created under this Host with model `haiku` (Haiku 5.5). Use the existing session create and inspect commands, and a fresh test recipient for each run. A failed or uncertain send is not replayed.

The driver checks the debug process, staging enrollment, debug-account health, the sender and recipient identities, and effective Haiku before input. It attaches before the send and requires continuous coverage of the recipient stream. In the idle case the one notice must appear in one new turn. In the running case a real Claude session streams a bounded numbered answer without tools. The driver sends once the product shows the exact turn producing output and still running. The notice must appear within that exact turn. The sender must read `Seen` for the same request. That status records the notice write.

Use a no-tools hold. The public prompt CLI refuses permission requests ([conversation command](../../../../crates/agent-collaboration/src/conversation_commands.rs#L50)), so a permissioned sleep can cancel the fixture before any message is sent. If the no-tools answer finishes before a complete notice window, retain that run as blocked. A notice that begins a turn after the session becomes idle can be valid delivery under the product contract. It cannot satisfy the running case. A fresh fixture can use a larger bounded answer on the same Haiku model.

After observing the notice, the driver keeps reading sender status through the next minute mark and its publication window. Late status from a separate-Mac operator has a further bounded allowance. The driver requires its own matching sender read before it records a pass.

For a bounded timing measurement, keep `journey: "running"` and set `timing: "notice"` or `timing: "control"`. Keep the same `answerLines` across specimens. A control starts the same answer and sends no message. A notice specimen keeps the normal sender and same-turn checks. Both wait for natural turn completion and record stream-observed start, end, duration, stop reason, and counts of answer updates before and after the notice. Coalesced event updates are observations of the stream, so their count does not measure generated tokens.

The adopted expectations come from the messages design's recipient-state table and the notice slice's scope ruling. The idle-only baseline should reach recipient Mac custody and complete the held turn without the notice in it. Compare the reported custody time to the observed end of that turn so delayed status publication does not shorten the measured window. An unavailable hold, lost stream or wrong target is a setup failure and does not establish the baseline red. Run `running` on the declared old build and on the candidate, and `idle` on the candidate. Retain the private receipts for each. An interrupted test cleans up its own captured hold turn and observer, leaving the Host and fixture sessions to their allocator.

### Separate-Mac sender

After the local positives, allocate a distinct already-enrolled debug Mac, its signed candidate build at the same source pin and its actual sender session. Use the same command with the remote `sender` SessionRef and optional `senderHandoff: { machineId, directory }`. The sender operator qualifies the actual debug Host and sender with product reads and strict signatures. Stash the outcome as `sender-qualified.json` in the declaration's fresh private handoff directory. Its fields are `source`, `machineId`, `sender`, `debugOnly: true`, `enrolledConnected: true`, `routerSha256`, and `cliSha256`.

When the recipient driver is armed, it emits the path to `sender-intent.json`. Relay it through the authorized operator channel. The sender operator uses the exact `to`, `body`, and `requestId` for one signed `agent-collaboration message send --json` from its debug service before `sendBefore`. Relay its CLI response as `<requestId>-send.json` in the private handoff directory. The driver grants four minutes for the relay while still requiring evidence that the send itself met its original deadline. Poll the same signed CLI's `message status --json --request-id` on the sender and relay changing outcomes through `<requestId>-status.json`. Each file is an atomic private publication with `requestId`, `machineId`, `sender`, UTC `observedAt`, process `exit`, and the actual CLI JSON as `output`. Use `null` if the send exit was lost but the exact structured reply survived. Completion still requires sender status with exit zero and the matching Central message ID. Do not retry uncertain sends or substitute a local sender. A missing allocation, timely send or matching sender readback blocks the two-Mac result. This route uses two authenticated operators and needs no SSH access.

## Retained obligations and historical evidence

The sections below preserve the governing obligations and earlier qualification limits. Descriptions of retired wrappers and their inputs are historical; they are not current command contracts. Use the retained source above to determine actual coverage, and report any obligation without a current route as unavailable. This documentation change supplies no new runtime proof or operating authority.


The replacement entry is `scripts.verification.central_staging`, commissioned
by decision 1098. Its four groups are qualified on local workerd with two real Routers;
deployed staging remains unobserved. Adopted sources are board
Verification Design OB7 and §5c–§5d, and one-core Program Design §13. Preparation
and local workerd do not close deployed staging obligations.

## Declaration and execution

Use an owner-private JSON declaration outside the checkout. Checked inputs are
in `scripts/verification/central_staging_models` (retired wrapper). Pin a full source SHA,
actual Router binaries, native reqwest probe and Worker bundle hashes. Private
evidence and runtime directories must be fresh and isolated. Credential fields
carry environment **names**, never values.

The retired command is unavailable; use the current entry and declaration above.

Default preparation resolves no credential, launches nothing, makes no request
and records OB7 unobserved. A `localWorkerd` declaration selects
`--execute-local` only after a local build/runtime grant. It owns an isolated
real workerd, fixture JWKS and two actual model-free Router Hosts. Synthetic
enrollment is local only. Five public SQLite bindings and the four required
settings use production durations (decision 1115).

A `staging` declaration selects `--execute-staging` only after a separate live
target/deploy grant. It pins script, deployed version/source, dedicated HTTPS
origin, Access policy identity, complete alternate-address inventory, owner
token environment, receiver/relay identities and two already-enrolled stopped
fixture installations. No staging enrollment operation exists in this driver.
It copies only declared private stopped fixture roots into owned system-temp
roots and verifies actual installation and peer identities before attaching
credentials. Normal account roots and ports 8787, 8788, 19741, 19742, 19743 and 43127 are excluded.

Deploy cases also pin the exact guarded deploy executable, arguments, working
directory, source/configuration hashes and namespace pin. Private fleet-ops
owns login, first creation, cutover, rollback and inventory. This driver never
calls cf or discovers Cloudflare credentials itself.

## Selected groups

- `access`: owner operations read through the real verifier; actual Router
  reqwest upgrade with A's Access identity and B's machine bearer preserves
  WrongKind; headerless 401/403 maps to Unknown; machine credentials fail on
  admin. Staging additionally checks missing/invalid Access and both session
  and admin paths at every independently inventoried alternate public origin,
  without following redirects. Two actual Router sessions and copied data
  supply the positive for header transit.
- `replication-deploy`: hold an actual Connection-to-Copy dispatch, commit an
  immutable post, observe the hold, deploy/reopen, and require the exact post
  once in the other Router's read copy. Both persisted session IDs must survive;
  exact retry preserves the decision and one feed effect.
- `takeover-recovery`: lose the Copy fence reply during owner takeover, read
  the advanced Copy fence and unchanged acknowledged prefix before deploy,
  replay the same transfer afterward, and require one exact next-epoch grant,
  original prefix, exact answer replay and target peer in owning Copy health.
  Wait for the successor to install the writer, then commit a real post there
  and read its exact bytes and enrolled origin through the former holder’s Copy
  before draining either Router.
- `idle-resume`: retain session IDs and Copy fence, wait the declared interval,
  then replicate a new post through both actual Routers. This proves survival;
  physical eviction still needs an existing platform construction signal.

Locally, the existing owner-gated verifier control receives valid signed bytes
and then a signature-byte forgery. The actual Worker must accept Owner and
return BadSignature respectively. Decision 1115 forbids a new injection route
or claiming this forged assertion passed through deployed Access. The local
Access fixture is a named stand-in. Headerless local HTTP is only an adapter
component fixture; it never proves the deployed edge's behavior.

## Qualification and limits

The retained local qualification at product source `600aadd3` covers all four
groups, clean shutdown, a real process-restart negative control, and a separate
production-bundle smoke. G47’s same-call Health sample correction is in
`600aadd3`, with 20 full parallel `copy_owner_health` runs and a default-feature
compiler control. The Lead retains acceptance authority; see the G4 handback for
exact apparatus source, counts, evidence and artifact hashes.

Run the current TypeScript package checks, retained private namespace guard tests,
and every selected mapped group on the real Worker and both Router binaries. Keep failing
controls at the deciding boundaries; restore sources for the final run. Build
production and conformance separately with the public Worker README commands
and bundle guards. Retain both hashes and the workerd/Miniflare tool root.
Local process reopen does not prove hosted deployment or platform eviction.

No existing object-construction/startup signal has been identified. Report
“eviction not evidenced; state and session survival across the idle wait
observed” unless an existing platform signal establishes construction. Request
invocation IDs, cf-ray and elapsed time cannot establish a new object instance.
No product instrumentation is authorized by this apparatus commission.

Declared address completeness is an input to compare, not a substitute for
collecting and inspecting live routing/settings/version responses. Staging
stays unknown until genuine Access, all alternate addresses, header transit,
actual guarded deploy and owner/read-copy outcomes are observed on the pinned
target. Retain every gap; the driver does not issue delivery acceptance.

## Cleanup

Stop only recorded children and exact fixture sockets; reap workerd before
reopening its persistence and drain both isolated Hosts. Retain evidence and
state for independent readback, then remove only owned task paths when their
consumers release them. Never delete staging scripts, namespaces or credentials
as journey cleanup. Routing rollback belongs to the private operator procedure.
