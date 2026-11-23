# Engine administration routes

Sign-in sends signed POST requests to the private `/organization-admin/v1/` listener. The [canonical intent bytes](engine-admin-intent-vectors.json), [reply bytes](engine-admin-reply-vectors.json), and [generated wire types](central-sign-in.ts) give the caller the exact contract.

| Path suffix | Signed operation | Permission | Fresh second factor |
| --- | --- | --- | --- |
| `engine-health` | `EngineHealth` | `organization:readAudit` | No |
| `engine-retired` | `EngineListRetired` | `organization:readAudit` | No |
| `engine-retry` | `EngineRetry` | `organization:repairWork` | Yes |
| `engine-discard` | `EngineDiscard` | `organization:repairWork` | Yes |

## Decision table

Each row runs as a separate signed integration test on Tokio and Workerd. A row marked “All four” runs once per route. The writes describe the request's own effects. Ordinary due work may run after an engine admin request under the shared engine policy.

| Route | Decision | HTTP and body | Writes | Test |
| --- | --- | --- | --- | --- |
| All four | Wrong method with a valid address | 405, empty | None | `rejects_wrong_method` |
| All four | Missing proof header with a valid address | 401, empty | None | `rejects_missing_proof` |
| All four | Invalid address with both headers | 400, empty | None | `rejects_bad_address` |
| All four | Signed intent fails verification | 401, empty | None | `rejects_wrong_signed_request` |
| All four | Authorization storage is unavailable | 503, empty | None in the test, which faults before admission | `reports_unavailable_authority` |
| All four | Permission absent or person inactive | 200, `notPermitted` | None | `refuses_missing_permission` |
| All four | Token behind the floor or permission policy changed | 200, `refreshRequired` | None | `refuses_old_authorization` |
| All four | Token ahead of the floor, or floor absent | 200, `authorizationSyncRequired` | None | `refuses_ahead_of_authorization_floor` |
| All four | Token, session or authorization snapshot expires after verification | 200, `expired` | None | `refuses_expiry_after_proof_verification` |
| All four | Request time outside the admission window or at or below the watermark | 200, `requestTime` | None | `refuses_proof_at_watermark` |
| All four | Proof marker already consumed | 200, `requestReused` | None | `refuses_consumed_proof` |
| Health | Health snapshot | 200, `engineHealth` | Consume the fresh proof | `shows_health` |
| Health | Applying floor guard misses | 200, `conflict` | None | `changed_admission_guard_conflicts` |
| Retired | Visible retired attempt | 200, `engineRetired` with items | Consume the fresh proof | `returns_visible_attempt` |
| Retired | Bounded scan yields an empty continuation page | 200, `engineRetired` with empty items and a cursor | Consume the fresh proof | `empty_page_continues_from_last_examined_attempt` |
| Retired | Scan reaches its end | 200, `engineRetired` with `next: null` | Consume the fresh proof | `end_has_required_null_cursor` |
| Retired | Applying floor guard misses | 200, `conflict` | None | `changed_admission_guard_conflicts` |
| Retry | Attempt is retired | 200, `engineRetried` | Consume the proof, resolve or create the actor, retain the result, remove the attempt, refresh due state and recompute the alarm | `retries_retired_attempt` and the existing idle-object witness |
| Retry | Attempt missing | 200, `notFound` | Consume the proof, resolve or create the actor and retain the refusal | `refuses_missing_attempt` |
| Retry | Attempt is still failing or its attempt record fails to decode | 200, `notRetired` | Consume the proof, resolve or create the actor and retain the refusal | `refuses_unretired_attempt` |
| Retry | Applying floor or retired-delete guard misses | 200, `notFound` | None for a failed batch | `changed_admission_guard_refuses_not_found` |
| Discard | Retired subject can finish | 200, `engineDiscarded` | Consume the proof, resolve or create the actor, retain the result, finish the subject and remove the attempt and due entry. Recompute the alarm. | `finishes_retired_subject` |
| Discard | Attempt missing | 200, `notFound` | Consume the proof, resolve or create the actor and retain the refusal | `refuses_missing_attempt` |
| Discard | Attempt is still failing or its attempt record fails to decode | 200, `notRetired` | Consume the proof, resolve or create the actor and retain the refusal | `refuses_unretired_attempt` |
| Discard | Subject kind has no registered finisher | 200, `discardUnsupported` | Consume the proof, resolve or create the actor and retain the refusal | `refuses_unsupported_subject` |
| Discard | Subject gone, moved, or superseded | 200, `stale` | Consume the proof, resolve or create the actor and retain the refusal | `refuses_missing_subject_as_stale` |
| Discard | Subject's lifecycle state fails to decode | 200, `subjectUnreadable` | Consume the proof, resolve or create the actor and retain the refusal | `refuses_unreadable_subject` |
| Discard | Applying floor guard misses | 200, `conflict` | None | `changed_admission_guard_conflicts` |
| Retry and discard | Fresh factor absent or too old | 200, `secondFactorRequired` | None | `requires_fresh_second_factor` |
| Retry and discard | Operation ID reused with a different intent or actor | 200, `conflict` | Consume the fresh proof and preserve the first retained reply | `conflicting_operation_keeps_original_answer` |
| Retry and discard | Exact retained operation replay using a fresh proof | 200, original bytes | Consume the fresh proof. Preserve the subject, actor and prior result. | `replays_retained_refusal_with_fresh_proof` and the existing success replay witnesses |

Every typed refusal uses `{"result":"refused","reason":…}`. These routes never generate `nameInUse` or `rejected`. Both are members of the shared admin result type. A 503 can also come from storage or response delivery outside the tested pre-admission fault, so it alone establishes no transaction outcome. The HTTP upload limit can separately return 413 at the transport boundary.

## Paging and time

`after` is an exclusive attempt-ID cursor. Continue until `next` is null, including when `items` is empty. `next` is required on every `engineRetired` reply. Main scans an attempt-ID-ordered range capped at four rows per requested item. The scan counts active and hidden attempts toward its limit. If it has more work, the cursor is the last examined attempt ID, which can be absent from `items`. [Pager](../../fleet-engine/src/machine_event_engine/attempt_administration.rs#L338)

Reply timestamps are UTC RFC 3339 with `Z`. Last-failure and retirement facts written by the engine use millisecond precision. Existing reads accept stored sub-millisecond fractions and forward them as Chrono UTC timestamps. `oldestFirstFailedAt` and `retiredAt` can therefore have zero, three, six, or nine digits. The alarm host APIs read milliseconds, so a present `alarm` has zero or three digits. Chrono's `DateTime` Serde formatter uses `SecondsFormat::AutoSi`, which uses no fraction for an exact second and selects three, six, or nine digits based on the nanosecond value. The vectors include nonzero values for each applicable width. [Attempt writer](../../fleet-engine/src/machine_event_engine/attempt_statements.rs#L83) · [Stored retirement decoder](../../fleet-engine/src/machine_event_engine/attempt_records.rs#L54) · [Health decoder](../../fleet-engine/src/machine_event_engine/attempt_administration.rs#L251) · [Tokio alarm](../../collaboration-central-tokio/src/object_storage.rs#L405) · [Cloudflare alarm](../../collaboration-central-cloudflare/src/workspace_central_object.rs#L136)
