# Person approval results

This is the current C3 result, HTTP status and writes matrix for Sign-in. It supersedes the November 22 draft `decision-table-person-approvals.md`. The [generated types](central-sign-in.ts), [signed intent vectors](approval-intent-vector.json) and [reply vectors](approval-person-reply-vectors.json) carry the exact wire shapes.

The private Sign-in listener accepts `POST` for `/organization-admin/v1/approval-list`, `/organization-admin/v1/approval-read` and `/organization-admin/v1/approval-decide`. Signed intent tags are `ApprovalList`, `ApprovalRead` and `ApprovalDecide`. List and read require a current `approval:decide` grant. Decide requires that grant for new work and can recover an exact retained reply after grant loss. See the retry row below.

The person websocket uses the same approval transaction and response bodies. In the table, cases named for the socket check the shared outcomes and owning-store writes. The signed route cases check HTTP admission, proof reservations and publication failures.

## Before the C3 reply union

Preflight checks method, authorization and DPoP headers, then organization address. The first failure determines the reply after the existing HTTP upload guard. Verified requests use the existing admin refusal envelope `{"result":"refused","reason":…}` for applying admission failures.

| Condition | HTTP and body | Writes | Shared host cases |
| --- | --- | --- | --- |
| Method other than POST | 405, empty, `Allow: POST` | None | `approval_preflight_refuses_method_then_headers_then_address` |
| Missing or non-text authorization or DPoP header | 401, empty | None | `approval_preflight_refuses_method_then_headers_then_address`, `missing_proof` |
| Missing or invalid organization address or malformed JSON at addressing | 400, empty | None | `approval_preflight_refuses_method_then_headers_then_address`, `bad_address` |
| Token, proof, signed method or route, token organization, or signed intent fails verification | 401, empty | None | `bad_proof`, `expired_token`, `foreign_organization`, `signed_*_tamper` |
| Grant missing, person inactive, or authority rejects the request | 200, admin `notPermitted` | None | `missing_permission`, `inactive_person`, `socket_decide_after_permission_loss_only_replays_the_committed_answer` for an altered operation |
| Token behind the current floor or policy | 200, admin `refreshRequired` | None | `outdated_floor` |
| Authority floor missing or behind the token | 200, admin `authorizationSyncRequired` | None | `missing_floor` |
| Token expires after proof verification and before applying | 200, admin `expired` | None | `applying_token_expiry_after_verification` |
| Proof or request time falls outside the applying window, including the floor's rejection watermark | 200, admin `requestTime` | None | `watermark` |
| DPoP proof marker already consumed | 200, admin `requestReused` | None | `replayed_proof` |
| Store, verifier, authority infrastructure or reply unavailable | 503, empty | Depends on completion. See the publication limit below. | `socket_decide_after_permission_loss_only_replays_the_committed_answer`, `socket_replay_refuses_saved_answer_without_committed_ending`, `socket_replay_refuses_a_changed_winner`, `socket_replay_refuses_a_changed_ending_time` |

The admin envelope also defines `secondFactorRequired` as HTTP 200. That is the existing admin step-up rule. C3 has no second-factor requirement or corresponding reply.

## Admitted C3 replies

Every row below consumes one fresh DPoP proof marker in the applying transaction. The writes column describes the request's own effects. An operation ID is separate from a proof ID. A new proof can retry a retained operation without rerunning its effects.

| Request and result | HTTP | Approval-domain writes and operation log | Shared host cases |
| --- | --- | --- | --- |
| List `page { items, next }` | 200 | Settle any timeout discovered on the page. Advance the approval time floor for a settlement. | `socket_list_pages_at_the_requested_cursor`, `socket_list_excludes_expired_waiting_requests`, `socket_list_excludes_foreign_and_hidden_rows_across_sparse_pages` |
| Read `request { view }` | 200 | Settle a discovered timeout and advance the floor for it. Return the destination's permitted view. | `socket_read_returns_the_addressed_offer`, `socket_read_projects_expiration_at_deadline`, `socket_read_hides_another_givers_answer` |
| Read `notVisible` | 200 | None. Missing and hidden requests share a reply and lookup path. | `socket_read_gives_hidden_and_missing_requests_the_same_answer`, `socket_read_uses_the_destination_index` |
| Decide `savedDecision { decision }` | 200 | Save the ending, advance the floor and record holder delivery debt. Resolve or mint the actor and retain the first result. | `socket_decide_accepts_signed_in_person_without_second_factor`, `tokio_socket_person_decision_delivers_one_ending_to_the_registered_holder`, `workerd_socket_person_decision_delivers_one_ending_to_the_registered_holder` |
| Decide `alreadyDecided { by, at }` | 200 | Resolve or mint the actor and retain the first result. Preserve the decision and its time. | `socket_decide_with_fresh_id_returns_the_actual_earlier_winner` |
| Decide `tooLate` | 200 | Resolve or mint the actor and retain the first result. Settle and advance the floor only if this attempt discovers a timeout. A previously cancelled or timed-out request keeps its ending. | `socket_decide_after_cancellation_returns_too_late`, `socket_decide_at_deadline_returns_too_late`, `applying_deadline_crossed_after_verification` |
| Decide `invalid { reason }` | 200 | Resolve or mint the actor and retain the first result. Preserve the approval. | `socket_decide_rejects_unoffered_option_without_ending_request` |
| Decide `notVisible` | 200 | None. Missing and hidden requests share a reply and lookup path. | `socket_decide_gives_hidden_and_missing_requests_the_same_answer` |
| Decide `operationConflict` | 200 | Preserve the first result. No actor mint or new approval or operation write. | `socket_decide_rejects_changed_answer_under_same_operation_id` |
| Exact retained Decide retry with a fresh proof, with or without the grant | 200, original result | Return the retained response for the same person, operation ID and canonical intent within the 24-hour retention window. Validate any claimed decision against the stored ending. The approval, actor and operation record remain intact. Applying token, authority, and proof checks still run. | `socket_decide_replays_the_exact_first_answer`, `socket_decide_after_permission_loss_only_replays_the_committed_answer` |

List scans 100 records per page in descending request ID order. `next` is required and null at the end. A waiting page can omit settled timeouts and still supply a continuation. Read uses the existing private projection, including the distinction between a person's own answer and another destination's standing. Approval times use UTC RFC 3339 with a `Z` suffix. Live replies emit whole seconds or exactly three fractional digits because approval rows store and check millisecond precision. A read that settles a timeout uses the current effective millisecond applying time for its ending and stores that same value before returning it. The byte vectors show both whole seconds and nonzero milliseconds, including the newly settled read.

## Permission and authorization

| Role receiving the grant | Grant checked by Central | Shared host cases |
| --- | --- | --- |
| Organization owner, as decided by the owner and granted by Sign-in | `approval:decide` on list, read and new decides | `missing_permission`, `socket_decide_accepts_signed_in_person_without_second_factor` |

Every supported answer needs the signed-in person with that grant, including held starts, held endings and lasting choices. Token and proof freshness still apply.

## Publication limit

Native SQLite can commit before alarm publication fails. Its committed holder ending still reaches the delivery owner. The native case `tokio_releases_committed_person_ending_when_alarm_publication_fails` observes delivery before the exact retained retry. Workerd can roll back when its transactional publication fails. Alarm publication parity remains a separate unit. The HTTP response alone does not identify the durable outcome at this boundary. Recover a retained Decide result with a fresh proof of the same operation and intent.
