/// Instantiates each route and expected outcome as a separately named host test.
#[macro_export]
macro_rules! engine_admin_decision_rows {
    ($declare_route:ident) => {
        $declare_route!(health, Health, [
            shows_health => HealthSnapshot,
            changed_admission_guard_conflicts => GuardChanged,
        ]);
        $declare_route!(retired, Retired, [
            returns_visible_attempt => RetiredPage,
            empty_page_continues_from_last_examined_attempt => RetiredEmptyContinuation,
            end_has_required_null_cursor => RetiredEnd,
            changed_admission_guard_conflicts => GuardChanged,
        ]);
        $declare_route!(retry, Retry, [
            retries_retired_attempt => Retried,
            refuses_missing_attempt => NotFound,
            refuses_unretired_attempt => NotRetired,
            requires_fresh_second_factor => SecondFactorRequired,
            changed_admission_guard_refuses_not_found => GuardChanged,
            conflicting_operation_keeps_original_answer => OperationConflict,
            replays_retained_refusal_with_fresh_proof => RetainedReplay,
        ]);
        $declare_route!(discard, Discard, [
            finishes_retired_subject => Discarded,
            refuses_missing_attempt => NotFound,
            refuses_unretired_attempt => NotRetired,
            refuses_unsupported_subject => DiscardUnsupported,
            refuses_missing_subject_as_stale => Stale,
            refuses_unreadable_subject => SubjectUnreadable,
            requires_fresh_second_factor => SecondFactorRequired,
            changed_admission_guard_conflicts => GuardChanged,
            conflicting_operation_keeps_original_answer => OperationConflict,
            replays_retained_refusal_with_fresh_proof => RetainedReplay,
        ]);
    };
}

/// These common outcomes remain observable on every engine route.
#[macro_export]
macro_rules! engine_admin_shared_decision_rows {
    ($declare_case:ident, $route:ident) => {
        $declare_case!($route, refuses_missing_permission, NotPermitted);
        $declare_case!($route, refuses_old_authorization, RefreshRequired);
        $declare_case!(
            $route,
            refuses_ahead_of_authorization_floor,
            AuthorizationSyncRequired
        );
        $declare_case!($route, refuses_expiry_after_proof_verification, Expired);
        $declare_case!($route, refuses_proof_at_watermark, RequestTime);
        $declare_case!($route, refuses_consumed_proof, RequestReused);
        $declare_case!($route, rejects_wrong_method, WrongMethod);
        $declare_case!($route, rejects_missing_proof, MissingProof);
        $declare_case!($route, rejects_bad_address, BadAddress);
        $declare_case!($route, rejects_wrong_signed_request, InvalidIntent);
        $declare_case!($route, reports_unavailable_authority, Unavailable);
    };
}

/// Additional transport, applying, receipt and paging witnesses from review.
#[macro_export]
macro_rules! engine_admin_boundary_rows {
    ($declare:ident, Health) => {
        $declare!(
            Health,
            refuses_oversized_valid_address_upload,
            OversizedUpload
        );
        $declare!(Health, lost_read_reply_commits_only_proof, AfterCommit);
    };
    ($declare:ident, Retired) => {
        $declare!(
            Retired,
            refuses_oversized_valid_address_upload,
            OversizedUpload
        );
        $declare!(Retired, lost_read_reply_commits_only_proof, AfterCommit);
        $declare!(
            Retired,
            full_hidden_scan_gets_cursor_then_explicit_end,
            FullHiddenScan
        );
        $declare!(
            Retired,
            invalid_stored_attempt_state_is_a_storage_error,
            InvalidAttemptState
        );
    };
    ($declare:ident, Retry) => {
        $declare!(
            Retry,
            refuses_oversized_valid_address_upload,
            OversizedUpload
        );
        $declare!(
            Retry,
            lost_before_apply_recovers_with_same_operation,
            BeforeApply
        );
        $declare!(Retry, lost_after_commit_replays_same_operation, AfterCommit);
        $declare!(
            Retry,
            retired_delete_guard_miss_writes_nothing,
            RetiredDeleteGuard
        );
        $declare!(
            Retry,
            retained_just_inside_cutoff_preserves_ledger,
            InsideReceiptCutoff
        );
        $declare!(
            Retry,
            exact_cutoff_replaces_and_prunes_receipts,
            AtReceiptCutoff
        );
        $declare!(Retry, same_intent_by_other_actor_conflicts, DifferentActor);
    };
    ($declare:ident, Discard) => {
        $declare!(
            Discard,
            refuses_oversized_valid_address_upload,
            OversizedUpload
        );
        $declare!(
            Discard,
            lost_before_apply_recovers_with_same_operation,
            BeforeApply
        );
        $declare!(
            Discard,
            lost_after_commit_replays_same_operation,
            AfterCommit
        );
        $declare!(
            Discard,
            retained_just_inside_cutoff_preserves_ledger,
            InsideReceiptCutoff
        );
        $declare!(
            Discard,
            exact_cutoff_replaces_and_prunes_receipts,
            AtReceiptCutoff
        );
        $declare!(
            Discard,
            same_intent_by_other_actor_conflicts,
            DifferentActor
        );
    };
}
