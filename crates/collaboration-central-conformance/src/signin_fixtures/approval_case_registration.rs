/// Register each contract row as its own isolated integration test.
#[macro_export]
macro_rules! person_approval_cases {
    ($case:ident) => {
        use $crate::signin_fixtures::approval_routes::{
            ClockBoundary, ExpandedCase, RetainedCase, SignedMutation,
        };
        $case!(list_page, Case::ListPage);
        $case!(list_deadline_precedence, Case::ListDeadlinePrecedence);
        $case!(read_request, Case::ReadRequest);
        $case!(
            signed_option_tamper,
            Case::Expanded(ExpandedCase::Binding(SignedMutation::Option))
        );
        $case!(
            signed_request_id_tamper,
            Case::Expanded(ExpandedCase::Binding(SignedMutation::RequestId))
        );
        $case!(
            signed_organization_tamper,
            Case::Expanded(ExpandedCase::Binding(SignedMutation::Organization))
        );
        $case!(
            signed_operation_id_tamper,
            Case::Expanded(ExpandedCase::Binding(SignedMutation::OperationId))
        );
        $case!(
            signed_route_tamper,
            Case::Expanded(ExpandedCase::Binding(SignedMutation::Route))
        );
        $case!(
            signed_filter_tamper,
            Case::Expanded(ExpandedCase::Binding(SignedMutation::Filter))
        );
        $case!(
            signed_cursor_tamper,
            Case::Expanded(ExpandedCase::Binding(SignedMutation::Cursor))
        );
        $case!(
            applying_token_expiry_after_verification,
            Case::Expanded(ExpandedCase::Applying(ClockBoundary::TokenExpiry))
        );
        $case!(
            applying_deadline_crossed_after_verification,
            Case::Expanded(ExpandedCase::Applying(ClockBoundary::DeadlineCutoff))
        );
        $case!(
            sparse_waiting_page_excludes_foreign_rows,
            Case::Expanded(ExpandedCase::SparseWaitingPage)
        );
        $case!(
            other_destination_answer_read_projection,
            Case::Expanded(ExpandedCase::OtherDestinationAnswer)
        );
        $case!(
            approval_decide_with_no_factor,
            Case::Expanded(ExpandedCase::SignedInDecisions)
        );
        $case!(
            retained_saved_decision_missing_ending,
            Case::Retained(RetainedCase::SavedDecisionMissingEnding)
        );
        $case!(
            retained_already_decided_wrong_winner,
            Case::Retained(RetainedCase::AlreadyDecidedWrongWinner)
        );
        $case!(
            retained_already_decided_wrong_time,
            Case::Retained(RetainedCase::AlreadyDecidedWrongTime)
        );
        $case!(
            people_visibility_uses_destination_index,
            Case::Retained(RetainedCase::PeopleVisibilityUsesDestinationIndex)
        );
        $case!(read_deadline_precedence, Case::ReadDeadlinePrecedence);
        $case!(read_not_visible, Case::ReadNotVisible);
        $case!(decide_not_visible, Case::DecideNotVisible);
        $case!(decide_timeout, Case::DecideTimeout);
        $case!(decide_cancelled, Case::DecideCancelled);
        $case!(applying_time_floor, Case::ApplyingTimeFloor);
        $case!(already_decided, Case::AlreadyDecided);
        $case!(invalid_answer, Case::InvalidAnswer);
        $case!(
            operation_conflict,
            Case::Replay(ReplayCase::OperationConflict)
        );
        $case!(exact_replay, Case::Replay(ReplayCase::Exact));
        $case!(
            replay_after_permission_loss,
            Case::Replay(ReplayCase::AfterPermissionLoss)
        );
        $case!(
            missing_proof,
            Case::Authentication(AuthenticationCase::MissingProof)
        );
        $case!(
            bad_proof,
            Case::Authentication(AuthenticationCase::BadProof)
        );
        $case!(replayed_proof, Case::ReplayedProof);
        $case!(bad_address, Case::BadAddress);
        $case!(
            expired_token,
            Case::Authentication(AuthenticationCase::ExpiredToken)
        );
        $case!(
            foreign_organization,
            Case::Authentication(AuthenticationCase::ForeignOrganization)
        );
        $case!(
            missing_permission,
            Case::Admission(AdmissionCase::MissingPermission)
        );
        $case!(
            outdated_floor,
            Case::Admission(AdmissionCase::OutdatedFloor)
        );
        $case!(missing_floor, Case::Admission(AdmissionCase::MissingFloor));
        $case!(watermark, Case::Admission(AdmissionCase::Watermark));
        $case!(
            inactive_person,
            Case::Admission(AdmissionCase::InactivePerson)
        );
    };
}
