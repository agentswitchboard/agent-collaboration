/// Register each contract row as its own isolated integration test.
#[macro_export]
macro_rules! person_approval_cases {
    ($case:ident) => {
        $case!(list_page, Case::ListPage);
        $case!(list_deadline_precedence, Case::ListDeadlinePrecedence);
        $case!(read_request, Case::ReadRequest);
        $case!(read_deadline_precedence, Case::ReadDeadlinePrecedence);
        $case!(read_not_visible, Case::ReadNotVisible);
        $case!(decide_not_visible, Case::DecideNotVisible);
        $case!(decide_timeout, Case::DecideTimeout);
        $case!(decide_cancelled, Case::DecideCancelled);
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
        $case!(once_fresh_factor, factor(AnswerClass::Once, true));
        $case!(once_stale_factor, factor(AnswerClass::Once, false));
        $case!(turn_fresh_factor, factor(AnswerClass::Turn, true));
        $case!(turn_stale_factor, factor(AnswerClass::Turn, false));
        $case!(session_fresh_factor, factor(AnswerClass::Session, true));
        $case!(session_stale_factor, factor(AnswerClass::Session, false));
        $case!(question_fresh_factor, factor(AnswerClass::Question, true));
        $case!(question_stale_factor, factor(AnswerClass::Question, false));
        $case!(
            lasting_fresh_factor,
            factor(AnswerClass::KeptUntilChanged, true)
        );
        $case!(
            lasting_stale_factor,
            factor(AnswerClass::KeptUntilChanged, false)
        );
        $case!(
            unverified_lifetime_fresh_factor,
            factor(AnswerClass::UnverifiedLifetime, true)
        );
        $case!(
            unverified_lifetime_stale_factor,
            factor(AnswerClass::UnverifiedLifetime, false)
        );
        $case!(
            held_start_allow_fresh_factor,
            factor(AnswerClass::HeldStartAllow, true)
        );
        $case!(
            held_start_allow_stale_factor,
            factor(AnswerClass::HeldStartAllow, false)
        );
        $case!(
            held_start_decline_fresh_factor,
            factor(AnswerClass::HeldStartDecline, true)
        );
        $case!(
            held_start_decline_stale_factor,
            factor(AnswerClass::HeldStartDecline, false)
        );
        $case!(
            held_end_allow_fresh_factor,
            factor(AnswerClass::HeldEndAllow, true)
        );
        $case!(
            held_end_allow_stale_factor,
            factor(AnswerClass::HeldEndAllow, false)
        );
        $case!(
            held_end_decline_fresh_factor,
            factor(AnswerClass::HeldEndDecline, true)
        );
        $case!(
            held_end_decline_stale_factor,
            factor(AnswerClass::HeldEndDecline, false)
        );
    };
}
