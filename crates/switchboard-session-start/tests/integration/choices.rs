use fleet_identity_protocol::{
    BindFleetSessionReply, CreateSessionRefusal, EndFleetSessionReply, NewAbandonmentCause,
    NewEndSettlement, RouterEndCause,
};
use switchboard_session_start::{
    BindContradiction, BindNotTaken, CentralRefusalNow, CloseResult, CreateAnswer,
    CreateContradiction, DirectiveResult, EndContradiction, LaunchNotTaken, LaunchResult,
    LinkNotTaken, LinkResult, OpeningNotCommitted, OpeningOutcome, PutSecretNotTaken,
    PutSecretResult, SaveResult, SetUpNotTaken, SetUpResult, StartDirective, StartRevision,
};

use super::fixtures::{abandoned, address, central_id, harness_id, reserved, time};

pub(super) fn results(directive: &StartDirective, revision: StartRevision) -> Vec<DirectiveResult> {
    match directive {
        StartDirective::Save { .. } => [
            SaveResult::Saved(revision),
            SaveResult::NotCommitted,
            SaveResult::RevisionMismatch,
            SaveResult::OutcomeNotKnown,
        ]
        .into_iter()
        .map(DirectiveResult::Save)
        .collect(),
        StartDirective::SendCreate(_) => [
            LinkResult::Answered(reserved()),
            LinkResult::Answered(CreateAnswer::Superseded),
            LinkResult::Answered(CreateAnswer::Active {
                session_id: central_id(),
                address: address(),
            }),
            LinkResult::Answered(CreateAnswer::Settled {
                settlement: abandoned(),
            }),
            LinkResult::Answered(CreateAnswer::Refused {
                reason: CreateSessionRefusal::CreatorNotAllowed {},
            }),
            LinkResult::Answered(CreateAnswer::Refused {
                reason: CreateSessionRefusal::ParentNotValid {},
            }),
            LinkResult::Answered(CreateAnswer::Conflict),
            LinkResult::Answered(CreateAnswer::NoSecret),
            LinkResult::Answered(CreateAnswer::NoSuchSession),
            LinkResult::Answered(CreateAnswer::SessionEnded),
            LinkResult::Answered(CreateAnswer::WrongSecret),
            LinkResult::NotTaken(LinkNotTaken::LinkNotReady),
            LinkResult::NotTaken(LinkNotTaken::EnrollmentEnded),
            LinkResult::AnswerLost,
            LinkResult::ProvenNotWritten,
            LinkResult::Contradicted(CreateContradiction::default()),
            LinkResult::EnrollmentEnded,
            LinkResult::RefusedBeforeApply(CentralRefusalNow::AuthorityLapsed),
        ]
        .into_iter()
        .map(DirectiveResult::SendCreate)
        .collect(),
        StartDirective::PutSecret { .. } => [
            PutSecretResult::Stored,
            PutSecretResult::NotTaken(PutSecretNotTaken::EnrollmentEnded),
            PutSecretResult::NotStored,
            PutSecretResult::OutcomeNotKnown,
            PutSecretResult::HeldWithDifferentValue,
            PutSecretResult::NotTaken(PutSecretNotTaken::PendingSecretMissing),
        ]
        .into_iter()
        .map(DirectiveResult::PutSecret)
        .collect(),
        StartDirective::Launch => [
            LaunchResult::Started {
                harness_session_id: harness_id(),
            },
            LaunchResult::NotTaken(LaunchNotTaken::LinkNotReady),
            LaunchResult::NotTaken(LaunchNotTaken::EnrollmentEnded),
            LaunchResult::NotTaken(LaunchNotTaken::ReservationElapsed),
            LaunchResult::FailedNoConversation,
            LaunchResult::FailedDuplicateId,
            LaunchResult::OutcomeNotKnown,
        ]
        .into_iter()
        .map(DirectiveResult::Launch)
        .collect(),
        StartDirective::SetUp => [
            SetUpResult::Done,
            SetUpResult::NotTaken(SetUpNotTaken::EnrollmentEnded),
            SetUpResult::NotTaken(SetUpNotTaken::HoldLost),
            SetUpResult::Failed,
            SetUpResult::OutcomeNotKnown,
        ]
        .into_iter()
        .map(DirectiveResult::SetUp)
        .collect(),
        StartDirective::SendBind { .. } => [
            LinkResult::Answered(BindFleetSessionReply::Active { address: address() }),
            LinkResult::Answered(BindFleetSessionReply::WrongSecret {}),
            LinkResult::Answered(BindFleetSessionReply::AlreadyBoundToDifferentId {}),
            LinkResult::Answered(BindFleetSessionReply::AddressTaken {}),
            LinkResult::Answered(BindFleetSessionReply::Settled {
                settlement: abandoned(),
            }),
            LinkResult::Answered(BindFleetSessionReply::NoSuchStart {}),
            LinkResult::NotTaken(BindNotTaken::LinkNotReady),
            LinkResult::NotTaken(BindNotTaken::EnrollmentEnded),
            LinkResult::NotTaken(BindNotTaken::SecretUnavailable),
            LinkResult::NotTaken(BindNotTaken::HoldLost),
            LinkResult::AnswerLost,
            LinkResult::ProvenNotWritten,
            LinkResult::Contradicted(BindContradiction::default()),
            LinkResult::EnrollmentEnded,
            LinkResult::RefusedBeforeApply(CentralRefusalNow::AuthorityLapsed),
        ]
        .into_iter()
        .map(DirectiveResult::SendBind)
        .collect(),
        StartDirective::CommitOpening { .. } => [
            OpeningOutcome::Committed(revision),
            OpeningOutcome::NotCommitted(OpeningNotCommitted::EnrollmentEnded),
            OpeningOutcome::NotCommitted(OpeningNotCommitted::RevisionMismatch),
            OpeningOutcome::NotCommitted(OpeningNotCommitted::RecordUnavailable),
            OpeningOutcome::NotCommitted(OpeningNotCommitted::PreparationFailed),
            OpeningOutcome::Uncertain,
            OpeningOutcome::NotCommitted(OpeningNotCommitted::ConnectionRetired),
            OpeningOutcome::NotCommitted(OpeningNotCommitted::HoldLost),
        ]
        .into_iter()
        .map(DirectiveResult::CommitOpening)
        .collect(),
        StartDirective::SendEnd(_) => [
            LinkResult::Answered(EndFleetSessionReply::Settled {
                settlement: NewEndSettlement::Abandoned {
                    cause: NewAbandonmentCause::GivenUp {},
                    time: time(),
                },
            }),
            LinkResult::Answered(EndFleetSessionReply::Settled {
                settlement: NewEndSettlement::Ended {
                    cause: RouterEndCause::GivenUp {},
                    time: time(),
                },
            }),
            LinkResult::Answered(EndFleetSessionReply::AlreadySettled {
                settlement: abandoned(),
            }),
            LinkResult::Answered(EndFleetSessionReply::NoSuchStart {}),
            LinkResult::Answered(EndFleetSessionReply::NotYoursToEnd {}),
            LinkResult::NotTaken(LinkNotTaken::LinkNotReady),
            LinkResult::NotTaken(LinkNotTaken::EnrollmentEnded),
            LinkResult::AnswerLost,
            LinkResult::ProvenNotWritten,
            LinkResult::Contradicted(EndContradiction::default()),
            LinkResult::EnrollmentEnded,
            LinkResult::RefusedBeforeApply(CentralRefusalNow::AuthorityLapsed),
        ]
        .into_iter()
        .map(DirectiveResult::SendEnd)
        .collect(),
        StartDirective::CloseConversation => [
            CloseResult::Closed,
            CloseResult::CloseFailed,
            CloseResult::OutcomeNotKnown,
        ]
        .into_iter()
        .map(DirectiveResult::CloseConversation)
        .collect(),
    }
}
