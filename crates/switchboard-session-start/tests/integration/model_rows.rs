use fleet_identity_protocol::{
    AbandonedSessionCause, BindFleetSessionReply, CreateFleetSessionReply, CreateSessionRefusal,
    EndFleetSessionReply, EndedSessionCause, NewEndSettlement, SessionSettlement,
};
use switchboard_session_start::{
    BindDispute, BindNotTaken, CleanupCall, CleanupOutcome, CloseResult, ConversationFact,
    CreateReplyKind, DirectiveResult, EndLeg, EndNotDue, EndNotSent, Exposure, FailureReason,
    HoldLostDuring, LaunchFact, LaunchNotTaken, LaunchResult, LinkNotTaken, LinkResult,
    NotSentNowReason, OpeningNotCommitted, OpeningOutcome, PointerFact, PutSecretResult, Recording,
    Reservation, SaveResult, SecretFact, SecretIssue, SetUpNotTaken, SetUpResult, SnapshotKind,
    StartAsker, StartDirective, StartFault, StartSnapshot, StartStateKind as State, StopReason,
    UnknownReason,
};

use super::super::fixtures::attempt;
use super::Model;

#[derive(Clone)]
pub(super) enum Destination {
    Failed(FailureReason),
    Fenced,
    Settled,
    Unknown(UnknownReason),
    Opened,
    Stopped(StopReason, SnapshotKind),
}

pub(super) enum Row {
    Issue(State, StartDirective),
    Save(State, StartSnapshot),
    Cleanup(CleanupOutcome, EndLeg),
    WindDown(Destination),
    SaveFailed(Recording, SnapshotKind),
    Idle(State, Option<NotSentNowReason>),
    Finish(Destination),
    WithFault(StartFault, Box<Row>),
}

impl Row {
    pub(super) fn select(model: &mut Model, result: DirectiveResult) -> Option<Self> {
        Some(match (model.state, result) {
            (
                State::SavingCreate
                | State::SavingLaunchIntent
                | State::SavingHeld
                | State::SavingBind
                | State::SavingCleanUp
                | State::SavingOutcome,
                DirectiveResult::Save(result),
            ) => save_row(model, result),
            (State::CreateSent, DirectiveResult::SendCreate(result)) => create_row(model, result),
            (State::StoringSecret, DirectiveResult::PutSecret(result)) => secret_row(model, result),
            (State::Launching, DirectiveResult::Launch(result)) => launch_row(model, result),
            (State::SettingUp, DirectiveResult::SetUp(SetUpResult::Done)) => {
                Self::Save(State::SavingBind, StartSnapshot::BindIntended(model.data()))
            }
            (State::SettingUp, DirectiveResult::SetUp(SetUpResult::Failed)) => {
                cleanup(FailureReason::SetUpFailed, EndLeg::Due)
            }
            (
                State::SettingUp,
                DirectiveResult::SetUp(SetUpResult::NotTaken(SetUpNotTaken::HoldLost)),
            ) => {
                model.facts.conversation = ConversationFact::NotKnown;
                cleanup(
                    FailureReason::HoldLost {
                        during: HoldLostDuring::SetUp,
                    },
                    EndLeg::Due,
                )
            }
            (State::SettingUp, DirectiveResult::SetUp(SetUpResult::OutcomeNotKnown)) => {
                unknown(UnknownReason::SetUp)
            }
            (
                State::SettingUp,
                DirectiveResult::SetUp(SetUpResult::NotTaken(SetUpNotTaken::EnrollmentEnded)),
            ) => fence(),
            (State::BindSent, DirectiveResult::SendBind(result)) => bind_row(model, result),
            (
                State::Opening,
                DirectiveResult::CommitOpening(OpeningOutcome::Committed(revision)),
            ) => {
                model.revision = Some(revision);
                Self::Finish(Destination::Opened)
            }
            (
                State::Opening,
                DirectiveResult::CommitOpening(OpeningOutcome::NotCommitted(
                    OpeningNotCommitted::EnrollmentEnded,
                )),
            ) => fence(),
            (
                State::Opening,
                DirectiveResult::CommitOpening(OpeningOutcome::NotCommitted(
                    OpeningNotCommitted::RevisionMismatch,
                )),
            ) => Self::SaveFailed(Recording::Conflict, SnapshotKind::Opened),
            (
                State::Opening,
                DirectiveResult::CommitOpening(OpeningOutcome::NotCommitted(
                    OpeningNotCommitted::RecordUnavailable,
                )),
            ) => Self::SaveFailed(Recording::NotRecorded, SnapshotKind::Opened),
            (
                State::Opening,
                DirectiveResult::CommitOpening(OpeningOutcome::NotCommitted(
                    OpeningNotCommitted::PreparationFailed,
                )),
            ) => cleanup(FailureReason::OpeningNotPrepared, EndLeg::Due),
            (State::Opening, DirectiveResult::CommitOpening(OpeningOutcome::Uncertain)) => {
                model.recording = Recording::NotKnown;
                Self::Finish(Destination::Unknown(UnknownReason::Opening))
            }
            (State::Ending, DirectiveResult::SendEnd(result)) => end_row(model, result),
            (State::Closing, DirectiveResult::CloseConversation(result)) => {
                model.facts.conversation = match result {
                    CloseResult::Closed => ConversationFact::Closed,
                    CloseResult::CloseFailed => ConversationFact::CloseFailed,
                    CloseResult::OutcomeNotKnown => ConversationFact::NotKnown,
                };
                match model.close_then.take()? {
                    stop @ Destination::Stopped(..) => Self::Finish(stop),
                    other => Self::WindDown(other),
                }
            }
            _ => return None,
        })
    }
}

fn save_row(model: &mut Model, result: SaveResult) -> Row {
    let kind = model.save.as_ref().unwrap().kind();
    let SaveResult::Saved(revision) = result else {
        let recording = match result {
            SaveResult::NotCommitted => Recording::NotRecorded,
            SaveResult::RevisionMismatch => Recording::Conflict,
            SaveResult::OutcomeNotKnown => Recording::NotKnown,
            SaveResult::Saved(_) => panic!("expected write failure"),
        };
        return Row::SaveFailed(recording, kind);
    };
    let saved = model.save.take().unwrap();
    model.revision = Some(revision);
    model.recording = Recording::Recorded;
    model.facts = saved.start().facts.clone();
    match saved {
        StartSnapshot::CreateIntended(_) => Row::Issue(State::CreateSent, model.create()),
        StartSnapshot::LaunchIntended(_) => Row::Issue(State::Launching, StartDirective::Launch),
        StartSnapshot::Held(_) => Row::Issue(State::SettingUp, StartDirective::SetUp),
        StartSnapshot::BindIntended(_) => Row::Issue(State::BindSent, model.bind()),
        StartSnapshot::CreateUnknown(_) => Row::Idle(State::CreateUnknown, None),
        StartSnapshot::BindUnknown(_) => Row::Idle(State::BindUnknown, None),
        StartSnapshot::CleaningUp { outcome, .. } if model.facts.end == EndLeg::Due => {
            model.cleanup = Some(outcome);
            Row::Issue(State::Ending, model.end())
        }
        StartSnapshot::CleaningUp { outcome, .. } => Row::WindDown(cleanup_destination(outcome)),
        StartSnapshot::Failed { reason, .. } => Row::Finish(Destination::Failed(reason)),
        StartSnapshot::Fenced(_) => Row::Finish(Destination::Fenced),
        StartSnapshot::Settled(_) => Row::Finish(Destination::Settled),
        StartSnapshot::Unknown { reason, .. } => Row::Finish(Destination::Unknown(reason)),
        StartSnapshot::Opened { .. } => panic!("opening cannot use Save"),
    }
}

fn create_row(model: &mut Model, result: LinkResult<CreateFleetSessionReply, LinkNotTaken>) -> Row {
    use CreateFleetSessionReply as Reply;
    let has_session_asker = matches!(
        model.envelope.as_ref().unwrap().asker(),
        StartAsker::Session { .. }
    );
    match result {
        LinkResult::Answered(reply) if has_session_asker => match reply {
            Reply::NoSecret {} => fail(FailureReason::NoSecret),
            Reply::Reserved {
                session_id,
                reserved_until,
                ..
            } => {
                model.facts.reservation = Some(Reservation {
                    session_id,
                    reserved_until,
                    attempt: attempt(model.attempt),
                });
                create_fault(model, Some(CreateReplyKind::Reserved))
            }
            impossible => create_fault(model, Some(reply_kind(&impossible))),
        },
        LinkResult::Answered(Reply::Reserved {
            session_id,
            secret,
            reserved_until,
        }) => {
            model.facts.create_exposure = Exposure::Witnessed;
            let issue = SecretIssue {
                enrollment: model.envelope.as_ref().unwrap().scope().enrollment.clone(),
                session_id: session_id.clone(),
                attempt: attempt(model.attempt),
            };
            model.facts.reservation = Some(Reservation {
                session_id,
                reserved_until,
                attempt: issue.attempt,
            });
            Row::Issue(
                State::StoringSecret,
                StartDirective::PutSecret { issue, secret },
            )
        }
        LinkResult::Answered(Reply::Superseded {}) => {
            model.facts.create_exposure = Exposure::Witnessed;
            Row::Save(
                State::SavingOutcome,
                StartSnapshot::CreateUnknown(model.data()),
            )
        }
        LinkResult::AnswerLost => {
            add_exposure(&mut model.facts.create_exposure);
            Row::Save(
                State::SavingOutcome,
                StartSnapshot::CreateUnknown(model.data()),
            )
        }
        LinkResult::NotTaken(LinkNotTaken::LinkNotReady) => {
            Row::Idle(State::CreateUnknown, Some(NotSentNowReason::LinkNotReady))
        }
        LinkResult::Answered(Reply::Refused {
            reason: CreateSessionRefusal::CreatorNotAllowed {},
        }) => fail(FailureReason::CreatorNotAllowed),
        LinkResult::Answered(Reply::Conflict {}) => fail(FailureReason::Conflict),
        LinkResult::Answered(Reply::Active {
            session_id,
            address,
        }) => fail(FailureReason::ActiveWithoutLocalProof {
            session_id,
            address,
        }),
        LinkResult::Answered(Reply::Settled { settlement }) => {
            model.facts.settlement = Some(settlement);
            Row::WindDown(Destination::Settled)
        }
        LinkResult::ProvenNotWritten => fail(FailureReason::CentralFault {
            call: CleanupCall::Create,
        }),
        LinkResult::Answered(bad) => create_fault(model, Some(reply_kind(&bad))),
        LinkResult::Contradicted => create_fault(model, None),
        LinkResult::NotTaken(LinkNotTaken::EnrollmentEnded) => fence(),
        LinkResult::EnrollmentEnded => {
            add_exposure(&mut model.facts.create_exposure);
            fence()
        }
    }
}

fn reply_kind(reply: &CreateFleetSessionReply) -> CreateReplyKind {
    use CreateFleetSessionReply as Reply;
    match reply {
        Reply::Reserved { .. } => CreateReplyKind::Reserved,
        Reply::Superseded {} => CreateReplyKind::Superseded,
        Reply::Active { .. } => CreateReplyKind::Active,
        Reply::Settled { .. } => CreateReplyKind::Settled,
        Reply::Refused { reason } => CreateReplyKind::Refused(reason.clone()),
        Reply::Conflict {} => CreateReplyKind::Conflict,
        Reply::NoSecret {} => CreateReplyKind::NoSecret,
        Reply::NoSuchSession {} => CreateReplyKind::NoSuchSession,
        Reply::SessionEnded {} => CreateReplyKind::SessionEnded,
        Reply::WrongSecret {} => CreateReplyKind::WrongSecret,
    }
}

fn create_fault(model: &mut Model, reply: Option<CreateReplyKind>) -> Row {
    model.facts.create_exposure = Exposure::Witnessed;
    fault(
        StartFault::CreateContradicted {
            reply,
            context: model.fault_context(),
        },
        unknown(UnknownReason::Contradicted),
    )
}

fn secret_row(model: &mut Model, result: PutSecretResult) -> Row {
    let reservation = model.facts.reservation.as_ref().unwrap();
    let issue = SecretIssue {
        enrollment: model.envelope.as_ref().unwrap().scope().enrollment.clone(),
        session_id: reservation.session_id.clone(),
        attempt: reservation.attempt,
    };
    match result {
        PutSecretResult::Stored => {
            model.facts.secret = SecretFact::Stored(issue.clone());
            let mut data = model.data();
            data.facts.pointer = PointerFact::Committed(issue);
            data.facts.launch = LaunchFact::Sent;
            Row::Save(
                State::SavingLaunchIntent,
                StartSnapshot::LaunchIntended(data),
            )
        }
        PutSecretResult::NotStored => cleanup(FailureReason::SecretNotStored, EndLeg::Due),
        PutSecretResult::NotTaken(_) => fence(),
        PutSecretResult::OutcomeNotKnown => {
            model.facts.secret = SecretFact::PossiblyStored(issue);
            cleanup(FailureReason::SecretStoreNotKnown, EndLeg::Due)
        }
    }
}

fn launch_row(model: &mut Model, result: LaunchResult) -> Row {
    if matches!(result, LaunchResult::NotTaken(_)) {
        model.facts.launch = LaunchFact::NotSent;
    }
    match result {
        LaunchResult::Started { harness_session_id } => {
            model.facts.conversation = ConversationFact::Held(Some(harness_session_id));
            Row::Save(State::SavingHeld, StartSnapshot::Held(model.data()))
        }
        LaunchResult::NotTaken(LaunchNotTaken::ReservationElapsed) => {
            model.facts.end = EndLeg::NotDue(EndNotDue::RanOutAtCentral);
            fail(FailureReason::ReservationRanOut)
        }
        LaunchResult::NotTaken(LaunchNotTaken::LinkNotReady) => {
            model.facts.end = EndLeg::NotSent(EndNotSent::LinkNotReady);
            fail(FailureReason::NotLaunched)
        }
        LaunchResult::NotTaken(LaunchNotTaken::EnrollmentEnded) => fence(),
        LaunchResult::FailedNoConversation => cleanup(FailureReason::LaunchFailed, EndLeg::Due),
        LaunchResult::FailedDuplicateId => {
            model.facts.conversation = ConversationFact::Held(None);
            cleanup(FailureReason::DuplicateHarnessId, EndLeg::Due)
        }
        LaunchResult::OutcomeNotKnown => {
            model.facts.conversation = ConversationFact::NotKnown;
            unknown(UnknownReason::Launch)
        }
    }
}

fn bind_row(model: &mut Model, result: LinkResult<BindFleetSessionReply, BindNotTaken>) -> Row {
    use BindFleetSessionReply as Reply;
    match result {
        LinkResult::Answered(Reply::Active { address }) => {
            model.facts.bind_exposure = Exposure::Witnessed;
            Row::Issue(State::Opening, model.open(address))
        }
        LinkResult::AnswerLost => {
            add_exposure(&mut model.facts.bind_exposure);
            Row::Save(
                State::SavingOutcome,
                StartSnapshot::BindUnknown(model.data()),
            )
        }
        LinkResult::NotTaken(BindNotTaken::LinkNotReady) => {
            Row::Idle(State::BindUnknown, Some(NotSentNowReason::LinkNotReady))
        }
        LinkResult::NotTaken(BindNotTaken::SecretUnavailable) => Row::Idle(
            State::BindUnknown,
            Some(NotSentNowReason::SecretUnavailable),
        ),
        LinkResult::NotTaken(BindNotTaken::HoldLost) => {
            model.facts.conversation = ConversationFact::NotKnown;
            let end = if model.facts.bind_exposure == Exposure::None {
                EndLeg::Due
            } else {
                EndLeg::NotDue(EndNotDue::BindMayHaveReached)
            };
            cleanup(
                FailureReason::HoldLost {
                    during: HoldLostDuring::Bind,
                },
                end,
            )
        }
        LinkResult::Contradicted => {
            add_exposure(&mut model.facts.bind_exposure);
            fault(
                StartFault::BindContradicted(model.fault_context()),
                unknown(UnknownReason::Contradicted),
            )
        }
        LinkResult::Answered(Reply::AddressTaken {}) => {
            cleanup(FailureReason::AddressTaken, EndLeg::Due)
        }
        LinkResult::Answered(Reply::WrongSecret {}) => disputed(model, BindDispute::WrongSecret),
        LinkResult::Answered(Reply::AlreadyBoundToDifferentId {}) => {
            disputed(model, BindDispute::AlreadyBoundToDifferentId)
        }
        LinkResult::Answered(Reply::NoSuchStart {}) => cleanup(
            FailureReason::NoSuchStart,
            EndLeg::NotDue(EndNotDue::NoSuchStart),
        ),
        LinkResult::Answered(Reply::Settled { settlement }) => {
            model.facts.settlement = Some(settlement);
            Row::Cleanup(CleanupOutcome::Settled, EndLeg::NotDue(EndNotDue::Settled))
        }
        LinkResult::ProvenNotWritten => cleanup(
            FailureReason::CentralFault {
                call: CleanupCall::Bind,
            },
            EndLeg::NotDue(EndNotDue::CentralFault),
        ),
        LinkResult::NotTaken(BindNotTaken::EnrollmentEnded) => fence(),
        LinkResult::EnrollmentEnded => {
            add_exposure(&mut model.facts.bind_exposure);
            fence()
        }
    }
}

fn disputed(model: &Model, dispute: BindDispute) -> Row {
    fault(
        StartFault::BindAssociationDisputed(dispute, model.fault_context()),
        cleanup(
            FailureReason::AssociationDisputed(dispute),
            EndLeg::NotDue(EndNotDue::AssociationDisputed),
        ),
    )
}

fn end_row(model: &mut Model, result: LinkResult<EndFleetSessionReply, LinkNotTaken>) -> Row {
    use EndFleetSessionReply as Reply;
    model.facts.end = match result {
        LinkResult::Answered(Reply::Settled { settlement }) => {
            let fact = match &settlement {
                NewEndSettlement::Abandoned { time, .. } => SessionSettlement::Abandoned {
                    cause: AbandonedSessionCause::GivenUp {},
                    time: time.clone(),
                },
                NewEndSettlement::Ended { time, .. } => SessionSettlement::Ended {
                    cause: EndedSessionCause::GivenUp {},
                    time: time.clone(),
                },
            };
            model.facts.settlement = Some(fact);
            EndLeg::Settled(settlement)
        }
        LinkResult::Answered(Reply::AlreadySettled { settlement }) => {
            model.facts.settlement = Some(settlement.clone());
            EndLeg::AlreadySettled(settlement)
        }
        LinkResult::Answered(Reply::NoSuchStart {}) => EndLeg::NoSuchStart,
        LinkResult::NotTaken(LinkNotTaken::LinkNotReady) => {
            EndLeg::NotSent(EndNotSent::LinkNotReady)
        }
        LinkResult::AnswerLost => EndLeg::NotKnown,
        LinkResult::ProvenNotWritten => EndLeg::NotSent(EndNotSent::CentralFault),
        LinkResult::NotTaken(LinkNotTaken::EnrollmentEnded) => {
            model.facts.end = EndLeg::NotSent(EndNotSent::EnrollmentEnded);
            return fence();
        }
        LinkResult::EnrollmentEnded => {
            model.facts.end = EndLeg::AuthorityRefused;
            return fence();
        }
        bad @ (LinkResult::Answered(Reply::NotYoursToEnd {}) | LinkResult::Contradicted) => {
            model.facts.end = EndLeg::NotKnown;
            let context = model.fault_context();
            let report = if matches!(bad, LinkResult::Contradicted) {
                StartFault::EndContradicted(context)
            } else {
                StartFault::EndNotYours(context)
            };
            return fault(
                report,
                Row::WindDown(cleanup_destination(model.cleanup.take().unwrap())),
            );
        }
    };
    Row::WindDown(cleanup_destination(model.cleanup.take().unwrap()))
}

fn cleanup_destination(outcome: CleanupOutcome) -> Destination {
    match outcome {
        CleanupOutcome::Failed(reason) => Destination::Failed(reason),
        CleanupOutcome::Settled => Destination::Settled,
    }
}

fn add_exposure(exposure: &mut Exposure) {
    if *exposure == Exposure::None {
        *exposure = Exposure::MayHaveReached;
    }
}

fn fence() -> Row {
    Row::WindDown(Destination::Fenced)
}
fn fail(reason: FailureReason) -> Row {
    Row::WindDown(Destination::Failed(reason))
}
fn unknown(reason: UnknownReason) -> Row {
    Row::WindDown(Destination::Unknown(reason))
}
fn cleanup(reason: FailureReason, end: EndLeg) -> Row {
    Row::Cleanup(CleanupOutcome::Failed(reason), end)
}
fn fault(fault: StartFault, next: Row) -> Row {
    Row::WithFault(fault, Box::new(next))
}
