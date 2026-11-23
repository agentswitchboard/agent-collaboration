use fleet_identity_protocol::{
    AbandonedSessionCause, BindFleetSessionReply, CreateFleetSessionReply, CreateSessionRefusal,
    EndFleetSessionReply, EndedSessionCause, NewAbandonmentCause, NewEndSettlement, RouterEndCause,
    SessionSettlement,
};

use super::{AcceptedStart, NOT_EXPECTED, Outputs, Reduced, Stage, WindDown};
use crate::{
    BindDispute, BindNotTaken, CleanupCall, CleanupOutcome, ConversationFact, CreateReplyKind,
    EndLeg, EndNotDue, EndNotSent, Exposure, FailureReason, HoldLostDuring, LinkNotTaken,
    LinkResult, NotSentNowReason, OpenedSession, OpeningCommit, PointerFact, Reservation,
    SecretIssue, StartAsker, StartDirective, StartFault, StartSnapshot, UnknownReason,
};

impl AcceptedStart {
    pub(super) fn create_result(
        &mut self,
        result: LinkResult<CreateFleetSessionReply, LinkNotTaken>,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::CreateSent = self.stage else {
            return Err(NOT_EXPECTED);
        };
        match result {
            LinkResult::Answered(reply) => self.create_answered(reply, outputs),
            LinkResult::NotTaken(LinkNotTaken::LinkNotReady) => {
                self.stage = Stage::CreateUnknown;
                self.tell_not_sent(NotSentNowReason::LinkNotReady, outputs);
                Ok(())
            }
            LinkResult::NotTaken(LinkNotTaken::EnrollmentEnded) => {
                self.wind_down(WindDown::Fenced, outputs)
            }
            LinkResult::AnswerLost => self.create_unknown(Exposure::MayHaveReached, outputs),
            LinkResult::ProvenNotWritten => self.fail(
                FailureReason::CentralFault {
                    call: CleanupCall::Create,
                },
                outputs,
            ),
            LinkResult::Contradicted => self.create_contradicted(None, outputs),
            LinkResult::EnrollmentEnded => {
                self.facts.create_exposure =
                    self.facts.create_exposure.max(Exposure::MayHaveReached);
                self.wind_down(WindDown::Fenced, outputs)
            }
        }
    }

    fn create_answered(
        &mut self,
        reply: CreateFleetSessionReply,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        if matches!(self.envelope.asker(), StartAsker::Session { .. }) {
            if reply == (CreateFleetSessionReply::NoSecret {}) {
                return self.fail(FailureReason::NoSecret, outputs);
            }
            if let CreateFleetSessionReply::Reserved {
                session_id,
                reserved_until,
                ..
            } = &reply
            {
                self.facts.reservation = Some(Reservation {
                    session_id: session_id.clone(),
                    reserved_until: reserved_until.clone(),
                    attempt: self.attempt,
                });
            }
            return self.create_contradicted(Some(CreateReplyKind::from(&reply)), outputs);
        }
        match reply {
            CreateFleetSessionReply::Reserved {
                session_id,
                secret,
                reserved_until,
            } => {
                self.facts.create_exposure = Exposure::Witnessed;
                self.facts.reservation = Some(Reservation {
                    session_id: session_id.clone(),
                    reserved_until,
                    attempt: self.attempt,
                });
                let issue = SecretIssue {
                    enrollment: self.envelope.scope().enrollment.clone(),
                    session_id,
                    attempt: self.attempt,
                };
                self.stage = Stage::StoringSecret(issue.clone());
                outputs.directive(StartDirective::PutSecret { issue, secret })
            }
            CreateFleetSessionReply::Superseded {} => {
                self.create_unknown(Exposure::Witnessed, outputs)
            }
            CreateFleetSessionReply::Refused {
                reason: CreateSessionRefusal::CreatorNotAllowed {},
            } => self.fail(FailureReason::CreatorNotAllowed, outputs),
            CreateFleetSessionReply::Conflict {} => self.fail(FailureReason::Conflict, outputs),
            CreateFleetSessionReply::Active {
                session_id,
                address,
            } => self.fail(
                FailureReason::ActiveWithoutLocalProof {
                    session_id,
                    address,
                },
                outputs,
            ),
            CreateFleetSessionReply::Settled { settlement } => {
                self.observe_settlement(settlement);
                self.wind_down(WindDown::Cleanup(CleanupOutcome::Settled), outputs)
            }
            ref contradicted @ (CreateFleetSessionReply::NoSecret {}
            | CreateFleetSessionReply::NoSuchSession {}
            | CreateFleetSessionReply::SessionEnded {}
            | CreateFleetSessionReply::WrongSecret {}
            | CreateFleetSessionReply::Refused {
                reason: CreateSessionRefusal::ParentNotValid {},
            }) => self.create_contradicted(Some(CreateReplyKind::from(contradicted)), outputs),
        }
    }

    fn create_unknown(&mut self, exposure: Exposure, outputs: &mut Outputs<'_>) -> Reduced {
        self.facts.create_exposure = self.facts.create_exposure.max(exposure);
        self.save(StartSnapshot::CreateUnknown(self.snapshot_facts()), outputs)
    }

    fn create_contradicted(
        &mut self,
        reply: Option<CreateReplyKind>,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        self.facts.create_exposure = Exposure::Witnessed;
        Self::report(
            StartFault::CreateContradicted {
                reply,
                context: self.fault_context(),
            },
            outputs,
        );
        self.wind_down(WindDown::Unknown(UnknownReason::Contradicted), outputs)
    }

    pub(super) fn fail(&mut self, reason: FailureReason, outputs: &mut Outputs<'_>) -> Reduced {
        self.wind_down(WindDown::Cleanup(CleanupOutcome::Failed(reason)), outputs)
    }

    pub(super) fn cleanup_failure(
        &mut self,
        reason: FailureReason,
        end: EndLeg,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        self.save_cleanup(CleanupOutcome::Failed(reason), end, outputs)
    }

    pub(super) fn send_bind(&mut self, outputs: &mut Outputs<'_>) -> Reduced {
        let (ConversationFact::Held(Some(harness_session_id)), PointerFact::Committed(issue)) =
            (&self.facts.conversation, &self.facts.pointer)
        else {
            return Err(NOT_EXPECTED);
        };
        self.stage = Stage::BindSent;
        outputs.directive(StartDirective::SendBind {
            session_id: issue.session_id.clone(),
            harness_session_id: harness_session_id.clone(),
            issue: issue.clone(),
        })
    }

    pub(super) fn bind_result(
        &mut self,
        result: LinkResult<BindFleetSessionReply, BindNotTaken>,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::BindSent = self.stage else {
            return Err(NOT_EXPECTED);
        };
        match result {
            LinkResult::Answered(BindFleetSessionReply::Active { address }) => {
                let (
                    ConversationFact::Held(Some(harness_session_id)),
                    PointerFact::Committed(issue),
                ) = (&self.facts.conversation, &self.facts.pointer)
                else {
                    return Err(NOT_EXPECTED);
                };
                let expected = self.revision.ok_or(NOT_EXPECTED)?;
                self.facts.bind_exposure = Exposure::Witnessed;
                let session = OpenedSession {
                    address: address.clone(),
                    session_id: issue.session_id.clone(),
                };
                let opening = OpeningCommit {
                    address,
                    session_id: issue.session_id.clone(),
                    harness_session_id: harness_session_id.clone(),
                    issue: issue.clone(),
                    opened: StartSnapshot::Opened {
                        start: self.snapshot_facts(),
                        session: session.clone(),
                    },
                };
                self.stage = Stage::Opening(session);
                outputs.directive(StartDirective::CommitOpening {
                    expected,
                    opening: Box::new(opening),
                })
            }
            LinkResult::AnswerLost => {
                self.mark_bind_exposure();
                self.save(StartSnapshot::BindUnknown(self.snapshot_facts()), outputs)
            }
            LinkResult::NotTaken(
                why @ (BindNotTaken::LinkNotReady | BindNotTaken::SecretUnavailable),
            ) => {
                self.stage = Stage::BindUnknown;
                let why = if why == BindNotTaken::LinkNotReady {
                    NotSentNowReason::LinkNotReady
                } else {
                    NotSentNowReason::SecretUnavailable
                };
                self.tell_not_sent(why, outputs);
                Ok(())
            }
            LinkResult::NotTaken(BindNotTaken::HoldLost) => {
                self.facts.conversation = ConversationFact::NotKnown;
                let end = if self.facts.bind_exposure == Exposure::None {
                    EndLeg::Due
                } else {
                    EndLeg::NotDue(EndNotDue::BindMayHaveReached)
                };
                self.cleanup_failure(
                    FailureReason::HoldLost {
                        during: HoldLostDuring::Bind,
                    },
                    end,
                    outputs,
                )
            }
            LinkResult::Contradicted => {
                self.mark_bind_exposure();
                Self::report(StartFault::BindContradicted(self.fault_context()), outputs);
                self.wind_down(WindDown::Unknown(UnknownReason::Contradicted), outputs)
            }
            LinkResult::Answered(BindFleetSessionReply::AddressTaken {}) => {
                self.cleanup_failure(FailureReason::AddressTaken, EndLeg::Due, outputs)
            }
            LinkResult::Answered(
                reply @ (BindFleetSessionReply::WrongSecret {}
                | BindFleetSessionReply::AlreadyBoundToDifferentId {}),
            ) => {
                let dispute = if reply == (BindFleetSessionReply::WrongSecret {}) {
                    BindDispute::WrongSecret
                } else {
                    BindDispute::AlreadyBoundToDifferentId
                };
                Self::report(
                    StartFault::BindAssociationDisputed(dispute, self.fault_context()),
                    outputs,
                );
                self.cleanup_failure(
                    FailureReason::AssociationDisputed(dispute),
                    EndLeg::NotDue(EndNotDue::AssociationDisputed),
                    outputs,
                )
            }
            LinkResult::Answered(BindFleetSessionReply::NoSuchStart {}) => self.cleanup_failure(
                FailureReason::NoSuchStart,
                EndLeg::NotDue(EndNotDue::NoSuchStart),
                outputs,
            ),
            LinkResult::Answered(BindFleetSessionReply::Settled { settlement }) => {
                self.observe_settlement(settlement);
                self.save_cleanup(
                    CleanupOutcome::Settled,
                    EndLeg::NotDue(EndNotDue::Settled),
                    outputs,
                )
            }
            LinkResult::ProvenNotWritten => self.cleanup_failure(
                FailureReason::CentralFault {
                    call: CleanupCall::Bind,
                },
                EndLeg::NotDue(EndNotDue::CentralFault),
                outputs,
            ),
            LinkResult::NotTaken(BindNotTaken::EnrollmentEnded) => {
                self.wind_down(WindDown::Fenced, outputs)
            }
            LinkResult::EnrollmentEnded => {
                self.mark_bind_exposure();
                self.wind_down(WindDown::Fenced, outputs)
            }
        }
    }

    fn mark_bind_exposure(&mut self) {
        self.facts.bind_exposure = self.facts.bind_exposure.max(Exposure::MayHaveReached);
    }

    fn observe_settlement(&mut self, settlement: SessionSettlement) {
        if self.facts.settlement.is_none() {
            self.facts.settlement = Some(settlement);
        }
    }

    pub(super) fn end_result(
        &mut self,
        result: LinkResult<EndFleetSessionReply, LinkNotTaken>,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::Ending(outcome) = &self.stage else {
            return Err(NOT_EXPECTED);
        };
        let destination = WindDown::Cleanup(outcome.clone());
        match result {
            LinkResult::Answered(EndFleetSessionReply::Settled { settlement }) => {
                self.observe_settlement(settlement_fact(&settlement));
                self.facts.end = EndLeg::Settled(settlement);
            }
            LinkResult::Answered(EndFleetSessionReply::AlreadySettled { settlement }) => {
                self.observe_settlement(settlement.clone());
                self.facts.end = EndLeg::AlreadySettled(settlement);
            }
            LinkResult::Answered(EndFleetSessionReply::NoSuchStart {}) => {
                self.facts.end = EndLeg::NoSuchStart
            }
            LinkResult::NotTaken(LinkNotTaken::LinkNotReady) => {
                self.facts.end = EndLeg::NotSent(EndNotSent::LinkNotReady)
            }
            LinkResult::AnswerLost => self.facts.end = EndLeg::NotKnown,
            LinkResult::ProvenNotWritten => {
                self.facts.end = EndLeg::NotSent(EndNotSent::CentralFault)
            }
            LinkResult::Contradicted => {
                self.facts.end = EndLeg::NotKnown;
                Self::report(StartFault::EndContradicted(self.fault_context()), outputs);
            }
            LinkResult::Answered(EndFleetSessionReply::NotYoursToEnd {}) => {
                self.facts.end = EndLeg::NotKnown;
                Self::report(StartFault::EndNotYours(self.fault_context()), outputs);
            }
            LinkResult::NotTaken(LinkNotTaken::EnrollmentEnded) => {
                self.facts.end = EndLeg::NotSent(EndNotSent::EnrollmentEnded);
                return self.wind_down(WindDown::Fenced, outputs);
            }
            LinkResult::EnrollmentEnded => {
                self.facts.end = EndLeg::AuthorityRefused;
                return self.wind_down(WindDown::Fenced, outputs);
            }
        }
        self.wind_down(destination, outputs)
    }
}

fn settlement_fact(settlement: &NewEndSettlement) -> SessionSettlement {
    match settlement {
        NewEndSettlement::Abandoned {
            cause: NewAbandonmentCause::GivenUp {},
            time,
        } => SessionSettlement::Abandoned {
            cause: AbandonedSessionCause::GivenUp {},
            time: time.clone(),
        },
        NewEndSettlement::Ended { cause, time } => {
            let cause = match cause {
                RouterEndCause::GivenUp {} => EndedSessionCause::GivenUp {},
                RouterEndCause::EndedForGood {} => EndedSessionCause::EndedForGood {},
            };
            SessionSettlement::Ended {
                cause,
                time: time.clone(),
            }
        }
    }
}
