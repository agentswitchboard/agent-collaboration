use fleet_identity_protocol::{
    BindFleetSessionReply, CreateSessionRefusal, EndFleetSessionReply, SessionSettlement,
};

use super::{AcceptedStart, NOT_EXPECTED, Outputs, Reduced, Stage, WindDown};
use crate::facts::settlement_fact;
use crate::{
    BindContradiction, BindDispute, BindNotTaken, CentralRefusalNow, CheckedActive, CleanupCall,
    CleanupOutcome, ConversationFact, CreateAnswer, CreateContradiction, CreateReplyKind,
    EndContradiction, EndLeg, EndNotApplied, EndNotDue, EndNotSent, EndReplyKind, Exposure,
    FailureReason, HoldLostDuring, LinkNotTaken, LinkResult, NotSentNowReason, ObservedReservation,
    OpeningCommit, PointerFact, ProjectRefusal, Reservation, SecretFact, SecretIssue,
    SnapshotPhase, StartAsker, StartDirective, StartFault, UnknownReason,
};

impl AcceptedStart {
    pub(super) fn create_result(
        &mut self,
        result: LinkResult<CreateAnswer, CreateContradiction>,
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
            LinkResult::RefusedBeforeApply(reason) => {
                self.stage = Stage::CreateUnknown;
                self.tell_refused_by_central(reason, outputs);
                Ok(())
            }
            LinkResult::ProvenNotWritten => self.fail(
                FailureReason::CentralFault {
                    call: CleanupCall::Create,
                },
                outputs,
            ),
            LinkResult::Contradicted(evidence) => {
                if let Some(reservation) = evidence.reservation {
                    self.observe_reservation(reservation);
                }
                self.create_contradicted(evidence.reply, outputs)
            }
            LinkResult::EnrollmentEnded => {
                self.facts.stored.create_exposure =
                    self.facts.create_exposure.max(Exposure::MayHaveReached);
                self.wind_down(WindDown::Fenced, outputs)
            }
        }
    }

    fn create_answered(&mut self, reply: CreateAnswer, outputs: &mut Outputs<'_>) -> Reduced {
        if matches!(self.envelope.asker(), StartAsker::Session { .. }) {
            if reply == CreateAnswer::NoSecret {
                return self.fail(FailureReason::NoSecret, outputs);
            }
            if let CreateAnswer::Reserved {
                session_id,
                reserved_until,
            } = &reply
            {
                self.observe_reservation(ObservedReservation {
                    session_id: session_id.clone(),
                    reserved_until: reserved_until.clone(),
                });
                outputs.destroy_pending_secret(SecretIssue {
                    enrollment: self.envelope.scope().enrollment.clone(),
                    session_id: session_id.clone(),
                    attempt: self.attempt,
                });
            }
            return self.create_contradicted(Some(CreateReplyKind::from(&reply)), outputs);
        }
        match reply {
            CreateAnswer::Reserved {
                session_id,
                reserved_until,
            } => {
                self.facts.stored.create_exposure = Exposure::Witnessed;
                self.observe_reservation(ObservedReservation {
                    session_id: session_id.clone(),
                    reserved_until,
                });
                let issue = SecretIssue {
                    enrollment: self.envelope.scope().enrollment.clone(),
                    session_id,
                    attempt: self.attempt,
                };
                let mut reserved = self.snapshot_facts();
                reserved.facts.stored.secret = SecretFact::PossiblyStored(issue);
                self.save_with_facts(SnapshotPhase::Reserved, reserved, outputs)
            }
            CreateAnswer::Superseded => self.create_unknown(Exposure::Witnessed, outputs),
            CreateAnswer::Refused { reason } => {
                let reason = match reason {
                    CreateSessionRefusal::CreatorNotAllowed {} => {
                        return self.fail(FailureReason::CreatorNotAllowed, outputs);
                    }
                    CreateSessionRefusal::ParentNotValid {} => {
                        let reply_kind = CreateReplyKind::Refused(reason);
                        return self.create_contradicted(Some(reply_kind), outputs);
                    }
                    CreateSessionRefusal::ProjectRequired {} => ProjectRefusal::ProjectRequired,
                    CreateSessionRefusal::ProjectNotInWorkspace {} => {
                        ProjectRefusal::ProjectNotInWorkspace
                    }
                    CreateSessionRefusal::MachineNotAllowed {} => ProjectRefusal::MachineNotAllowed,
                    CreateSessionRefusal::HelperOutsideWorkspace {} => {
                        ProjectRefusal::HelperOutsideWorkspace
                    }
                };
                self.fail(FailureReason::ProjectRefused { reason }, outputs)
            }
            CreateAnswer::Conflict => self.fail(FailureReason::Conflict, outputs),
            CreateAnswer::Active {
                session_id,
                address,
            } => self.fail(
                FailureReason::ActiveWithoutLocalProof {
                    session_id,
                    address,
                },
                outputs,
            ),
            CreateAnswer::Settled { settlement } => {
                self.observe_settlement(settlement);
                self.wind_down(WindDown::Cleanup(CleanupOutcome::Settled), outputs)
            }
            ref contradicted @ (CreateAnswer::NoSecret
            | CreateAnswer::NoSuchSession
            | CreateAnswer::SessionEnded
            | CreateAnswer::WrongSecret) => {
                self.create_contradicted(Some(CreateReplyKind::from(contradicted)), outputs)
            }
        }
    }

    fn observe_reservation(&mut self, reservation: ObservedReservation) {
        if self.facts.reservation.is_none() {
            self.facts.stored.reservation = Some(Reservation {
                session_id: reservation.session_id,
                reserved_until: reservation.reserved_until,
                attempt: self.attempt,
            });
        }
    }

    fn create_unknown(&mut self, exposure: Exposure, outputs: &mut Outputs<'_>) -> Reduced {
        self.facts.stored.create_exposure = self.facts.create_exposure.max(exposure);
        self.save(SnapshotPhase::CreateUnknown, outputs)
    }

    fn create_contradicted(
        &mut self,
        reply: Option<CreateReplyKind>,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        self.facts.stored.create_exposure = Exposure::Witnessed;
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
        let (ConversationFact::Held, Some(harness_session_id), PointerFact::Committed(issue)) = (
            self.facts.conversation,
            &self.facts.harness_session_id,
            &self.facts.pointer,
        ) else {
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
        result: LinkResult<BindFleetSessionReply, BindContradiction>,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::BindSent = self.stage else {
            return Err(NOT_EXPECTED);
        };
        match result {
            LinkResult::Answered(BindFleetSessionReply::Active { address }) => {
                let expected = self.revision.ok_or(NOT_EXPECTED)?;
                self.facts.stored.bind_exposure = Exposure::Witnessed;
                self.facts.stored.active_proof = Some(CheckedActive {
                    address: address.clone(),
                });
                let opening = OpeningCommit::new(&self.envelope, &self.facts, address)
                    .map_err(|_| NOT_EXPECTED)?;
                self.stage = Stage::Opening(opening.association().clone());
                outputs.directive(StartDirective::CommitOpening {
                    expected,
                    opening: Box::new(opening),
                })
            }
            LinkResult::AnswerLost => {
                self.mark_bind_exposure();
                self.save(SnapshotPhase::BindUnknown, outputs)
            }
            LinkResult::RefusedBeforeApply(reason) => {
                self.stage = Stage::BindUnknown;
                self.tell_refused_by_central(reason, outputs);
                Ok(())
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
                self.facts.stored.conversation = ConversationFact::NotKnown;
                let end = if self.facts.bind_exposure == Exposure::None
                    || (self.facts.active_proof.is_some() && self.facts.settlement.is_none())
                {
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
            LinkResult::Contradicted(_) => {
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
        self.facts.stored.bind_exposure = self.facts.bind_exposure.max(Exposure::MayHaveReached);
    }

    fn observe_settlement(&mut self, settlement: SessionSettlement) {
        if self.facts.settlement.is_none() {
            self.facts.stored.settlement = Some(settlement);
        }
    }

    pub(super) fn end_result(
        &mut self,
        result: LinkResult<EndFleetSessionReply, EndContradiction>,
        outputs: &mut Outputs<'_>,
    ) -> Reduced {
        let Stage::Ending(outcome) = &self.stage else {
            return Err(NOT_EXPECTED);
        };
        let destination = WindDown::Cleanup(outcome.clone());
        match result {
            LinkResult::Answered(EndFleetSessionReply::Settled { settlement }) => {
                self.observe_settlement(settlement_fact(&settlement));
                self.facts.stored.end = EndLeg::Settled(settlement);
            }
            LinkResult::Answered(EndFleetSessionReply::AlreadySettled { settlement }) => {
                self.observe_settlement(settlement.clone());
                self.facts.stored.end = EndLeg::AlreadySettled(settlement);
            }
            LinkResult::Answered(EndFleetSessionReply::NoSuchStart {}) => {
                self.facts.stored.end = EndLeg::NoSuchStart
            }
            LinkResult::NotTaken(LinkNotTaken::LinkNotReady) => {
                self.facts.stored.end = EndLeg::NotSent(EndNotSent::LinkNotReady)
            }
            LinkResult::AnswerLost => self.facts.stored.end = EndLeg::NotKnown,
            LinkResult::ProvenNotWritten => {
                self.facts.stored.end = EndLeg::NotApplied(EndNotApplied::ProvenNotWritten)
            }
            LinkResult::RefusedBeforeApply(CentralRefusalNow::AuthorityLapsed) => {
                self.facts.stored.end = EndLeg::NotApplied(EndNotApplied::AuthorityLapsed);
            }
            LinkResult::Contradicted(evidence) => {
                self.facts.stored.end = EndLeg::NotKnown;
                let context = self.fault_context();
                let fault = if evidence.reply == Some(EndReplyKind::NotYoursToEnd) {
                    StartFault::EndNotYours(context)
                } else {
                    StartFault::EndContradicted(context)
                };
                Self::report(fault, outputs);
            }
            LinkResult::Answered(EndFleetSessionReply::NotYoursToEnd {}) => {
                self.facts.stored.end = EndLeg::NotKnown;
                Self::report(StartFault::EndNotYours(self.fault_context()), outputs);
            }
            LinkResult::NotTaken(LinkNotTaken::EnrollmentEnded) => {
                self.facts.stored.end = EndLeg::NotSent(EndNotSent::EnrollmentEnded);
                return self.wind_down(WindDown::Fenced, outputs);
            }
            LinkResult::EnrollmentEnded => {
                self.facts.stored.end = EndLeg::AuthorityRefused;
                return self.wind_down(WindDown::Fenced, outputs);
            }
        }
        self.wind_down(destination, outputs)
    }
}
