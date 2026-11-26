//! Provisional creation and its separately admitted configuration worker.

use super::row::{CreateStart, OpenCreateRequest};
use super::{
    Arc, Command, CreateAttempt, Effect, HarnessSessionId, LifecycleRefusal, LifecycleReply, Mutex,
    OpenAttempt, OpenSetup, OpeningAttempts, OpeningFact, OpeningFailure, OpeningInstance,
    OpeningSlot, OwnerAcpWire, OwnerOpenings, RequestedProviderSettings, RunRetirement,
    lifecycle_transport_refusal, lock, opening_failure,
};
use crate::owner_session_open::OpenReceiptFailure;
use provider_session_machines::harness_lifecycle::{
    AcceptedRunSettings, CreatedSetupFact, CreationEffect, SessionCreatedSetupFact, SetupReport,
};
use session_event_model::{CapabilityReport, SessionSettings};

impl OwnerOpenings {
    pub(super) fn open_create(
        &self,
        attempt: CreateAttempt,
        setup: OpenSetup,
        requested: RequestedProviderSettings,
        reply: LifecycleReply<OpeningFact>,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        #[cfg(any(test, feature = "test-observation"))]
        let reply = self.observe_reply(crate::owner_wire::OpeningDisposalKind::OpeningReply, reply);
        let slot = OpeningSlot::Create(attempt);
        let opening = lock(&self.attempts).begin_create(
            self,
            attempt,
            OpenCreateRequest {
                setup,
                requested,
                stopped: self.stopped.child_token(),
            },
        );
        let (instance, effect) = opening.map_err(|rejected| rejected.reason)?;
        let Effect::OfferCreate(start) = require_effect(Ok(effect), &self.wire)? else {
            return Err(LifecycleRefusal::OperationNotOffered);
        };
        let CreateStart {
            setup,
            offer,
            stopped,
        } = *start;
        let receipt = match self
            .wire
            .offer_create(self.owner.incarnation, setup, &offer)
        {
            Ok(receipt) => receipt,
            Err(refusal) => {
                if let Err(reason) = self.finish_unsent_open(slot, &instance) {
                    tracing::debug!(
                        ?attempt,
                        ?reason,
                        "ACP create offer ended after its row was retired"
                    );
                }
                return Err(lifecycle_transport_refusal(refusal));
            }
        };
        let accepted = lock(&self.attempts).apply(slot, &instance, Command::WriterAccepted);
        match accepted {
            Ok(effect) if effect.faulted() => {
                effect.finish(&self.wire);
                reply.send(create_fault(attempt));
                return Ok(());
            }
            Ok(effect) => {
                effect.finish(&self.wire);
            }
            Err(rejected) => {
                tracing::debug!(?attempt, reason=?rejected.reason, "ACP had taken the create before the row was retired");
                return Ok(());
            }
        }
        let attempts = Arc::clone(&self.attempts);
        let session_id = Arc::clone(&self.session);
        let wire = Arc::clone(&self.wire);
        self.tasks.spawn(async move {
            let result = tokio::select! {
                biased;
                () = stopped.cancelled() => None,
                () = retired => None,
                result = receipt => Some(result),
            };
            let fact = match result {
                Some(Ok(Ok(session))) => {
                    #[cfg(any(test, feature = "test-observation"))]
                    let session = {
                        let mut session = session;
                        session.opening_disposal = wire.take_opening_disposal_for_test(
                            crate::owner_wire::OpeningDisposalKind::SdkSession,
                            &attempts,
                        );
                        session
                    };
                    let provider_id = session.provider_id();
                    let accepted = {
                        let mut attempts = lock(&attempts);
                        let result =
                            attempts.apply(slot, &instance, Command::Created(Box::new(session)));
                        if result.as_ref().is_ok_and(|effect| !effect.faulted()) {
                            session_id.get_or_init(|| provider_id.clone());
                        }
                        result
                    };
                    match accepted {
                        Ok(effect) if effect.faulted() => {
                            effect.finish(&wire);
                            create_fault(attempt)
                        }
                        Ok(effect) => {
                            effect.finish(&wire);
                            OpeningFact::Created {
                                attempt,
                                session: HarnessSessionId(provider_id),
                            }
                        }
                        _ => return,
                    }
                }
                Some(Ok(Err(OpenReceiptFailure::Provider(error)))) => {
                    let remote = if error
                        .coded_wire_failure()
                        .is_some_and(|failure| failure.definitely_no_effect())
                    {
                        CreationEffect::None
                    } else {
                        CreationEffect::Unknown
                    };
                    if !finish_create(&attempts, &wire, slot, &instance, Command::OpenFailed) {
                        return;
                    }
                    OpeningFact::CreateFailed {
                        attempt,
                        remote,
                        failure: opening_failure(error),
                    }
                }
                Some(Ok(Err(OpenReceiptFailure::OpenNotSent)) | Err(_)) => {
                    if !finish_create(&attempts, &wire, slot, &instance, Command::OpenFailed) {
                        return;
                    }
                    OpeningFact::CreateFailed {
                        attempt,
                        remote: CreationEffect::Unknown,
                        failure: OpeningFailure::OutcomeUnknown,
                    }
                }
                None => {
                    let _finished =
                        finish_create(&attempts, &wire, slot, &instance, Command::WorkerExited);
                    return;
                }
            };
            reply.send(fact);
        });
        Ok(())
    }

    pub(in super::super) fn configure_created(
        &self,
        attempt: CreateAttempt,
        reply: LifecycleReply<SessionCreatedSetupFact>,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        #[cfg(any(test, feature = "test-observation"))]
        let reply = self.observe_reply(
            crate::owner_wire::OpeningDisposalKind::ConfigureReply,
            reply,
        );
        self.observe_health()?;
        let slot = OpeningSlot::Create(attempt);
        let stopped = self.stopped.child_token();
        let (instance, admitted) = {
            let mut attempts = lock(&self.attempts);
            let Some(row) = attempts.get(slot) else {
                return Err(self.opening_refusal(&attempts, OpenAttempt::Create(attempt)));
            };
            let instance = row.instance().clone();
            let admitted = attempts.apply(slot, &instance, Command::Configure(stopped.clone()));
            (instance, admitted)
        };
        let Effect::Configure {
            mut session,
            requested,
        } = require_effect(admitted, &self.wire)?
        else {
            return Err(LifecycleRefusal::OperationNotOffered);
        };
        let attempts = Arc::clone(&self.attempts);
        let clock = Arc::clone(&self.clock);
        let wire = Arc::clone(&self.wire);
        self.tasks.spawn(async move {
            let fact = tokio::select! {
                biased;
                () = stopped.cancelled() => None,
                () = retired => None,
                result = session.configure_lifecycle_create(attempt, &requested, clock.as_ref()) => Some(result),
            };
            #[cfg(any(test, feature = "test-observation"))]
            wire.at_opening_effect_for_test(crate::owner_wire::OpeningPausePoint::ConfigureReceiptBuffered);
            let (event, fact) = match fact {
                Some(fact @ CreatedSetupFact::ClosedAfterInvalidSetting { .. }) => (Command::ConfigureClosed, Some(fact)),
                Some(fact) => (Command::Configured(session), Some(fact)),
                None => (Command::WorkerExited, None),
            };
            if finish_create(&attempts, &wire, slot, &instance, event) && let Some(fact) = fact {
                reply.send(fact);
            }
        });
        Ok(())
    }

    pub(in super::super) fn accept_settings(
        &self,
        attempt: OpenAttempt,
        accepted: &AcceptedRunSettings,
    ) -> Result<SetupReport<CapabilityReport, SessionSettings>, LifecycleRefusal> {
        self.observe_health()?;
        let accepted = {
            let mut attempts = lock(&self.attempts);
            let Some(row) = attempts.targeted(attempt) else {
                return Err(self.opening_refusal(&attempts, attempt));
            };
            let instance = row.instance().clone();
            attempts.apply(
                OpeningSlot::for_attempt(attempt),
                &instance,
                Command::AcceptSettings(accepted.clone()),
            )
        };
        let Effect::Settings(report) = require_effect(accepted, &self.wire)? else {
            return Err(LifecycleRefusal::OperationNotOffered);
        };
        Ok(*report)
    }

    pub(super) fn finish_unsent_open(
        &self,
        slot: OpeningSlot,
        instance: &OpeningInstance,
    ) -> Result<(), LifecycleRefusal> {
        let ended = lock(&self.attempts).apply(slot, instance, Command::WriterRefused);
        ended
            .map_err(|rejected| rejected.reason)?
            .finish(&self.wire);
        Ok(())
    }
}

fn finish_create(
    attempts: &Mutex<OpeningAttempts>,
    wire: &OwnerAcpWire,
    slot: OpeningSlot,
    instance: &OpeningInstance,
    command: Command,
) -> bool {
    let result = lock(attempts).apply(slot, instance, command);
    result.is_ok_and(|effect| {
        let faulted = effect.faulted();
        effect.finish(wire) || faulted
    })
}

fn create_fault(attempt: CreateAttempt) -> OpeningFact {
    OpeningFact::CreateFailed {
        attempt,
        remote: CreationEffect::Unknown,
        failure: OpeningFailure::OutcomeUnknown,
    }
}

pub(super) fn require_effect(
    result: Result<Effect, super::Rejected>,
    wire: &OwnerAcpWire,
) -> Result<Effect, LifecycleRefusal> {
    let effect = result.map_err(|rejected| rejected.reason)?;
    if effect.faulted() {
        effect.finish(wire);
        return Err(LifecycleRefusal::OperationNotOffered);
    }
    match effect {
        Effect::WithdrawSetters(setters, next) => {
            drop(setters);
            require_effect(Ok(*next), wire)
        }
        other => Ok(other),
    }
}
