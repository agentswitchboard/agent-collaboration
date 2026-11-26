//! Provisional creation and its separately admitted configuration worker.

use super::row::{HeldCreate, PendingCreate};
use super::{
    Arc, CreateAttempt, HarnessSessionId, LifecycleRefusal, LifecycleReply, Mutex, OpenAttempt,
    OpenSetup, OpeningAttempts, OpeningEvent, OpeningFact, OpeningFailure, OpeningInstance,
    OpeningPhase, OpeningRow, OpeningSlot, OpeningState, OwnerOpenings, RequestedProviderSettings,
    RunRetirement, Transition, lifecycle_transport_refusal, lock, opening_failure,
};
use crate::owner_opening_instance::OpeningOffer;
use crate::owner_session_open::OwnerOpenedSession;
use provider_session_machines::ProviderSettingsCatalog;
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
        let identity = OpenAttempt::Create(attempt);
        let slot = OpeningSlot::Create(attempt);
        let instance = OpeningInstance::default();
        let offer = OpeningOffer::default();
        let stopped = self.stopped.child_token();
        {
            let mut attempts = lock(&self.attempts);
            let phase = self.idle_phase();
            if !attempts.creates.is_empty()
                || !attempts.runs.is_empty()
                || attempts.port_refusal.is_some()
            {
                return Err(self.opening_refusal(&attempts, identity));
            }
            if !phase.accepts(OpeningEvent::OpenCreate) {
                return Err(phase.refusal());
            }
            attempts.insert(
                slot,
                OpeningRow {
                    attempt: identity,
                    instance: instance.clone(),
                    state: OpeningState::OfferingCreate {
                        offer: offer.clone(),
                        pending: PendingCreate {
                            requested: Box::new(requested),
                            stopped: stopped.clone(),
                        },
                    },
                },
            );
        }
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
        let accepted = {
            let mut attempts = lock(&self.attempts);
            attempts.apply(
                slot,
                &instance,
                OpeningEvent::WriterAccepted,
                (),
                |state, ()| match state {
                    OpeningState::OfferingCreate { pending, .. } => {
                        Ok(Transition::Store(OpeningState::AwaitingCreate(pending), ()))
                    }
                    state @ OpeningState::StoppedAwaitingReply(_) => {
                        Ok(Transition::Store(state, ()))
                    }
                    other => Err((other, ())),
                },
            )
        };
        if let Err((reason, ())) = accepted {
            tracing::debug!(
                ?attempt,
                ?reason,
                "ACP had taken the create before the row was retired"
            );
            return Ok(());
        }
        let attempts = Arc::clone(&self.attempts);
        let session_id = Arc::clone(&self.session);
        self.tasks.spawn(async move {
            let result = tokio::select! {
                biased;
                () = stopped.cancelled() => None,
                () = retired => None,
                result = receipt => Some(result),
            };
            let fact = match result {
                Some(Ok(Ok(session))) => {
                    let provider_id = session.provider_id();
                    let result = {
                        let mut attempts = lock(&attempts);
                        attempts.apply(
                            slot,
                            &instance,
                            OpeningEvent::ProviderCreated,
                            Box::new(session),
                            |state, opened| match state {
                                OpeningState::AwaitingCreate(pending)
                                    if session_id.set(provider_id.clone()).is_ok() =>
                                {
                                    Ok(Transition::Store(
                                        OpeningState::HeldCreate(HeldCreate {
                                            session: opened,
                                            requested: pending.requested,
                                        }),
                                        (),
                                    ))
                                }
                                other => Err((other, opened)),
                            },
                        )
                    };
                    if result.is_err() {
                        return;
                    }
                    OpeningFact::Created {
                        attempt,
                        session: HarnessSessionId(provider_id),
                    }
                }
                Some(Ok(Err(error))) => {
                    let remote = if error
                        .coded_wire_failure()
                        .is_some_and(|failure| failure.definitely_no_effect())
                    {
                        CreationEffect::None
                    } else {
                        CreationEffect::Unknown
                    };
                    if !finish_create(&attempts, slot, &instance, OpeningEvent::OpenFailed) {
                        return;
                    }
                    OpeningFact::CreateFailed {
                        attempt,
                        remote,
                        failure: opening_failure(error),
                    }
                }
                Some(Err(_)) => {
                    if !finish_create(&attempts, slot, &instance, OpeningEvent::OpenFailed) {
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
                        finish_create(&attempts, slot, &instance, OpeningEvent::WorkerExited);
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
        self.observe_health()?;
        let slot = OpeningSlot::Create(attempt);
        let stopped = self.stopped.child_token();
        let (instance, held) = {
            let mut attempts = lock(&self.attempts);
            let Some(row) = attempts.get(slot) else {
                return Err(self.opening_refusal(&attempts, OpenAttempt::Create(attempt)));
            };
            let instance = row.instance.clone();
            let held = attempts
                .apply(
                    slot,
                    &instance,
                    OpeningEvent::Configure,
                    (),
                    |state, ()| match state {
                        OpeningState::HeldCreate(held) => Ok(Transition::Store(
                            OpeningState::ConfiguringCreate(stopped.clone()),
                            held,
                        )),
                        other => Err((other, ())),
                    },
                )
                .map_err(|(why, ())| why)?;
            (instance, held)
        };
        let HeldCreate {
            mut session,
            requested,
        } = held;
        let attempts = Arc::clone(&self.attempts);
        let clock = Arc::clone(&self.clock);
        self.tasks.spawn(async move {
            let fact = tokio::select! {
                biased;
                () = stopped.cancelled() => None,
                () = retired => None,
                result = session.configure_lifecycle_create(attempt, &requested, clock.as_ref()) => Some(result),
            };
            match fact {
                Some(fact @ CreatedSetupFact::ClosedAfterInvalidSetting { .. }) => {
                    if finish_create(&attempts, slot, &instance, OpeningEvent::ClosedDuringConfigure) { reply.send(fact); }
                }
                Some(fact) => {
                    let result = {
                        let mut attempts = lock(&attempts);
                        attempts.apply(slot, &instance, OpeningEvent::Configured, session, |state, session| match state {
                            OpeningState::ConfiguringCreate(_) => Ok(Transition::Store(OpeningState::ConfiguredCreate(session), ())),
                            other => Err((other, session)),
                        })
                    };
                    if result.is_ok()
                        || finish_create(&attempts, slot, &instance, OpeningEvent::FinishOnStoppedWriter) {
                        reply.send(fact);
                    }
                }
                None => { let _finished = finish_create(&attempts, slot, &instance, OpeningEvent::WorkerExited); }
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
        let mut attempts = lock(&self.attempts);
        let Some(row) = attempts.targeted(attempt) else {
            return Err(self.opening_refusal(&attempts, attempt));
        };
        let instance = row.instance.clone();
        let slot = OpeningSlot::for_attempt(attempt);
        let catalogue = ProviderSettingsCatalog::from_record(Arc::clone(&accepted.catalogue));
        let event = match attempt {
            OpenAttempt::Create(_) => OpeningEvent::AcceptCreatedSettings,
            OpenAttempt::Restore(_) => OpeningEvent::AcceptSettings,
        };
        attempts
            .apply(slot, &instance, event, (), |state, ()| match state {
                OpeningState::ConfiguredCreate(mut session) => {
                    let report = apply_catalogue(&mut session, &catalogue);
                    Ok(Transition::Store(
                        OpeningState::ConfiguredCreate(session),
                        report,
                    ))
                }
                OpeningState::HeldRestore(mut restore) => {
                    let report = apply_catalogue(&mut restore.held.session, &catalogue);
                    Ok(Transition::Store(
                        OpeningState::PreparedRestore(restore),
                        report,
                    ))
                }
                other => Err((other, ())),
            })
            .map_err(|(why, ())| why)
    }

    pub(super) fn finish_unsent_open(
        &self,
        slot: OpeningSlot,
        instance: &OpeningInstance,
    ) -> Result<(), LifecycleRefusal> {
        let ended = {
            lock(&self.attempts).apply(
                slot,
                instance,
                OpeningEvent::WriterRefused,
                (),
                |state, ()| {
                    state.mark_retired();
                    Ok(Transition::Remove(state))
                },
            )
        }
        .map_err(|(why, ())| why)?;
        super::observations::dispose(self.wire.as_ref(), instance, ended);
        Ok(())
    }
}

fn finish_create(
    attempts: &Mutex<OpeningAttempts>,
    slot: OpeningSlot,
    instance: &OpeningInstance,
    event: OpeningEvent,
) -> bool {
    let result = {
        let mut attempts = lock(attempts);
        let event = if attempts
            .exact(slot, instance)
            .is_some_and(|row| row.phase() == OpeningPhase::StoppedAwaitingReply)
        {
            OpeningEvent::FinishOnStoppedWriter
        } else {
            event
        };
        attempts.apply(slot, instance, event, (), |state, ()| {
            state.mark_retired();
            Ok(Transition::Remove(state))
        })
    };
    result.is_ok()
}

fn apply_catalogue(
    session: &mut OwnerOpenedSession,
    catalogue: &ProviderSettingsCatalog,
) -> SetupReport<CapabilityReport, SessionSettings> {
    session.accept_restore_catalogue(catalogue);
    SetupReport {
        capabilities: session.capabilities().to_session_model(),
        settings: session.settings(),
        observed: crate::owner_session_creation::catalogue_evidence(session.catalog()),
    }
}
