//! Restore receipts, replay publication, and exact held settings.

use super::observations::{dispose, remove_state, run_resources};
use super::row::{DeferredRestore, HeldRestore, RestoreMilestone, RoutedRestore};
use super::{
    Arc, LifecycleRefusal, LifecycleReply, Mutex, OpenAttempt, OpeningAttempts, OpeningEvent,
    OpeningFact, OpeningFailure, OpeningInstance, OpeningPhase, OpeningRow, OpeningSlot,
    OpeningState, OwnerAcpWire, OwnerOpenings, RestoreOpening, RestoreVia, RunRetirement,
    SessionLifecycleFact, SessionLifecyclePort, Transition, lifecycle_transport_refusal, lock,
    opening_failure,
};
use crate::owner_opening_instance::OpeningOffer;
use crate::owner_session_setting::{OwnerSettingWirePlan, RestoreSettingPlan};
use crate::owner_wire::RestoreOffer;
use provider_session_machines::{
    ProviderSettingsCatalog,
    harness_lifecycle::{
        RestoreLoadReadback, RestoreSettingRefusal, RestoredOpening, SettingAttempt, SettingFact,
    },
    session_setting_machine::SettingValueId,
};
use switchboard_harness_contracts::{ReplyAdmission, WorkOfferRefusal};
use tokio::sync::oneshot;

type AppliedRestore = Result<Option<OpeningState>, (LifecycleRefusal, RestoreMilestone)>;

impl OwnerOpenings {
    pub(super) fn open_restore(
        &self,
        opening: RestoreOpening,
        reply: LifecycleReply<OpeningFact>,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        let RestoreOpening {
            run,
            session,
            via,
            setup,
            requested_mode,
        } = opening;
        let identity = OpenAttempt::Restore(run);
        let slot = OpeningSlot::Run(run.generation);
        let instance = OpeningInstance::default();
        let offer = OpeningOffer::default();
        let resources = run_resources(self.owner, run.generation, &self.stopped);
        {
            let mut attempts = lock(&self.attempts);
            if let Some(refusal) = attempts.port_refusal {
                return Err(refusal);
            }
            if self.session.get().is_some_and(|bound| bound != &session.0) {
                return Err(LifecycleRefusal::AttemptMismatch);
            }
            if !attempts.creates.is_empty() || attempts.get(slot).is_some() {
                return Err(self.opening_refusal(&attempts, identity));
            }
            let mut phase = self.idle_phase();
            if phase.advance(OpeningEvent::BindSavedSession) {
                let _bound = self.session.set(session.0.clone());
            }
            if !phase.accepts(OpeningEvent::OpenRestore) {
                return Err(phase.refusal());
            }
            attempts.insert(
                slot,
                OpeningRow {
                    attempt: identity,
                    instance: instance.clone(),
                    state: OpeningState::OfferingRestore {
                        offer: offer.clone(),
                        run: resources.clone(),
                    },
                },
            );
        }
        let receipt = self.wire.offer_restore(
            RestoreOffer {
                route: &resources.route,
                instance: &instance,
                claim: &offer,
            },
            run.attempt,
            session.0,
            via,
            setup,
        );
        #[cfg(feature = "test-observation")]
        self.wire.fence_dispatch_for_test(
            resources.route.run,
            crate::owner_wire::FixtureDispatchPoint::Restore,
        );
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(refusal) => {
                if let Err(reason) = self.finish_unsent_open(slot, &instance) {
                    tracing::debug!(
                        ?run,
                        ?reason,
                        "ACP restore offer ended after its row was retired"
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
                    OpeningState::OfferingRestore { run, .. } => {
                        Ok(Transition::Store(OpeningState::AwaitingRestore(run), ()))
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
                ?run,
                ?reason,
                "ACP had taken the restore before the row was retired"
            );
            return Ok(());
        }
        self.watch_route(slot, instance.clone(), resources.route);
        let restored = RestoreWorker {
            attempts: Arc::clone(&self.attempts),
            wire: Arc::clone(&self.wire),
            slot,
            instance,
        };
        self.tasks.spawn(async move {
            let open = async {
                let mut session = match receipt.await {
                    Ok(Ok(session)) => session,
                    Ok(Err(error)) => return Err(opening_failure(error)),
                    Err(_) => return Err(OpeningFailure::OutcomeUnknown),
                };
                let replay = if via == RestoreVia::Load {
                    Some(
                        session
                            .prepare_replay()
                            .map_err(|_| OpeningFailure::ReplayNotKnown)?,
                    )
                } else {
                    None
                };
                let RestoreSettingPlan {
                    mode,
                    accepted_mode_fallback,
                    fast_off,
                } = session.restore_setting_plan(requested_mode.as_deref());
                let readback = RestoreLoadReadback {
                    catalogue: session.catalog().clone().into_record(),
                    router_auto_requested: requested_mode.as_deref()
                        == Some(crate::AUTO_PERMISSION_MODE),
                    mode,
                    fast_on: fast_off,
                };
                let reported_fallback = accepted_mode_fallback.clone().map(SettingValueId);
                let held = Box::new(HeldRestore {
                    session: Box::new(session),
                    accepted_mode_fallback,
                    skills: replay.as_ref().and_then(|replay| replay.skills.clone()),
                });
                let opening = if let Some(replay) = replay {
                    if !restored.advance(RestoreMilestone::Loaded(held)).await {
                        return Ok(None);
                    }
                    if events
                        .send(SessionLifecycleFact::Replay {
                            run,
                            events: replay.events,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(None);
                    }
                    if !restored.advance(RestoreMilestone::ReplayAdmitted).await {
                        return Ok(None);
                    }
                    RestoredOpening::Loaded {
                        readback,
                        accepted_mode_fallback: reported_fallback,
                        replay: replay.completeness,
                    }
                } else {
                    #[cfg(any(test, feature = "test-observation"))]
                    restored.wire.at_opening_effect_for_test(
                        crate::owner_wire::OpeningPausePoint::ResumeReaderBeforeCommit,
                    );
                    if !restored.advance(RestoreMilestone::Resumed(held)).await {
                        return Ok(None);
                    }
                    RestoredOpening::Resumed {
                        readback,
                        accepted_mode_fallback: reported_fallback,
                    }
                };
                Ok(Some(opening))
            };
            let answer = tokio::select! {
                biased;
                () = resources.stopped.cancelled() => None,
                () = retired => None,
                answer = open => Some(answer),
            };
            match answer {
                Some(Ok(Some(opening))) => reply.send(OpeningFact::Restored { run, opening }),
                Some(Err(failure)) => {
                    if restored.advance(RestoreMilestone::Failed).await {
                        reply.send(OpeningFact::RestoreFailed { run, failure });
                    }
                }
                Some(Ok(None)) | None => {
                    let _ended = restored.advance(RestoreMilestone::WorkerExited).await;
                }
            }
        });
        Ok(())
    }

    pub(in super::super) fn put_setting_back(
        &self,
        attempt: SettingAttempt,
        reserve_reply: &mut dyn FnMut() -> ReplyAdmission<SettingFact>,
    ) -> Result<(), RestoreSettingRefusal> {
        let refusal = RestoreSettingRefusal::Harness;
        self.observe_health().map_err(refusal)?;
        if attempt.setting.change.generation != attempt.restore.generation {
            return Err(refusal(LifecycleRefusal::AttemptMismatch));
        }
        let identity = OpenAttempt::Restore(attempt.restore);
        let (route, fallback) = {
            let attempts = lock(&self.attempts);
            let Some(row) = attempts.targeted(identity) else {
                return Err(refusal(self.opening_refusal(&attempts, identity)));
            };
            let OpeningState::HeldRestore(restore) = &row.state else {
                return Err(refusal(row.phase().refusal()));
            };
            (
                restore.run.route.clone(),
                restore.held.accepted_mode_fallback.clone(),
            )
        };
        let session = self
            .session
            .get()
            .ok_or_else(|| refusal(LifecycleRefusal::RouteUnavailable))?;
        let request = OwnerSettingWirePlan::from_work_catalog(
            session.clone(),
            &ProviderSettingsCatalog::from_record(attempt.catalogue),
            attempt.setting.kind,
            attempt.value.0,
            fallback,
        )
        .map_err(|reason| {
            refusal(match reason {
                WorkOfferRefusal::SettingValueNotOffered => {
                    LifecycleRefusal::SettingValueNotOffered
                }
                WorkOfferRefusal::SettingSetterUnavailable => {
                    LifecycleRefusal::SettingSetterUnavailable
                }
                _ => LifecycleRefusal::Encoding,
            })
        })?;
        let reply = match reserve_reply() {
            ReplyAdmission::Reserved(reply) => reply,
            ReplyAdmission::Refused(reason) => {
                return Err(RestoreSettingRefusal::ReplyReservation(reason));
            }
        };
        self.wire
            .reapply_exact_setting(&route, attempt.restore, attempt.setting, request, reply)
            .map_err(|reason| refusal(lifecycle_transport_refusal(reason)))
    }
}

struct RestoreWorker {
    attempts: Arc<Mutex<OpeningAttempts>>,
    wire: Arc<OwnerAcpWire>,
    slot: OpeningSlot,
    instance: OpeningInstance,
}

enum RestoreProgress {
    Applied(AppliedRestore),
    Deferred,
    Rejected(DeferredRestore),
}

impl RestoreWorker {
    async fn advance(&self, milestone: RestoreMilestone) -> bool {
        let (outcome, deferred) = oneshot::channel();
        let admitted = {
            let mut attempts = lock(&self.attempts);
            match attempts
                .exact(self.slot, &self.instance)
                .map(OpeningRow::phase)
            {
                Some(OpeningPhase::OfferingClose) => {
                    let event = DeferredRestore { milestone, outcome };
                    let result = attempts.apply(
                        self.slot,
                        &self.instance,
                        OpeningEvent::DeferRestore,
                        event,
                        |state, event| match state {
                            OpeningState::OfferingClose {
                                origin,
                                attempt,
                                offer,
                                deferred: None,
                            } => Ok(Transition::Store(
                                OpeningState::OfferingClose {
                                    origin,
                                    attempt,
                                    offer,
                                    deferred: Some(event),
                                },
                                (),
                            )),
                            other => Err((other, event)),
                        },
                    );
                    match result {
                        Ok(()) => RestoreProgress::Deferred,
                        Err((_, event)) => RestoreProgress::Rejected(event),
                    }
                }
                _ => RestoreProgress::Applied(apply_milestone(
                    &mut attempts,
                    self.slot,
                    &self.instance,
                    milestone,
                )),
            }
        };
        match admitted {
            RestoreProgress::Deferred => {
                #[cfg(any(test, feature = "test-observation"))]
                self.wire.at_opening_effect_for_test(
                    crate::owner_wire::OpeningPausePoint::RestoreDeferred,
                );
                deferred.await.unwrap_or(false)
            }
            RestoreProgress::Rejected(event) => {
                drop(event);
                false
            }
            RestoreProgress::Applied(Ok(ended)) => {
                if let Some(ended) = ended {
                    dispose(&self.wire, &self.instance, ended);
                }
                true
            }
            RestoreProgress::Applied(Err(_)) => false,
        }
    }
}

pub(super) fn apply_milestone(
    attempts: &mut OpeningAttempts,
    slot: OpeningSlot,
    instance: &OpeningInstance,
    milestone: RestoreMilestone,
) -> AppliedRestore {
    let phase = attempts.exact(slot, instance).map(OpeningRow::phase);
    let event = match (&milestone, phase) {
        (
            RestoreMilestone::Failed | RestoreMilestone::WorkerExited,
            Some(OpeningPhase::StoppedAwaitingReply),
        ) => OpeningEvent::FinishOnStoppedWriter,
        (
            RestoreMilestone::Loaded(_) | RestoreMilestone::Resumed(_) | RestoreMilestone::Failed,
            Some(OpeningPhase::ClosingRestore),
        ) => {
            // The physical SDK receipt already settled its exact tap claim.
            // Retain the local close and discard only this worker's result.
            return attempts
                .apply(
                    slot,
                    instance,
                    OpeningEvent::LateOpenReply,
                    milestone,
                    |state, milestone| {
                        Ok(Transition::Store(
                            state,
                            (LifecycleRefusal::RouteClosing, milestone),
                        ))
                    },
                )
                .and_then(Err);
        }
        _ => milestone.event(),
    };
    attempts.apply(
        slot,
        instance,
        event,
        milestone,
        |state, milestone| match (state, milestone) {
            (OpeningState::AwaitingRestore(run), RestoreMilestone::Loaded(held)) => {
                Ok(Transition::Store(
                    OpeningState::PublishingReplay(RoutedRestore { run, held }),
                    None,
                ))
            }
            (OpeningState::AwaitingRestore(run), RestoreMilestone::Resumed(held)) => Ok(
                Transition::Store(OpeningState::HeldRestore(RoutedRestore { run, held }), None),
            ),
            (OpeningState::PublishingReplay(restore), RestoreMilestone::ReplayAdmitted) => {
                Ok(Transition::Store(OpeningState::HeldRestore(restore), None))
            }
            (state, RestoreMilestone::Failed | RestoreMilestone::WorkerExited) => {
                remove_state(state, ())
                    .map(|ending| match ending {
                        Transition::Remove(state) => Transition::Remove(Some(state)),
                        Transition::Store(state, effects) => {
                            Transition::Store(state, Some(effects))
                        }
                    })
                    .map_err(|(state, ())| (state, RestoreMilestone::WorkerExited))
            }
            (state, milestone) => Err((state, milestone)),
        },
    )
}
