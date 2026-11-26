//! Restore receipts, replay publication, and exact held settings.

use super::row::{HeldRestore, RestoreMilestone};
use super::{
    Arc, Command, Effect, LifecycleRefusal, LifecycleReply, Mutex, OpenAttempt, OpeningAttempts,
    OpeningFact, OpeningFailure, OpeningInstance, OpeningSlot, OwnerAcpWire, OwnerOpenings,
    RestoreOpening, RestoreVia, RunRetirement, SessionLifecycleFact, SessionLifecyclePort,
    lifecycle_transport_refusal, lock, opening_failure,
};
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
        let slot = OpeningSlot::Run(run.generation);
        let (instance, offer, resources) =
            lock(&self.attempts).begin_restore(self, run, &session)?;
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
        let accepted = lock(&self.attempts).apply(slot, &instance, Command::WriterAccepted);
        match accepted {
            Ok(effect) if effect.faulted() => {
                effect.finish(&self.wire);
                reply.send(OpeningFact::RestoreFailed {
                    run,
                    failure: OpeningFailure::OutcomeUnknown,
                });
                return Ok(());
            }
            Ok(effect) => {
                effect.finish(&self.wire);
            }
            Err(rejected) => {
                tracing::debug!(?run, reason=?rejected.reason, "ACP had taken the restore before the row was retired");
                return Ok(());
            }
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
                    Ok(Err(error)) => {
                        return Err(RestoreWorkerFailure::Provider(opening_failure(error)));
                    }
                    Err(_) => {
                        return Err(RestoreWorkerFailure::Provider(
                            OpeningFailure::OutcomeUnknown,
                        ));
                    }
                };
                let replay = if via == RestoreVia::Load {
                    Some(session.prepare_replay().map_err(|_| {
                        RestoreWorkerFailure::Provider(OpeningFailure::ReplayNotKnown)
                    })?)
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
                    if !restored.advance(RestoreMilestone::Loaded(held)).await? {
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
                    if !restored.advance(RestoreMilestone::ReplayAdmitted).await? {
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
                    if !restored.advance(RestoreMilestone::Resumed(held)).await? {
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
                Some(Err(RestoreWorkerFailure::Provider(failure))) => {
                    if restored
                        .advance(RestoreMilestone::Failed)
                        .await
                        .unwrap_or(true)
                    {
                        reply.send(OpeningFact::RestoreFailed { run, failure });
                    }
                }
                Some(Err(RestoreWorkerFailure::Internal)) => {
                    reply.send(OpeningFact::RestoreFailed {
                        run,
                        failure: OpeningFailure::OutcomeUnknown,
                    });
                }
                Some(Ok(None)) | None => {
                    if restored
                        .advance(RestoreMilestone::WorkerExited)
                        .await
                        .is_err()
                    {
                        reply.send(OpeningFact::RestoreFailed {
                            run,
                            failure: OpeningFailure::OutcomeUnknown,
                        });
                    }
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
            row.held_restore_offer_data()
                .ok_or_else(|| refusal(row.phase().refusal()))?
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

enum RestoreWorkerFailure {
    Provider(OpeningFailure),
    Internal,
}

impl RestoreWorker {
    async fn advance(&self, milestone: RestoreMilestone) -> Result<bool, RestoreWorkerFailure> {
        let (outcome, deferred) = oneshot::channel();
        let admitted = lock(&self.attempts).apply(
            self.slot,
            &self.instance,
            Command::RestoreProgress(milestone, Some(outcome)),
        );
        match admitted {
            Ok(Effect::Deferred) => {
                #[cfg(any(test, feature = "test-observation"))]
                self.wire.at_opening_effect_for_test(
                    crate::owner_wire::OpeningPausePoint::RestoreDeferred,
                );
                Ok(deferred.await.unwrap_or(false))
            }
            Ok(effect) if effect.faulted() => {
                effect.finish(&self.wire);
                Err(RestoreWorkerFailure::Internal)
            }
            Ok(effect) => Ok(effect.finish(&self.wire)),
            Err(_) => Ok(false),
        }
    }
}
