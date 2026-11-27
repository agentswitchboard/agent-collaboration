//! Restore receipts, replay publication, and exact held settings.

use super::adapter::RestoreSignal;
use super::{
    Arc, Command, Effect, LifecycleRefusal, LifecycleReply, Mutex, OpeningAttempts, OpeningFact,
    OpeningFailure, OpeningInstance, OpeningSlot, OwnerAcpWire, OwnerOpenings, RestoreOpening,
    RestoreVia, RunRetirement, SessionLifecycleFact, SessionLifecyclePort,
    lifecycle_transport_refusal, lock, opening_failure,
};
use super::{RestoreDecision, RestoreMilestone, RestoreStart};
use crate::owner_session_open::OpenReceiptFailure;
use crate::owner_session_setting::RestoreSettingPlan;
use crate::owner_wire::RestoreOffer;
use provider_session_machines::{
    harness_lifecycle::{RestoreLoadReadback, RestoredOpening},
    session_setting_machine::SettingValueId,
};
use tokio::sync::oneshot;

impl OwnerOpenings {
    pub(super) fn open_restore(
        &self,
        opening: RestoreOpening,
        reply: LifecycleReply<OpeningFact>,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        #[cfg(any(test, feature = "test-observation"))]
        let reply = self.observe_reply(crate::owner_wire::OpeningDisposalKind::OpeningReply, reply);
        let slot = OpeningSlot::Run(opening.run.generation);
        let admitted = {
            let mut attempts = lock(&self.attempts);
            let admitted = attempts.begin_restore(
                self.session.get().map(String::as_str),
                self.owner,
                opening,
                self.stopped.child_token(),
            );
            if let Ok((_, Effect::OfferRestore(start))) = &admitted {
                self.session.get_or_init(|| start.request.session.0.clone());
            }
            admitted
        };
        let (instance, effect) = admitted.map_err(|rejected| rejected.reason)?;
        let Effect::OfferRestore(start) = super::create::require_effect(Ok(effect), &self.wire)?
        else {
            return Err(LifecycleRefusal::OperationNotOffered);
        };
        let RestoreStart {
            request:
                RestoreOpening {
                    run,
                    session,
                    via,
                    setup,
                    requested_mode,
                },
            resources,
            offer,
        } = *start;
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
                effect.finish(self.wire.as_ref());
                reply.send(OpeningFact::RestoreFailed {
                    run,
                    failure: OpeningFailure::OutcomeUnknown,
                });
                return Ok(());
            }
            Ok(effect) => {
                effect.finish(self.wire.as_ref());
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
                    Ok(Err(OpenReceiptFailure::Provider(error))) => {
                        return Err(RestoreWorkerFailure::Provider(opening_failure(error)));
                    }
                    Ok(Err(OpenReceiptFailure::OpenNotSent)) | Err(_) => {
                        return Err(RestoreWorkerFailure::Provider(
                            OpeningFailure::OutcomeUnknown,
                        ));
                    }
                };
                #[cfg(any(test, feature = "test-observation"))]
                restored.wire.at_opening_effect_for_test(
                    crate::owner_wire::OpeningPausePoint::RestoreReceiptBuffered,
                );
                #[cfg(any(test, feature = "test-observation"))]
                {
                    session.opening_disposal = restored.wire.take_opening_disposal_for_test(
                        crate::owner_wire::OpeningDisposalKind::SdkSession,
                        &restored.attempts,
                    );
                }
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
                let held = RestoreMilestone::held(
                    session,
                    accepted_mode_fallback,
                    replay.as_ref().and_then(|replay| replay.skills.clone()),
                    replay.is_some(),
                );
                let opening = if let Some(replay) = replay {
                    if !restored.advance(held).await? {
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
                        let _released = restored.advance(RestoreMilestone::ReplayRejected).await?;
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
                    if !restored.advance(held).await? {
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
                answer = open => Some(answer),
                () = resources.stopped.cancelled() => None,
                () = retired => None,
            };
            let failure = match answer {
                Some(Ok(Some(opening))) => {
                    reply.send(OpeningFact::Restored { run, opening });
                    return;
                }
                Some(Err(RestoreWorkerFailure::Provider(failure))) => {
                    match restored.advance(RestoreMilestone::Failed).await {
                        Ok(true) => failure,
                        Ok(false) => return,
                        Err(failure) => failure.into_opening_failure(),
                    }
                }
                Some(Err(failure)) => failure.into_opening_failure(),
                Some(Ok(None)) | None => {
                    match restored.advance(RestoreMilestone::WorkerExited).await {
                        Err(failure) => failure.into_opening_failure(),
                        Ok(_) => return,
                    }
                }
            };
            reply.send(OpeningFact::RestoreFailed { run, failure });
        });
        Ok(())
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
    Local(OpeningFailure),
    Internal,
}

impl RestoreWorkerFailure {
    fn into_opening_failure(self) -> OpeningFailure {
        match self {
            Self::Provider(failure) | Self::Local(failure) => failure,
            Self::Internal => OpeningFailure::OutcomeUnknown,
        }
    }
}

fn continue_restore(decision: RestoreDecision) -> Result<bool, RestoreWorkerFailure> {
    match decision {
        RestoreDecision::Continue => Ok(true),
        RestoreDecision::Superseded => Ok(false),
        RestoreDecision::ReceiptMissing => {
            Err(RestoreWorkerFailure::Local(OpeningFailure::OutcomeUnknown))
        }
        RestoreDecision::LocallyUnavailable(via) => {
            let method = match via {
                RestoreVia::Load => "session/load",
                RestoreVia::Resume => "session/resume",
            };
            Err(RestoreWorkerFailure::Local(OpeningFailure::Unavailable {
                diagnostic: format!(
                    "provider confirmed {method}; the local ACP route could not admit the restored opening"
                ),
            }))
        }
    }
}

impl RestoreWorker {
    async fn advance(&self, milestone: RestoreMilestone) -> Result<bool, RestoreWorkerFailure> {
        let (outcome, mut deferred) = oneshot::channel();
        let outcome = RestoreSignal::new(outcome);
        #[cfg(any(test, feature = "test-observation"))]
        let outcome = {
            let mut outcome = outcome;
            outcome._disposal = self
                .wire
                .take_opening_disposal_for_test(
                    crate::owner_wire::OpeningDisposalKind::RestoreSignal,
                    &self.attempts,
                )
                .map(Box::new);
            outcome
        };
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
                continue_restore(deferred.await.unwrap_or(RestoreDecision::Superseded))
            }
            Ok(effect) if effect.faulted() => {
                effect.finish(self.wire.as_ref());
                Err(RestoreWorkerFailure::Internal)
            }
            Ok(effect) => {
                let accepted = effect.finish(self.wire.as_ref());
                match deferred.try_recv() {
                    Ok(decision) => continue_restore(decision),
                    Err(_) => Ok(accepted),
                }
            }
            Err(_) => Ok(false),
        }
    }
}
