#![deny(clippy::wildcard_enum_match_arm)]

//! The transition selected by the ACP opening declaration.

#[path = "owner_acp_opening_edge.rs"]
mod selector;
use selector::edge;

use super::{
    Command, CreateStart, DeferredRestore, Edge, Effect, HeldCreate, HeldRestore, LostReceipt,
    OpenCreateRequest, OpenRestoreRequest, OpeningInstance, OpeningPhase, OpeningPolicy,
    OpeningState, PendingCreate, PendingSetter, ReaderEnd, ReaderHandoff, Release,
    RestoreMilestone, RestoreStart, RoutedRestore, RunResources, SetterPlan, SetterWrite,
    StateMachine, StepFault, StoppedReceipt,
};
use crate::{
    owner_opening_instance::OpeningOffer, owner_session_open::OwnerOpenedSession,
    owner_session_setting::OwnerSettingWirePlan,
};
use provider_session_machines::{
    ProviderSettingsCatalog,
    harness_lifecycle::{AcceptedRunSettings, CloseFact, LifecycleRefusal, SetupReport},
};
use session_event_model::{CapabilityReport, SessionSettings};
use std::sync::Arc;
use switchboard_harness_contracts::WorkOfferRefusal;

type Step = Result<(Option<OpeningState>, Effect), (OpeningState, Command)>;

pub(super) fn step(
    state: OpeningState,
    command: Command,
    instance: &OpeningInstance,
    taken: bool,
) -> Step {
    let from = state.phase();
    let Some(edge) = edge(&state, &command, taken) else {
        return Err((state, command));
    };
    let Ok((declared, _)) = OpeningPolicy::transition(&from, edge) else {
        return Err((state, command));
    };
    let (command, signal) = match command {
        Command::RestoreProgress(payload, signal) if edge != Edge::DeferRestore => {
            (Command::RestoreProgress(payload, None), signal)
        }
        other @ Command::OpenCreate(_)
        | other @ Command::OpenRestore(_)
        | other @ Command::WriterAccepted
        | other @ Command::WriterRefused
        | other @ Command::Created(_)
        | other @ Command::OpenFailed
        | other @ Command::WorkerExited
        | other @ Command::Configure(_)
        | other @ Command::Configured(_)
        | other @ Command::ConfigureClosed
        | other @ Command::AcceptSettings(_)
        | other @ Command::PutSettingBack { .. }
        | other @ Command::SetterReserved(..)
        | other @ Command::SetterDone(_)
        | other @ Command::Activate { .. }
        | other @ Command::ActivationFailed
        | other @ Command::ReaderAccepted
        | other @ Command::ReaderStopped(_)
        | other @ Command::RestoreProgress(..)
        | other @ Command::CloseAsked(_)
        | other @ Command::CloseOffered(..)
        | other @ Command::CloseSettled(_)
        | other @ Command::AdmitAgain(_)
        | other @ Command::ReadmitResult(..)
        | other @ Command::Retire
        | other @ Command::WriterStopped
        | other @ Command::ConnectionRetired
        | other @ Command::RouteRemoved(_) => (other, None),
    };
    let previous_run = state.run().cloned();
    let previous_offer = state.offer().cloned();
    let (next, effect) = match apply_edge(state, command, instance, edge, taken) {
        Ok(value) => value,
        Err((state, Command::RestoreProgress(payload, None))) => {
            return Err((state, Command::RestoreProgress(payload, signal)));
        }
        Err(value) => return Err(value),
    };
    let effect = if let Some(signal) = signal {
        Effect::DropSignal(signal, Box::new(effect))
    } else {
        effect
    };
    let (next, effect) = DeclaredOutcome {
        phase: from,
        edge,
        declared,
        instance,
        previous_run,
        previous_offer,
    }
    .validate(next, effect);
    match (next, effect) {
        (Some(origin), Effect::Rollback(Some(DeferredRestore { milestone, outcome }))) => {
            match step(
                origin,
                Command::RestoreProgress(milestone, None),
                instance,
                false,
            ) {
                Ok((next, Effect::Release(release))) => Ok((
                    next,
                    Effect::ResolveRestore {
                        outcome,
                        accepted: true,
                        release: Some(release),
                    },
                )),
                Ok((next, Effect::None)) => Ok((
                    next,
                    Effect::ResolveRestore {
                        outcome,
                        accepted: true,
                        release: None,
                    },
                )),
                Ok((next, fault @ Effect::Fault(_))) => {
                    Ok((next, Effect::DropSignal(outcome, Box::new(fault))))
                }
                Ok((next, other)) => Ok((
                    next,
                    Effect::ReleaseBefore(
                        Box::new(other),
                        Box::new(Effect::ResolveRestore {
                            outcome,
                            accepted: false,
                            release: None,
                        }),
                    ),
                )),
                Err((origin, command)) => Ok((
                    Some(origin),
                    Effect::DiscardAfter(
                        Box::new(command),
                        Box::new(Effect::ResolveRestore {
                            outcome,
                            accepted: false,
                            release: None,
                        }),
                    ),
                )),
            }
        }
        (next, effect) => Ok((next, effect)),
    }
}

struct DeclaredOutcome<'a> {
    phase: OpeningPhase,
    edge: Edge,
    declared: OpeningPhase,
    instance: &'a OpeningInstance,
    previous_run: Option<super::RunResources>,
    previous_offer: Option<OpeningOffer>,
}

impl DeclaredOutcome<'_> {
    fn validate(
        self,
        next: Option<OpeningState>,
        effect: Effect,
    ) -> (Option<OpeningState>, Effect) {
        let actual = next.as_ref().map(OpeningState::phase);
        let matches = actual.map_or(
            matches!(
                self.declared,
                OpeningPhase::Terminal | OpeningPhase::RetiredAwaitingReply
            ),
            |actual| actual == self.declared,
        );
        if matches {
            return (next, effect);
        }
        (
            None,
            Effect::Fault(Box::new(StepFault {
                phase: self.phase,
                edge: self.edge,
                declared: self.declared,
                actual,
                instance: self.instance.clone(),
                previous_run: self.previous_run,
                previous_offer: self.previous_offer,
                rejected: next.map(|state| Release::new(self.instance, state, true)),
                effect,
            })),
        )
    }
}

fn store(state: OpeningState, effect: Effect) -> Step {
    Ok((Some(state), effect))
}

fn finish(state: OpeningState, instance: &OpeningInstance) -> Step {
    Ok((None, Effect::Release(Release::new(instance, state, true))))
}

fn apply_edge(
    state: OpeningState,
    command: Command,
    instance: &OpeningInstance,
    edge: Edge,
    taken: bool,
) -> Step {
    use OpeningState as State;
    match (state, command) {
        (State::Vacant { .. }, Command::OpenCreate(request)) => {
            let OpenCreateRequest {
                setup,
                requested,
                stopped,
            } = *request;
            let offer = OpeningOffer::default();
            store(
                State::OfferingCreate {
                    offer: offer.clone(),
                    pending: PendingCreate {
                        requested: Box::new(requested),
                        stopped: stopped.clone(),
                    },
                },
                Effect::OfferCreate(Box::new(CreateStart {
                    setup,
                    offer,
                    stopped,
                })),
            )
        }
        (State::Vacant { .. }, Command::OpenRestore(request)) => {
            let OpenRestoreRequest {
                request,
                owner,
                stopped,
            } = *request;
            let offer = OpeningOffer::default();
            let resources = RunResources {
                route: crate::acp_transport::LifecycleRoute::new(super::super::transport_run(
                    owner,
                    request.run.generation,
                )),
                stopped,
            };
            store(
                State::OfferingRestore {
                    offer: offer.clone(),
                    run: resources.clone(),
                },
                Effect::OfferRestore(Box::new(RestoreStart {
                    request,
                    offer,
                    resources,
                })),
            )
        }
        (State::OfferingCreate { pending, .. }, Command::WriterAccepted) => {
            store(State::AwaitingCreate(pending), Effect::None)
        }
        (State::OfferingRestore { run, .. }, Command::WriterAccepted) => {
            store(State::AwaitingRestore(run), Effect::None)
        }
        (state @ State::StoppedAwaitingReply(_), Command::WriterAccepted) => {
            store(state, Effect::None)
        }
        (State::AwaitingCreate(pending), Command::Created(session)) => store(
            State::HeldCreate(HeldCreate {
                session,
                requested: pending.requested,
            }),
            Effect::None,
        ),
        (State::HeldCreate(held), Command::Configure(stopped)) => store(
            State::ConfiguringCreate(stopped),
            Effect::Configure {
                session: held.session,
                requested: held.requested,
            },
        ),
        (State::ConfiguringCreate(_), Command::Configured(session)) => {
            store(State::ConfiguredCreate(session), Effect::None)
        }
        (State::ConfiguredCreate(mut session), Command::AcceptSettings(accepted)) => {
            let report = apply_catalogue(&mut session, accepted);
            store(
                State::ConfiguredCreate(session),
                Effect::Settings(Box::new(report)),
            )
        }
        (State::HeldRestore(mut restore), Command::AcceptSettings(accepted)) => {
            let report = apply_catalogue(&mut restore.held.session, accepted);
            let setters = std::mem::take(&mut restore.held.setters);
            store(
                State::PreparedRestore(restore),
                withdraw_setters(setters, Effect::Settings(Box::new(report))),
            )
        }
        (State::HeldRestore(mut restore), Command::PutSettingBack { attempt, session }) => {
            let plan = if attempt.setting.change.generation == attempt.restore.generation {
                OwnerSettingWirePlan::from_work_catalog(
                    session,
                    &ProviderSettingsCatalog::from_record(attempt.catalogue),
                    attempt.setting.kind,
                    attempt.value.0,
                    restore.held.accepted_mode_fallback.clone(),
                )
                .map_err(|reason| match reason {
                    WorkOfferRefusal::SettingValueNotOffered => {
                        LifecycleRefusal::SettingValueNotOffered
                    }
                    WorkOfferRefusal::SettingSetterUnavailable => {
                        LifecycleRefusal::SettingSetterUnavailable
                    }
                    WorkOfferRefusal::PreparationFailed
                    | WorkOfferRefusal::SettingCatalogueUnavailable
                    | WorkOfferRefusal::RouteUnavailable
                    | WorkOfferRefusal::RouteClosing
                    | WorkOfferRefusal::WorkDoesNotFitAuthorization
                    | WorkOfferRefusal::OtherAuthorizationOffered
                    | WorkOfferRefusal::AuthorizationSequenceOnRecord => LifecycleRefusal::Encoding,
                })
            } else {
                Err(LifecycleRefusal::AttemptMismatch)
            };
            let wire = match plan {
                Ok(wire) => wire,
                Err(reason) => {
                    return store(State::HeldRestore(restore), Effect::RefuseSetter(reason));
                }
            };
            let claim = OpeningOffer::default();
            restore.held.setters.push(PendingSetter {
                claim: claim.clone(),
                plan: Some(SetterPlan {
                    restore: attempt.restore,
                    setting: attempt.setting,
                    wire,
                }),
            });
            store(State::HeldRestore(restore), Effect::ReserveSetter(claim))
        }
        (State::HeldRestore(mut restore), Command::SetterReserved(claim, reply)) => {
            let plan = restore
                .held
                .setters
                .iter_mut()
                .find(|pending| pending.claim.is_same_as(&claim))
                .and_then(|pending| pending.plan.take());
            let Some(SetterPlan {
                restore: run,
                setting,
                wire,
            }) = plan
            else {
                return Err((
                    State::HeldRestore(restore),
                    Command::SetterReserved(claim, reply),
                ));
            };
            let route = restore.run.route.clone();
            store(
                State::HeldRestore(restore),
                Effect::WriteSetter(Box::new(SetterWrite {
                    route,
                    restore: run,
                    setting,
                    plan: wire,
                    reply,
                })),
            )
        }
        (State::HeldRestore(mut restore), Command::SetterDone(claim)) => {
            let Some(index) = restore
                .held
                .setters
                .iter()
                .position(|pending| pending.claim.is_same_as(&claim))
            else {
                return Err((State::HeldRestore(restore), Command::SetterDone(claim)));
            };
            let offer = restore.held.setters.swap_remove(index);
            store(
                State::HeldRestore(restore),
                withdraw_setters(vec![offer], Effect::None),
            )
        }

        (State::ConfiguredCreate(session), Command::Activate { run, created: true }) => {
            let work = ReaderHandoff {
                session,
                skills: None,
                run: run.clone(),
                install_route: true,
            };
            store(State::StartingReader(run), Effect::StartReader(work))
        }
        (State::PreparedRestore(restore), Command::Activate { created: false, .. }) => {
            let HeldRestore {
                session, skills, ..
            } = *restore.held;
            let run = restore.run;
            let work = ReaderHandoff {
                session,
                skills,
                run: run.clone(),
                install_route: false,
            };
            store(State::StartingReader(run), Effect::StartReader(work))
        }
        (State::StartingReader(run), Command::ReaderAccepted) if !run.route.is_removed() => {
            store(State::Active(run), Effect::None)
        }
        (
            State::OfferingClose {
                origin,
                attempt,
                offer,
                deferred: None,
            },
            Command::RestoreProgress(milestone, Some(outcome)),
        ) => store(
            State::OfferingClose {
                origin,
                attempt,
                offer,
                deferred: Some(DeferredRestore { milestone, outcome }),
            },
            Effect::Deferred,
        ),
        (
            State::AwaitingRestore(run),
            Command::RestoreProgress(RestoreMilestone::Loaded(held), _),
        ) => store(
            State::PublishingReplay(RoutedRestore { run, held }),
            Effect::None,
        ),
        (
            State::AwaitingRestore(run),
            Command::RestoreProgress(RestoreMilestone::Resumed(held), _),
        ) => store(
            State::HeldRestore(RoutedRestore { run, held }),
            Effect::None,
        ),
        (
            State::PublishingReplay(restore),
            Command::RestoreProgress(RestoreMilestone::ReplayAdmitted, _),
        ) => store(State::HeldRestore(restore), Effect::None),
        (state @ State::ClosingRestore { .. }, command @ Command::RestoreProgress(_, _))
            if edge == Edge::LateOpenReply =>
        {
            store(state, Effect::Discarded(Box::new(command)))
        }
        (
            State::RouteLost {
                run,
                receipt: LostReceipt::Open,
            },
            command @ Command::RestoreProgress(
                RestoreMilestone::Failed | RestoreMilestone::WorkerExited,
                _,
            ),
        ) => store(
            State::RouteLost {
                run,
                receipt: LostReceipt::None,
            },
            Effect::Discarded(Box::new(command)),
        ),
        (
            state,
            Command::RestoreProgress(
                RestoreMilestone::Failed
                | RestoreMilestone::WorkerExited
                | RestoreMilestone::ReplayRejected,
                _,
            ),
        ) => finish(state, instance),
        (mut origin, Command::CloseAsked(close)) => {
            let Some(route) = origin.run().map(|run| run.route.clone()) else {
                return Err((origin, Command::CloseAsked(close)));
            };
            let offer = OpeningOffer::default();
            let setters = match &mut origin {
                State::HeldRestore(restore) => std::mem::take(&mut restore.held.setters),
                State::Vacant { .. }
                | State::OfferingCreate { .. }
                | State::AwaitingCreate(_)
                | State::HeldCreate(_)
                | State::ConfiguringCreate(_)
                | State::ConfiguredCreate(_)
                | State::OfferingRestore { .. }
                | State::AwaitingRestore(_)
                | State::PublishingReplay(_)
                | State::PreparedRestore(_)
                | State::StartingReader(_)
                | State::Active(_)
                | State::OfferingClose { .. }
                | State::ClosingRestore { .. }
                | State::ClosingLive { .. }
                | State::CloseRefusedLive { .. }
                | State::Readmitting { .. }
                | State::RouteLost { .. }
                | State::StoppedAwaitingReply(_) => Vec::new(),
            };
            store(
                State::OfferingClose {
                    origin: Box::new(origin),
                    attempt: close.attempt,
                    offer: offer.clone(),
                    deferred: None,
                },
                withdraw_setters(setters, Effect::CloseOffer(route, offer)),
            )
        }
        (
            State::OfferingClose {
                origin,
                attempt,
                offer,
                deferred,
            },
            command @ Command::CloseOffered(_, true),
        ) => {
            let Some(run) = origin.run().cloned() else {
                return Err((
                    State::OfferingClose {
                        origin,
                        attempt,
                        offer,
                        deferred,
                    },
                    command,
                ));
            };
            let state = if edge == Edge::CloseAcceptedLive {
                State::ClosingLive { run, attempt }
            } else {
                State::ClosingRestore { run, attempt }
            };
            store(
                state,
                Effect::CloseConfirmed(
                    Release::new(instance, *origin, false),
                    deferred.map(|deferred| deferred.outcome),
                ),
            )
        }
        (
            State::OfferingClose {
                origin, deferred, ..
            },
            Command::CloseOffered(_, false),
        ) => store(*origin, Effect::Rollback(deferred)),
        (
            State::ClosingLive { run, attempt },
            Command::CloseSettled(CloseFact::RefusedStillLive { .. }),
        ) => store(State::CloseRefusedLive { run, attempt }, Effect::None),
        (State::CloseRefusedLive { run, attempt }, Command::AdmitAgain(_)) => {
            let route = run.route.clone();
            store(
                State::Readmitting { run, attempt },
                Effect::Readmit(route, attempt),
            )
        }
        (State::Readmitting { run, .. }, Command::ReadmitResult(..))
            if edge == Edge::Readmitted =>
        {
            store(State::Active(run), Effect::None)
        }
        (state, Command::WriterStopped) => stop_writer(state, instance, edge),
        (state, Command::ReaderStopped(reason)) => end_reader(state, instance, reason, taken),
        (state, command @ (Command::RouteRemoved(_) | Command::ReadmitResult(..))) => {
            let Some(run) = state.run().cloned() else {
                return Err((state, command));
            };
            let receipt = lost_receipt(&state, taken);
            store(
                State::RouteLost { run, receipt },
                Effect::Release(Release::new(instance, state, true)),
            )
        }
        (state, command @ (Command::Configured(_) | Command::Created(_)))
            if edge == Edge::FinishOnStoppedWriter =>
        {
            let (next, effect) = finish(state, instance)?;
            Ok((
                next,
                Effect::DiscardAfter(Box::new(command), Box::new(effect)),
            ))
        }
        (
            state,
            Command::CloseSettled(_)
            | Command::Retire
            | Command::ConnectionRetired
            | Command::WriterRefused
            | Command::ActivationFailed
            | Command::WorkerExited
            | Command::OpenFailed
            | Command::ConfigureClosed,
        ) => finish(state, instance),
        (state, command) => Err((state, command)),
    }
}

fn end_reader(
    state: OpeningState,
    instance: &OpeningInstance,
    reason: ReaderEnd,
    taken: bool,
) -> Step {
    use OpeningState as State;
    let report_loss = match reason {
        ReaderEnd::Failed | ReaderEnd::HandoffClosed => true,
        ReaderEnd::Retired | ReaderEnd::Finished | ReaderEnd::IngressClosed => false,
    };
    let run = match &state {
        State::Active(_) => {
            return Ok((
                None,
                Effect::ReaderEnded {
                    release: Release::new(instance, state, true),
                    report_loss,
                },
            ));
        }
        State::ClosingLive { run, .. }
        | State::CloseRefusedLive { run, .. }
        | State::Readmitting { run, .. } => Some(run),
        State::OfferingClose { origin, .. } => origin.live_reader(),
        State::Vacant { .. }
        | State::OfferingCreate { .. }
        | State::AwaitingCreate(_)
        | State::HeldCreate(_)
        | State::ConfiguringCreate(_)
        | State::ConfiguredCreate(_)
        | State::OfferingRestore { .. }
        | State::AwaitingRestore(_)
        | State::PublishingReplay(_)
        | State::HeldRestore(_)
        | State::PreparedRestore(_)
        | State::StartingReader(_)
        | State::ClosingRestore { .. }
        | State::RouteLost { .. }
        | State::StoppedAwaitingReply(_) => None,
    };
    let Some(run) = run.cloned() else {
        return Err((state, Command::ReaderStopped(reason)));
    };
    let receipt = lost_receipt(&state, taken);
    store(
        State::RouteLost { run, receipt },
        Effect::ReaderEnded {
            release: Release::new(instance, state, true),
            report_loss,
        },
    )
}

fn stop_writer(state: OpeningState, instance: &OpeningInstance, edge: Edge) -> Step {
    use OpeningState as State;
    let receipt = match &state {
        State::OfferingCreate { pending, .. } if edge == Edge::WriterStoppedAfterOffer => {
            StoppedReceipt::OpenCreate(pending.stopped.clone())
        }
        State::AwaitingCreate(pending) => StoppedReceipt::OpenCreate(pending.stopped.clone()),
        State::OfferingRestore { run, .. } if edge == Edge::WriterStoppedAfterOffer => {
            StoppedReceipt::OpenRestore(run.clone())
        }
        State::AwaitingRestore(run) => StoppedReceipt::OpenRestore(run.clone()),
        State::ConfiguringCreate(stopped) => StoppedReceipt::Configure(stopped.clone()),
        State::ClosingRestore { run, attempt }
        | State::ClosingLive { run, attempt }
        | State::RouteLost {
            run,
            receipt: LostReceipt::Close(attempt),
        } => StoppedReceipt::Close(run.clone(), *attempt),
        State::OfferingClose {
            origin, attempt, ..
        } if edge == Edge::WriterStoppedAfterOffer => {
            let Some(run) = origin.run() else {
                return Err((state, Command::WriterStopped));
            };
            StoppedReceipt::Close(run.clone(), *attempt)
        }
        State::RouteLost {
            run,
            receipt: LostReceipt::Open,
        } => StoppedReceipt::OpenRestore(run.clone()),
        State::Vacant { .. }
        | State::OfferingCreate { .. }
        | State::HeldCreate(_)
        | State::ConfiguredCreate(_)
        | State::OfferingRestore { .. }
        | State::PublishingReplay(_)
        | State::HeldRestore(_)
        | State::PreparedRestore(_)
        | State::StartingReader(_)
        | State::Active(_)
        | State::OfferingClose { .. }
        | State::CloseRefusedLive { .. }
        | State::Readmitting { .. }
        | State::RouteLost { .. }
        | State::StoppedAwaitingReply(_) => return finish(state, instance),
    };
    store(
        State::StoppedAwaitingReply(receipt),
        Effect::Release(Release::new(instance, state, false)),
    )
}

fn apply_catalogue(
    session: &mut OwnerOpenedSession,
    accepted: AcceptedRunSettings,
) -> SetupReport<CapabilityReport, SessionSettings> {
    session.accept_restore_catalogue(&ProviderSettingsCatalog::from_record(Arc::clone(
        &accepted.catalogue,
    )));
    SetupReport {
        capabilities: session.capabilities().to_session_model(),
        settings: session.settings(),
        observed: crate::owner_session_creation::catalogue_evidence(session.catalog()),
    }
}

#[cfg(test)]
#[path = "owner_acp_opening_step_test.rs"]
mod tests;

fn withdraw_setters(setters: Vec<PendingSetter>, effect: Effect) -> Effect {
    if setters.is_empty() {
        effect
    } else {
        Effect::WithdrawSetters(setters, Box::new(effect))
    }
}

fn lost_receipt(state: &OpeningState, taken: bool) -> LostReceipt {
    use OpeningState as State;
    match state {
        State::OfferingRestore { .. } if taken => LostReceipt::Open,
        State::AwaitingRestore(_) => LostReceipt::Open,
        State::ClosingRestore { attempt, .. } | State::ClosingLive { attempt, .. } => {
            LostReceipt::Close(*attempt)
        }
        State::OfferingClose { attempt, .. } if taken => LostReceipt::Close(*attempt),
        State::OfferingClose {
            origin,
            deferred: None,
            ..
        } => lost_receipt(origin, false),
        State::Vacant { .. }
        | State::OfferingCreate { .. }
        | State::AwaitingCreate(_)
        | State::HeldCreate(_)
        | State::ConfiguringCreate(_)
        | State::ConfiguredCreate(_)
        | State::OfferingRestore { .. }
        | State::PublishingReplay(_)
        | State::HeldRestore(_)
        | State::PreparedRestore(_)
        | State::StartingReader(_)
        | State::Active(_)
        | State::OfferingClose {
            deferred: Some(_), ..
        }
        | State::CloseRefusedLive { .. }
        | State::Readmitting { .. }
        | State::RouteLost { .. }
        | State::StoppedAwaitingReply(_) => LostReceipt::None,
    }
}

#[cfg(test)]
#[path = "owner_acp_opening_matrix_test.rs"]
mod matrix;
