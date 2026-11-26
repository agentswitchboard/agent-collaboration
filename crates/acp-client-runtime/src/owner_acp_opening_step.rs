//! The transition selected by the ACP opening declaration.

use super::{
    Command, CreateStart, DeferredRestore, Edge, Effect, HeldCreate, HeldRestore, LostReceipt,
    OpenCreateRequest, OpenRestoreRequest, OpeningInstance, OpeningPhase, OpeningPolicy,
    OpeningState, PendingCreate, PendingSetter, ReaderEnd, ReaderHandoff, Release,
    RestoreMilestone, RestoreStart, RoutedRestore, RunResources, SetterPlan, SetterWrite,
    StateMachine, StepFault, StoppedReceipt,
};
use crate::{
    acp_transport::ReadmitOutcome, owner_opening_instance::OpeningOffer,
    owner_session_open::OwnerOpenedSession, owner_session_setting::OwnerSettingWirePlan,
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
        other => (other, None),
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

fn edge(state: &OpeningState, command: &Command, taken: bool) -> Option<Edge> {
    use OpeningState as State;
    let stopped = matches!(state, State::StoppedAwaitingReply(_));
    let event = match command {
        Command::OpenCreate(_) => Edge::OpenCreate,
        Command::OpenRestore(_) => Edge::OpenRestore,
        Command::WriterAccepted => Edge::WriterAccepted,
        Command::WriterRefused => Edge::WriterRefused,
        Command::Created(_)
            if matches!(
                state,
                State::StoppedAwaitingReply(StoppedReceipt::OpenCreate(_))
            ) =>
        {
            Edge::FinishOnStoppedWriter
        }
        Command::Created(_) => Edge::ProviderCreated,
        Command::OpenFailed
            if matches!(
                state,
                State::StoppedAwaitingReply(StoppedReceipt::OpenCreate(_))
            ) =>
        {
            Edge::FinishOnStoppedWriter
        }
        Command::OpenFailed if matches!(state, State::AwaitingCreate(_)) => Edge::OpenFailed,
        Command::OpenFailed => return None,
        Command::WorkerExited
            if matches!(
                state,
                State::StoppedAwaitingReply(
                    StoppedReceipt::OpenCreate(_) | StoppedReceipt::Configure(_)
                )
            ) =>
        {
            Edge::FinishOnStoppedWriter
        }
        Command::WorkerExited
            if matches!(
                state,
                State::AwaitingCreate(_) | State::ConfiguringCreate(_)
            ) =>
        {
            Edge::WorkerExited
        }
        Command::WorkerExited => return None,
        Command::Configure(_) => Edge::Configure,
        Command::Configured(_) | Command::ConfigureClosed
            if matches!(
                state,
                State::StoppedAwaitingReply(StoppedReceipt::Configure(_))
            ) =>
        {
            Edge::FinishOnStoppedWriter
        }
        Command::Configured(_) => Edge::Configured,
        Command::ConfigureClosed => Edge::ClosedDuringConfigure,
        Command::AcceptSettings(_) if matches!(state, State::ConfiguredCreate(_)) => {
            Edge::AcceptCreatedSettings
        }
        Command::AcceptSettings(_) => Edge::AcceptSettings,
        Command::PutSettingBack { .. } => Edge::PutSettingBack,
        Command::SetterReserved(_, _) => Edge::SetterReserved,
        Command::SetterDone(_) => Edge::SetterDone,
        Command::Activate { .. } => Edge::Activate,
        Command::ActivationFailed => Edge::ActivationFailed,
        Command::ReaderAccepted => Edge::ReaderAccepted,
        Command::ReaderStopped(_) if state.reader_alive() => Edge::ReaderStopped,
        Command::ReaderStopped(_) => return None,
        Command::RestoreProgress(milestone, _) => match state {
            State::RouteLost {
                receipt: LostReceipt::None | LostReceipt::Close(_),
                ..
            } => return None,
            State::OfferingClose { deferred: None, .. } => Edge::DeferRestore,
            State::ClosingRestore { .. }
                if matches!(
                    milestone,
                    RestoreMilestone::Loaded(_)
                        | RestoreMilestone::Resumed(_)
                        | RestoreMilestone::Failed
                ) =>
            {
                Edge::LateOpenReply
            }
            State::StoppedAwaitingReply(StoppedReceipt::OpenRestore(_))
                if matches!(
                    milestone,
                    RestoreMilestone::Failed | RestoreMilestone::WorkerExited
                ) =>
            {
                Edge::FinishOnStoppedWriter
            }
            _ => milestone.event(),
        },
        Command::CloseAsked(_) => Edge::CloseAsked,
        Command::CloseOffered(close, accepted) => {
            let State::OfferingClose {
                origin, attempt, ..
            } = state
            else {
                return None;
            };
            if *attempt != close.attempt {
                return None;
            }
            if *accepted {
                if origin.phase() == OpeningPhase::Active {
                    Edge::CloseAcceptedLive
                } else {
                    Edge::CloseAcceptedRestore
                }
            } else {
                match origin.phase() {
                    OpeningPhase::AwaitingRestore => Edge::CloseUnsentAwaiting,
                    OpeningPhase::CapturingReplay => Edge::CloseUnsentReplay,
                    OpeningPhase::HeldRestore => Edge::CloseUnsentHeld,
                    OpeningPhase::PreparedRestore => Edge::CloseUnsentPrepared,
                    OpeningPhase::Active => Edge::CloseUnsentActive,
                    _ => return None,
                }
            }
        }
        Command::CloseSettled(fact) => {
            let (close, answer) = match fact {
                CloseFact::Closed { run } => (run, Edge::CloseConfirmed),
                CloseFact::RefusedStillLive { run } => (run, Edge::CloseRefused),
                CloseFact::Unknown { run, .. } | CloseFact::NotOffered { run } => {
                    (run, Edge::CloseUnknown)
                }
            };
            let owed = match state {
                State::ClosingRestore { attempt, .. }
                | State::ClosingLive { attempt, .. }
                | State::RouteLost {
                    receipt: LostReceipt::Close(attempt),
                    ..
                }
                | State::StoppedAwaitingReply(StoppedReceipt::Close(_, attempt)) => attempt,
                _ => return None,
            };
            if *owed != close.attempt
                || state
                    .run()
                    .is_none_or(|run| run.route.run.generation != close.generation)
            {
                return None;
            }
            if stopped {
                Edge::FinishOnStoppedWriter
            } else {
                answer
            }
        }
        Command::AdmitAgain(attempt) => {
            if !matches!(state, State::CloseRefusedLive { attempt: owed, .. } if owed == attempt) {
                return None;
            }
            Edge::AdmitAgain
        }
        Command::ReadmitResult(attempt, outcome) => {
            if !matches!(state, State::Readmitting { attempt: owed, .. } if owed == attempt) {
                return None;
            }
            match outcome {
                ReadmitOutcome::Reopened => Edge::Readmitted,
                ReadmitOutcome::RouteLost => Edge::RouteRemoved,
                ReadmitOutcome::StaleMarker => Edge::ReadmitFailed,
            }
        }
        Command::Retire => Edge::Retire,
        Command::WriterStopped if taken => Edge::WriterStoppedAfterOffer,
        Command::WriterStopped
            if matches!(
                state,
                State::RouteLost {
                    receipt: LostReceipt::None,
                    ..
                }
            ) =>
        {
            Edge::WriterStoppedWithoutReceipt
        }
        Command::WriterStopped => Edge::WriterStopped,
        Command::ConnectionRetired => Edge::ConnectionRetired,
        Command::RouteRemoved(route) => {
            if state.run().is_none_or(|run| !run.route.is_same_as(route)) {
                return None;
            }
            Edge::RouteRemoved
        }
    };
    Some(event)
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
                    _ => LifecycleRefusal::Encoding,
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
                _ => Vec::new(),
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
        (State::Readmitting { run, .. }, Command::ReadmitResult(_, ReadmitOutcome::Reopened)) => {
            store(State::Active(run), Effect::None)
        }
        (
            State::Readmitting { run, attempt },
            Command::ReadmitResult(_, ReadmitOutcome::StaleMarker),
        ) => store(State::CloseRefusedLive { run, attempt }, Effect::None),
        (state, Command::WriterStopped) => stop_writer(state, instance, edge),
        (
            state,
            command @ (Command::RouteRemoved(_)
            | Command::ReaderStopped(_)
            | Command::ReadmitResult(_, ReadmitOutcome::RouteLost)),
        ) => {
            let report_loss = matches!(
                command,
                Command::ReaderStopped(ReaderEnd::Failed | ReaderEnd::HandoffClosed)
            );
            let reader = matches!(command, Command::ReaderStopped(_));
            if reader && matches!(state, State::Active(_)) {
                return Ok((
                    None,
                    Effect::ReaderEnded {
                        release: Release::new(instance, state, true),
                        report_loss,
                    },
                ));
            }
            let Some(run) = state.run().cloned() else {
                return Err((state, command));
            };
            let receipt = lost_receipt(&state, taken);
            let release = Release::new(instance, state, true);
            let effect = if reader {
                Effect::ReaderEnded {
                    release,
                    report_loss,
                }
            } else {
                Effect::Release(release)
            };
            store(State::RouteLost { run, receipt }, effect)
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
        _ => return finish(state, instance),
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
        State::ClosingRestore { attempt, .. }
        | State::ClosingLive { attempt, .. }
        | State::OfferingClose { attempt, .. }
            if taken || !matches!(state, State::OfferingClose { .. }) =>
        {
            LostReceipt::Close(*attempt)
        }
        State::OfferingClose {
            origin,
            deferred: None,
            ..
        } if matches!(**origin, State::AwaitingRestore(_)) => LostReceipt::Open,
        _ => LostReceipt::None,
    }
}

#[cfg(test)]
#[path = "owner_acp_opening_matrix_test.rs"]
mod matrix;
