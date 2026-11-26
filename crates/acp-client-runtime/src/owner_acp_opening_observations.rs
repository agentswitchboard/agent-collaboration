//! Deliver real writer, connection, and route facts to each affected row.

use super::row::{RunResources, StoppedReceipt};
use super::{
    Arc, CancellationToken, CreateAttempt, LifecycleRefusal, Mutex, OpeningAttempts, OpeningEvent,
    OpeningInstance, OpeningPhase, OpeningSlot, OpeningState, OwnerAcpWire, OwnerAddress,
    OwnerOpenings, SessionGeneration, Transition, WriterState, lock, transport_run,
};
use crate::acp_transport::LifecycleRoute;

impl OwnerOpenings {
    pub(super) fn watch_writer(&self) {
        let attempts = Arc::clone(&self.attempts);
        let stopped = self.stopped.clone();
        let wire = Arc::clone(&self.wire);
        let connection_retired = self.connection_retired.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                () = stopped.cancelled() => return,
                () = connection_retired.cancelled() => {
                    stop_port(&attempts, &wire, LifecycleRefusal::ConnectionRetired);
                    return;
                }
                () = wire.writer_stopped() => {
                    stop_port(&attempts, &wire, LifecycleRefusal::Writer(WriterState::Stopped));
                    #[cfg(any(test, feature = "test-observation"))]
                    wire.at_opening_effect_for_test(crate::owner_wire::OpeningPausePoint::WriterStopDelivered);
                }
            }
            tokio::select! {
                biased;
                () = stopped.cancelled() => {}
                () = connection_retired.cancelled() => {
                    stop_port(&attempts, &wire, LifecycleRefusal::ConnectionRetired);
                }
            }
        });
    }

    pub(in super::super) fn connection_retirement_observer(
        &self,
    ) -> impl FnOnce() + Send + 'static {
        let attempts = Arc::clone(&self.attempts);
        let wire = Arc::clone(&self.wire);
        move || stop_port(&attempts, &wire, LifecycleRefusal::ConnectionRetired)
    }

    pub(super) fn observe_health(&self) -> Result<(), LifecycleRefusal> {
        if self.connection_retired.is_cancelled() {
            stop_port(
                &self.attempts,
                &self.wire,
                LifecycleRefusal::ConnectionRetired,
            );
        } else if self.wire.writer_has_stopped() {
            stop_port(
                &self.attempts,
                &self.wire,
                LifecycleRefusal::Writer(WriterState::Stopped),
            );
        }
        let (refusal, removed) = {
            let attempts = lock(&self.attempts);
            let removed = attempts
                .runs
                .iter()
                .filter_map(|(&generation, row)| {
                    let route = &row.state.run()?.route;
                    (route.is_removed() && row.phase() != OpeningPhase::RouteLost).then(|| {
                        (
                            OpeningSlot::Run(generation),
                            row.instance.clone(),
                            route.clone(),
                        )
                    })
                })
                .collect::<Vec<_>>();
            (attempts.port_refusal, removed)
        };
        for (slot, instance, route) in removed {
            deliver_route_removed(&self.attempts, &self.wire, slot, &instance, &route);
        }
        refusal.map_or(Ok(()), Err)
    }

    pub(super) fn watch_route(
        &self,
        slot: OpeningSlot,
        instance: OpeningInstance,
        route: LifecycleRoute,
    ) {
        let attempts = Arc::clone(&self.attempts);
        let wire = Arc::clone(&self.wire);
        let stopped = self.stopped.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                () = stopped.cancelled() => {}
                () = route.removed() => deliver_route_removed(&attempts, &wire, slot, &instance, &route),
            }
        });
    }

    pub(in super::super) fn retire(&self, generation: SessionGeneration) {
        if let Ok((instance, state)) = self.take_retired(generation) {
            dispose(&self.wire, &instance, state);
        }
    }

    pub(super) fn take_retired(
        &self,
        generation: SessionGeneration,
    ) -> Result<(OpeningInstance, OpeningState), LifecycleRefusal> {
        let slot = OpeningSlot::Run(generation);
        let mut attempts = lock(&self.attempts);
        let row = attempts
            .get(slot)
            .ok_or_else(|| self.idle_phase().refusal())?;
        let instance = row.instance.clone();
        attempts
            .apply(slot, &instance, OpeningEvent::Retire, (), remove_state)
            .map(|state| (instance, state))
            .map_err(|(why, ())| why)
    }
}

pub(super) fn remove_state(
    state: OpeningState,
    (): (),
) -> Result<Transition<OpeningState>, (OpeningState, ())> {
    state.mark_retired();
    Ok(Transition::Remove(state))
}

pub(super) fn dispose(wire: &OwnerAcpWire, instance: &OpeningInstance, state: OpeningState) {
    if let Some(run) = state.run() {
        wire.retire_opening_route(&run.route, instance);
        run.route.notify_removed();
        run.stopped.cancel();
    }
    match state {
        OpeningState::OfferingCreate { pending, .. } | OpeningState::AwaitingCreate(pending) => {
            pending.stopped.cancel()
        }
        OpeningState::ConfiguringCreate(stopped)
        | OpeningState::StoppedAwaitingReply(
            StoppedReceipt::OpenCreate(stopped) | StoppedReceipt::Configure(stopped),
        ) => stopped.cancel(),
        OpeningState::OfferingClose {
            deferred: Some(deferred),
            ..
        } => {
            let _worker_gone = deferred.outcome.send(false);
        }
        _ => {}
    }
}

fn stop_port(attempts: &Mutex<OpeningAttempts>, wire: &OwnerAcpWire, refusal: LifecycleRefusal) {
    let ended = {
        let mut attempts = lock(attempts);
        if attempts.port_refusal == Some(LifecycleRefusal::ConnectionRetired)
            || attempts.port_refusal == Some(refusal)
        {
            return;
        }
        let event = if refusal == LifecycleRefusal::ConnectionRetired {
            OpeningEvent::ConnectionRetired
        } else {
            OpeningEvent::WriterStopped
        };
        attempts.port_refusal = Some(refusal);
        let rows = attempts
            .creates
            .iter()
            .map(|(&id, row)| (OpeningSlot::Create(CreateAttempt(id)), row.instance.clone()))
            .chain(
                attempts
                    .runs
                    .iter()
                    .map(|(&generation, row)| (OpeningSlot::Run(generation), row.instance.clone())),
            )
            .collect::<Vec<_>>();
        let mut ended = Vec::new();
        for (slot, instance) in rows {
            let result = if event == OpeningEvent::ConnectionRetired {
                attempts.apply(slot, &instance, event, (), |state, ()| {
                    state.mark_retired();
                    Ok(Transition::Remove(PortEffect::Dispose(state)))
                })
            } else {
                let taken = attempts
                    .exact(slot, &instance)
                    .and_then(|row| row.state.offer())
                    .is_some_and(
                        crate::owner_opening_instance::OpeningOffer::was_taken_or_withdraw,
                    );
                let event = if taken {
                    OpeningEvent::WriterStoppedAfterOffer
                } else {
                    event
                };
                attempts
                    .apply(slot, &instance, event, taken, writer_stopped)
                    .map_err(|(why, _)| (why, ()))
            };
            if let Ok(effect) = result {
                ended.push((instance, effect));
            }
        }
        ended
    };
    for (instance, effect) in ended {
        match effect {
            PortEffect::Dispose(state) => dispose(wire, &instance, state),
            PortEffect::ReleasePayload(state) => {
                if let OpeningState::OfferingClose {
                    deferred: Some(deferred),
                    ..
                } = state
                {
                    let _worker_gone = deferred.outcome.send(false);
                }
            }
        }
    }
}

enum PortEffect {
    Dispose(OpeningState),
    ReleasePayload(OpeningState),
}

fn writer_stopped(
    state: OpeningState,
    taken: bool,
) -> Result<Transition<PortEffect>, (OpeningState, bool)> {
    let next = match &state {
        OpeningState::OfferingCreate { pending, .. } if taken => {
            StoppedReceipt::OpenCreate(pending.stopped.clone())
        }
        OpeningState::AwaitingCreate(pending) => {
            StoppedReceipt::OpenCreate(pending.stopped.clone())
        }
        OpeningState::OfferingRestore { run, .. } if taken => {
            StoppedReceipt::OpenRestore(run.clone())
        }
        OpeningState::AwaitingRestore(run) => StoppedReceipt::OpenRestore(run.clone()),
        OpeningState::ConfiguringCreate(stopped) => StoppedReceipt::Configure(stopped.clone()),
        OpeningState::ClosingRestore { run, attempt }
        | OpeningState::ClosingLive { run, attempt }
        | OpeningState::RouteLost {
            run,
            close: Some(attempt),
        } => StoppedReceipt::Close(run.clone(), *attempt),
        OpeningState::OfferingClose {
            origin, attempt, ..
        } if taken => {
            let Some(run) = origin.run() else {
                return Err((state, taken));
            };
            StoppedReceipt::Close(run.clone(), *attempt)
        }
        OpeningState::RouteLost { run, close: None } => StoppedReceipt::OpenRestore(run.clone()),
        _ => {
            state.mark_retired();
            return Ok(Transition::Remove(PortEffect::Dispose(state)));
        }
    };
    Ok(Transition::Store(
        OpeningState::StoppedAwaitingReply(next),
        PortEffect::ReleasePayload(state),
    ))
}

fn deliver_route_removed(
    attempts: &Mutex<OpeningAttempts>,
    wire: &OwnerAcpWire,
    slot: OpeningSlot,
    instance: &OpeningInstance,
    route: &LifecycleRoute,
) {
    let result = {
        let mut attempts = lock(attempts);
        let true_route = attempts
            .exact(slot, instance)
            .and_then(|row| row.state.run())
            .is_some_and(|resources| resources.route.is_same_as(route));
        if !true_route {
            return;
        }
        attempts.apply(
            slot,
            instance,
            OpeningEvent::RouteRemoved,
            (),
            |state, ()| {
                let Some(run) = state.run().cloned() else {
                    return Err((state, ()));
                };
                let close = match &state {
                    OpeningState::OfferingClose { attempt, offer, .. }
                        if offer.was_taken_or_withdraw() =>
                    {
                        Some(*attempt)
                    }
                    OpeningState::ClosingRestore { attempt, .. }
                    | OpeningState::ClosingLive { attempt, .. } => Some(*attempt),
                    _ => None,
                };
                Ok(Transition::Store(
                    OpeningState::RouteLost { run, close },
                    state,
                ))
            },
        )
    };
    if let Ok(state) = result {
        dispose(wire, instance, state);
    }
}

pub(super) fn run_resources(
    owner: OwnerAddress,
    generation: SessionGeneration,
    stopped: &CancellationToken,
) -> RunResources {
    RunResources {
        route: LifecycleRoute::new(transport_run(owner, generation)),
        stopped: stopped.child_token(),
    }
}
