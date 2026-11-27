//! Deliver real writer, connection, and route facts to each affected row.

use super::{
    Arc, Command, CreateAttempt, Effect, LifecycleRefusal, Mutex, OpeningAttempts, OpeningInstance,
    OpeningSlot, OwnerAcpWire, OwnerOpenings, SessionGeneration, WriterState, lock,
};
use switchboard_harness_shared_mechanics::opening::LifecycleRoute;

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
                    row.removed_route()
                        .map(|route| (OpeningSlot::Run(generation), row.instance().clone(), route))
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
        if let Ok((_instance, effect)) = self.take_retired(generation) {
            effect.finish(&self.wire);
        }
    }

    pub(super) fn take_retired(
        &self,
        generation: SessionGeneration,
    ) -> Result<(OpeningInstance, Effect), LifecycleRefusal> {
        let slot = OpeningSlot::Run(generation);
        let mut attempts = lock(&self.attempts);
        let row = attempts
            .get(slot)
            .ok_or_else(|| self.idle_phase().refusal())?;
        let instance = row.instance().clone();
        let retired = attempts.apply(slot, &instance, Command::Retire);
        drop(attempts);
        retired
            .map(|effect| (instance, effect))
            .map_err(|rejected| rejected.reason)
    }
}

fn stop_port(attempts: &Mutex<OpeningAttempts>, wire: &OwnerAcpWire, refusal: LifecycleRefusal) {
    let effects = {
        let mut attempts = lock(attempts);
        if attempts.port_refusal == Some(LifecycleRefusal::ConnectionRetired)
            || attempts.port_refusal == Some(refusal)
        {
            return;
        }
        attempts.port_refusal = Some(refusal);
        let rows =
            attempts
                .creates
                .iter()
                .map(|(&id, row)| {
                    (
                        OpeningSlot::Create(CreateAttempt(id)),
                        row.instance().clone(),
                    )
                })
                .chain(attempts.runs.iter().map(|(&generation, row)| {
                    (OpeningSlot::Run(generation), row.instance().clone())
                }))
                .collect::<Vec<_>>();
        let mut effects = Vec::with_capacity(rows.len());
        for (slot, instance) in rows {
            let command = if refusal == LifecycleRefusal::ConnectionRetired {
                Command::ConnectionRetired
            } else {
                Command::WriterStopped
            };
            effects.push(attempts.apply(slot, &instance, command));
        }
        effects
    };
    for effect in effects.into_iter().flatten() {
        effect.finish(wire);
    }
}

fn deliver_route_removed(
    attempts: &Mutex<OpeningAttempts>,
    wire: &OwnerAcpWire,
    slot: OpeningSlot,
    instance: &OpeningInstance,
    route: &LifecycleRoute,
) {
    let removed = lock(attempts).apply(slot, instance, Command::RouteRemoved(route.clone()));
    if let Ok(effect) = removed {
        effect.finish(wire);
    }
}
