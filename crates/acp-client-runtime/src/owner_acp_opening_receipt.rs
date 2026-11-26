//! Preserve or settle the exact in-flight receipt when its local route stops.

use super::super::{RestoreReceiptStage, RestoreSignal};
use super::{
    Command, Edge, Effect, LostReceipt, OpeningInstance, OpeningState, Release, RestoreMilestone,
    Step, StoppedReceipt, finish, store,
};
use OpeningState as State;

pub(super) fn stop_writer(mut state: State, instance: &OpeningInstance, edge: Edge) -> Step {
    let taken = edge == Edge::WriterStoppedAfterOffer;
    if let Some(stage) = state.pending_restore(taken) {
        if let State::OfferingClose { deferred, .. } = &mut state
            && let Some(deferred) = deferred.take()
        {
            let decision = deferred.milestone.after_local_loss(stage);
            return Ok((
                None,
                Effect::ResolveDeferredRestore {
                    deferred,
                    decision,
                    release: Release::new(instance, state, true),
                },
            ));
        }
        let Some(run) = state.run().cloned() else {
            return Err((state, Command::WriterStopped));
        };
        // A pending peer receipt still has a reader. A load already waiting
        // on replay must wake to settle with its known peer acknowledgement.
        let interrupt = match stage {
            RestoreReceiptStage::AwaitingPeer => false,
            RestoreReceiptStage::PublishingLoad => true,
        };
        return store(
            State::StoppedAwaitingReply(StoppedReceipt::OpenRestore(run, stage)),
            Effect::Release(Release::new(instance, state, interrupt)),
        );
    }
    let receipt = match &state {
        State::OfferingCreate { pending, .. } if taken => {
            StoppedReceipt::OpenCreate(pending.stopped.clone())
        }
        State::AwaitingCreate(pending) => StoppedReceipt::OpenCreate(pending.stopped.clone()),
        State::ConfiguringCreate(stopped) => StoppedReceipt::Configure(stopped.clone()),
        State::ClosingRestore { run, attempt }
        | State::ClosingLive { run, attempt }
        | State::RouteLost {
            run,
            receipt: LostReceipt::Close(attempt),
        } => StoppedReceipt::Close(run.clone(), *attempt),
        State::OfferingClose {
            origin, attempt, ..
        } if taken => {
            let Some(run) = origin.run() else {
                return Err((state, Command::WriterStopped));
            };
            StoppedReceipt::Close(run.clone(), *attempt)
        }
        State::Vacant { .. }
        | State::OfferingCreate { .. }
        | State::HeldCreate(_)
        | State::ConfiguredCreate(_)
        | State::OfferingRestore { .. }
        | State::AwaitingRestore(_)
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

pub(super) fn lose_route(
    mut state: State,
    instance: &OpeningInstance,
    command: Command,
    taken: bool,
) -> Step {
    let Some(run) = state.run().cloned() else {
        return Err((state, command));
    };
    if let Some(stage) = state.pending_restore(taken)
        && let State::OfferingClose { deferred, .. } = &mut state
        && let Some(deferred) = deferred.take()
    {
        let decision = deferred.milestone.after_local_loss(stage);
        return store(
            State::RouteLost {
                run,
                receipt: LostReceipt::None,
            },
            Effect::ResolveDeferredRestore {
                deferred,
                decision,
                release: Release::new(instance, state, true),
            },
        );
    }
    let receipt = lost_receipt(&state, taken);
    store(
        State::RouteLost { run, receipt },
        Effect::Release(Release::new(instance, state, true)),
    )
}

pub(super) fn resolve_after_loss(
    state: State,
    instance: &OpeningInstance,
    milestone: RestoreMilestone,
    signal: Option<RestoreSignal>,
) -> Step {
    let (next, stage) = match &state {
        State::StoppedAwaitingReply(StoppedReceipt::OpenRestore(_, stage)) => (None, *stage),
        State::RouteLost {
            run,
            receipt: LostReceipt::Open(stage),
        } => (
            Some(State::RouteLost {
                run: run.clone(),
                receipt: LostReceipt::None,
            }),
            *stage,
        ),
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
        | State::Active(_)
        | State::OfferingClose { .. }
        | State::ClosingRestore { .. }
        | State::ClosingLive { .. }
        | State::CloseRefusedLive { .. }
        | State::Readmitting { .. }
        | State::RouteLost { .. }
        | State::StoppedAwaitingReply(_) => {
            return Err((state, Command::RestoreProgress(milestone, signal)));
        }
    };
    let decision = milestone.after_local_loss(stage);
    let release = Release::new(instance, state, true);
    let settled = match signal {
        Some(outcome) => Effect::ResolveRestore {
            outcome,
            decision,
            release: Some(release),
        },
        None => Effect::Release(release),
    };
    Ok((
        next,
        Effect::DiscardAfter(
            Box::new(Command::RestoreProgress(milestone, None)),
            Box::new(settled),
        ),
    ))
}

pub(super) fn lost_receipt(state: &State, taken: bool) -> LostReceipt {
    if let Some(stage) = state.pending_restore(taken) {
        return LostReceipt::Open(stage);
    }
    match state {
        State::ClosingRestore { attempt, .. } | State::ClosingLive { attempt, .. } => {
            LostReceipt::Close(*attempt)
        }
        State::OfferingClose { attempt, .. } if taken => LostReceipt::Close(*attempt),
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
        | State::Active(_)
        | State::OfferingClose { .. }
        | State::CloseRefusedLive { .. }
        | State::Readmitting { .. }
        | State::RouteLost { .. }
        | State::StoppedAwaitingReply(_) => LostReceipt::None,
    }
}
