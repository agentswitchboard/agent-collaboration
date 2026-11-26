//! Select the declared edge from a typed command and owned custody.

use super::{
    CloseFact, Command, Edge, LostReceipt, OpeningState, RestoreMilestone, StoppedReceipt,
};
use crate::acp_transport::ReadmitOutcome;

pub(super) fn edge(state: &OpeningState, command: &Command, taken: bool) -> Option<Edge> {
    use OpeningState as State;
    let event = match command {
        Command::OpenCreate(_) => Edge::OpenCreate,
        Command::OpenRestore(_) => Edge::OpenRestore,
        Command::WriterAccepted
            if let State::RouteLost {
                receipt: LostReceipt::None | LostReceipt::Close(_),
                ..
            } = state =>
        {
            return None;
        }
        Command::WriterAccepted => Edge::WriterAccepted,
        Command::WriterRefused => Edge::WriterRefused,
        Command::Created(_) => worker_edge(state, CreateWorkerFact::Created)?,
        Command::OpenFailed => worker_edge(state, CreateWorkerFact::OpenFailed)?,
        Command::WorkerExited => worker_edge(state, CreateWorkerFact::Exited)?,
        Command::Configure(_) => Edge::Configure,
        Command::Configured(_) => worker_edge(state, CreateWorkerFact::Configured)?,
        Command::ConfigureClosed => worker_edge(state, CreateWorkerFact::ConfigureClosed)?,
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
        Command::ReaderStopped(_) if state.live_reader().is_some() => Edge::ReaderStopped,
        Command::ReaderStopped(_) => return None,
        Command::RestoreProgress(milestone, signal) => match state {
            State::RouteLost {
                receipt: LostReceipt::None | LostReceipt::Close(_),
                ..
            } => return None,
            State::RouteLost {
                receipt: LostReceipt::Open(_),
                ..
            } => match milestone {
                RestoreMilestone::Failed => Edge::OpenFailed,
                RestoreMilestone::Loaded(_)
                | RestoreMilestone::Resumed(_)
                | RestoreMilestone::ReplayAdmitted
                | RestoreMilestone::ReplayRejected
                | RestoreMilestone::WorkerExited => Edge::RestoreAfterLocalLoss,
            },
            State::OfferingClose {
                origin,
                deferred: None,
                ..
            } if signal.is_some() && origin.pending_restore(false).is_some() => Edge::DeferRestore,
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
            State::StoppedAwaitingReply(StoppedReceipt::OpenRestore(..)) => match milestone {
                RestoreMilestone::Failed => Edge::FinishOnStoppedWriter,
                RestoreMilestone::Loaded(_)
                | RestoreMilestone::Resumed(_)
                | RestoreMilestone::ReplayAdmitted
                | RestoreMilestone::ReplayRejected
                | RestoreMilestone::WorkerExited => Edge::RestoreAfterLocalLoss,
            },
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
            | State::StoppedAwaitingReply(_) => milestone.event(),
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
                if origin.live_reader().is_some() {
                    Edge::CloseAcceptedLive
                } else {
                    Edge::CloseAcceptedRestore
                }
            } else {
                match &**origin {
                    State::AwaitingRestore(_) => Edge::CloseUnsentAwaiting,
                    State::PublishingReplay(_) => Edge::CloseUnsentReplay,
                    State::HeldRestore(_) => Edge::CloseUnsentHeld,
                    State::PreparedRestore(_) => Edge::CloseUnsentPrepared,
                    State::Active(_) => Edge::CloseUnsentActive,
                    State::Vacant { .. }
                    | State::OfferingCreate { .. }
                    | State::AwaitingCreate(_)
                    | State::HeldCreate(_)
                    | State::ConfiguringCreate(_)
                    | State::ConfiguredCreate(_)
                    | State::OfferingRestore { .. }
                    | State::StartingReader(_)
                    | State::OfferingClose { .. }
                    | State::ClosingRestore { .. }
                    | State::ClosingLive { .. }
                    | State::CloseRefusedLive { .. }
                    | State::Readmitting { .. }
                    | State::RouteLost { .. }
                    | State::StoppedAwaitingReply(_) => return None,
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
            let (owed, answer) = match state {
                State::ClosingRestore { attempt, .. }
                | State::ClosingLive { attempt, .. }
                | State::RouteLost {
                    receipt: LostReceipt::Close(attempt),
                    ..
                } => (attempt, answer),
                State::StoppedAwaitingReply(StoppedReceipt::Close(_, attempt)) => {
                    (attempt, Edge::FinishOnStoppedWriter)
                }
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
                | State::StoppedAwaitingReply(_) => return None,
            };
            if *owed != close.attempt
                || state
                    .run()
                    .is_none_or(|run| run.route.run.generation != close.generation)
            {
                return None;
            }
            answer
        }
        Command::AdmitAgain(attempt) => {
            if !matches!(state, State::CloseRefusedLive { attempt: owed, .. } if owed == attempt) {
                return None;
            }
            Edge::AdmitAgain
        }
        Command::ReadmitResult(attempt, outcome) => {
            let State::Readmitting { run, attempt: owed } = state else {
                return None;
            };
            if owed != attempt {
                return None;
            }
            match outcome {
                ReadmitOutcome::Reopened | ReadmitOutcome::MarkerAbsent
                    if !run.route.is_removed() =>
                {
                    Edge::Readmitted
                }
                ReadmitOutcome::Reopened
                | ReadmitOutcome::MarkerAbsent
                | ReadmitOutcome::RouteLost => Edge::RouteRemoved,
                ReadmitOutcome::DifferentMarker => Edge::ReadmitFailed,
            }
        }
        Command::Retire => Edge::Retire,
        Command::WriterStopped if taken => Edge::WriterStoppedAfterOffer,
        Command::WriterStopped
            if let State::OfferingClose {
                origin, deferred, ..
            } = state
                && origin.pending_restore(false).is_some() =>
        {
            match deferred {
                Some(_) => Edge::WriterStoppedWithDeferredRestore,
                None => Edge::WriterStoppedWithOlderRestore,
            }
        }
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
        Command::ConnectionRetired => connection_loss_edge(state, taken),
        Command::RouteRemoved(route) => {
            if state.run().is_none_or(|run| !run.route.is_same_as(route)) {
                return None;
            }
            Edge::RouteRemoved
        }
    };
    Some(event)
}

#[derive(Clone, Copy)]
enum CreateWorkerFact {
    Created,
    OpenFailed,
    Configured,
    ConfigureClosed,
    Exited,
}

fn worker_edge(state: &OpeningState, fact: CreateWorkerFact) -> Option<Edge> {
    use CreateWorkerFact as Fact;
    use OpeningState as State;
    match state {
        State::AwaitingCreate(_) => match fact {
            Fact::Created => Some(Edge::ProviderCreated),
            Fact::OpenFailed => Some(Edge::OpenFailed),
            Fact::Exited => Some(Edge::WorkerExited),
            Fact::Configured | Fact::ConfigureClosed => None,
        },
        State::ConfiguringCreate(_) => match fact {
            Fact::Configured => Some(Edge::Configured),
            Fact::ConfigureClosed => Some(Edge::ClosedDuringConfigure),
            Fact::Exited => Some(Edge::WorkerExited),
            Fact::Created | Fact::OpenFailed => None,
        },
        State::StoppedAwaitingReply(receipt) => match receipt {
            StoppedReceipt::OpenCreate(_) => match fact {
                Fact::Created | Fact::OpenFailed | Fact::Exited => {
                    Some(Edge::FinishOnStoppedWriter)
                }
                Fact::Configured | Fact::ConfigureClosed => None,
            },
            StoppedReceipt::Configure(_) => match fact {
                Fact::Configured | Fact::ConfigureClosed | Fact::Exited => {
                    Some(Edge::FinishOnStoppedWriter)
                }
                Fact::Created | Fact::OpenFailed => None,
            },
            StoppedReceipt::OpenRestore(..) | StoppedReceipt::Close(..) => None,
        },
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
        | State::ClosingRestore { .. }
        | State::ClosingLive { .. }
        | State::CloseRefusedLive { .. }
        | State::Readmitting { .. }
        | State::RouteLost { .. } => None,
    }
}

fn connection_loss_edge(state: &OpeningState, taken: bool) -> Edge {
    use OpeningState as State;
    match state {
        State::StoppedAwaitingReply(_) => Edge::RetainReceiptAfterConnectionLoss,
        State::OfferingCreate { .. }
        | State::OfferingRestore { .. }
        | State::OfferingClose { .. }
            if taken =>
        {
            Edge::WriterStoppedAfterOffer
        }
        State::OfferingClose {
            origin, deferred, ..
        } if origin.pending_restore(false).is_some() => match deferred {
            Some(_) => Edge::WriterStoppedWithDeferredRestore,
            None => Edge::WriterStoppedWithOlderRestore,
        },
        State::AwaitingCreate(_)
        | State::AwaitingRestore(_)
        | State::PublishingReplay(_)
        | State::ConfiguringCreate(_)
        | State::ClosingLive { .. }
        | State::ClosingRestore { .. }
        | State::RouteLost {
            receipt: LostReceipt::Open(_) | LostReceipt::Close(_),
            ..
        } => Edge::WriterStopped,
        State::Vacant { .. }
        | State::OfferingCreate { .. }
        | State::HeldCreate(_)
        | State::ConfiguredCreate(_)
        | State::OfferingRestore { .. }
        | State::HeldRestore(_)
        | State::PreparedRestore(_)
        | State::StartingReader(_)
        | State::Active(_)
        | State::OfferingClose { .. }
        | State::CloseRefusedLive { .. }
        | State::Readmitting { .. }
        | State::RouteLost {
            receipt: LostReceipt::None,
            ..
        } => Edge::ConnectionRetired,
    }
}
