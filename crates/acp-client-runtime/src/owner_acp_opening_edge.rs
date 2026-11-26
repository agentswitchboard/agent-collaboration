//! Select the declared edge from a typed command and owned custody.

use super::{
    CloseFact, Command, Edge, LostReceipt, OpeningState, RestoreMilestone, StoppedReceipt,
};
use crate::acp_transport::ReadmitOutcome;

pub(super) fn edge(state: &OpeningState, command: &Command, taken: bool) -> Option<Edge> {
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
        Command::ReaderStopped(_) if state.live_reader().is_some() => Edge::ReaderStopped,
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
            let owed = match state {
                State::ClosingRestore { attempt, .. }
                | State::ClosingLive { attempt, .. }
                | State::RouteLost {
                    receipt: LostReceipt::Close(attempt),
                    ..
                }
                | State::StoppedAwaitingReply(StoppedReceipt::Close(_, attempt)) => attempt,
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
