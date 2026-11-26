//! Independent expectations for the declared and executable opening outcomes.

use super::{Action, E, OpeningPolicy, P, StateMachine};

// The expected machine is expressed by lifecycle responsibility, independently
// of the declaration macro and without using a production transition adapter.
pub(super) fn expected(from: P, event: E) -> Option<P> {
    use E as Event;
    use P as Phase;
    let phase = match event {
        Event::ConnectionRetired
            if !matches!(
                from,
                Phase::UnboundIdle | Phase::BoundIdle | Phase::Terminal
            ) =>
        {
            Phase::Terminal
        }
        Event::RouteRemoved
            if matches!(
                from,
                Phase::QueuedRestore
                    | Phase::AwaitingRestore
                    | Phase::CapturingReplay
                    | Phase::HeldRestore
                    | Phase::PreparedRestore
                    | Phase::StartingReader
                    | Phase::Active
                    | Phase::OfferingClose
                    | Phase::ClosingRestore
                    | Phase::ClosingLive
                    | Phase::CloseRefusedLive
                    | Phase::Readmitting
            ) =>
        {
            Phase::RouteLost
        }
        Event::Retire => match from {
            Phase::AwaitingCreate | Phase::AwaitingRestore | Phase::ClosingRestore => {
                Phase::RetiredAwaitingReply
            }
            Phase::UnboundIdle
            | Phase::BoundIdle
            | Phase::RetiredAwaitingReply
            | Phase::Terminal => return None,
            Phase::QueuedCreate
            | Phase::QueuedRestore
            | Phase::HeldCreate
            | Phase::ConfiguringCreate
            | Phase::ConfiguredCreate
            | Phase::CapturingReplay
            | Phase::HeldRestore
            | Phase::PreparedRestore
            | Phase::StartingReader
            | Phase::Active
            | Phase::OfferingClose
            | Phase::ClosingLive
            | Phase::CloseRefusedLive
            | Phase::Readmitting
            | Phase::RouteLost
            | Phase::StoppedAwaitingReply => Phase::Terminal,
        },
        Event::WriterStopped => match from {
            Phase::AwaitingCreate
            | Phase::AwaitingRestore
            | Phase::ConfiguringCreate
            | Phase::ClosingRestore
            | Phase::ClosingLive
            | Phase::RouteLost => Phase::StoppedAwaitingReply,
            Phase::QueuedCreate
            | Phase::QueuedRestore
            | Phase::HeldCreate
            | Phase::ConfiguredCreate
            | Phase::CapturingReplay
            | Phase::HeldRestore
            | Phase::PreparedRestore
            | Phase::StartingReader
            | Phase::Active
            | Phase::OfferingClose
            | Phase::CloseRefusedLive
            | Phase::Readmitting => Phase::Terminal,
            Phase::RetiredAwaitingReply => from,
            Phase::UnboundIdle
            | Phase::BoundIdle
            | Phase::StoppedAwaitingReply
            | Phase::Terminal => return None,
        },
        Event::WriterStoppedAfterOffer
            if matches!(
                from,
                Phase::QueuedCreate | Phase::QueuedRestore | Phase::OfferingClose
            ) =>
        {
            Phase::StoppedAwaitingReply
        }
        Event::WriterStoppedWithoutReceipt if from == Phase::RouteLost => Phase::Terminal,
        Event::WaitOpening
            if matches!(
                from,
                Phase::QueuedCreate
                    | Phase::QueuedRestore
                    | Phase::AwaitingCreate
                    | Phase::ConfiguringCreate
                    | Phase::AwaitingRestore
                    | Phase::CapturingReplay
                    | Phase::StartingReader
            ) =>
        {
            from
        }
        Event::WaitClose
            if matches!(
                from,
                Phase::OfferingClose
                    | Phase::ClosingRestore
                    | Phase::ClosingLive
                    | Phase::Readmitting
            ) =>
        {
            from
        }
        Event::CloseAsked
            if matches!(
                from,
                Phase::AwaitingRestore
                    | Phase::CapturingReplay
                    | Phase::HeldRestore
                    | Phase::PreparedRestore
                    | Phase::Active
            ) =>
        {
            Phase::OfferingClose
        }
        Event::CloseConfirmed | Event::CloseUnknown
            if matches!(
                from,
                Phase::ClosingRestore | Phase::ClosingLive | Phase::RouteLost
            ) =>
        {
            Phase::Terminal
        }
        Event::CloseRefused if matches!(from, Phase::ClosingRestore | Phase::RouteLost) => {
            Phase::Terminal
        }
        Event::ReaderStopped
            if matches!(
                from,
                Phase::ClosingLive
                    | Phase::OfferingClose
                    | Phase::CloseRefusedLive
                    | Phase::Readmitting
            ) =>
        {
            Phase::RouteLost
        }
        Event::OpenFailed
            if matches!(
                from,
                Phase::AwaitingCreate | Phase::AwaitingRestore | Phase::CapturingReplay
            ) =>
        {
            Phase::Terminal
        }
        Event::WorkerExited if matches!(from, Phase::AwaitingCreate | Phase::AwaitingRestore) => {
            Phase::RetiredAwaitingReply
        }
        Event::WorkerExited
            if matches!(from, Phase::ConfiguringCreate | Phase::CapturingReplay) =>
        {
            Phase::Terminal
        }
        Event::OpenFailed | Event::WorkerExited if from == Phase::RouteLost => Phase::RouteLost,
        Event::ActivationFailed
            if matches!(
                from,
                Phase::ConfiguredCreate | Phase::PreparedRestore | Phase::StartingReader
            ) =>
        {
            Phase::Terminal
        }
        Event::Activate if matches!(from, Phase::ConfiguredCreate | Phase::PreparedRestore) => {
            Phase::StartingReader
        }
        Event::OpenCreate
        | Event::OpenRestore
        | Event::WriterAccepted
        | Event::WriterRefused
        | Event::ProviderCreated
        | Event::Configure
        | Event::Configured
        | Event::ClosedDuringConfigure
        | Event::RestoreLoadReply
        | Event::RestoreResumeReply
        | Event::ReplayAdmitted
        | Event::ReplayRejected
        | Event::OpenFailed
        | Event::PutSettingBack
        | Event::SetterReserved
        | Event::SetterDone
        | Event::AcceptSettings
        | Event::AcceptCreatedSettings
        | Event::Activate
        | Event::ReaderAccepted
        | Event::ReaderStopped
        | Event::ActivationFailed
        | Event::CloseAsked
        | Event::CloseAcceptedRestore
        | Event::CloseAcceptedLive
        | Event::DeferRestore
        | Event::CloseUnsentAwaiting
        | Event::CloseUnsentReplay
        | Event::CloseUnsentHeld
        | Event::CloseUnsentPrepared
        | Event::CloseUnsentActive
        | Event::CloseConfirmed
        | Event::CloseRefused
        | Event::CloseUnknown
        | Event::AdmitAgain
        | Event::Readmitted
        | Event::ReadmitFailed
        | Event::RouteRemoved
        | Event::FinishOnStoppedWriter
        | Event::LateOpenReply
        | Event::WorkerExited
        | Event::WriterStoppedAfterOffer
        | Event::WriterStoppedWithoutReceipt
        | Event::ConnectionRetired
        | Event::WaitOpening
        | Event::WaitClose
        | Event::KeepActive
        | Event::ReportRouteLost
        | Event::StopOnWriterLoss => match (from, event) {
            (Phase::UnboundIdle, Event::OpenCreate) => Phase::QueuedCreate,
            (Phase::UnboundIdle, Event::OpenRestore) => Phase::QueuedRestore,
            (Phase::BoundIdle, Event::OpenRestore) => Phase::QueuedRestore,
            (Phase::QueuedCreate, Event::WriterAccepted) => Phase::AwaitingCreate,
            (Phase::QueuedRestore, Event::WriterAccepted) => Phase::AwaitingRestore,
            (Phase::QueuedCreate | Phase::QueuedRestore, Event::WriterRefused) => Phase::Terminal,
            (Phase::AwaitingCreate, Event::ProviderCreated) => Phase::HeldCreate,
            (Phase::HeldCreate, Event::Configure) => Phase::ConfiguringCreate,
            (Phase::ConfiguringCreate, Event::Configured) => Phase::ConfiguredCreate,
            (Phase::ConfiguringCreate, Event::ClosedDuringConfigure) => Phase::Terminal,
            (Phase::ConfiguredCreate, Event::AcceptCreatedSettings) => from,
            (Phase::AwaitingRestore, Event::RestoreLoadReply) => Phase::CapturingReplay,
            (Phase::AwaitingRestore, Event::RestoreResumeReply) => Phase::HeldRestore,
            (Phase::CapturingReplay, Event::ReplayAdmitted) => Phase::HeldRestore,
            (Phase::CapturingReplay, Event::ReplayRejected) => Phase::Terminal,
            (
                Phase::HeldRestore,
                Event::PutSettingBack | Event::SetterReserved | Event::SetterDone,
            ) => from,
            (Phase::HeldRestore, Event::AcceptSettings) => Phase::PreparedRestore,
            (Phase::StartingReader, Event::ReaderAccepted) => Phase::Active,
            (Phase::Active, Event::KeepActive) => from,
            (Phase::Active, Event::ReaderStopped) => Phase::Terminal,
            (Phase::ClosingLive, Event::CloseRefused) => Phase::CloseRefusedLive,
            (Phase::ClosingRestore, Event::LateOpenReply) => from,
            (Phase::OfferingClose, Event::CloseAcceptedRestore) => Phase::ClosingRestore,
            (Phase::OfferingClose, Event::CloseAcceptedLive) => Phase::ClosingLive,
            (Phase::OfferingClose, Event::DeferRestore) => from,
            (Phase::OfferingClose, Event::CloseUnsentAwaiting) => Phase::AwaitingRestore,
            (Phase::OfferingClose, Event::CloseUnsentReplay) => Phase::CapturingReplay,
            (Phase::OfferingClose, Event::CloseUnsentHeld) => Phase::HeldRestore,
            (Phase::OfferingClose, Event::CloseUnsentPrepared) => Phase::PreparedRestore,
            (Phase::OfferingClose, Event::CloseUnsentActive) => Phase::Active,
            (Phase::CloseRefusedLive, Event::AdmitAgain) => Phase::Readmitting,
            (Phase::Readmitting, Event::Readmitted) => Phase::Active,
            (Phase::Readmitting, Event::ReadmitFailed) => Phase::RouteLost,
            (Phase::RetiredAwaitingReply, Event::LateOpenReply) => Phase::Terminal,
            (Phase::RouteLost, Event::ReportRouteLost) => from,
            (Phase::StoppedAwaitingReply, Event::FinishOnStoppedWriter) => Phase::Terminal,
            (Phase::StoppedAwaitingReply, Event::WriterAccepted | Event::StopOnWriterLoss) => from,
            _ => return None,
        },
    };
    Some(phase)
}

#[test]
fn every_declared_pair_matches_the_independent_opening_matrix() {
    for &phase in P::ALL {
        for &edge in E::ALL {
            assert_eq!(
                OpeningPolicy::transition(&phase, edge)
                    .ok()
                    .map(|(next, _)| next),
                expected(phase, edge),
                "independent declaration for {phase:?} + {edge:?}",
            );
        }
    }
}

pub(super) fn expected_actions(phase: P, event: E, next: P) -> Vec<Action> {
    use Action as A;
    match event {
        E::Configure => vec![A::Configure],
        E::AcceptSettings => vec![A::FenceSetter, A::ReportSettings],
        E::AcceptCreatedSettings => vec![A::ReportSettings],
        E::PutSettingBack => vec![A::ReserveSetter],
        E::SetterReserved => vec![A::SendSetter],
        E::SetterDone => vec![A::FenceSetter, A::Hold],
        E::Activate => vec![A::Reader],
        E::ReaderStopped => vec![A::ReaderEnded],
        E::CloseAsked if phase == P::HeldRestore => vec![A::FenceSetter, A::OfferClose],
        E::CloseAsked => vec![A::OfferClose],
        E::CloseAcceptedLive | E::CloseAcceptedRestore => vec![A::ConfirmClose],
        E::DeferRestore => vec![A::Wait],
        E::AdmitAgain => vec![A::Readmit],
        E::LateOpenReply => vec![A::Discard],
        E::OpenFailed | E::WorkerExited if phase == P::RouteLost => vec![A::Discard],
        E::RouteRemoved | E::ReadmitFailed => vec![A::Remove],
        _ if next == P::StoppedAwaitingReply && event != E::WriterAccepted => vec![A::ReleaseAfter],
        _ if matches!(next, P::Terminal | P::RetiredAwaitingReply) => vec![A::Remove],
        E::OpenCreate
        | E::OpenRestore
        | E::WriterAccepted
        | E::WriterRefused
        | E::ProviderCreated
        | E::Configured
        | E::ClosedDuringConfigure
        | E::RestoreLoadReply
        | E::RestoreResumeReply
        | E::ReplayAdmitted
        | E::ReplayRejected
        | E::OpenFailed
        | E::ReaderAccepted
        | E::ActivationFailed
        | E::CloseUnsentAwaiting
        | E::CloseUnsentReplay
        | E::CloseUnsentHeld
        | E::CloseUnsentPrepared
        | E::CloseUnsentActive
        | E::CloseConfirmed
        | E::CloseRefused
        | E::CloseUnknown
        | E::Readmitted
        | E::Retire
        | E::FinishOnStoppedWriter
        | E::WorkerExited
        | E::WriterStopped
        | E::WriterStoppedAfterOffer
        | E::WriterStoppedWithoutReceipt
        | E::ConnectionRetired
        | E::WaitOpening
        | E::WaitClose
        | E::KeepActive
        | E::ReportRouteLost
        | E::StopOnWriterLoss => vec![A::Hold],
    }
}
