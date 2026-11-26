//! Declared ACP opening transitions and the owner actions derived from them.

use provider_session_machines::harness_lifecycle::{ActivationRefusal, LifecycleRefusal};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use switchboard_harness_contracts::{HeldOpeningKind, WriterState};

macro_rules! tagged_enum {
    ($name:ident, $($variant:ident => $tag:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(super) enum $name { $($variant),+ }

        impl $name {
            #[cfg(test)]
            const ALL: &'static [Self] = &[$(Self::$variant),+];

            const fn tag(self) -> &'static str {
                match self { $(Self::$variant => $tag),+ }
            }
        }
    };
}

tagged_enum!(OpeningPhase,
    UnboundIdle => "unbound_idle",
    BoundIdle => "bound_idle",
    QueuedCreate => "queued_create",
    QueuedRestore => "queued_restore",
    AwaitingCreate => "awaiting_create",
    HeldCreate => "held_create",
    ConfiguringCreate => "configuring_create",
    ConfiguredCreate => "configured_create",
    AwaitingRestore => "awaiting_restore",
    CapturingReplay => "capturing_replay",
    HeldRestore => "held_restore",
    PreparedRestore => "prepared_restore",
    StartingReader => "starting_reader",
    Active => "active",
    OfferingClose => "offering_close",
    ClosingRestore => "closing_restore",
    ClosingLive => "closing_live",
    CloseRefusedLive => "close_refused_live",
    Readmitting => "readmitting",
    RetiredAwaitingReply => "retired_awaiting_reply",
    RouteLost => "route_lost",
    StoppedAwaitingReply => "stopped_awaiting_reply",
    Terminal => "terminal",
);

impl StateTag for OpeningPhase {
    fn state_tag(&self) -> &'static str {
        self.tag()
    }

    fn is_terminal(&self) -> bool {
        *self == Self::Terminal
    }
}

tagged_enum!(OpeningEvent,
    OpenCreate => "open_create",
    BindSavedSession => "bind_saved_session",
    OpenRestore => "open_restore",
    WriterAccepted => "writer_accepted",
    WriterRefused => "writer_refused",
    ProviderCreated => "provider_created",
    Configure => "configure",
    Configured => "configured",
    ClosedDuringConfigure => "closed_during_configure",
    RestoreLoadReply => "restore_load_reply",
    RestoreResumeReply => "restore_resume_reply",
    ReplayAdmitted => "replay_admitted",
    ReplayRejected => "replay_rejected",
    OpenFailed => "open_failed",
    PutSettingBack => "put_setting_back",
    SetterReserved => "setter_reserved",
    SetterDone => "setter_done",
    AcceptSettings => "accept_settings",
    AcceptCreatedSettings => "accept_created_settings",
    Activate => "activate",
    ReaderAccepted => "reader_accepted",
    ReaderStopped => "reader_stopped",
    ActivationFailed => "activation_failed",
    CloseAsked => "close_asked",
    CloseAcceptedRestore => "close_accepted_restore",
    CloseAcceptedLive => "close_accepted_live",
    DeferRestore => "defer_restore",
    CloseUnsentAwaiting => "close_unsent_awaiting",
    CloseUnsentReplay => "close_unsent_replay",
    CloseUnsentHeld => "close_unsent_held",
    CloseUnsentPrepared => "close_unsent_prepared",
    CloseUnsentActive => "close_unsent_active",
    CloseConfirmed => "close_confirmed",
    CloseRefused => "close_refused",
    CloseUnknown => "close_unknown",
    AdmitAgain => "admit_again",
    Readmitted => "readmitted",
    ReadmitFailed => "readmit_failed",
    Retire => "retire",
    RouteRemoved => "route_removed",
    FinishOnStoppedWriter => "finish_on_stopped_writer",
    LateOpenReply => "late_open_reply",
    WorkerExited => "worker_exited",
    WriterStopped => "writer_stopped",
    WriterStoppedAfterOffer => "writer_stopped_after_offer",
    WriterStoppedWithoutReceipt => "writer_stopped_without_receipt",
    ConnectionRetired => "connection_retired",
    WaitOpening => "wait_opening",
    WaitClose => "wait_close",
    KeepActive => "keep_active",
    ReportRouteLost => "report_route_lost",
    StopOnWriterLoss => "stop_on_writer_loss",
);

impl EventTag for OpeningEvent {
    fn event_tag(&self) -> &'static str {
        self.tag()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct WrongOpeningPhase;

impl RefusalTag for WrongOpeningPhase {
    fn refusal_tag(&self) -> &'static str {
        "wrong_opening_phase"
    }
}

pub(super) struct OpeningPolicy;

// The same declaration supplies the reducer and its inspection table.
macro_rules! opening_transitions {
    ($( $from:ident + $event:ident => $to:ident; )+) => {
        fn transition(state: &OpeningPhase, event: OpeningEvent) -> StateMachineResult<Self> {
            match (*state, event) {
                $((OpeningPhase::$from, OpeningEvent::$event) => Ok((OpeningPhase::$to, Vec::new())),)+
                (OpeningPhase::Terminal, _) => Err(TransitionError::Stale),
                _ => Err(TransitionError::Domain(WrongOpeningPhase)),
            }
        }

        fn transitions() -> &'static [Transition] {
            const EDGES: &[Transition] = &[
                $(Transition {
                    from: OpeningPhase::$from.tag(),
                    event: OpeningEvent::$event.tag(),
                    to: OpeningPhase::$to.tag(),
                    terminal: matches!(OpeningPhase::$to, OpeningPhase::Terminal),
                }),+
            ];
            EDGES
        }
    };
}

impl StateMachine for OpeningPolicy {
    type State = OpeningPhase;
    type Event = OpeningEvent;
    type Effect = ();
    type Error = WrongOpeningPhase;

    opening_transitions! {
        UnboundIdle + OpenCreate => QueuedCreate;
        UnboundIdle + BindSavedSession => BoundIdle;
        BoundIdle + OpenRestore => QueuedRestore;
        QueuedCreate + WriterAccepted => AwaitingCreate;
        QueuedRestore + WriterAccepted => AwaitingRestore;
        QueuedCreate + WriterRefused => Terminal;
        QueuedRestore + WriterRefused => Terminal;
        QueuedCreate + Retire => Terminal;
        QueuedRestore + Retire => Terminal;
        QueuedCreate + WaitOpening => QueuedCreate;
        QueuedRestore + WaitOpening => QueuedRestore;
        AwaitingCreate + ProviderCreated => HeldCreate;
        AwaitingCreate + OpenFailed => Terminal;
        AwaitingCreate + Retire => RetiredAwaitingReply;
        AwaitingCreate + WorkerExited => RetiredAwaitingReply;
        AwaitingCreate + WaitOpening => AwaitingCreate;
        HeldCreate + Configure => ConfiguringCreate;
        HeldCreate + Retire => Terminal;
        ConfiguringCreate + Configured => ConfiguredCreate;
        ConfiguringCreate + ClosedDuringConfigure => Terminal;
        ConfiguringCreate + Retire => Terminal;
        ConfiguringCreate + WorkerExited => Terminal;
        ConfiguringCreate + WaitOpening => ConfiguringCreate;
        ConfiguredCreate + Activate => StartingReader;
        ConfiguredCreate + AcceptCreatedSettings => ConfiguredCreate;
        ConfiguredCreate + ActivationFailed => Terminal;
        ConfiguredCreate + Retire => Terminal;
        AwaitingRestore + RestoreLoadReply => CapturingReplay;
        AwaitingRestore + RestoreResumeReply => HeldRestore;
        AwaitingRestore + OpenFailed => Terminal;
        AwaitingRestore + CloseAsked => OfferingClose;
        AwaitingRestore + Retire => RetiredAwaitingReply;
        AwaitingRestore + WorkerExited => RetiredAwaitingReply;
        AwaitingRestore + WaitOpening => AwaitingRestore;
        CapturingReplay + ReplayAdmitted => HeldRestore;
        CapturingReplay + ReplayRejected => Terminal;
        CapturingReplay + OpenFailed => Terminal;
        CapturingReplay + CloseAsked => OfferingClose;
        CapturingReplay + Retire => Terminal;
        CapturingReplay + WorkerExited => Terminal;
        CapturingReplay + WaitOpening => CapturingReplay;
        HeldRestore + PutSettingBack => HeldRestore;
        HeldRestore + SetterReserved => HeldRestore;
        HeldRestore + SetterDone => HeldRestore;
        HeldRestore + AcceptSettings => PreparedRestore;
        HeldRestore + CloseAsked => OfferingClose;
        HeldRestore + Retire => Terminal;
        PreparedRestore + Activate => StartingReader;
        PreparedRestore + ActivationFailed => Terminal;
        PreparedRestore + CloseAsked => OfferingClose;
        PreparedRestore + Retire => Terminal;
        Active + ReaderStopped => Terminal;
        ClosingLive + ReaderStopped => RouteLost;
        OfferingClose + ReaderStopped => RouteLost;
        Readmitting + ReaderStopped => RouteLost;
        Active + KeepActive => Active;
        StartingReader + ReaderAccepted => Active;
        StartingReader + ActivationFailed => Terminal;
        StartingReader + Retire => Terminal;
        StartingReader + WaitOpening => StartingReader;
        Active + CloseAsked => OfferingClose;
        Active + Retire => Terminal;
        ClosingLive + CloseConfirmed => Terminal;
        ClosingLive + CloseRefused => CloseRefusedLive;
        ClosingLive + CloseUnknown => Terminal;
        ClosingLive + WaitClose => ClosingLive;
        ClosingLive + Retire => Terminal;
        ClosingRestore + LateOpenReply => ClosingRestore;
        ClosingRestore + CloseConfirmed => Terminal;
        ClosingRestore + CloseRefused => Terminal;
        ClosingRestore + CloseUnknown => Terminal;
        ClosingRestore + WaitClose => ClosingRestore;
        ClosingRestore + Retire => RetiredAwaitingReply;
        OfferingClose + CloseAcceptedRestore => ClosingRestore;
        OfferingClose + CloseAcceptedLive => ClosingLive;
        OfferingClose + DeferRestore => OfferingClose;
        OfferingClose + CloseUnsentAwaiting => AwaitingRestore;
        OfferingClose + CloseUnsentReplay => CapturingReplay;
        OfferingClose + CloseUnsentHeld => HeldRestore;
        OfferingClose + CloseUnsentPrepared => PreparedRestore;
        OfferingClose + CloseUnsentActive => Active;
        OfferingClose + Retire => Terminal;
        OfferingClose + WaitClose => OfferingClose;
        CloseRefusedLive + AdmitAgain => Readmitting;
        Readmitting + Readmitted => Active;
        Readmitting + ReadmitFailed => CloseRefusedLive;
        Readmitting + WaitClose => Readmitting;
        Readmitting + Retire => Terminal;
        Readmitting + ConnectionRetired => Terminal;
        Readmitting + RouteRemoved => RouteLost;
        Readmitting + WriterStopped => Terminal;
        CloseRefusedLive + Retire => Terminal;
        RetiredAwaitingReply + LateOpenReply => Terminal;
        RetiredAwaitingReply + ConnectionRetired => Terminal;
        RetiredAwaitingReply + WriterStopped => RetiredAwaitingReply;
        QueuedCreate + WriterStopped => Terminal;
        QueuedRestore + WriterStopped => Terminal;
        QueuedCreate + WriterStoppedAfterOffer => StoppedAwaitingReply;
        QueuedRestore + WriterStoppedAfterOffer => StoppedAwaitingReply;
        QueuedCreate + ConnectionRetired => Terminal;
        QueuedRestore + ConnectionRetired => Terminal;
        AwaitingCreate + ConnectionRetired => Terminal;
        AwaitingRestore + ConnectionRetired => Terminal;
        CapturingReplay + ConnectionRetired => Terminal;
        HeldCreate + ConnectionRetired => Terminal;
        ConfiguringCreate + ConnectionRetired => Terminal;
        ConfiguredCreate + ConnectionRetired => Terminal;
        HeldRestore + ConnectionRetired => Terminal;
        PreparedRestore + ConnectionRetired => Terminal;
        Active + ConnectionRetired => Terminal;
        ClosingRestore + ConnectionRetired => Terminal;
        ClosingLive + ConnectionRetired => Terminal;
        CloseRefusedLive + ConnectionRetired => Terminal;
        StartingReader + ConnectionRetired => Terminal;
        OfferingClose + ConnectionRetired => Terminal;
        RouteLost + ConnectionRetired => Terminal;
        StoppedAwaitingReply + ConnectionRetired => Terminal;
        AwaitingRestore + RouteRemoved => RouteLost;
        CapturingReplay + RouteRemoved => RouteLost;
        HeldRestore + RouteRemoved => RouteLost;
        PreparedRestore + RouteRemoved => RouteLost;
        StartingReader + RouteRemoved => RouteLost;
        Active + RouteRemoved => RouteLost;
        OfferingClose + RouteRemoved => RouteLost;
        QueuedRestore + RouteRemoved => RouteLost;
        ClosingRestore + RouteRemoved => RouteLost;
        ClosingLive + RouteRemoved => RouteLost;
        CloseRefusedLive + RouteRemoved => RouteLost;
        RouteLost + CloseConfirmed => Terminal;
        RouteLost + CloseRefused => Terminal;
        RouteLost + CloseUnknown => Terminal;
        RouteLost + Retire => Terminal;
        RouteLost + ReportRouteLost => RouteLost;
        AwaitingCreate + WriterStopped => StoppedAwaitingReply;
        AwaitingRestore + WriterStopped => StoppedAwaitingReply;
        HeldCreate + WriterStopped => Terminal;
        ConfiguringCreate + WriterStopped => StoppedAwaitingReply;
        ConfiguredCreate + WriterStopped => Terminal;
        CapturingReplay + WriterStopped => Terminal;
        HeldRestore + WriterStopped => Terminal;
        PreparedRestore + WriterStopped => Terminal;
        StartingReader + WriterStopped => Terminal;
        Active + WriterStopped => Terminal;
        OfferingClose + WriterStopped => Terminal;
        OfferingClose + WriterStoppedAfterOffer => StoppedAwaitingReply;
        ClosingRestore + WriterStopped => StoppedAwaitingReply;
        ClosingLive + WriterStopped => StoppedAwaitingReply;
        CloseRefusedLive + WriterStopped => Terminal;
        RouteLost + WriterStopped => StoppedAwaitingReply;
        RouteLost + WriterStoppedWithoutReceipt => Terminal;
        RouteLost + OpenFailed => RouteLost;
        RouteLost + WorkerExited => RouteLost;
        StoppedAwaitingReply + FinishOnStoppedWriter => Terminal;
        StoppedAwaitingReply + WriterAccepted => StoppedAwaitingReply;
        StoppedAwaitingReply + Retire => Terminal;
        StoppedAwaitingReply + StopOnWriterLoss => StoppedAwaitingReply;
    }
}

impl OpeningPhase {
    pub(super) fn advance(&mut self, event: OpeningEvent) -> bool {
        if let Ok((phase, _)) = OpeningPolicy::transition(self, event) {
            *self = phase;
            true
        } else {
            false
        }
    }

    pub(super) fn accepts(self, event: OpeningEvent) -> bool {
        OpeningPolicy::transition(&self, event).is_ok()
    }

    /// Project owner actions that the actual state machine admits. Wire,
    /// reader and provider facts do not appear as caller actions.
    pub(super) fn refusal(self) -> LifecycleRefusal {
        use OpeningEvent as Event;
        if self.accepts(Event::OpenCreate) {
            LifecycleRefusal::OpeningNotHeld
        } else if self.accepts(Event::OpenRestore) {
            LifecycleRefusal::BoundSessionNotLoaded
        } else if self.accepts(Event::Configure) {
            LifecycleRefusal::OpeningAlreadyHeld(HeldOpeningKind::Create)
        } else if self.accepts(Event::AcceptSettings) {
            LifecycleRefusal::OpeningAlreadyHeld(HeldOpeningKind::Restore)
        } else if self.accepts(Event::Activate) {
            LifecycleRefusal::OpeningPrepared
        } else if self.accepts(Event::WaitOpening) {
            LifecycleRefusal::OpeningInProgress
        } else if self.accepts(Event::WaitClose) {
            LifecycleRefusal::RouteClosing
        } else if self.accepts(Event::AdmitAgain) && self.accepts(Event::Retire) {
            LifecycleRefusal::CloseAlreadyRefused
        } else if self.accepts(Event::ReportRouteLost) && self.accepts(Event::Retire) {
            LifecycleRefusal::BoundRouteUnavailable
        } else if self.accepts(Event::StopOnWriterLoss) {
            LifecycleRefusal::Writer(WriterState::Stopped)
        } else if [Event::KeepActive, Event::CloseAsked, Event::Retire]
            .into_iter()
            .all(|event| self.accepts(event))
        {
            LifecycleRefusal::OpeningActive
        } else {
            LifecycleRefusal::OpeningEnded
        }
    }
}

#[cfg(test)]
#[path = "owner_acp_opening_policy_test.rs"]
mod tests;

pub(super) fn activation_refusal(refusal: LifecycleRefusal) -> ActivationRefusal {
    match refusal {
        LifecycleRefusal::Writer(WriterState::Stopped) => ActivationRefusal::WriterStopped,
        LifecycleRefusal::ConnectionRetired => ActivationRefusal::ConnectionRetired,
        LifecycleRefusal::AttemptMismatch => ActivationRefusal::AttemptMismatch,
        LifecycleRefusal::OpeningNotHeld => ActivationRefusal::OpeningNotHeld,
        LifecycleRefusal::OpeningInProgress => ActivationRefusal::OpeningInProgress,
        LifecycleRefusal::OpeningAlreadyHeld(kind) => ActivationRefusal::OpeningAlreadyHeld(kind),
        other => ActivationRefusal::OpeningState(other),
    }
}
