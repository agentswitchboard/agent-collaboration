//! An ACP session owner's unique binding and its lifecycle effects.

#[path = "owner_acp_lifecycle_opening.rs"]
mod opening;

use super::OwnerAcpBinding;
use crate::{
    AcpRequestPayloads, OwnerApprovalBindings,
    acp_transport::{TransportRefusal, TransportRun},
    owner_wire::{OwnerAcpWire, OwnerWireRefusal},
};
use opening::OwnerOpenings;
use provider_session_machines::{
    ProviderSettingsCatalog, SessionGeneration,
    harness_lifecycle::{
        AcceptedRunSettings, ActivationRefusal, ActivationSetup, CatalogueFact, CatalogueRequest,
        CloseFact, CreateAttempt, HarnessOwnerEvents, HarnessProvider, Held, LifecycleRefusal,
        LifecycleReply, OpenAttempt, OpenRequest, OpeningFact, OwnerHarnessBinding,
        RestoreSettingRefusal, RunRetirement, SessionCreatedSetupFact, SessionHarnessPayloads,
        SessionLifecyclePort, SettingAttempt, SettingFact, SetupReport,
    },
    session_lifecycle_machine::CloseAttempt,
    session_owner::{OwnerAddress, RunAttempt},
};
use router_clock::RuntimeClock;
use session_event_model::{CapabilityReport, ProviderAuthStatus, SessionSettings};
use std::{
    collections::BTreeSet,
    sync::{Arc, OnceLock},
};
use switchboard_harness_contracts::{
    ApprovalLease, ApprovalLeaseBinding, ApprovalLeaseRefusalReason,
    BeforeOfferApprovalLeaseRefusal, CancelDrainStart, CancelDrainWait, DispatchAuthorization,
    DispatchResult, ExactTurn, HandOverRefusal, HarnessBinding, HarnessContent,
    HarnessOwnerRecovery, InputId, ObservedTurnApprovalLeaseRefusal, OwnerEvidence,
    OwnerIncarnation, ProviderSettingKind, ReplyAdmission, ReservedWorkReply, SettingOffer,
    SettingValueId, SteerIdlePolicy, TakeBackRefusal, TurnId, TurnMembership, WorkOfferRefusal,
    WorkReply, WriterState,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub struct OwnerAcpProvider<Port: AcpRequestPayloads> {
    wire: Arc<OwnerAcpWire>,
    approvals: OwnerApprovalBindings<Port>,
    clock: Arc<dyn RuntimeClock>,
    tasks: TaskTracker,
    connection_retired: CancellationToken,
}

impl<Port: AcpRequestPayloads> OwnerAcpProvider<Port> {
    #[must_use]
    pub fn new(
        wire: Arc<OwnerAcpWire>,
        approvals: OwnerApprovalBindings<Port>,
        clock: Arc<dyn RuntimeClock>,
        tasks: TaskTracker,
        connection_retired: CancellationToken,
    ) -> Self {
        Self {
            wire,
            approvals,
            clock,
            tasks,
            connection_retired,
        }
    }
}

impl<Port: AcpRequestPayloads> HarnessOwnerRecovery for OwnerAcpProvider<Port> {
    fn give_up_owner(&self, owner: OwnerIncarnation, inputs: &BTreeSet<InputId>) -> OwnerEvidence {
        self.wire.abandon_incarnation(owner, inputs)
    }
}

impl<Port: AcpRequestPayloads> HarnessProvider<SessionHarnessPayloads<Port::Context>>
    for OwnerAcpProvider<Port>
{
    fn bind(
        &self,
        owner: OwnerAddress,
        events: Arc<dyn HarnessOwnerEvents<ProviderAuthStatus>>,
    ) -> Box<dyn OwnerHarnessBinding<SessionHarnessPayloads<Port::Context>>> {
        let provider_session = Arc::new(OnceLock::new());
        let work = OwnerAcpBinding::with_session_slot(
            Arc::clone(&self.wire),
            Arc::clone(&provider_session),
            self.approvals.clone(),
            self.tasks.clone(),
        );
        let openings = OwnerOpenings::new(
            owner,
            Arc::clone(&self.wire),
            provider_session,
            Arc::clone(&self.clock),
            self.tasks.clone(),
            self.connection_retired.clone(),
        );
        self.wire.register_owner_events(owner.incarnation, &events);
        let owner_stopped = CancellationToken::new();
        let stopped = owner_stopped.clone();
        let connection_retired = self.connection_retired.clone();
        let reports = Arc::clone(&events);
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                () = stopped.cancelled() => {}
                () = async {
                    connection_retired.cancelled().await;
                    let _owner_stopped = reports.report_harness_gone().await;
                } => {}
            }
        });
        Box::new(AcpOwnerLifecycleBinding {
            work,
            approvals: self.approvals.clone(),
            openings,
            owner_events: events,
            owner_stopped,
        })
    }

    fn model_catalogue(
        &self,
        request: CatalogueRequest,
        reply: LifecycleReply<CatalogueFact>,
    ) -> Result<(), LifecycleRefusal> {
        let CatalogueRequest::Session { owner, run } = request else {
            return Err(LifecycleRefusal::CatalogueUnavailable);
        };
        let fact = self.wire.session_catalog(transport_run(owner, run)).map_or(
            CatalogueFact::Unavailable(LifecycleRefusal::CatalogueUnavailable),
            |catalogue| CatalogueFact::Reported(catalogue.into_record()),
        );
        reply.send(fact);
        Ok(())
    }
}

struct AcpOwnerLifecycleBinding<Port: AcpRequestPayloads> {
    work: OwnerAcpBinding<Port>,
    approvals: OwnerApprovalBindings<Port>,
    openings: OwnerOpenings,
    owner_events: Arc<dyn HarnessOwnerEvents<ProviderAuthStatus>>,
    owner_stopped: CancellationToken,
}

impl<Port: AcpRequestPayloads> Drop for AcpOwnerLifecycleBinding<Port> {
    fn drop(&mut self) {
        self.owner_stopped.cancel();
        self.work
            .wire
            .unregister_owner_events(self.openings.owner().incarnation, &self.owner_events);
    }
}

impl<Port: AcpRequestPayloads> OwnerHarnessBinding<SessionHarnessPayloads<Port::Context>>
    for AcpOwnerLifecycleBinding<Port>
{
    fn open(
        &mut self,
        request: OpenRequest,
        reply: LifecycleReply<OpeningFact>,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        self.openings.open(request, reply, events, retired)
    }

    fn configure_created(
        &self,
        attempt: CreateAttempt,
        reply: LifecycleReply<SessionCreatedSetupFact>,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        self.openings.configure_created(attempt, reply, retired)
    }

    fn put_setting_back(
        &self,
        attempt: SettingAttempt,
        reserve_reply: &mut dyn FnMut() -> ReplyAdmission<SettingFact>,
    ) -> Result<(), RestoreSettingRefusal> {
        self.openings.put_setting_back(attempt, reserve_reply)
    }

    fn close(
        &self,
        attempt: RunAttempt<CloseAttempt>,
        reply: LifecycleReply<CloseFact>,
    ) -> Result<(), LifecycleRefusal> {
        if let Some(refusal) = self.openings.writer_refusal() {
            return Err(refusal);
        }
        let session = self
            .work
            .session()
            .map_err(|_| LifecycleRefusal::RouteUnavailable)?;
        self.work
            .wire
            .enqueue_lifecycle_close(
                self.openings.owner().incarnation,
                attempt,
                session.clone(),
                reply,
            )
            .map_err(lifecycle_wire_refusal)
    }

    fn opening_is_held(&self, attempt: OpenAttempt) -> Held {
        self.openings.opening_is_held(attempt)
    }

    fn accept_settings(
        &self,
        attempt: OpenAttempt,
        accepted: &AcceptedRunSettings,
    ) -> Result<SetupReport<CapabilityReport, SessionSettings>, LifecycleRefusal> {
        self.openings.accept_settings(attempt, accepted)
    }

    fn activate(
        &self,
        setup: ActivationSetup,
        _accepted: AcceptedRunSettings,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), ActivationRefusal> {
        self.openings.activate(setup, events, retired)
    }

    fn use_settings(&self, generation: SessionGeneration, accepted: AcceptedRunSettings) {
        self.work.wire.publish_run_settings(
            transport_run(self.openings.owner(), generation),
            provider_session_machines::session_owner::AcceptedRunSettings {
                catalogue: Arc::new(ProviderSettingsCatalog::from_record(accepted.catalogue)),
                router_auto_requested: accepted.router_auto_requested,
            },
        );
    }

    fn admit_again(&self, refused: RunAttempt<CloseAttempt>) {
        self.work.wire.reopen_route(
            transport_run(self.openings.owner(), refused.generation),
            refused.attempt,
        );
    }

    fn retire(&self, generation: SessionGeneration) {
        if let Ok(session) = self.work.session() {
            self.approvals
                .retire_run(session, transport_run(self.openings.owner(), generation));
        }
        self.openings.retire(generation);
    }

    fn set_scope(&self, change: TurnMembership) {
        if let Ok(session) = self.work.session() {
            self.approvals
                .apply_membership(session, self.openings.owner().incarnation, change);
        }
        self.work
            .wire
            .apply_membership(self.openings.owner().incarnation, change);
    }

    #[cfg(feature = "test-observation")]
    fn detached_take_for_test(
        &self,
        authorization: DispatchAuthorization,
    ) -> Option<switchboard_harness_contracts::DetachedTake> {
        let wire = Arc::clone(&self.work.wire);
        Some(Box::new(move || {
            wire.hand_over_work(&authorization, || true)
        }))
    }
}

impl<Port: AcpRequestPayloads> HarnessBinding for AcpOwnerLifecycleBinding<Port> {
    fn offer_prompt(
        &self,
        authorization: DispatchAuthorization,
        turn: TurnId,
        content: HarnessContent<'_>,
        reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        self.work.offer_prompt(authorization, turn, content, reply)
    }

    fn offer_steer(
        &self,
        authorization: DispatchAuthorization,
        content: HarnessContent<'_>,
        idle: SteerIdlePolicy,
        reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        self.work.offer_steer(authorization, content, idle, reply)
    }

    fn offer_setting(
        &self,
        authorization: DispatchAuthorization,
        kind: ProviderSettingKind,
        wanted: SettingValueId,
        reply: ReservedWorkReply,
    ) -> Result<SettingOffer, WorkOfferRefusal> {
        self.work.offer_setting(authorization, kind, wanted, reply)
    }

    fn hand_over(
        &self,
        authorization: &DispatchAuthorization,
        still_requested: &mut dyn FnMut() -> bool,
    ) -> Result<DispatchResult, HandOverRefusal> {
        self.work.hand_over(authorization, still_requested)
    }

    fn take_back(
        &mut self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, TakeBackRefusal> {
        self.work.take_back(authorization)
    }

    fn begin_cancel(
        &mut self,
        authorization: DispatchAuthorization,
        reserve_reply: &mut dyn FnMut() -> ReplyAdmission<WorkReply>,
        retired: RunRetirement,
    ) -> CancelDrainStart {
        self.work
            .begin_cancel(authorization, reserve_reply, retired)
    }

    fn send_cancel(
        &mut self,
        authorization: DispatchAuthorization,
        wait: CancelDrainWait,
    ) -> DispatchResult {
        #[cfg(feature = "test-observation")]
        let run = transport_run(self.openings.owner(), authorization.run);
        let result = self.work.send_cancel(authorization, wait);
        #[cfg(feature = "test-observation")]
        self.work
            .wire
            .fence_dispatch_for_test(run, crate::owner_wire::FixtureDispatchPoint::Cancel);
        result
    }
}

impl<Port: AcpRequestPayloads> ApprovalLeaseBinding<Port::Context>
    for AcpOwnerLifecycleBinding<Port>
{
    fn install_approval_lease_before_offer(
        &self,
        turn: ExactTurn,
        context: Port::Context,
    ) -> Result<Box<dyn ApprovalLease>, BeforeOfferApprovalLeaseRefusal> {
        self.install_lease(turn, context)
            .map_err(BeforeOfferApprovalLeaseRefusal)
    }

    fn install_approval_lease_for_observed_turn(
        &self,
        turn: ExactTurn,
        context: Port::Context,
    ) -> Result<Box<dyn ApprovalLease>, ObservedTurnApprovalLeaseRefusal> {
        self.install_lease(turn, context)
            .map_err(ObservedTurnApprovalLeaseRefusal)
    }
}

impl<Port: AcpRequestPayloads> AcpOwnerLifecycleBinding<Port> {
    fn install_lease(
        &self,
        turn: ExactTurn,
        context: Port::Context,
    ) -> Result<Box<dyn ApprovalLease>, ApprovalLeaseRefusalReason> {
        if turn.run.incarnation != self.openings.owner().incarnation {
            return Err(ApprovalLeaseRefusalReason::ContextUnavailable);
        }
        let session = self
            .work
            .session()
            .map_err(|_| ApprovalLeaseRefusalReason::ContextUnavailable)?;
        self.approvals
            .install_lease(session.clone(), turn.run, turn.turn, context)
            .map(|lease| Box::new(lease) as Box<dyn ApprovalLease>)
    }
}

fn transport_run(owner: OwnerAddress, generation: SessionGeneration) -> TransportRun {
    TransportRun {
        incarnation: owner.incarnation,
        generation,
    }
}

fn lifecycle_transport_refusal(refusal: TransportRefusal) -> LifecycleRefusal {
    match refusal {
        TransportRefusal::RouteUnavailable => LifecycleRefusal::RouteUnavailable,
        TransportRefusal::Closing => LifecycleRefusal::RouteClosing,
        TransportRefusal::AuthorizationMismatch => LifecycleRefusal::AttemptMismatch,
        TransportRefusal::WriterFull => LifecycleRefusal::Writer(WriterState::Full),
        TransportRefusal::WriterStopped => LifecycleRefusal::Writer(WriterState::Stopped),
    }
}

fn lifecycle_wire_refusal(refusal: OwnerWireRefusal) -> LifecycleRefusal {
    match refusal {
        OwnerWireRefusal::Transport(refusal) => lifecycle_transport_refusal(refusal),
        OwnerWireRefusal::Encoding => LifecycleRefusal::Encoding,
    }
}
