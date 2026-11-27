//! Ordered ACP output and the transport's authorization fence.

mod lifecycle_routes;
mod restore_registration;
pub(crate) use lifecycle_routes::ReadmitOutcome;
pub(crate) use restore_registration::RestoreSelection;

use crate::callback_router::ToolClaims;
pub use crate::callback_router::{CallbackKind, CallbackResolution};
use crate::in_folder_allowance::{InFolderGrant, InFolderPermission};
use crate::owner_opening_instance::OpeningInstance;
pub use crate::transport_run::TransportRun;
use agent_client_protocol::schema::v1::PermissionOptionId;
use provider_session_machines::{
    SessionGeneration,
    session_lifecycle_machine::CloseAttempt,
    session_owner::{
        AcceptedRunSettings, DispatchAuthorization, DispatchResult, OwnerIncarnation,
        TurnMembership,
    },
    session_turn_machine::TurnId,
    tool_ownership::{ToolObservation, ToolOwnershipEffect},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use switchboard_harness_contracts::SlotEvidence;
use switchboard_harness_contracts::{HandOverRefusal, InputId, OwnerEvidence};
use switchboard_harness_shared_mechanics::run_ledger::{
    HandOverCheck, HandOverCommit, MembershipOutcome, NoSuchOffer, OfferRefusal, RefusedOffer,
    RouteRefusal, RunLedger, RunOpening,
};

#[cfg(test)]
use provider_session_machines::session_owner::{AuthorizationTarget, OwnerInputEvidence};

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TransportRefusal {
    #[error("the exact ACP route is unavailable")]
    RouteUnavailable,
    #[error("the ACP route is already closing")]
    Closing,
    #[error("the ACP authorization does not match the slot")]
    AuthorizationMismatch,
    #[error("the ACP writer queue is full")]
    WriterFull,
    #[error("the ACP writer stopped")]
    WriterStopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportOfferRefusal {
    RouteUnavailable,
    Closing,
    WorkDoesNotFitAuthorization,
    OtherAuthorizationOffered,
    AuthorizationSequenceOnRecord,
}

impl From<TransportOfferRefusal> for TransportRefusal {
    fn from(refusal: TransportOfferRefusal) -> Self {
        match refusal {
            TransportOfferRefusal::RouteUnavailable => Self::RouteUnavailable,
            TransportOfferRefusal::Closing => Self::Closing,
            TransportOfferRefusal::WorkDoesNotFitAuthorization
            | TransportOfferRefusal::OtherAuthorizationOffered
            | TransportOfferRefusal::AuthorizationSequenceOnRecord => Self::AuthorizationMismatch,
        }
    }
}

impl From<OfferRefusal> for TransportOfferRefusal {
    fn from(refusal: OfferRefusal) -> Self {
        match refusal {
            OfferRefusal::RouteUnavailable => Self::RouteUnavailable,
            OfferRefusal::Closing => Self::Closing,
            OfferRefusal::WorkDoesNotFitAuthorization => Self::WorkDoesNotFitAuthorization,
            OfferRefusal::OtherAuthorizationOffered => Self::OtherAuthorizationOffered,
            OfferRefusal::AuthorizationSequenceOnRecord => Self::AuthorizationSequenceOnRecord,
        }
    }
}

impl From<RouteRefusal> for TransportRefusal {
    fn from(refusal: RouteRefusal) -> Self {
        match refusal {
            RouteRefusal::RouteUnavailable => Self::RouteUnavailable,
            RouteRefusal::Closing => Self::Closing,
        }
    }
}

impl From<HandOverRefusal> for TransportRefusal {
    fn from(refusal: HandOverRefusal) -> Self {
        match refusal {
            HandOverRefusal::WriterFull => Self::WriterFull,
            HandOverRefusal::WriterStopped => Self::WriterStopped,
            HandOverRefusal::NoSuchOffer => Self::AuthorizationMismatch,
        }
    }
}

#[derive(Debug)]
pub struct OrderedOutput<Frame> {
    pub run: TransportRun,
    pub frame: Frame,
}

struct AcpRoute {
    folders: Option<InFolderGrant>,
    settings: Option<AcceptedRunSettings>,
    lifecycle: Option<LifecycleRoute>,
    restore_registration: Option<OpeningInstance>,
}

#[derive(Clone)]
pub(crate) struct LifecycleRoute {
    pub(crate) run: TransportRun,
    lifetime: Arc<RouteLifetime>,
}

struct RouteLifetime {
    removed: AtomicBool,
    notify_removed: CancellationToken,
}

impl LifecycleRoute {
    pub(crate) fn new(run: TransportRun) -> Self {
        Self {
            run,
            lifetime: Arc::new(RouteLifetime {
                removed: AtomicBool::new(false),
                notify_removed: CancellationToken::new(),
            }),
        }
    }

    pub(crate) fn is_removed(&self) -> bool {
        self.lifetime.removed.load(Ordering::Acquire)
    }

    pub(crate) fn mark_removed(&self) {
        self.lifetime.removed.store(true, Ordering::Release);
    }

    pub(crate) async fn removed(&self) {
        self.lifetime.notify_removed.cancelled().await;
    }

    pub(crate) fn notify_removed(&self) {
        self.lifetime.notify_removed.cancel();
    }

    pub(crate) fn is_same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.lifetime, &other.lifetime)
    }
}

impl AcpRoute {
    fn new(lifecycle: Option<LifecycleRoute>) -> Self {
        Self {
            folders: None,
            settings: None,
            lifecycle,
            restore_registration: None,
        }
    }

    fn names(&self, route: &LifecycleRoute) -> bool {
        self.lifecycle
            .as_ref()
            .is_some_and(|current| current.is_same_as(route))
    }
}

struct TransportState<Frame> {
    ledger: RunLedger<Frame>,
    routes: BTreeMap<TransportRun, AcpRoute>,
    claims: ToolClaims,
}

impl<Frame> TransportState<Frame> {
    fn route_for_session(&self, session: &str) -> Option<TransportRun> {
        self.find_route_for_session(session).map(|(run, _)| run)
    }

    fn find_route_for_session(&self, session: &str) -> Option<(TransportRun, &AcpRoute)> {
        let mut matches = self.ledger.runs_named(session).filter_map(|run| {
            let route = self.routes.get(&run)?;
            (!route
                .lifecycle
                .as_ref()
                .is_some_and(LifecycleRoute::is_removed))
            .then_some((run, route))
        });
        let (run, route) = matches.next()?;
        matches.next().is_none().then_some((run, route))
    }

    fn debug_check_run_keys(&self) {
        debug_assert!(self.routes.keys().copied().eq(self.ledger.run_keys()));
    }
}

/// One instance and output receiver per ACP connection. The same short lock
/// protects route, slot, callback membership and enqueue. It never spans I/O.
pub struct AcpTransport<Frame> {
    state: Mutex<TransportState<Frame>>,
    ordered: mpsc::Sender<OrderedOutput<Frame>>,
    #[cfg(all(debug_assertions, feature = "test-observation"))]
    pub(crate) handover_test: crate::handover_test_control::HandoverTestControl,
}

impl<Frame> AcpTransport<Frame> {
    pub fn new(capacity: NonZeroUsize) -> (Self, mpsc::Receiver<OrderedOutput<Frame>>) {
        let (ordered, output) = mpsc::channel(capacity.get());
        let state = Mutex::new(TransportState {
            ledger: RunLedger::default(),
            routes: BTreeMap::new(),
            claims: ToolClaims::default(),
        });
        (
            Self {
                state,
                ordered,
                #[cfg(all(debug_assertions, feature = "test-observation"))]
                handover_test: Default::default(),
            },
            output,
        )
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn hold_ordered_capacity(&self) -> Vec<mpsc::OwnedPermit<OrderedOutput<Frame>>> {
        std::iter::from_fn(|| self.ordered.clone().try_reserve_owned().ok()).collect()
    }

    fn state(&self) -> MutexGuard<'_, TransportState<Frame>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn writer_has_stopped(&self) -> bool {
        self.ordered.is_closed()
    }

    pub(crate) async fn writer_stopped(&self) {
        self.ordered.closed().await;
    }

    pub fn open_route(&self, run: TransportRun, session: String) {
        let mut state = self.state();
        if state.ledger.open_run(run, session) == RunOpening::Opened {
            state.routes.insert(run, AcpRoute::new(None));
            state.claims.open(run);
        }
        state.debug_check_run_keys();
    }

    pub fn activate_route(&self, run: TransportRun) -> bool {
        self.state().ledger.activate_run(run)
    }

    #[must_use]
    pub fn current_turn(&self, run: TransportRun) -> Option<TurnId> {
        self.state().ledger.current_turn(run)
    }

    pub(crate) fn provider_started_turn(&self, run: TransportRun) -> Option<TurnId> {
        self.state().ledger.provider_started_turn(run)
    }

    pub(crate) fn route_for_session(&self, session: &str) -> Option<TransportRun> {
        self.state().route_for_session(session)
    }

    pub(crate) fn session_for_route(&self, run: TransportRun) -> Option<String> {
        self.state().ledger.session_of(run).map(str::to_owned)
    }

    pub(crate) fn routed_owner_sessions(&self) -> Vec<(OwnerIncarnation, String)> {
        self.state()
            .ledger
            .owner_sessions()
            .map(|(owner, session)| (owner, session.to_owned()))
            .collect()
    }

    pub(crate) fn allows_explicit_tool_request(&self, session: &str, tool: &str) -> bool {
        let state = self.state();
        let Some(run) = state.route_for_session(session) else {
            return false;
        };
        state.claims.allows_explicit_tool_request(
            session,
            run,
            tool,
            state.ledger.current_turn(run),
        )
    }

    pub(crate) fn current_scope(&self, session: &str) -> Option<(TransportRun, TurnId)> {
        let state = self.state();
        let run = state.route_for_session(session)?;
        Some((run, state.ledger.current_turn(run)?))
    }

    pub(crate) fn install_folders(&self, run: TransportRun, folders: InFolderGrant) {
        if let Some(route) = self.state().routes.get_mut(&run) {
            route.folders = Some(folders);
        }
    }

    pub(crate) fn accept_settings(&self, run: TransportRun, settings: AcceptedRunSettings) {
        if let Some(route) = self.state().routes.get_mut(&run) {
            route.settings = Some(settings);
        }
    }

    pub(crate) fn catalogue(
        &self,
        run: TransportRun,
    ) -> Option<provider_session_machines::ProviderSettingsCatalog> {
        let state = self.state();
        let settings = state.routes.get(&run)?.settings.as_ref()?;
        Some(settings.catalogue.as_ref().clone())
    }

    pub(crate) fn select_in_folder_permission(
        &self,
        session: &str,
        tool: &str,
        (run, turn): (TransportRun, TurnId),
        permission: InFolderPermission,
        auto: crate::AutoPermissionMode,
    ) -> Option<PermissionOptionId> {
        let state = self.state();
        let route = state.routes.get(&run)?;
        if state.ledger.check_takes_requests(run).is_err()
            || state.route_for_session(session) != Some(run)
            || state.ledger.current_turn(run) != Some(turn)
            || !state
                .claims
                .allows_explicit_tool_request(session, run, tool, Some(turn))
        {
            return None;
        }
        let settings = route.settings.as_ref()?;
        if !crate::provider_setting_application::current_mode_grants_auto(settings, auto) {
            return None;
        }
        route.folders.as_ref()?.select(permission)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn router_auto_grant_for_test(
        &self,
        run: TransportRun,
        auto: crate::AutoPermissionMode,
    ) -> Option<bool> {
        let state = self.state();
        let route = state.routes.get(&run)?;
        route.folders.as_ref()?;
        Some(
            crate::provider_setting_application::current_mode_grants_auto(
                route.settings.as_ref()?,
                auto,
            ),
        )
    }

    pub fn apply_membership(&self, incarnation: OwnerIncarnation, change: TurnMembership) {
        let mut state = self.state();
        let retired = match state.ledger.apply_membership(incarnation, change) {
            MembershipOutcome::Applied
            | MembershipOutcome::Ignored(_)
            | MembershipOutcome::NoSuchRun => None,
            MembershipOutcome::Ended { run, turn, session } => {
                state.claims.end_turn(&session, run, turn);
                None
            }
            MembershipOutcome::Retired(retired) => {
                let route = state.routes.remove(&retired.run);
                state.claims.retire(&retired.session, retired.run);
                if let Some(lifecycle) = route.as_ref().and_then(|route| route.lifecycle.as_ref()) {
                    lifecycle.mark_removed();
                }
                state.debug_check_run_keys();
                route
            }
        };
        drop(state);
        if let Some(route) = retired.and_then(|route| route.lifecycle) {
            route.notify_removed();
        }
    }

    pub fn observe_tool(
        &self,
        run: TransportRun,
        tool_id: &str,
        observation: ToolObservation,
    ) -> ToolOwnershipEffect {
        let mut state = self.state();
        let (Some(session), Some(membership)) = (
            state.ledger.session_of(run).map(str::to_owned),
            state.ledger.membership(run),
        ) else {
            return ToolOwnershipEffect::None;
        };
        state
            .claims
            .observe(&session, run, tool_id, observation, membership)
    }

    pub fn resolve_callback(&self, tool_id: &str, kind: CallbackKind) -> CallbackResolution {
        let state = self.state();
        state.claims.resolve(&state.ledger, tool_id, kind)
    }

    pub(crate) fn tool_in_current_turn(&self, run: TransportRun, tool: &str) -> bool {
        let state = self.state();
        state.ledger.session_of(run).is_some_and(|session| {
            state
                .claims
                .tool_in_current_turn(session, run, tool, state.ledger.current_turn(run))
        })
    }

    pub fn offer(
        &self,
        authorization: DispatchAuthorization,
        new_turn: Option<TurnId>,
        frame: Frame,
    ) -> Result<(), TransportRefusal> {
        self.offer_work(authorization, new_turn, frame)
            .map_err(TransportRefusal::from)
    }

    pub(crate) fn offer_work(
        &self,
        authorization: DispatchAuthorization,
        new_turn: Option<TurnId>,
        frame: Frame,
    ) -> Result<(), TransportOfferRefusal> {
        #[cfg(all(debug_assertions, feature = "test-observation"))]
        self.handover_test.reach(
            crate::handover_test_control::HandoverPausePoint::BeforeOffer,
            &authorization,
        );
        #[cfg(all(debug_assertions, feature = "test-observation"))]
        let recorded_authorization = authorization.clone();
        let offer = {
            let mut state = self.state();
            state.ledger.offer(authorization, new_turn, frame)
        };
        offer.map_err(|RefusedOffer { refusal, frame }| {
            drop(frame);
            TransportOfferRefusal::from(refusal)
        })?;
        #[cfg(all(debug_assertions, feature = "test-observation"))]
        self.handover_test.reach(
            crate::handover_test_control::HandoverPausePoint::AfterOffer,
            &recorded_authorization,
        );
        Ok(())
    }

    /// Only the exact live token can enqueue. A full writer leaves the offer
    /// available for the same token until revoke or the owner's deadline.
    pub fn take(
        &self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, TransportRefusal> {
        self.take_when(authorization, || true)
    }

    pub(crate) fn take_when(
        &self,
        authorization: &DispatchAuthorization,
        still_requested: impl FnOnce() -> bool,
    ) -> Result<DispatchResult, TransportRefusal> {
        self.hand_over_work(authorization, still_requested)
            .map_err(TransportRefusal::from)
    }

    pub(crate) fn hand_over_work(
        &self,
        authorization: &DispatchAuthorization,
        still_requested: impl FnOnce() -> bool,
    ) -> Result<DispatchResult, HandOverRefusal> {
        let mut state = self.state();
        let ready = match state
            .ledger
            .check_hand_over(authorization)
            .map_err(|NoSuchOffer| HandOverRefusal::NoSuchOffer)?
        {
            HandOverCheck::Settled(result) => return Ok(result),
            HandOverCheck::Ready(ready) => ready,
        };
        let permit = self.ordered.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => HandOverRefusal::WriterFull,
            mpsc::error::TrySendError::Closed(_) => HandOverRefusal::WriterStopped,
        })?;
        #[cfg(all(debug_assertions, feature = "test-observation"))]
        self.handover_test.reach(
            crate::handover_test_control::HandoverPausePoint::BeforeQuestion,
            authorization,
        );
        let still_requested = still_requested();
        #[cfg(all(debug_assertions, feature = "test-observation"))]
        if still_requested {
            self.handover_test.reach(
                crate::handover_test_control::HandoverPausePoint::BeforePublication,
                authorization,
            );
        }
        match ready.commit(still_requested) {
            HandOverCommit::Send { run, frame } => {
                permit.send(OrderedOutput { run, frame });
                Ok(DispatchResult::Taken(authorization.clone()))
            }
            HandOverCommit::Unsent(result) => Ok(result),
        }
    }

    pub fn revoke(
        &self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, TransportRefusal> {
        self.state()
            .ledger
            .revoke(authorization)
            .ok_or(TransportRefusal::AuthorizationMismatch)
    }

    /// Revokes pending work while preserving proof of earlier takes, including
    /// takes belonging to a route which has already retired.
    pub fn revoke_incarnation(
        &self,
        incarnation: OwnerIncarnation,
    ) -> Vec<(DispatchAuthorization, SlotEvidence)> {
        self.state().ledger.revoke_incarnation(incarnation)
    }

    /// Withdraw pending offers and answer each asked input using the same locked ledger.
    pub(crate) fn give_up_owner(
        &self,
        incarnation: OwnerIncarnation,
        asked_inputs: &BTreeSet<InputId>,
    ) -> OwnerEvidence {
        self.state().ledger.give_up_owner(incarnation, asked_inputs)
    }

    pub(crate) fn runs_of(&self, incarnation: OwnerIncarnation) -> Vec<SessionGeneration> {
        self.state().ledger.runs_of(incarnation).collect()
    }

    /// Production retains slots across route retirement and until owner recovery.
    /// Removing one would make an earlier offer look never offered at hand-back.
    pub fn forget(&self, authorization: &DispatchAuthorization) {
        self.state().ledger.forget(authorization);
    }

    pub fn enqueue_request(&self, run: TransportRun, frame: Frame) -> Result<(), TransportRefusal> {
        let state = self.state();
        state.ledger.check_takes_requests(run)?;
        self.enqueue(run, frame)
    }

    pub(crate) fn enqueue_control(
        &self,
        run: TransportRun,
        frame: Frame,
    ) -> Result<(), TransportRefusal> {
        let _ordered_with_authorizations = self.state();
        self.enqueue(run, frame)
    }

    pub fn enqueue_close(
        &self,
        run: TransportRun,
        attempt: CloseAttempt,
        frame: Frame,
    ) -> Result<(), TransportRefusal> {
        let mut state = self.state();
        let close = state.ledger.begin_close(run)?;
        self.enqueue(run, frame)?;
        close.mark(attempt);
        Ok(())
    }

    pub fn reopen_route(&self, run: TransportRun, attempt: CloseAttempt) {
        self.state().ledger.readmit(run, attempt);
    }

    fn enqueue(&self, run: TransportRun, frame: Frame) -> Result<(), TransportRefusal> {
        self.ordered
            .try_send(OrderedOutput { run, frame })
            .map_err(writer_refusal)
    }
}

fn writer_refusal<T>(error: mpsc::error::TrySendError<T>) -> TransportRefusal {
    match error {
        mpsc::error::TrySendError::Full(_) => TransportRefusal::WriterFull,
        mpsc::error::TrySendError::Closed(_) => TransportRefusal::WriterStopped,
    }
}

#[cfg(test)]
#[path = "acp_transport_test.rs"]
mod tests;

#[cfg(test)]
#[path = "acp_transport_run_ledger_test.rs"]
mod run_ledger_tests;
