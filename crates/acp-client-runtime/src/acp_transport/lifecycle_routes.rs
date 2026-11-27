//! Install exact lifecycle routes and take their ordered writer offers.

use super::{
    AcpRoute, AcpTransport, LifecycleRoute, OrderedOutput, TransportRefusal, TransportRun,
    writer_refusal,
};
use crate::in_folder_allowance::InFolderGrant;
use provider_session_machines::session_lifecycle_machine::CloseAttempt;
use switchboard_harness_shared_mechanics::opening::OpeningOffer;
use switchboard_harness_shared_mechanics::run_ledger::ReadmitOutcome;

impl<Frame> AcpTransport<Frame> {
    pub(crate) fn activate_lifecycle_route(&self, route: &LifecycleRoute) -> bool {
        let mut state = self.state();
        !route.is_removed()
            && state
                .routes
                .get(&route.run)
                .is_some_and(|current| current.names(route))
            && state.ledger.activate_run(route.run)
    }

    pub(crate) fn has_lifecycle_route(&self, route: &LifecycleRoute) -> bool {
        !route.is_removed()
            && self
                .state()
                .routes
                .get(&route.run)
                .is_some_and(|current| current.names(route))
    }

    pub(crate) fn install_lifecycle_folders(&self, route: &LifecycleRoute, folders: InFolderGrant) {
        if let Some(current) = self.state().routes.get_mut(&route.run)
            && !route.is_removed()
            && current.names(route)
        {
            current.folders = Some(folders);
        }
    }

    pub(crate) fn install_lifecycle_route(
        &self,
        route: &LifecycleRoute,
        session: String,
    ) -> Result<bool, TransportRefusal> {
        let (replaced, previous) = {
            let mut state = self.state();
            if route.is_removed()
                || state.routes.get(&route.run).is_some_and(|current| {
                    !current
                        .lifecycle
                        .as_ref()
                        .is_some_and(LifecycleRoute::is_removed)
                })
            {
                return Err(TransportRefusal::RouteUnavailable);
            }
            let retired = state.ledger.replace_run(route.run, session);
            if let Some(retired) = &retired {
                state.claims.retire(&retired.session, retired.run);
            }
            state.claims.open(route.run);
            let previous = state
                .routes
                .insert(route.run, AcpRoute::new(Some(route.clone())));
            state.debug_check_run_keys();
            debug_assert_eq!(previous.is_some(), retired.is_some());
            (retired.is_some(), previous)
        };
        if let Some(previous) = previous.and_then(|previous| previous.lifecycle) {
            previous.notify_removed();
        }
        Ok(replaced)
    }

    pub(crate) fn retire_lifecycle_route(&self, route: &LifecycleRoute) -> bool {
        let retired = {
            let mut state = self.state();
            if !state
                .routes
                .get(&route.run)
                .is_some_and(|current| current.names(route))
            {
                return false;
            }
            let retired = state.routes.remove(&route.run);
            if let Some(record) = state.ledger.retire_run(route.run) {
                route.mark_removed();
                state.claims.retire(&record.session, record.run);
            }
            state.debug_check_run_keys();
            retired
        };
        route.notify_removed();
        retired.is_some()
    }

    pub(crate) fn offer_lifecycle_open(
        &self,
        run: TransportRun,
        route: Option<&LifecycleRoute>,
        offer: &OpeningOffer,
        frame: Frame,
    ) -> Result<(), TransportRefusal> {
        let state = self.state();
        if let Some(route) = route
            && (route.is_removed()
                || !state
                    .routes
                    .get(&run)
                    .is_some_and(|current| current.names(route)))
        {
            return Err(TransportRefusal::RouteUnavailable);
        }
        self.enqueue_opening_claim(run, offer, frame)
    }

    pub(crate) fn offer_lifecycle_close(
        &self,
        route: &LifecycleRoute,
        attempt: CloseAttempt,
        offer: &OpeningOffer,
        frame: Frame,
    ) -> Result<(), TransportRefusal> {
        let mut state = self.state();
        state
            .routes
            .get(&route.run)
            .filter(|current| !route.is_removed() && current.names(route))
            .ok_or(TransportRefusal::RouteUnavailable)?;
        let close = state.ledger.begin_close(route.run)?;
        self.enqueue_opening_claim(route.run, offer, frame)?;
        close.mark(attempt);
        Ok(())
    }

    pub(crate) fn enqueue_lifecycle_request(
        &self,
        route: &LifecycleRoute,
        claim: &OpeningOffer,
        frame: Frame,
    ) -> Result<(), TransportRefusal> {
        let state = self.state();
        state
            .routes
            .get(&route.run)
            .filter(|current| !route.is_removed() && current.names(route))
            .ok_or(TransportRefusal::RouteUnavailable)?;
        state.ledger.check_takes_requests(route.run)?;
        self.enqueue_opening_claim(route.run, claim, frame)
    }

    pub(crate) fn reopen_lifecycle_route(
        &self,
        route: &LifecycleRoute,
        attempt: CloseAttempt,
    ) -> ReadmitOutcome {
        let mut state = self.state();
        if !state
            .routes
            .get(&route.run)
            .is_some_and(|entry| !route.is_removed() && entry.names(route))
        {
            return ReadmitOutcome::RouteLost;
        }
        state.ledger.readmit(route.run, attempt)
    }

    #[cfg(any(test, feature = "test-observation"))]
    pub(crate) fn closing_marker_for_test(&self, run: TransportRun) -> Option<CloseAttempt> {
        self.state().ledger.closing_marker(run)
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn opening_route_is_registered_for_test(&self, run: TransportRun) -> bool {
        self.state()
            .routes
            .get(&run)
            .is_some_and(|entry| entry.lifecycle.is_some())
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn replace_closing_marker_for_test(
        &self,
        run: TransportRun,
        previous: CloseAttempt,
        replacement: CloseAttempt,
    ) -> bool {
        let mut state = self.state();
        if state.ledger.readmit(run, previous) != ReadmitOutcome::Reopened {
            return false;
        }
        state.ledger.begin_close(run).is_ok_and(|close| {
            close.mark(replacement);
            true
        })
    }

    fn enqueue_opening_claim(
        &self,
        run: TransportRun,
        offer: &OpeningOffer,
        frame: Frame,
    ) -> Result<(), TransportRefusal> {
        let permit = self.ordered.try_reserve().map_err(writer_refusal)?;
        if !offer.take() {
            return Err(TransportRefusal::AuthorizationMismatch);
        }
        permit.send(OrderedOutput { run, frame });
        Ok(())
    }
}
