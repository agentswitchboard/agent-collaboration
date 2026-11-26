//! Install exact lifecycle routes and take their ordered writer offers.

use super::{
    AcpTransport, ConnectionRoute, LifecycleRoute, OrderedOutput, TransportRefusal, TransportRun,
    writer_refusal,
};
use crate::{in_folder_allowance::InFolderGrant, owner_opening_instance::OpeningOffer};
use provider_session_machines::session_lifecycle_machine::CloseAttempt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadmitOutcome {
    Reopened,
    RouteLost,
    MarkerAbsent,
    DifferentMarker,
}

impl<Frame> AcpTransport<Frame> {
    pub(crate) fn activate_lifecycle_route(&self, route: &LifecycleRoute) -> bool {
        let mut state = self.state();
        !route.is_removed()
            && state
                .routes
                .get(&route.run)
                .is_some_and(|current| current.names(route) && current.closing.is_none())
            && state.callbacks.activate(route.run)
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
        let previous = {
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
            let previous = state.routes.insert(
                route.run,
                ConnectionRoute::new(session, Some(route.clone())),
            );
            if let Some(previous) = &previous {
                state.callbacks.retire(&previous.session, route.run);
            }
            state.callbacks.open(route.run);
            previous
        };
        let replaced = previous.is_some();
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
            if let Some(retired) = &retired {
                route.mark_removed();
                state.callbacks.retire(&retired.session, route.run);
            }
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
        let current = state
            .routes
            .get_mut(&route.run)
            .filter(|current| !route.is_removed() && current.names(route))
            .ok_or(TransportRefusal::RouteUnavailable)?;
        if current.closing.is_some() {
            return Err(TransportRefusal::Closing);
        }
        self.enqueue_opening_claim(route.run, offer, frame)?;
        current.closing = Some(attempt);
        Ok(())
    }

    pub(crate) fn enqueue_lifecycle_request(
        &self,
        route: &LifecycleRoute,
        claim: &OpeningOffer,
        frame: Frame,
    ) -> Result<(), TransportRefusal> {
        let state = self.state();
        let current = state
            .routes
            .get(&route.run)
            .filter(|current| !route.is_removed() && current.names(route))
            .ok_or(TransportRefusal::RouteUnavailable)?;
        if current.closing.is_some() {
            return Err(TransportRefusal::Closing);
        }
        self.enqueue_opening_claim(route.run, claim, frame)
    }

    pub(crate) fn reopen_lifecycle_route(
        &self,
        route: &LifecycleRoute,
        attempt: CloseAttempt,
    ) -> ReadmitOutcome {
        let mut state = self.state();
        let Some(entry) = state
            .routes
            .get_mut(&route.run)
            .filter(|entry| !route.is_removed() && entry.names(route))
        else {
            return ReadmitOutcome::RouteLost;
        };
        match entry.closing {
            Some(current) if current == attempt => {
                entry.closing = None;
                ReadmitOutcome::Reopened
            }
            Some(_) => ReadmitOutcome::DifferentMarker,
            None => ReadmitOutcome::MarkerAbsent,
        }
    }

    #[cfg(any(test, feature = "test-observation"))]
    pub(crate) fn closing_marker_for_test(&self, run: TransportRun) -> Option<CloseAttempt> {
        self.state().routes.get(&run)?.closing
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
        let Some(route) = state
            .routes
            .get_mut(&run)
            .filter(|route| route.closing == Some(previous))
        else {
            return false;
        };
        route.closing = Some(replacement);
        true
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
