//! Snapshot one route's immutable restore registration at notification ingress.

use super::{AcpTransport, LifecycleRoute, TransportRun};
use crate::owner_opening_instance::OpeningInstance;

#[derive(Clone)]
pub(crate) enum RestoreSelection {
    NoRoute,
    UntappedRoute,
    Registered {
        run: TransportRun,
        instance: OpeningInstance,
    },
}

impl<Frame> AcpTransport<Frame> {
    pub(crate) fn select_restore_registration(&self, session: &str) -> RestoreSelection {
        let state = self.state();
        let Some((run, route)) = state.find_route_for_session(session) else {
            return RestoreSelection::NoRoute;
        };
        match &route.restore_registration {
            Some(instance) => RestoreSelection::Registered {
                run,
                instance: instance.clone(),
            },
            None => RestoreSelection::UntappedRoute,
        }
    }

    pub(crate) fn attach_restore_registration(
        &self,
        run: TransportRun,
        route: Option<&LifecycleRoute>,
        instance: &OpeningInstance,
    ) -> bool {
        let mut state = self.state();
        let Some(entry) = state.routes.get_mut(&run).filter(|entry| {
            route.map_or_else(
                || entry.lifecycle.is_none(),
                |route| !route.is_removed() && entry.names(route),
            )
        }) else {
            return false;
        };
        entry.restore_registration = Some(instance.clone());
        true
    }

    pub(crate) fn clear_restore_registration(&self, run: TransportRun, instance: &OpeningInstance) {
        if let Some(entry) = self.state().routes.get_mut(&run)
            && entry
                .restore_registration
                .as_ref()
                .is_some_and(|current| current.is_same_as(instance))
        {
            entry.restore_registration = None;
        }
    }
}
