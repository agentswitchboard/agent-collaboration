//! Take exact lifecycle offers at the transport's ordered writing boundary.

use super::{AcpFrame, OpenReply, OpenRequest, OwnerAcpWire, OwnerWireRefusal, ResponseKind};
use crate::{
    acp_transport::{TransportRefusal, TransportRun},
    owner_session_open::{LifecycleOpenReceipt, OwnerSessionOpenSetup},
};
use agent_client_protocol::{JsonRpcMessage as _, schema::v1::CloseSessionRequest};
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{CloseFact, LifecycleReply},
    session_lifecycle_machine::{CloseAttempt, LoadAttempt, RestoreVia},
    session_owner::{OwnerIncarnation, RunAttempt},
};
use switchboard_harness_shared_mechanics::opening::{
    LifecycleRoute, OpeningInstance, OpeningOffer,
};
use switchboard_harness_shared_mechanics::run_ledger::ReadmitOutcome;

pub(crate) struct RestoreOffer<'a> {
    pub(crate) route: &'a LifecycleRoute,
    pub(crate) instance: &'a OpeningInstance,
    pub(crate) claim: &'a OpeningOffer,
}

impl OwnerAcpWire {
    pub(crate) async fn writer_stopped(&self) {
        self.transport.writer_stopped().await;
    }

    pub(crate) fn install_opening_route(
        &self,
        route: &LifecycleRoute,
        session: String,
    ) -> Result<(), TransportRefusal> {
        let _edit = self
            .route_edits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.install_exact_route(route, session)
    }

    fn install_exact_route(
        &self,
        route: &LifecycleRoute,
        session: String,
    ) -> Result<(), TransportRefusal> {
        if self.transport.install_lifecycle_route(route, session)? {
            self.clear_run_resources(route.run);
        }
        Ok(())
    }

    pub(crate) fn retire_opening_route(&self, route: &LifecycleRoute, instance: &OpeningInstance) {
        let _edit = self
            .route_edits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.retire_exact_route(route, instance);
    }

    fn retire_exact_route(&self, route: &LifecycleRoute, instance: &OpeningInstance) {
        if self.transport.retire_lifecycle_route(route) {
            self.clear_run_resources(route.run);
        }
        self.restore_updates.retire_instance(route.run, instance);
    }

    pub(crate) fn activate_opening_route(&self, route: &LifecycleRoute) -> bool {
        self.transport.activate_lifecycle_route(route)
    }

    pub(crate) fn register_opening_reader(
        &self,
        route: &LifecycleRoute,
    ) -> Option<super::PromptTerminalReceiver> {
        let _edit = self
            .route_edits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.transport
            .has_lifecycle_route(route)
            .then(|| self.register_reader(route.run))
    }

    pub(crate) fn register_opening_folders(
        &self,
        route: &LifecycleRoute,
        grant: crate::in_folder_allowance::InFolderGrant,
    ) {
        self.transport.install_lifecycle_folders(route, grant);
    }

    pub(crate) fn reopen_opening_route(
        &self,
        route: &LifecycleRoute,
        close: CloseAttempt,
    ) -> ReadmitOutcome {
        self.transport.reopen_lifecycle_route(route, close)
    }

    #[cfg(any(test, feature = "test-observation"))]
    pub fn closing_marker_for_test(&self, run: TransportRun) -> Option<CloseAttempt> {
        self.transport.closing_marker_for_test(run)
    }

    pub(crate) fn offer_create(
        &self,
        owner: OwnerIncarnation,
        setup: OwnerSessionOpenSetup,
        claim: &OpeningOffer,
    ) -> Result<LifecycleOpenReceipt, TransportRefusal> {
        let (frame, result) = self.prepare_open(OpenRequest::New(setup), OpenReply::Lifecycle);
        self.transport.offer_lifecycle_open(
            TransportRun {
                incarnation: owner,
                generation: SessionGeneration::UNTRACKED,
            },
            None,
            claim,
            frame,
        )?;
        Ok(result)
    }

    pub(crate) fn offer_restore(
        &self,
        offer: RestoreOffer<'_>,
        attempt: LoadAttempt,
        session: String,
        via: RestoreVia,
        setup: OwnerSessionOpenSetup,
    ) -> Result<LifecycleOpenReceipt, TransportRefusal> {
        let RestoreOffer {
            route,
            instance,
            claim,
        } = offer;
        let _edit = self
            .route_edits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.install_exact_route(route, session.clone())?;
        if !self
            .transport
            .attach_restore_registration(route.run, Some(route), instance)
        {
            self.retire_exact_route(route, instance);
            return Err(TransportRefusal::RouteUnavailable);
        }
        let updates = self
            .restore_updates
            .open_instance(route.run, session.clone(), instance);
        let (frame, result) = self.prepare_open(
            OpenRequest::Restore {
                provider_id: session,
                attempt,
                tap: instance.clone(),
                updates,
                via,
                setup,
            },
            OpenReply::Lifecycle,
        );
        if let Err(refusal) =
            self.transport
                .offer_lifecycle_open(route.run, Some(route), claim, frame)
        {
            self.restore_updates.discard_unoffered(route.run, instance);
            self.retire_exact_route(route, instance);
            return Err(refusal);
        }
        Ok(result)
    }

    pub(crate) fn offer_close(
        &self,
        route: &LifecycleRoute,
        run: RunAttempt<CloseAttempt>,
        session: String,
        claim: &OpeningOffer,
        completion: LifecycleReply<CloseFact>,
    ) -> Result<(), OwnerWireRefusal> {
        let message = CloseSessionRequest::new(session)
            .to_untyped_message()
            .map_err(|_| OwnerWireRefusal::Encoding)?;
        let frame = AcpFrame::Request {
            message,
            correlation: ResponseKind::Close { run, completion },
        };
        Ok(self
            .transport
            .offer_lifecycle_close(route, run.attempt, claim, frame)?)
    }
}
