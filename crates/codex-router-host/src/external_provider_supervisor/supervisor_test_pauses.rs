//! Test-only pauses that hold an operation inside one of the supervisor's
//! race windows: the first receiver fires there, and sending on the returned
//! sender lets it go on.

use super::{AdmissionTestPause, ExternalProviderSupervisor, OperationId};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ReconcilePausePoint {
    BeforeStore,
    HoldingStore,
}

impl AdmissionTestPause {
    async fn wait(self) {
        let _test_ended = self.entered.send(());
        let _test_ended = self.release.await;
    }
}

impl ExternalProviderSupervisor {
    pub(crate) fn hold_binding_wide_dispatch_for_test(
        &self,
        binding: &collaboration_protocol::ProviderBindingIdentity,
    ) -> OperationId {
        use super::provider_admission_policy::AdmissionMode;
        let operation = OperationId::generate();
        let live = super::LiveOperation::reserving(
            binding,
            None,
            collaboration_protocol::ProviderOperationKind::ConversationCreate,
            AdmissionMode::HANDOFF,
        );
        live.admitted();
        live.dispatch_marked();
        self.inner
            .live_operations
            .lock()
            .expect("operation index")
            .operations
            .insert(operation.clone(), super::Arc::new(live));
        operation
    }

    pub(crate) fn observe_turn_start_fixture(
        &self,
        target: &collaboration_protocol::SessionRef,
        wait: crate::binding_store_fixture::BindingFixtureWait,
    ) -> tokio::sync::oneshot::Receiver<()> {
        self.inner
            .uncertainty
            .slot(&target.endpoint)
            .fixture
            .notice_wait(target.clone(), wait)
    }

    pub(crate) fn hold_pending_dispatch_for_test(
        &self,
        target: &collaboration_protocol::SessionRef,
        operation: OperationId,
    ) -> crate::binding_dispatch_permit::PendingDispatch {
        let cell = self.inner.uncertainty.slot(&target.endpoint);
        crate::binding_dispatch_permit::lock(cell.outcomes.get().expect("binding outcomes"))
            .begin_dispatch(cell.clone(), operation, target.clone())
            .expect("no earlier dispatch intent")
    }

    pub(crate) fn observe_direct_dispatch_wait_for_test(
        &self,
        target: &collaboration_protocol::SessionRef,
        operation: OperationId,
    ) -> tokio::sync::oneshot::Receiver<()> {
        self.inner
            .uncertainty
            .slot(&target.endpoint)
            .fixture
            .notice_wait(
                target.clone(),
                crate::binding_store_fixture::BindingFixtureWait::DirectDispatchWaiting(operation),
            )
    }

    pub(crate) fn wake_eligibility_for_test(&self, target: &collaboration_protocol::SessionRef) {
        self.inner
            .uncertainty
            .slot(&target.endpoint)
            .eligibility_changed();
    }

    pub(crate) fn observe_direct_dispatch_block_for_test(
        &self,
        target: &collaboration_protocol::SessionRef,
        operation: OperationId,
    ) -> tokio::sync::oneshot::Receiver<()> {
        self.inner
            .uncertainty
            .slot(&target.endpoint)
            .fixture
            .notice_wait(
                target.clone(),
                crate::binding_store_fixture::BindingFixtureWait::DirectDispatchBlocked(operation),
            )
    }

    pub(crate) fn observe_ended_turn_admission_for_test(
        &self,
        target: &collaboration_protocol::SessionRef,
        holder: OperationId,
    ) -> tokio::sync::oneshot::Receiver<()> {
        self.inner
            .uncertainty
            .slot(&target.endpoint)
            .fixture
            .notice_wait(
                target.clone(),
                crate::binding_store_fixture::BindingFixtureWait::EndedTurnAdmission(holder),
            )
    }

    pub(crate) fn assert_only_turn_start_blocks_admission_for_test(
        &self,
        target: &collaboration_protocol::SessionRef,
        holder: &OperationId,
    ) {
        use super::provider_admission_policy::{AdmissionClass, AdmissionMode, AdmissionRequest};
        use collaboration_protocol::ProviderOperationKind;

        let cell = self.inner.uncertainty.slot(&target.endpoint);
        let binding = cell.identity.get().expect("bound identity");
        let outcomes = crate::binding_dispatch_permit::lock(
            cell.outcomes.get().expect("initialized eligibility"),
        );
        assert!(outcomes.pending_dispatches_for(target).next().is_none());
        assert!(outcomes.blockers(Some(target), false).is_empty());
        let operations = self.inner.live_operations.lock().expect("operation index");
        let blockers = operations.admission_blockers(
            binding,
            AdmissionRequest {
                operation: &OperationId::generate(),
                kind: ProviderOperationKind::ConversationPrompt,
                target: Some(target),
                mode: AdmissionMode::deferred(AdmissionClass::Independent),
            },
        );
        assert_eq!(blockers.as_slice(), std::slice::from_ref(holder));
        let held = operations.operations.get(holder).expect("live holder");
        assert!(held.is_turn_start_for(target));
        assert!(held.holds_custody());
    }

    pub(crate) fn pause_steer_mark_receipt_for_test(
        &self,
        target: &collaboration_protocol::SessionRef,
        operation: OperationId,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        self.inner
            .uncertainty
            .slot(&target.endpoint)
            .fixture
            .pause_steer_mark_receipt(operation)
    }

    pub(crate) fn pause_undo_receipt_for_test(
        &self,
        target: &collaboration_protocol::SessionRef,
        operation: OperationId,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        self.inner
            .uncertainty
            .slot(&target.endpoint)
            .fixture
            .pause_undo_receipt(operation)
    }

    pub(super) async fn pause_before_owner_prompt_for_test(&self, operation: &OperationId) {
        let pause = self
            .inner
            .before_owner_prompt_test_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_if(|(selected, _)| selected == operation);
        if let Some((_, pause)) = pause {
            pause.wait().await;
        }
    }

    pub(super) async fn pause_before_admission_reservation_for_test(&self) {
        let pause = self
            .inner
            .admission_before_reservation_test_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(pause) = pause {
            pause.wait().await;
        }
    }

    pub(super) async fn pause_reconcile_for_test(&self, point: ReconcilePausePoint) {
        let pause = self
            .inner
            .reconcile_test_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_if(|(selected, _)| *selected == point);
        if let Some((_, pause)) = pause {
            pause.wait().await;
        }
    }

    pub(crate) fn tracked_operation_tasks_for_test(&self) -> tokio_util::task::TaskTracker {
        self.inner.operation_tasks.clone()
    }

    /// Pauses the next steer with its approver records pinned, before its
    /// injection is sent.
    pub(crate) fn pause_next_steer_before_injection(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, entered_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release) = tokio::sync::oneshot::channel();
        *self
            .inner
            .steer_injection_test_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(AdmissionTestPause { entered, release });
        (entered_receiver, release_sender)
    }

    /// Pauses the next steer after its injection reply and before it reads
    /// the joined turn's approver.
    pub(crate) fn pause_next_steer_after_reply(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, entered_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release) = tokio::sync::oneshot::channel();
        *self
            .inner
            .steer_reply_test_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(AdmissionTestPause { entered, release });
        (entered_receiver, release_sender)
    }

    pub(crate) fn pause_after_result_publication(
        &self,
        operation_id: OperationId,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, entered_receiver) = tokio::sync::oneshot::channel();
        let (release_sender, release) = tokio::sync::oneshot::channel();
        *self
            .inner
            .result_published_test_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((operation_id, AdmissionTestPause { entered, release }));
        (entered_receiver, release_sender)
    }
}
