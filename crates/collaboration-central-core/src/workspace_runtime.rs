//! Organization feature composition over one Fleet runtime.
pub(crate) mod engine_admin_dispatch;
pub(crate) mod engine_dispatch;

use fleet_contracts::MailDeliveryReady;
use fleet_runtime::FleetRuntime;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_workspace_access::WorkspaceAccess;
use std::sync::Arc;

/// Composes one Fleet runtime with the retained organization features.
pub struct WorkspaceRuntime<Host: CentralStore, Stores: WorkspaceStores> {
    pub(crate) direct_messages: Arc<dyn fleet_mail::DirectMessageStore>,
    pub(crate) execution: FleetRuntime<Host, Stores>,
    pub(crate) stamp_store: Arc<dyn fleet_signin::stamp_store::StampStore>,
    pub(crate) mail_delivery_ready: Arc<dyn Fn(MailDeliveryReady) + Send + Sync>,
    pub(crate) enrollment_invalidated:
        Arc<dyn Fn(fleet_contracts::EnrollmentInvalidation) + Send + Sync>,
    pub(crate) approval_delivery_completion:
        Option<Arc<dyn Fn(crate::LinkCallCompletion) + Send + Sync>>,
    pub(crate) link_call_dispatcher: Option<Arc<dyn crate::CentralLinkDispatcher>>,
    #[cfg(feature = "test-support")]
    pub(crate) fixture_link_calls: bool,
    pub(crate) person_approval_transactions: Option<Arc<dyn crate::PersonApprovalTransactions>>,
    pub(crate) reply_due_owner: std::sync::OnceLock<std::sync::Weak<Self>>,
    pub(crate) yield_due_batches: Arc<dyn Fn() -> fleet_runtime::DueBatchYield + Send + Sync>,
    pub(crate) signin_authority: crate::organization_authority_cache::OrganizationAuthority,
    #[cfg(feature = "test-support")]
    pub(crate) machine_restore_failure:
        std::sync::Mutex<Option<(usize, central_storage::StoreFailure)>>,
    #[cfg(feature = "test-support")]
    idle_renewal_send_failure: std::sync::atomic::AtomicBool,
}
impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub fn new(
        execution: FleetRuntime<Host, Stores>,
        stamp_store: Arc<dyn fleet_signin::stamp_store::StampStore>,
        direct_messages: Arc<dyn fleet_mail::DirectMessageStore>,
    ) -> Self {
        Self {
            direct_messages,
            execution,
            stamp_store,
            approval_delivery_completion: None,
            link_call_dispatcher: None,
            #[cfg(feature = "test-support")]
            fixture_link_calls: false,
            reply_due_owner: Default::default(),
            yield_due_batches: Arc::new(|| Box::pin(std::future::ready(Ok(())))),
            signin_authority: Default::default(),
            person_approval_transactions: None,
            mail_delivery_ready: Arc::new(|_| {}),
            enrollment_invalidated: Arc::new(|_| {}),
            #[cfg(feature = "test-support")]
            machine_restore_failure: Default::default(),
            #[cfg(feature = "test-support")]
            idle_renewal_send_failure: Default::default(),
        }
    }

    pub fn runtime(&self) -> &FleetRuntime<Host, Stores> {
        &self.execution
    }

    #[cfg(feature = "test-support")]
    pub fn fail_next_idle_renewal_send(&self) {
        self.idle_renewal_send_failure
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(feature = "test-support")]
    pub fn take_idle_renewal_send_failure(&self) -> bool {
        self.idle_renewal_send_failure
            .swap(false, std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn access(&self) -> WorkspaceAccess<'_, Host, Stores> {
        WorkspaceAccess::new(&self.execution)
    }

    #[cfg(feature = "test-support")]
    pub fn lose_next_provisioning_reply(&self) {
        self.execution.test_faults().arm_lost_provisioning_reply();
    }
    #[cfg(feature = "test-support")]
    pub fn fail_next_issuer_cache(&self, operation: fleet_signin::IssuerCacheOperation) -> bool {
        self.signin_authority
            .check()
            .is_some_and(|verifier| verifier.fail_next_issuer_cache(operation))
    }
    pub fn with_mail_delivery_observer(
        mut self,
        observer: Arc<dyn Fn(MailDeliveryReady) + Send + Sync>,
    ) -> Self {
        self.mail_delivery_ready = observer;
        self
    }
    pub fn with_enrollment_invalidation_observer(
        mut self,
        observer: Arc<dyn Fn(fleet_contracts::EnrollmentInvalidation) + Send + Sync>,
    ) -> Self {
        self.enrollment_invalidated = observer;
        self
    }
    pub fn with_person_approval_transactions(
        mut self,
        transactions: Arc<dyn crate::PersonApprovalTransactions>,
    ) -> Self {
        self.person_approval_transactions = Some(transactions);
        self
    }

    pub fn with_sign_in_verification(
        self,
        check: Arc<dyn crate::organization_authority::SignInVerification>,
    ) -> Self {
        self.signin_authority.install_check(check);
        self
    }
}
