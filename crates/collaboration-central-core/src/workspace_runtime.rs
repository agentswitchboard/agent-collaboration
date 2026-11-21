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
    pub(crate) execution: FleetRuntime<Host, Stores>,
    pub(crate) stamp_store: Arc<dyn fleet_signin::stamp_store::StampStore>,
    pub(crate) mail_delivery_ready: Arc<dyn Fn(MailDeliveryReady) + Send + Sync>,
    pub(crate) machine_revoked: Arc<dyn Fn(workspace_ids::MachineId) + Send + Sync>,
    pub(crate) signin_authority: crate::organization_authority_cache::OrganizationAuthority,
    #[cfg(feature = "test-support")]
    pub(crate) machine_restore_failure:
        std::sync::Mutex<Option<(usize, central_storage::StoreFailure)>>,
}
impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub fn new(
        execution: FleetRuntime<Host, Stores>,
        stamp_store: Arc<dyn fleet_signin::stamp_store::StampStore>,
    ) -> Self {
        Self {
            execution,
            stamp_store,
            signin_authority: Default::default(),
            mail_delivery_ready: Arc::new(|_| {}),
            machine_revoked: Arc::new(|_| {}),
            #[cfg(feature = "test-support")]
            machine_restore_failure: Default::default(),
        }
    }

    pub fn runtime(&self) -> &FleetRuntime<Host, Stores> {
        &self.execution
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
    pub fn with_machine_revocation_observer(
        mut self,
        observer: Arc<dyn Fn(workspace_ids::MachineId) + Send + Sync>,
    ) -> Self {
        self.machine_revoked = observer;
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
