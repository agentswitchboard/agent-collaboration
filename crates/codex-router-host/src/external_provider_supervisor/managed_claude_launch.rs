use std::sync::Arc;

use claude_code_peer_messaging::{ManagedClaudeLaunches, ProcessIncarnation};
use collaboration_protocol::{EndpointRef, ProviderKind, SessionId, SessionRef};
use provider_session_machines::session_lifecycle_machine::SessionLifecycle;

use super::ExternalProviderSupervisor;

struct ManagedClaudeLaunchRead {
    supervisor: Arc<ExternalProviderSupervisor>,
    endpoint: EndpointRef,
}

impl ManagedClaudeLaunches for ManagedClaudeLaunchRead {
    fn live_adapter(&self, session: &SessionId) -> Option<ProcessIncarnation> {
        let binding = self.supervisor.inner.bindings.get(&self.endpoint)?;
        if binding.identity.runtime.provider != ProviderKind::ClaudeCode {
            return None;
        }
        let target = SessionRef {
            endpoint: self.endpoint.clone(),
            session_id: session.clone(),
        };
        let owner = binding.owners.existing(&target)?;
        if !matches!(
            owner.snapshots().borrow().lifecycle,
            SessionLifecycle::Live { .. }
        ) {
            return None;
        }
        binding.runtime.live_claude_adapter()
    }
}

impl ExternalProviderSupervisor {
    pub(crate) fn managed_claude_launches(
        self: &Arc<Self>,
        endpoint: EndpointRef,
    ) -> Arc<dyn ManagedClaudeLaunches> {
        Arc::new(ManagedClaudeLaunchRead {
            supervisor: self.clone(),
            endpoint,
        })
    }
}
