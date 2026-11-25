//! Translate the session-only descriptor at the ACP setup boundary.

use std::sync::Arc;

use agent_client_protocol::schema::v1::{HttpHeader, McpServer, McpServerHttp, Meta};
use collaboration_protocol::ManagedToolDescriptor;
use serde_json::{Map, Value};

use crate::ExternalProviderRuntimeError;

type ObserveHandshakes = dyn Fn(&ManagedToolDescriptor) -> u64 + Send + Sync;

enum ManagedHarness {
    Claude,
    Cursor(Arc<ObserveHandshakes>),
}

/// Session setup chosen by the managed provider owner. The descriptor stays
/// opaque until the ACP writer constructs the outgoing frame.
pub struct ManagedAcpToolSetup {
    descriptor: ManagedToolDescriptor,
    harness: ManagedHarness,
}

impl ManagedAcpToolSetup {
    pub fn claude(descriptor: ManagedToolDescriptor) -> Self {
        Self {
            descriptor,
            harness: ManagedHarness::Claude,
        }
    }

    /// The listener observer is local IPC evidence for this descriptor. A
    /// successful `session/new` or `session/load` reply alone is insufficient.
    pub fn cursor(
        descriptor: ManagedToolDescriptor,
        completed_handshakes: impl Fn(&ManagedToolDescriptor) -> u64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            descriptor,
            harness: ManagedHarness::Cursor(Arc::new(completed_handshakes)),
        }
    }

    pub(crate) fn request_setup(
        &self,
        meta: &mut Option<Meta>,
        supports_http: bool,
    ) -> Result<(McpServer, Option<u64>), ExternalProviderRuntimeError> {
        if !supports_http {
            return Err(ExternalProviderRuntimeError::UnsupportedCapability {
                capability: "HTTP MCP for managed sessions",
            });
        }
        let before_dispatch = match &self.harness {
            ManagedHarness::Claude => {
                let claude = nested_options(meta.get_or_insert_with(Map::new), "claudeCode")?;
                nested_options(claude, "options")?
                    .insert("strictMcpConfig".to_owned(), Value::Bool(true));
                None
            }
            ManagedHarness::Cursor(observe) => Some(observe(&self.descriptor)),
        };
        let server = McpServerHttp::new("agent-router", self.descriptor.url.clone()).headers(vec![
            HttpHeader::new(
                "Authorization",
                format!("Bearer {}", self.descriptor.bearer.expose_header_value()),
            ),
        ]);
        Ok((McpServer::Http(server), before_dispatch))
    }

    pub(crate) fn require_setup_evidence(
        &self,
        before_dispatch: Option<u64>,
        provider_id: &str,
    ) -> Result<(), ExternalProviderRuntimeError> {
        match (&self.harness, before_dispatch) {
            (ManagedHarness::Claude, None) => Ok(()),
            (ManagedHarness::Cursor(observe), Some(before))
                if observe(&self.descriptor) > before =>
            {
                Ok(())
            }
            _ => Err(ExternalProviderRuntimeError::ManagedToolsNotConnected {
                provider_session_id: provider_id.to_owned(),
            }),
        }
    }
}

fn nested_options<'meta>(
    meta: &'meta mut Map<String, Value>,
    name: &str,
) -> Result<&'meta mut Map<String, Value>, ExternalProviderRuntimeError> {
    meta.entry(name.to_owned())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or(ExternalProviderRuntimeError::ManagedToolSetupUnconfirmed)
}
