//! Translate the session-only descriptor at the ACP setup boundary.

use std::sync::Arc;

use agent_client_protocol::schema::v1::{HttpHeader, McpServer, McpServerHttp, Meta};
use serde_json::{Map, Value};

use crate::ExternalProviderRuntimeError;

type ObserveHandshakes = dyn Fn() -> u64 + Send + Sync;

enum ManagedHarness {
    Claude,
    Cursor(Arc<ObserveHandshakes>),
}

/// Session setup chosen by the managed provider owner. The descriptor stays
/// opaque until the ACP writer constructs the outgoing frame.
pub struct ManagedAcpToolSetup {
    server: McpServerHttp,
    harness: ManagedHarness,
}

impl ManagedAcpToolSetup {
    pub fn claude(url: String, bearer: &str) -> Self {
        Self {
            server: managed_http_server(url, bearer),
            harness: ManagedHarness::Claude,
        }
    }

    /// The caller binds the listener observer to this exact descriptor. A
    /// successful `session/new` or `session/load` reply alone is insufficient.
    pub fn cursor(
        url: String,
        bearer: &str,
        completed_handshakes: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> Self {
        Self {
            server: managed_http_server(url, bearer),
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
            ManagedHarness::Cursor(observe) => Some(observe()),
        };
        Ok((McpServer::Http(self.server.clone()), before_dispatch))
    }

    pub(crate) fn require_setup_evidence(
        &self,
        before_dispatch: Option<u64>,
        provider_id: &str,
    ) -> Result<(), ExternalProviderRuntimeError> {
        match (&self.harness, before_dispatch) {
            (ManagedHarness::Claude, None) => Ok(()),
            (ManagedHarness::Cursor(observe), Some(before)) if observe() > before => Ok(()),
            _ => Err(ExternalProviderRuntimeError::ManagedToolsNotConnected {
                provider_session_id: provider_id.to_owned(),
            }),
        }
    }
}

fn managed_http_server(url: String, bearer: &str) -> McpServerHttp {
    McpServerHttp::new("agent-router", url).headers(vec![HttpHeader::new(
        "Authorization",
        format!("Bearer {bearer}"),
    )])
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
