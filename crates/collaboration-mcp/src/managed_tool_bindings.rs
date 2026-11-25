//! In-memory credentials issued by the private MCP listener for exact start bindings.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use collaboration_protocol::{
    ManagedSessionToolBinding, ManagedStartResolver, ManagedToolBearer, ManagedToolDescriptor,
    ManagedToolPreparationError, ManagedToolResolution, PrepareManagedToolDescriptor,
};
use subtle::ConstantTimeEq as _;

pub(crate) const MANAGED_TOOL_PREFIX: &str = "/mcp/managed/";
pub(crate) const MANAGED_TOOL_PATH: &str = "/mcp/managed/{start_id}";

/// Issues one credential for each exact start binding on this loopback listener.
/// All lifecycle decisions come from the start owner.
pub struct ManagedToolBindings {
    base_url: String,
    prepared: Mutex<HashMap<String, Vec<Arc<PreparedToolBinding>>>>,
    pub(crate) resolver: Arc<dyn ManagedStartResolver>,
}

pub(crate) struct PreparedToolBinding {
    pub(crate) binding: ManagedSessionToolBinding,
    bearer: ManagedToolBearer,
    initialized: AtomicU64,
}

impl PreparedToolBinding {
    pub(crate) fn record_initialized(&self) {
        self.initialized.fetch_add(1, Ordering::Release);
    }
}

impl ManagedToolBindings {
    pub(crate) fn new(
        address: SocketAddr,
        resolver: Arc<dyn ManagedStartResolver>,
    ) -> Result<Self, ManagedToolPreparationError> {
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(ManagedToolPreparationError::ListenerUnavailable);
        }
        Ok(Self {
            base_url: format!("http://{address}{MANAGED_TOOL_PREFIX}"),
            prepared: Mutex::default(),
            resolver,
        })
    }

    pub(crate) fn authenticate(
        &self,
        start: &str,
        candidate: &[u8],
    ) -> Option<Arc<PreparedToolBinding>> {
        let prepared = self.prepared.lock().ok()?;
        prepared
            .get(start)?
            .iter()
            .find(|entry| {
                entry
                    .bearer
                    .expose_header_value()
                    .as_bytes()
                    .ct_eq(candidate)
                    .into()
            })
            .cloned()
    }

    /// Counts completed MCP handshakes presenting this exact descriptor. Snapshot
    /// before sending a Cursor setup frame and require a new handshake on reply.
    pub fn completed_handshakes(&self, descriptor: &ManagedToolDescriptor) -> u64 {
        descriptor
            .url
            .strip_prefix(&self.base_url)
            .and_then(|start| {
                self.authenticate(start, descriptor.bearer.expose_header_value().as_bytes())
            })
            .map_or(0, |entry| entry.initialized.load(Ordering::Acquire))
    }
}

impl PrepareManagedToolDescriptor for ManagedToolBindings {
    fn prepare_managed_descriptor(
        &self,
        binding: &ManagedSessionToolBinding,
    ) -> Result<ManagedToolDescriptor, ManagedToolPreparationError> {
        let start = binding.start.to_string();
        let url = format!("{}{start}", self.base_url);
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| ManagedToolPreparationError::ListenerUnavailable)?;
        let entries = prepared.entry(start).or_default();
        if let Some(entry) = entries.iter().find(|entry| entry.binding == *binding) {
            return Ok(ManagedToolDescriptor {
                url,
                bearer: entry.bearer.clone(),
            });
        }
        let mut bytes = [0; 32];
        getrandom::fill(&mut bytes)
            .map_err(|_| ManagedToolPreparationError::CredentialUnavailable)?;
        let bearer = ManagedToolBearer::from_random_bytes(&bytes);
        entries.push(Arc::new(PreparedToolBinding {
            binding: binding.clone(),
            bearer: bearer.clone(),
            initialized: AtomicU64::new(0),
        }));
        Ok(ManagedToolDescriptor { url, bearer })
    }
}

/// The request carries one resolver verdict and can consume its lease once.
pub(crate) struct ManagedToolRequest(Mutex<Option<ManagedToolResolution>>);

impl ManagedToolRequest {
    pub(crate) fn new(resolution: ManagedToolResolution) -> Self {
        Self(Mutex::new(Some(resolution)))
    }

    pub(crate) fn take_resolution(&self) -> Option<ManagedToolResolution> {
        self.0.lock().ok()?.take()
    }
}
