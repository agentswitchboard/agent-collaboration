//! Opt-in metadata for pairing a debug Host's work calls with peer ingress.

use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use switchboard_harness_contracts::{AuthorizationTarget, DispatchAuthorization};
use uuid::Uuid;

pub use switchboard_harness_shared_mechanics::work_observation::{
    WorkObservationKind, WorkObservationSink,
};

const DIRECTORY: &str = "CODEX_ROUTER_WORK_WITNESS_DIRECTORY";
const NONCE: &str = "CODEX_ROUTER_WORK_WITNESS_NONCE";
const BINDING_FILE: &str = "binding.ndjson";
static APPEND: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkWitnessKind {
    Prompt,
    ManagedNotice,
}

#[derive(Serialize)]
#[serde(
    tag = "event",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum WorkWitnessEvent<'method> {
    Offered {
        work_kind: WorkWitnessKind,
    },
    NewlyTaken {
        work_kind: WorkWitnessKind,
    },
    FrameId {
        method: &'method str,
        rpc_id_sha256: String,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WitnessRow<'event> {
    nonce: String,
    authorization_fingerprint: String,
    #[serde(flatten)]
    event: WorkWitnessEvent<'event>,
}

struct WitnessConfig {
    directory: PathBuf,
    nonce: Uuid,
}

impl WitnessConfig {
    fn load() -> Option<Self> {
        let directory = std::env::var_os(DIRECTORY);
        let nonce = std::env::var(NONCE);
        if directory.is_none() && matches!(nonce, Err(std::env::VarError::NotPresent)) {
            return None;
        }
        let config = directory.zip(nonce.ok().and_then(|nonce| Uuid::parse_str(&nonce).ok()));
        let Some((directory, nonce)) = config.filter(|(_, nonce)| nonce.get_version_num() == 7)
        else {
            witness_error("invalid opt-in configuration");
            return None;
        };
        Some(Self {
            directory: directory.into(),
            nonce,
        })
    }

    fn record(&self, authorization: &DispatchAuthorization, event: WorkWitnessEvent<'_>) {
        let result = authorization_fingerprint(self.nonce, authorization)
            .and_then(|authorization_fingerprint| {
                let row = WitnessRow {
                    nonce: self.nonce.to_string(),
                    authorization_fingerprint,
                    event,
                };
                let mut bytes = serde_json::to_vec(&row)?;
                bytes.push(b'\n');
                Ok(bytes)
            })
            .map_err(io::Error::other)
            .and_then(|bytes| append_row(&self.directory, &bytes));
        if let Err(error) = result {
            witness_error(&format!("append failed ({:?})", error.kind()));
        }
    }
}

fn record_host_work(authorization: &DispatchAuthorization, event: WorkWitnessEvent<'_>) {
    if let Some(config) = WitnessConfig::load() {
        config.record(authorization, event);
    }
}

struct WorkWitnessRecorder;

#[must_use]
pub fn host_work_observation_sink() -> Arc<dyn WorkObservationSink> {
    Arc::new(WorkWitnessRecorder)
}

impl WorkObservationSink for WorkWitnessRecorder {
    fn offered(&self, authorization: &DispatchAuthorization, work_kind: WorkObservationKind) {
        record_host_work(
            authorization,
            WorkWitnessEvent::Offered {
                work_kind: work_kind.into(),
            },
        );
    }

    fn newly_taken(&self, authorization: &DispatchAuthorization, work_kind: WorkObservationKind) {
        record_host_work(
            authorization,
            WorkWitnessEvent::NewlyTaken {
                work_kind: work_kind.into(),
            },
        );
    }
}

impl From<WorkObservationKind> for WorkWitnessKind {
    fn from(kind: WorkObservationKind) -> Self {
        match kind {
            WorkObservationKind::Prompt => Self::Prompt,
            WorkObservationKind::ManagedNotice => Self::ManagedNotice,
        }
    }
}

pub(crate) fn record_frame_id(
    authorization: &DispatchAuthorization,
    method: &str,
    request_id: &impl Serialize,
) {
    let Some(config) = WitnessConfig::load() else {
        return;
    };
    match json_sha256(request_id) {
        Ok(rpc_id_sha256) => config.record(
            authorization,
            WorkWitnessEvent::FrameId {
                method,
                rpc_id_sha256,
            },
        ),
        Err(_) => {
            witness_error("request ID encoding failed");
        }
    }
}

// The fingerprint includes each field in fixed array order. The final item is
// the target tag and, for an existing turn, its exact ID.
fn authorization_fingerprint(
    nonce: Uuid,
    authorization: &DispatchAuthorization,
) -> Result<String, serde_json::Error> {
    let target = match authorization.target {
        AuthorizationTarget::NewTurn => json!(["newTurn"]),
        AuthorizationTarget::Session => json!(["session"]),
        AuthorizationTarget::ExistingTurn { turn } => json!(["existingTurn", turn.0.to_string()]),
    };
    let identity: Value = json!([
        nonce.to_string(),
        authorization.incarnation.0.to_string(),
        authorization.run.number(),
        authorization.input_id.as_str(),
        authorization.authorization_seq,
        target,
    ]);
    json_sha256(&identity)
}

fn json_sha256(value: &impl Serialize) -> Result<String, serde_json::Error> {
    serde_json::to_vec(value).map(|bytes| format!("{:x}", Sha256::digest(bytes)))
}

fn append_row(directory: &Path, bytes: &[u8]) -> io::Result<()> {
    let _append = APPEND
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let directory_metadata = fs::symlink_metadata(directory)?;
    if !directory.is_absolute()
        || !directory_metadata.is_dir()
        || directory_metadata.permissions().mode() & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "private witness directory required",
        ));
    }
    let path = directory.join(BINDING_FILE);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if !is_private_file(&metadata) => {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private witness file required",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)
}

fn witness_error(reason: &str) {
    let _written = writeln!(io::stderr().lock(), "harness_work_witness_error: {reason}");
}

fn is_private_file(metadata: &fs::Metadata) -> bool {
    metadata.is_file() && metadata.permissions().mode() & 0o777 == 0o600
}

#[cfg(test)]
#[path = "work_call_observation_test.rs"]
mod tests;
