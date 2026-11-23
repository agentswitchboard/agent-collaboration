//! Session existence for mail custody, independent of peer delivery capability.

use crate::{
    ClaudeCodeSessionRegistry, PeerRegistryError,
    claude_code_session_registry::{process_id_from_filename, process_is_live, read_peer_envelope},
    registry_process_start::process_started_at,
};
use collaboration_protocol::SessionId;
use serde_json::Value;
use std::{collections::HashMap, fs, future::Future, io, path::PathBuf};

/// A lower bound and whether every relevant registry record was checked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerCensusAnswer {
    pub at_least: usize,
    pub complete: bool,
}

/// One registry read shared by callers with different recipient IDs. The
/// process-incarnation checks run only for IDs requested from this scan.
#[derive(Default)]
pub struct PeerSessionCensus {
    sessions: HashMap<SessionId, Vec<RegistryClaim>>,
    unidentified_record: bool,
}

enum RegistryClaim {
    Identified { pid: u32, started_at_ms: i64 },
    Unverified,
}

enum Incarnation {
    Verified,
    Stale,
    Unverified,
}

impl PeerSessionCensus {
    #[must_use]
    pub fn scan(registry: &ClaudeCodeSessionRegistry) -> Self {
        match fs::read_dir(registry.directory()) {
            Ok(entries) => Self::read(
                entries.map(|entry| entry.map(|entry| entry.path())),
                process_is_live,
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Self::default(),
            Err(_) => Self {
                unidentified_record: true,
                ..Self::default()
            },
        }
    }

    fn read(
        entries: impl IntoIterator<Item = io::Result<PathBuf>>,
        mut probe: impl FnMut(u32) -> Result<bool, PeerRegistryError>,
    ) -> Self {
        let mut census = Self::default();
        for entry in entries {
            let Ok(path) = entry else {
                census.unidentified_record = true;
                continue;
            };
            let Some(pid) = path.file_name().and_then(process_id_from_filename) else {
                continue;
            };
            let envelope = read_peer_envelope(&path);
            let session_id = envelope
                .as_ref()
                .and_then(|record| record.get("sessionId"))
                .and_then(Value::as_str)
                .and_then(|value| SessionId::try_from(value.to_owned()).ok());
            let liveness = probe(pid);
            if matches!(liveness, Ok(false)) {
                continue;
            }
            let Some(session_id) = session_id else {
                census.unidentified_record = true;
                continue;
            };
            let claim = match (liveness, envelope) {
                (Ok(true), Some(envelope)) => registry_claim(pid, &envelope),
                _ => RegistryClaim::Unverified,
            };
            census.sessions.entry(session_id).or_default().push(claim);
        }
        census
    }

    /// Resolve process incarnations for one ID with a fresh deadline per probe.
    pub async fn answer<TDeadline: Future<Output = ()>>(
        &self,
        session_id: &SessionId,
        probe_deadline: impl Fn() -> TDeadline,
    ) -> PeerCensusAnswer {
        self.answer_with(session_id, process_is_live, |pid| {
            process_started_at(pid, probe_deadline())
        })
        .await
    }

    async fn answer_with<TStart: Future<Output = Option<i64>>>(
        &self,
        session_id: &SessionId,
        mut is_live: impl FnMut(u32) -> Result<bool, PeerRegistryError>,
        mut start_time: impl FnMut(u32) -> TStart,
    ) -> PeerCensusAnswer {
        let mut answer = PeerCensusAnswer {
            at_least: 0,
            complete: !self.unidentified_record,
        };
        let Some(claims) = self.sessions.get(session_id) else {
            return answer;
        };
        for claim in claims {
            let outcome = match *claim {
                RegistryClaim::Unverified => Incarnation::Unverified,
                RegistryClaim::Identified { pid, started_at_ms } => match is_live(pid) {
                    Ok(false) => Incarnation::Stale,
                    Err(_) => Incarnation::Unverified,
                    Ok(true) => compare_incarnation(started_at_ms, start_time(pid).await),
                },
            };
            match outcome {
                Incarnation::Verified => answer.at_least += 1,
                Incarnation::Stale => {}
                Incarnation::Unverified => answer.complete = false,
            }
        }
        answer
    }
}

fn registry_claim(pid: u32, envelope: &Value) -> RegistryClaim {
    if envelope.get("pid").and_then(Value::as_u64) != Some(u64::from(pid)) {
        return RegistryClaim::Unverified;
    }
    let Some(started_at_ms) = envelope
        .get("startedAt")
        .and_then(Value::as_i64)
        .filter(|timestamp| (0..=253_402_300_799_999).contains(timestamp))
    else {
        return RegistryClaim::Unverified;
    };
    RegistryClaim::Identified { pid, started_at_ms }
}

fn compare_incarnation(recorded_ms: i64, process_ms: Option<i64>) -> Incarnation {
    match process_ms {
        Some(started_ms) if started_ms <= recorded_ms => Incarnation::Verified,
        Some(started_ms) if started_ms > recorded_ms + 1_000 => Incarnation::Stale,
        _ => Incarnation::Unverified,
    }
}

#[cfg(test)]
#[path = "peer_session_census_test.rs"]
mod tests;
