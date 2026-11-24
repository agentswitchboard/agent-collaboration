//! Session existence for mail custody, independent of peer delivery capability.

use crate::{
    ClaudeCodeSessionRegistry, PeerRegistryError,
    claude_code_session_registry::{process_id_from_filename, process_is_live, read_peer_envelope},
    process_ancestry::observe_strict_descendant,
    registry_process_start::{
        ProcessIncarnation, ProcessObservation, ProcessStart, ProcessStartObservation,
        parse_ps_start, process_started_at,
    },
};
use collaboration_protocol::SessionId;
use serde_json::Value;
use std::{fs, future::Future, io, num::NonZeroU32, path::PathBuf};

/// A lower bound and whether every relevant registry record was checked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerCensusAnswer {
    /// Proved independent claims, excluding any managed alias.
    pub at_least: usize,
    pub complete: bool,
    /// A terminal observation of the same proven live managed Claude session.
    pub managed_claude_alias: bool,
}

/// A read of the adapter owned by the Host's live managed Claude session.
/// Absence and an unavailable read both preserve the census's original answer.
pub trait ManagedClaudeLaunches: Send + Sync {
    fn live_adapter(&self, session: &SessionId) -> Option<ProcessIncarnation>;
}

/// The managed launch observed when starting a terminal recipient lookup.
pub struct ManagedClaudeClaim<'a> {
    pub launches: &'a dyn ManagedClaudeLaunches,
    pub adapter: ProcessIncarnation,
}

enum ClaimDisposition {
    Count,
    ManagedAlias,
    ProbeReapDeferred,
}

/// A fresh registry read for one exact recipient, with no observation of
/// another ID carried into its answer.
pub struct PeerSessionCensus {
    target: SessionId,
    claims: Vec<RegistryClaim>,
    unidentified_record: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PeerCensusOutcome {
    Answer(PeerCensusAnswer),
    ProbeReapDeferred {
        process_id: u32,
        answer: PeerCensusAnswer,
    },
}

enum RegistryClaim {
    Identified {
        pid: u32,
        recorded: RecordedProcessStart,
    },
    Unverified,
}

#[derive(Clone, Copy)]
enum RecordedProcessStart {
    DisplayedSecond(ProcessStart),
    RegistrationTimestamp(i64),
}

#[derive(Debug, PartialEq, Eq)]
enum Incarnation {
    Verified(ProcessStart),
    Stale,
    Unverified,
}

impl PeerSessionCensus {
    fn empty(target: &SessionId) -> Self {
        Self {
            target: target.clone(),
            claims: Vec::new(),
            unidentified_record: false,
        }
    }

    #[must_use]
    pub fn scan(
        registry: &ClaudeCodeSessionRegistry,
        target: &SessionId,
        still_observing: impl Fn() -> bool,
    ) -> Self {
        match fs::read_dir(registry.directory()) {
            Ok(entries) => Self::read(
                entries.map(|entry| entry.map(|entry| entry.path())),
                target,
                process_is_live,
                still_observing,
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Self::empty(target),
            Err(_) => Self {
                unidentified_record: true,
                ..Self::empty(target)
            },
        }
    }

    fn read(
        entries: impl IntoIterator<Item = io::Result<PathBuf>>,
        target: &SessionId,
        mut probe: impl FnMut(u32) -> Result<bool, PeerRegistryError>,
        still_observing: impl Fn() -> bool,
    ) -> Self {
        let mut census = Self::empty(target);
        let mut entries = entries.into_iter();
        while still_observing() {
            let Some(entry) = entries.next() else {
                return census;
            };
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
            if session_id.as_ref().is_some_and(|id| id != target) {
                continue;
            }
            let liveness = probe(pid);
            if matches!(liveness, Ok(false)) {
                continue;
            }
            if session_id.is_none() {
                census.unidentified_record = true;
                continue;
            }
            let claim = match (liveness, envelope) {
                (Ok(true), Some(envelope)) => registry_claim(pid, &envelope),
                _ => RegistryClaim::Unverified,
            };
            census.claims.push(claim);
        }
        census.unidentified_record = true;
        census
    }

    /// Resolve process incarnations in registry order. Every probe accepts the
    /// same stop signal and starts its reap window only when interrupted.
    pub async fn answer<TStop: Future<Output = ()>, TReap: Future<Output = ()>>(
        &self,
        still_observing: impl Fn() -> bool,
        stop: impl Fn() -> TStop,
        reap_deadline: impl Fn() -> TReap,
        proved: impl FnMut(usize),
    ) -> PeerCensusOutcome {
        self.answer_with_managed_claim(None, still_observing, stop, reap_deadline, proved)
            .await
    }

    /// Suppress a verified registry claim only while the same managed launch
    /// still owns the session after a process-table ancestry check.
    pub async fn answer_with_managed_claim<
        TStop: Future<Output = ()>,
        TReap: Future<Output = ()>,
    >(
        &self,
        managed: Option<ManagedClaudeClaim<'_>>,
        still_observing: impl Fn() -> bool,
        stop: impl Fn() -> TStop,
        reap_deadline: impl Fn() -> TReap,
        proved: impl FnMut(usize),
    ) -> PeerCensusOutcome {
        self.count_verified_claims(
            process_is_live,
            |pid| process_started_at(pid, stop(), &reap_deadline),
            still_observing,
            |worker| {
                classify_claim(
                    managed.as_ref(),
                    &self.target,
                    worker,
                    stop(),
                    &reap_deadline,
                )
            },
            proved,
        )
        .await
    }

    #[cfg(test)]
    async fn answer_with<TStart: Future<Output = ProcessStartObservation>>(
        &self,
        is_live: impl FnMut(u32) -> Result<bool, PeerRegistryError>,
        start_time: impl FnMut(u32) -> TStart,
        still_observing: impl Fn() -> bool,
        proved: impl FnMut(usize),
    ) -> PeerCensusOutcome {
        self.count_verified_claims(
            is_live,
            start_time,
            still_observing,
            |_| std::future::ready(ClaimDisposition::Count),
            proved,
        )
        .await
    }

    async fn count_verified_claims<TStart, TDisposition>(
        &self,
        mut is_live: impl FnMut(u32) -> Result<bool, PeerRegistryError>,
        mut start_time: impl FnMut(u32) -> TStart,
        still_observing: impl Fn() -> bool,
        mut classify: impl FnMut(ProcessIncarnation) -> TDisposition,
        mut proved: impl FnMut(usize),
    ) -> PeerCensusOutcome
    where
        TStart: Future<Output = ProcessStartObservation>,
        TDisposition: Future<Output = ClaimDisposition>,
    {
        let mut answer = PeerCensusAnswer {
            at_least: 0,
            complete: !self.unidentified_record,
            managed_claude_alias: false,
        };
        let mut first_verified_pid = None;
        for (index, claim) in self.claims.iter().enumerate() {
            if !still_observing() {
                answer.complete = false;
                break;
            }
            let RegistryClaim::Identified { pid, recorded } = *claim else {
                answer.complete = false;
                continue;
            };
            if first_verified_pid == Some(pid) {
                continue;
            }
            let outcome = match is_live(pid) {
                Ok(false) => Incarnation::Stale,
                Err(_) => Incarnation::Unverified,
                Ok(true) => match start_time(pid).await {
                    ProcessStartObservation::Observed(started) => {
                        compare_incarnation(recorded, Some(started))
                    }
                    ProcessStartObservation::Unavailable => Incarnation::Unverified,
                    ProcessStartObservation::Interrupted => {
                        answer.complete = false;
                        break;
                    }
                    ProcessStartObservation::ReapDeferred => {
                        answer.complete = false;
                        return PeerCensusOutcome::ProbeReapDeferred {
                            process_id: pid,
                            answer,
                        };
                    }
                },
            };
            match outcome {
                Incarnation::Verified(started) => {
                    let Some(process_id) = NonZeroU32::new(pid) else {
                        answer.complete = false;
                        continue;
                    };
                    let disposition = classify(ProcessIncarnation {
                        pid: process_id,
                        started,
                    })
                    .await;
                    if matches!(disposition, ClaimDisposition::ManagedAlias) {
                        answer.managed_claude_alias = true;
                        continue;
                    }
                    answer.at_least += 1;
                    proved(answer.at_least);
                    if matches!(disposition, ClaimDisposition::ProbeReapDeferred) {
                        answer.complete = false;
                        return PeerCensusOutcome::ProbeReapDeferred {
                            process_id: pid,
                            answer,
                        };
                    }
                    if first_verified_pid.replace(pid).is_some() {
                        // Enough evidence for ambiguity regardless of unvisited claims.
                        answer.complete &= index + 1 == self.claims.len();
                        break;
                    }
                }
                Incarnation::Stale => {}
                Incarnation::Unverified => answer.complete = false,
            }
        }
        PeerCensusOutcome::Answer(answer)
    }
}

async fn classify_claim<TReap: Future<Output = ()>>(
    managed: Option<&ManagedClaudeClaim<'_>>,
    target: &SessionId,
    worker: ProcessIncarnation,
    stop: impl Future<Output = ()>,
    reap_deadline: impl FnOnce() -> TReap,
) -> ClaimDisposition {
    let Some(managed) = managed else {
        return ClaimDisposition::Count;
    };
    let ancestry = observe_strict_descendant(worker, managed.adapter, stop, reap_deadline).await;
    managed_claim_disposition(managed, target, ancestry)
}

fn managed_claim_disposition(
    managed: &ManagedClaudeClaim<'_>,
    target: &SessionId,
    ancestry: ProcessObservation<bool>,
) -> ClaimDisposition {
    match ancestry {
        ProcessObservation::Observed(true)
            if managed.launches.live_adapter(target) == Some(managed.adapter) =>
        {
            ClaimDisposition::ManagedAlias
        }
        ProcessObservation::ReapDeferred => ClaimDisposition::ProbeReapDeferred,
        ProcessObservation::Observed(_)
        | ProcessObservation::Unavailable
        | ProcessObservation::Interrupted => ClaimDisposition::Count,
    }
}

fn registry_claim(pid: u32, envelope: &Value) -> RegistryClaim {
    if envelope.get("pid").and_then(Value::as_u64) != Some(u64::from(pid)) {
        return RegistryClaim::Unverified;
    }
    if let Some(displayed_start) = envelope
        .get("procStart")
        .and_then(Value::as_str)
        .and_then(parse_ps_start)
    {
        return RegistryClaim::Identified {
            pid,
            recorded: RecordedProcessStart::DisplayedSecond(ProcessStart(displayed_start)),
        };
    }
    let Some(started_at_ms) = envelope
        .get("startedAt")
        .and_then(Value::as_i64)
        .filter(|timestamp| (0..=253_402_300_799_999).contains(timestamp))
    else {
        return RegistryClaim::Unverified;
    };
    RegistryClaim::Identified {
        pid,
        recorded: RecordedProcessStart::RegistrationTimestamp(started_at_ms),
    }
}

/// A recorded `procStart` and the live probe use the same UTC displayed
/// second. Equal seconds count as the same process. A PID reused by a process
/// that started within the original displayed second is indistinguishable.
/// Without a parseable `procStart`, a displayed start second at or before the
/// registration stamp counts as the same process. That fallback also accepts
/// a reused PID when the replacement began later in the stamp's same second.
fn compare_incarnation(
    recorded: RecordedProcessStart,
    process_start: Option<ProcessStart>,
) -> Incarnation {
    match (recorded, process_start) {
        (RecordedProcessStart::DisplayedSecond(recorded), Some(started)) => {
            if started == recorded {
                Incarnation::Verified(started)
            } else {
                Incarnation::Stale
            }
        }
        (RecordedProcessStart::RegistrationTimestamp(recorded), Some(started))
            if started.0 <= recorded =>
        {
            Incarnation::Verified(started)
        }
        (RecordedProcessStart::RegistrationTimestamp(recorded), Some(started))
            if started.0 > recorded + 1_000 =>
        {
            Incarnation::Stale
        }
        _ => Incarnation::Unverified,
    }
}

#[cfg(test)]
#[path = "peer_session_census_test.rs"]
mod tests;
