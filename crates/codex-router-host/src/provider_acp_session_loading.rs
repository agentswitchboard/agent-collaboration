//! Load a recorded provider session only after checking live peer ownership.
use crate::external_provider_supervisor::SessionQueuePlan;
use crate::external_provider_supervisor::restore_failure_text::{
    CodedRestoreFailure, RestoreRejectionKind,
};
use crate::{
    ExternalProviderSupervisor, LiveSessionOwnership, LiveSessionOwnershipCheck,
    ProviderSessionActivity,
};
use acp_client_runtime::acp_transport::TransportRun;
use collaboration_contracts::LoadPolicy;
use collaboration_protocol::{DeliveryCorrelationId, SessionRef};
use provider_session_machines::session_owner::{ProviderWireFailure, RestorePhase};
use std::sync::Arc;
use switchboard_session_store::ProviderOperationStore;
use tokio::sync::Mutex;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProviderSessionLoadOutcome {
    Ready,
    AlreadyLoaded,
    NotLoaded,
    UnsupportedLoad,
    MissingRecord,
    LiveElsewhere,
    Rejected {
        phase: RestorePhase,
        reason: ProviderSessionLoadRejection,
    },
    Unavailable {
        phase: RestorePhase,
        reason: String,
    },
    /// A close of the session won while this load was in flight: the
    /// connection dropped the restored session, and the work that asked for
    /// the load ends not sent. A later load starts a new run.
    ClosedWhileLoading {
        phase: RestorePhase,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProviderSessionLoadability {
    AlreadyLoaded,
    /// The owner can load the record and rebuild its setup when requested.
    Loadable,
    UnsupportedLoad,
    MissingRecord,
    LiveElsewhere,
    Unavailable {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProviderSessionLoadRejection {
    SessionNotFound {
        code: i64,
        correlation_id: DeliveryCorrelationId,
    },
    ProviderRejected {
        code: i64,
        correlation_id: DeliveryCorrelationId,
    },
}

impl ProviderSessionLoadRejection {
    #[must_use]
    pub(crate) fn code(&self) -> i64 {
        match self {
            Self::SessionNotFound { code, .. } | Self::ProviderRejected { code, .. } => *code,
        }
    }

    #[must_use]
    pub(crate) fn safe_detail(&self) -> String {
        match self {
            Self::SessionNotFound {
                code,
                correlation_id,
            } => RestoreRejectionKind::SessionNotFound.text(*code, correlation_id),
            Self::ProviderRejected {
                code,
                correlation_id,
            } => RestoreRejectionKind::ProviderRejected.text(*code, correlation_id),
        }
    }
}

pub(crate) async fn ensure_provider_session_loaded(
    supervisor: &ExternalProviderSupervisor,
    store: &Arc<Mutex<ProviderOperationStore>>,
    ownership: &dyn LiveSessionOwnershipCheck,
    target: &SessionRef,
    load_policy: LoadPolicy,
) -> ProviderSessionLoadOutcome {
    if !supervisor.has_binding(&target.endpoint) {
        return unavailable("provider runtime is unavailable");
    }
    use provider_session_machines::{
        session_lifecycle_machine::SessionLifecycle,
        session_owner::{RunAttempt, SessionOwnerReply},
    };
    if let Some(snapshot) = supervisor.owner_session_snapshot(target) {
        match snapshot.lifecycle {
            SessionLifecycle::Live { .. } => {
                return if load_policy == LoadPolicy::LoadedOnly {
                    ProviderSessionLoadOutcome::AlreadyLoaded
                } else {
                    ProviderSessionLoadOutcome::Ready
                };
            }
            SessionLifecycle::Loading {
                generation,
                attempt,
                ..
            } => {
                if load_policy == LoadPolicy::LoadedOnly {
                    return ProviderSessionLoadOutcome::NotLoaded;
                }
                let Ok(owner) = supervisor.session_owner_for(target) else {
                    return unavailable("provider session owner is unavailable");
                };
                return match owner
                    .join_restore(
                        uuid::Uuid::now_v7(),
                        RunAttempt {
                            generation,
                            attempt,
                        },
                    )
                    .await
                {
                    Ok(result) => owner_restore_outcome(result),
                    Err(error) => unavailable(error.to_string()),
                };
            }
            SessionLifecycle::NotLoaded { .. } | SessionLifecycle::Closing { .. } => {}
        }
    }
    match checked_loadability(supervisor, store, ownership, target).await {
        ProviderSessionLoadability::AlreadyLoaded => {
            return if load_policy == LoadPolicy::LoadedOnly {
                ProviderSessionLoadOutcome::AlreadyLoaded
            } else {
                ProviderSessionLoadOutcome::Ready
            };
        }
        ProviderSessionLoadability::Loadable if load_policy == LoadPolicy::LoadedOnly => {
            return ProviderSessionLoadOutcome::NotLoaded;
        }
        ProviderSessionLoadability::Loadable => {}
        ProviderSessionLoadability::UnsupportedLoad => {
            return ProviderSessionLoadOutcome::UnsupportedLoad;
        }
        ProviderSessionLoadability::MissingRecord => {
            return ProviderSessionLoadOutcome::MissingRecord;
        }
        ProviderSessionLoadability::LiveElsewhere => {
            return ProviderSessionLoadOutcome::LiveElsewhere;
        }
        ProviderSessionLoadability::Unavailable { reason } => return unavailable(reason),
    }
    let Ok(owner) = supervisor.session_owner_for(target) else {
        return unavailable("provider session owner is unavailable");
    };
    match owner.restore_for_delivery(uuid::Uuid::now_v7()).await {
        Ok(settled) => match settled.reply {
            SessionOwnerReply::RestoreFinished(result) => owner_restore_outcome(result),
            _ => unavailable("provider session owner returned another result"),
        },
        Err(error) => unavailable(error.to_string()),
    }
}

async fn checked_loadability(
    supervisor: &ExternalProviderSupervisor,
    store: &Arc<Mutex<ProviderOperationStore>>,
    ownership: &dyn LiveSessionOwnershipCheck,
    target: &SessionRef,
) -> ProviderSessionLoadability {
    let clock = supervisor.clock();
    router_clock::timeout(
        clock.as_ref(),
        std::time::Duration::from_secs(10),
        inspect_provider_session_loadability(supervisor, store, ownership, target),
    )
    .await
    .unwrap_or_else(|_| ProviderSessionLoadability::Unavailable {
        reason: "the Router could not read the session setup in time".to_owned(),
    })
}

pub(crate) async fn session_queue_plan(
    supervisor: &ExternalProviderSupervisor,
    store: &Arc<Mutex<ProviderOperationStore>>,
    ownership: &dyn LiveSessionOwnershipCheck,
    target: &SessionRef,
    policy: LoadPolicy,
) -> Result<SessionQueuePlan, ProviderSessionLoadOutcome> {
    use provider_session_machines::session_lifecycle_machine::{LoadAttempt, SessionLifecycle};
    if let Some(snapshot) = supervisor.owner_session_snapshot(target)
        && let Some(run) = queue_run(&snapshot, policy)
    {
        return Ok(SessionQueuePlan::OnRun(run));
    }
    if policy == LoadPolicy::LoadedOnly {
        return Err(ProviderSessionLoadOutcome::NotLoaded);
    }
    if !supervisor.has_binding(&target.endpoint) {
        return Err(unavailable("provider runtime is unavailable"));
    }
    match checked_loadability(supervisor, store, ownership, target).await {
        ProviderSessionLoadability::Loadable | ProviderSessionLoadability::AlreadyLoaded => {}
        ProviderSessionLoadability::UnsupportedLoad => {
            return Err(ProviderSessionLoadOutcome::UnsupportedLoad);
        }
        ProviderSessionLoadability::MissingRecord => {
            return Err(ProviderSessionLoadOutcome::MissingRecord);
        }
        ProviderSessionLoadability::LiveElsewhere => {
            return Err(ProviderSessionLoadOutcome::LiveElsewhere);
        }
        ProviderSessionLoadability::Unavailable { reason } => return Err(unavailable(reason)),
    }
    let owner = supervisor
        .session_owner_for(target)
        .map_err(|error| unavailable(error.to_string()))?;
    let before = owner.snapshots().borrow().clone();
    if let Some(run) = queue_run(&before, policy) {
        return Ok(SessionQueuePlan::OnRun(run));
    }
    let SessionLifecycle::NotLoaded { last } = before.lifecycle else {
        return Err(ProviderSessionLoadOutcome::NotLoaded);
    };
    let generation = last
        .number()
        .checked_add(1)
        .map(provider_session_machines::SessionGeneration::from_number)
        .ok_or_else(|| unavailable("the session has no remaining generation"))?;
    Ok(SessionQueuePlan::Restore {
        run: TransportRun {
            incarnation: before.owner.incarnation,
            generation,
        },
        attempt: LoadAttempt(uuid::Uuid::now_v7()),
    })
}

fn queue_run(
    snapshot: &provider_session_machines::session_owner::SessionSnapshot,
    policy: LoadPolicy,
) -> Option<TransportRun> {
    use provider_session_machines::session_lifecycle_machine::SessionLifecycle;
    let generation = match snapshot.lifecycle {
        SessionLifecycle::Live { generation } => generation,
        SessionLifecycle::Loading { generation, .. } if policy == LoadPolicy::MayLoad => generation,
        _ => return None,
    };
    Some(TransportRun {
        incarnation: snapshot.owner.incarnation,
        generation,
    })
}

fn owner_restore_outcome(
    result: provider_session_machines::session_owner::RestoreResult,
) -> ProviderSessionLoadOutcome {
    use provider_session_machines::session_owner::{
        RestoreDisposition, RestoreFailure as OwnerFailure,
    };
    let phase = result.phase;
    match result.disposition {
        RestoreDisposition::Restored { .. } => ProviderSessionLoadOutcome::Ready,
        RestoreDisposition::Retired {
            failure: OwnerFailure::Closed,
            ..
        } => ProviderSessionLoadOutcome::ClosedWhileLoading { phase },
        RestoreDisposition::Retired { failure, .. }
            if let Some(coded) = failure.coded_evidence() =>
        {
            coded_load_outcome(coded)
        }
        RestoreDisposition::Retired { failure, .. } => load_failure(phase, failure.next_step()),
    }
}
/// What a failed load means for the work that asked for it.
pub(crate) fn coded_load_outcome(failure: ProviderWireFailure) -> ProviderSessionLoadOutcome {
    let phase = RestorePhase::Attempted;
    match CodedRestoreFailure::from_wire(failure) {
        CodedRestoreFailure::Rejected {
            kind,
            code,
            correlation_id,
        } => {
            let reason = match kind {
                RestoreRejectionKind::SessionNotFound => {
                    ProviderSessionLoadRejection::SessionNotFound {
                        code,
                        correlation_id,
                    }
                }
                RestoreRejectionKind::ProviderRejected => {
                    ProviderSessionLoadRejection::ProviderRejected {
                        code,
                        correlation_id,
                    }
                }
            };
            ProviderSessionLoadOutcome::Rejected { phase, reason }
        }
        CodedRestoreFailure::Unavailable(reason) => load_failure(phase, reason),
    }
}

pub(crate) fn closed_restore_reason(phase: RestorePhase) -> &'static str {
    match phase {
        RestorePhase::Preflight => "session closed",
        RestorePhase::Attempted => {
            "the session closed during restore, so retry this session when ready"
        }
    }
}

pub(crate) async fn inspect_provider_session_loadability(
    supervisor: &ExternalProviderSupervisor,
    store: &Arc<Mutex<ProviderOperationStore>>,
    ownership: &dyn LiveSessionOwnershipCheck,
    target: &SessionRef,
) -> ProviderSessionLoadability {
    match supervisor.owner_session_activity(target) {
        ProviderSessionActivity::Idle | ProviderSessionActivity::Running => {
            return ProviderSessionLoadability::AlreadyLoaded;
        }
        ProviderSessionActivity::NotLoaded => {}
    }
    let Some(capabilities) = supervisor.session_capabilities(target).await else {
        return unavailable_loadability("provider runtime is unavailable".to_owned());
    };
    if !capabilities.load {
        return ProviderSessionLoadability::UnsupportedLoad;
    }
    match ownership.check(target).await {
        Ok(LiveSessionOwnership::NotLive) => {}
        Ok(LiveSessionOwnership::LiveWritable | LiveSessionOwnership::LiveUnsupported) => {
            return ProviderSessionLoadability::LiveElsewhere;
        }
        Err(error) => return unavailable_loadability(error.to_string()),
    }
    match store.lock().await.session_record(target).await {
        Ok(Some(_)) => ProviderSessionLoadability::Loadable,
        Ok(None) => ProviderSessionLoadability::MissingRecord,
        Err(error) => unavailable_loadability(error.to_string()),
    }
}

fn unavailable(reason: impl Into<String>) -> ProviderSessionLoadOutcome {
    load_failure(RestorePhase::Preflight, reason)
}

pub(crate) fn load_failure(
    phase: RestorePhase,
    reason: impl Into<String>,
) -> ProviderSessionLoadOutcome {
    ProviderSessionLoadOutcome::Unavailable {
        phase,
        reason: reason.into(),
    }
}

fn unavailable_loadability(reason: impl Into<String>) -> ProviderSessionLoadability {
    ProviderSessionLoadability::Unavailable {
        reason: reason.into(),
    }
}

#[cfg(test)]
#[path = "provider_acp_session_loading_test.rs"]
mod tests;
