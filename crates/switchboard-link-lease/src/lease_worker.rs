use std::{future::pending, sync::Arc, time::Duration};

use router_clock::{ClockInstant, RuntimeClock};
use switchboard_central_link::{
    lease_source::{LeaseFailure, LinkLease},
    link_policy::{LeaseVersion, LeaseWindow},
};
use switchboard_machine_enrollment::{
    AccessRequest, BindingGeneration, CredentialStore, EnrollmentKey, LeaseUnavailable,
    RefreshOwner, RefreshSource,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{Id, JoinError, JoinSet},
};

use crate::{CLOSE_BUDGET, UnfinishedLeaseWork};

const RETRY_RETIREMENT: Duration = Duration::from_secs(5);

type LeaseAnswer = Result<LinkLease<EnrollmentKey>, LeaseFailure>;

pub(crate) struct LeaseCommand {
    pub(crate) key: EnrollmentKey,
    pub(crate) newer_than: Option<LeaseVersion>,
    pub(crate) reply: oneshot::Sender<LeaseAnswer>,
}

enum StartedWork {
    Acquire(EnrollmentKey),
    Retire(EnrollmentKey),
}

enum FinishedWork {
    Acquired,
    Retired,
    StillPending,
}

pub(crate) async fn run<Store, Source>(
    owner: RefreshOwner<Store, Source>,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
    mut commands: mpsc::UnboundedReceiver<LeaseCommand>,
    mut closing: watch::Receiver<Option<ClockInstant>>,
) -> UnfinishedLeaseWork
where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    let owner = Arc::new(owner);
    let fence = owner.retirement_fence();
    let mut changes = fence.subscribe();
    let mut tasks = JoinSet::new();
    let mut started = Vec::new();
    let mut final_attempts = Vec::new();
    let mut unfinished_acquisitions = Vec::new();
    let mut deadline = None;

    loop {
        if deadline.is_none() {
            deadline = *closing.borrow_and_update();
        }
        if let Some(cutoff) = deadline {
            commands.close();
            if clock.monotonic_now() >= cutoff {
                break;
            }
        }

        changes.borrow_and_update();
        for key in fence.pending_keys() {
            if final_attempts.contains(&key)
                || started
                    .iter()
                    .any(|(_, work)| matches!(work, StartedWork::Retire(active) if active == &key))
            {
                continue;
            }
            let owner = Arc::clone(&owner);
            let clock = Arc::clone(&clock);
            let closing = closing.clone();
            let task_key = key.clone();
            let task = tasks
                .spawn(async move { retire(&owner, clock.as_ref(), &task_key, closing).await });
            started.push((task.id(), StartedWork::Retire(key)));
        }

        if deadline.is_some() && tasks.is_empty() {
            break;
        }
        tokio::select! {
            biased;
            result = closing.changed(), if deadline.is_none() => {
                if result.is_err() {
                    deadline = Some(clock.monotonic_now().after(CLOSE_BUDGET));
                }
            }
            () = async {
                if let Some(cutoff) = deadline {
                    clock.sleep_until(cutoff).await;
                } else {
                    pending().await
                }
            } => break,
            result = tasks.join_next_with_id(), if !tasks.is_empty() => {
                if let Some(result) = result {
                    settle(result, &mut started, &mut unfinished_acquisitions, &mut final_attempts);
                }
            }
            _ = changes.changed() => {}
            command = commands.recv(), if deadline.is_none() => match command {
                Some(command) => dispatch(command, &owner, &closing, &mut tasks, &mut started),
                None => deadline = Some(clock.monotonic_now().after(CLOSE_BUDGET)),
            },
        }
    }

    tasks.abort_all();
    while let Some(result) = tasks.try_join_next_with_id() {
        settle(
            result,
            &mut started,
            &mut unfinished_acquisitions,
            &mut final_attempts,
        );
    }
    for (_, work) in started {
        if let StartedWork::Acquire(key) = work {
            remember(&mut unfinished_acquisitions, key);
        }
    }
    UnfinishedLeaseWork {
        acquisitions: unfinished_acquisitions,
        retirements: fence.pending_keys(),
    }
}

fn dispatch<Store, Source>(
    command: LeaseCommand,
    owner: &Arc<RefreshOwner<Store, Source>>,
    closing: &watch::Receiver<Option<ClockInstant>>,
    tasks: &mut JoinSet<FinishedWork>,
    started: &mut Vec<(Id, StartedWork)>,
) where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    let LeaseCommand {
        key,
        newer_than,
        reply,
    } = command;
    let closing = closing.borrow();
    if closing.is_some() || reply.is_closed() {
        return;
    }
    if owner.retirement_fence().is_fenced(&key) {
        let _ = reply.send(Err(LeaseFailure::EnrollmentEnded));
        return;
    }
    let owner = Arc::clone(owner);
    let task_key = key.clone();
    let task = tasks.spawn(async move {
        let _ = reply.send(acquire(&owner, &task_key, newer_than).await);
        FinishedWork::Acquired
    });
    started.push((task.id(), StartedWork::Acquire(key)));
}

async fn acquire<Store, Source>(
    owner: &RefreshOwner<Store, Source>,
    key: &EnrollmentKey,
    newer_than: Option<LeaseVersion>,
) -> LeaseAnswer
where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    let request = match newer_than {
        Some(version) => AccessRequest::NewerThan(
            BindingGeneration::from_persisted(version.0).ok_or(LeaseFailure::Fault)?,
        ),
        None => AccessRequest::Usable,
    };
    let lease = owner.lease_for(key, request).await.map_err(lease_failure)?;
    if &lease.key != key {
        return Err(retry_after(1));
    }
    Ok(LinkLease {
        window: LeaseWindow {
            binding: lease.key,
            version: LeaseVersion(lease.binding_generation.get()),
            usable_until: lease.usable_until,
        },
        token: lease
            .token
            .expose_secret()
            .to_owned()
            .try_into()
            .map_err(|_| LeaseFailure::Fault)?,
    })
}

fn lease_failure(error: LeaseUnavailable) -> LeaseFailure {
    match error {
        LeaseUnavailable::NotEnrolled
        | LeaseUnavailable::DifferentEnrollment
        | LeaseUnavailable::StaleAccessGrant => retry_after(1),
        LeaseUnavailable::Ended { .. } => LeaseFailure::EnrollmentEnded,
        LeaseUnavailable::SignInUnreachable { retry_after } => LeaseFailure::Unavailable {
            retry_after: retry_after.unwrap_or(Duration::from_secs(30)),
        },
        LeaseUnavailable::Store(_) => retry_after(5),
        LeaseUnavailable::OversizedRefreshReply | LeaseUnavailable::Configuration(_) => {
            LeaseFailure::Fault
        }
    }
}

const fn retry_after(seconds: u64) -> LeaseFailure {
    LeaseFailure::Unavailable {
        retry_after: Duration::from_secs(seconds),
    }
}

async fn retire<Store, Source>(
    owner: &RefreshOwner<Store, Source>,
    clock: &dyn RuntimeClock,
    key: &EnrollmentKey,
    mut closing: watch::Receiver<Option<ClockInstant>>,
) -> FinishedWork
where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    loop {
        let final_attempt = closing.borrow_and_update().is_some() || closing.has_changed().is_err();
        match owner.finish_retirement(key).await {
            Ok(_) => return FinishedWork::Retired,
            Err(_) if final_attempt => return FinishedWork::StillPending,
            Err(_) => {}
        }
        tokio::select! {
            biased;
            _ = closing.changed() => {}
            () = clock.sleep_for(RETRY_RETIREMENT) => {}
        }
    }
}

fn settle(
    result: Result<(Id, FinishedWork), JoinError>,
    started: &mut Vec<(Id, StartedWork)>,
    unfinished_acquisitions: &mut Vec<EnrollmentKey>,
    final_attempts: &mut Vec<EnrollmentKey>,
) {
    let (id, result) = match result {
        Ok((id, work)) => (id, Some(work)),
        Err(error) => (error.id(), None),
    };
    let Some(index) = started.iter().position(|(started, _)| *started == id) else {
        return;
    };
    let (_, work) = started.swap_remove(index);
    match (work, result) {
        (StartedWork::Acquire(key), None) => remember(unfinished_acquisitions, key),
        (StartedWork::Retire(key), Some(FinishedWork::StillPending)) => {
            remember(final_attempts, key);
        }
        _ => {}
    }
}

fn remember(keys: &mut Vec<EnrollmentKey>, key: EnrollmentKey) {
    if !keys.contains(&key) {
        keys.push(key);
    }
}

#[cfg(test)]
#[path = "lease_mapping_test.rs"]
mod tests;
