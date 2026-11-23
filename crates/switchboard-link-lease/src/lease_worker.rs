use std::{sync::Arc, time::Duration};

use router_clock::{RuntimeClock, timeout_at};
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

use crate::UnfinishedLeaseWork;

const RETRY_RETIREMENT: Duration = Duration::from_secs(5);
const CLOSE_BUDGET: Duration = Duration::from_secs(5);

type LeaseAnswer = Result<LinkLease<EnrollmentKey>, LeaseFailure>;

pub(crate) enum LeaseCommand {
    Acquire {
        key: EnrollmentKey,
        newer_than: Option<LeaseVersion>,
        reply: oneshot::Sender<LeaseAnswer>,
    },
    Retire {
        key: EnrollmentKey,
    },
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
    mut closing: watch::Receiver<bool>,
) -> UnfinishedLeaseWork
where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    let owner = Arc::new(owner);
    let mut tasks = JoinSet::new();
    let mut started = Vec::new();
    let mut unfinished = UnfinishedLeaseWork::default();
    loop {
        if *closing.borrow_and_update() {
            break;
        }
        tokio::select! {
            biased;
            _ = closing.changed() => break,
            result = tasks.join_next_with_id(), if !tasks.is_empty() => {
                if let Some(result) = result {
                    settle(result, &mut started, &mut unfinished);
                }
            }
            command = commands.recv() => match command {
                Some(command) => dispatch(command, &owner, &clock, &closing, &mut tasks, &mut started),
                None => break,
            },
        }
    }

    let deadline = clock.monotonic_now().after(CLOSE_BUDGET);
    commands.close();
    while let Some(command) = commands.recv().await {
        if matches!(command, LeaseCommand::Retire { .. }) {
            dispatch(command, &owner, &clock, &closing, &mut tasks, &mut started);
        }
    }

    let drained = timeout_at(clock.as_ref(), deadline, async {
        while let Some(result) = tasks.join_next_with_id().await {
            settle(result, &mut started, &mut unfinished);
        }
    })
    .await;
    if drained.is_err() {
        tasks.shutdown().await;
    }
    for (_, work) in started {
        remember_unfinished(&mut unfinished, work);
    }
    unfinished
}

fn dispatch<Store, Source>(
    command: LeaseCommand,
    owner: &Arc<RefreshOwner<Store, Source>>,
    clock: &Arc<dyn RuntimeClock + Send + Sync>,
    closing: &watch::Receiver<bool>,
    tasks: &mut JoinSet<FinishedWork>,
    started: &mut Vec<(Id, StartedWork)>,
) where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    match command {
        LeaseCommand::Acquire {
            key,
            newer_than,
            reply,
        } => {
            let closing = closing.borrow();
            if *closing || reply.is_closed() {
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
        LeaseCommand::Retire { key } => {
            let owner = Arc::clone(owner);
            let clock = Arc::clone(clock);
            let closing = closing.clone();
            let task_key = key.clone();
            let task = tasks
                .spawn(async move { retire(&owner, clock.as_ref(), &task_key, closing).await });
            started.push((task.id(), StartedWork::Retire(key)));
        }
    }
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
    mut closing: watch::Receiver<bool>,
) -> FinishedWork
where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    loop {
        let final_attempt = *closing.borrow_and_update() || closing.has_changed().is_err();
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
    unfinished: &mut UnfinishedLeaseWork,
) {
    let (id, result) = match result {
        Ok((id, work)) => (id, Some(work)),
        Err(error) => (error.id(), None),
    };
    let Some(index) = started.iter().position(|(started, _)| *started == id) else {
        return;
    };
    let (_, work) = started.swap_remove(index);
    if matches!(result, None | Some(FinishedWork::StillPending)) {
        remember_unfinished(unfinished, work);
    }
}

fn remember_unfinished(report: &mut UnfinishedLeaseWork, work: StartedWork) {
    let (keys, key) = match work {
        StartedWork::Acquire(key) => (&mut report.acquisitions, key),
        StartedWork::Retire(key) => (&mut report.retirements, key),
    };
    if !keys.contains(&key) {
        keys.push(key);
    }
}

#[cfg(test)]
#[path = "lease_mapping_test.rs"]
mod tests;
