use std::{
    collections::BTreeSet,
    future::{Future, pending, poll_fn},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use router_clock::{ClockInstant, RuntimeClock};
use switchboard_central_link::{
    lease_source::{LeaseFailure, LinkLease},
    link_policy::{LeaseVersion, LeaseWindow},
};
use switchboard_machine_enrollment::{
    AccessRequest, BindingGeneration, CredentialStore, EnrollmentId, EnrollmentKey,
    LeaseUnavailable, RefreshOwner, RefreshSource,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{Id, JoinError, JoinSet},
};

use crate::LeaseWorkerFailure;

const RETRY_RETIREMENT: Duration = Duration::from_secs(5);

type LeaseAnswer = Result<LinkLease<EnrollmentKey>, LeaseFailure>;

pub(crate) struct LeaseCommand {
    pub(crate) key: EnrollmentKey,
    pub(crate) newer_than: Option<LeaseVersion>,
    pub(crate) reply: oneshot::Sender<LeaseAnswer>,
}

enum StartedWork {
    Acquire,
    Retire(EnrollmentKey),
}

enum FinishedWork {
    Acquired,
    Retired,
    StillPending,
}

struct ChildTasks(Mutex<JoinSet<FinishedWork>>);

impl ChildTasks {
    fn new() -> Self {
        Self(Mutex::new(JoinSet::new()))
    }

    fn spawn(&self, task: impl Future<Output = FinishedWork> + Send + 'static) -> Id {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .spawn(task)
            .id()
    }

    fn is_empty(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }

    async fn join_next(&self) -> Option<Result<(Id, FinishedWork), JoinError>> {
        poll_fn(|context| {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .poll_join_next_with_id(context)
        })
        .await
    }

    async fn reap(
        &self,
        clock: &dyn RuntimeClock,
        mut closing: watch::Receiver<Option<ClockInstant>>,
    ) {
        let mut stop_open = true;
        while !self.is_empty() {
            let cutoff = observed_cutoff(clock, &mut closing);
            if cutoff.is_some_and(|cutoff| clock.monotonic_now() >= cutoff) {
                self.0
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .abort_all();
                while self.join_next().await.is_some() {}
                return;
            }
            tokio::select! {
                biased;
                result = closing.changed(), if stop_open => stop_open = result.is_ok(),
                _ = self.join_next() => {}
                () = wait_until(clock, cutoff) => {}
            }
        }
    }
}

pub(crate) async fn run<Store, Source>(
    owner: RefreshOwner<Store, Source>,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
    commands: mpsc::UnboundedReceiver<LeaseCommand>,
    closing: watch::Receiver<Option<ClockInstant>>,
    excluded: Arc<BTreeSet<EnrollmentId>>,
) -> Result<(), LeaseWorkerFailure>
where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    let children = Arc::new(ChildTasks::new());
    let worker = tokio::spawn(work(
        owner,
        clock.clone(),
        commands,
        closing.clone(),
        excluded,
        children.clone(),
    ));
    reap_worker(worker, &children, clock.as_ref(), closing).await
}

async fn reap_worker(
    worker: tokio::task::JoinHandle<()>,
    children: &ChildTasks,
    clock: &dyn RuntimeClock,
    closing: watch::Receiver<Option<ClockInstant>>,
) -> Result<(), LeaseWorkerFailure> {
    let outcome = worker.await.map_err(LeaseWorkerFailure::from);
    // Child ownership survives a failed worker and only the supplied cutoff
    // can cancel unfinished tasks while the adapter is still retained.
    children.reap(clock, closing).await;
    outcome
}

async fn work<Store, Source>(
    owner: RefreshOwner<Store, Source>,
    clock: Arc<dyn RuntimeClock + Send + Sync>,
    mut commands: mpsc::UnboundedReceiver<LeaseCommand>,
    mut closing: watch::Receiver<Option<ClockInstant>>,
    excluded: Arc<BTreeSet<EnrollmentId>>,
    tasks: Arc<ChildTasks>,
) where
    Store: CredentialStore + 'static,
    Source: RefreshSource + 'static,
{
    let owner = Arc::new(owner);
    let fence = owner.retirement_fence();
    let mut changes = fence.subscribe();
    let mut started = Vec::new();
    let mut final_attempts = Vec::new();
    let mut stop_open = true;

    loop {
        let cutoff = observed_cutoff(clock.as_ref(), &mut closing);
        if let Some(cutoff) = cutoff {
            commands.close();
            if clock.monotonic_now() >= cutoff {
                return;
            }
        }

        changes.borrow_and_update();
        for key in fence.pending_keys() {
            if excluded.contains(&key.enrollment_id)
                || final_attempts.contains(&key)
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
            let id = tasks
                .spawn(async move { retire(&owner, clock.as_ref(), &task_key, closing).await });
            started.push((id, StartedWork::Retire(key)));
        }
        if cutoff.is_some() && tasks.is_empty() {
            return;
        }
        tokio::select! {
            biased;
            result = closing.changed(), if stop_open => stop_open = result.is_ok(),
            () = wait_until(clock.as_ref(), cutoff) => {}
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(result) = result {
                    settle(result, &mut started, &mut final_attempts);
                }
            }
            _ = changes.changed() => {}
            command = commands.recv(), if cutoff.is_none() => match command {
                Some(command) => dispatch(command, &owner, &closing, &excluded, &tasks, &mut started),
                None => return,
            },
        }
    }
}

fn observed_cutoff(
    clock: &dyn RuntimeClock,
    closing: &mut watch::Receiver<Option<ClockInstant>>,
) -> Option<ClockInstant> {
    let cutoff = *closing.borrow_and_update();
    cutoff.or_else(|| {
        closing
            .has_changed()
            .is_err()
            .then(|| clock.monotonic_now())
    })
}

async fn wait_until(clock: &dyn RuntimeClock, cutoff: Option<ClockInstant>) {
    if let Some(cutoff) = cutoff {
        clock.sleep_until(cutoff).await;
    } else {
        pending().await
    }
}

fn dispatch<Store, Source>(
    command: LeaseCommand,
    owner: &Arc<RefreshOwner<Store, Source>>,
    closing: &watch::Receiver<Option<ClockInstant>>,
    excluded: &BTreeSet<EnrollmentId>,
    tasks: &ChildTasks,
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
    if excluded.contains(&key.enrollment_id) || owner.retirement_fence().is_fenced(&key) {
        let _ = reply.send(Err(LeaseFailure::EnrollmentEnded));
        return;
    }
    let owner = Arc::clone(owner);
    let id = tasks.spawn(async move {
        let _ = reply.send(acquire(&owner, &key, newer_than).await);
        FinishedWork::Acquired
    });
    started.push((id, StartedWork::Acquire));
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
    if let (StartedWork::Retire(key), Some(FinishedWork::StillPending)) = (work, result) {
        final_attempts.push(key);
    }
}

#[cfg(test)]
#[path = "lease_mapping_test.rs"]
mod tests;

#[cfg(test)]
#[path = "lease_worker_join_test.rs"]
mod join_tests;
