use std::{collections::HashMap, future::Future, num::NonZeroU32, path::Path};

use crate::registry_process_start::{
    ProcessIncarnation, ProcessObservation, ProcessStart, parse_ps_start, read_ps_probe,
    spawn_ps_probe,
};

/// One process-table sample verifies each parent link and both endpoint
/// incarnations. This keeps unrelated process trees distinct even when their
/// session IDs agree. PID reuse within the same displayed second remains
/// indistinguishable at the precision of the existing registry check.
pub(super) async fn observe_strict_descendant<TReap: Future<Output = ()>>(
    worker: ProcessIncarnation,
    adapter: ProcessIncarnation,
    stop: impl Future<Output = ()>,
    reap_deadline: impl FnOnce() -> TReap,
) -> ProcessObservation<bool> {
    let Some(mut probe) = spawn_ps_probe(Path::new("/bin/ps"), ["-axo", "pid=,ppid=,lstart="])
    else {
        return ProcessObservation::Unavailable;
    };
    read_ps_probe(&mut probe, 512 * 1024, stop, reap_deadline, |output| {
        Some(ProcessAncestry::parse(output)?.proves(worker, adapter))
    })
    .await
}

struct ProcessAncestry(HashMap<NonZeroU32, ProcessParent>);

struct ProcessParent {
    parent: Option<NonZeroU32>,
    started: ProcessStart,
}

impl ProcessAncestry {
    fn parse(output: &str) -> Option<Self> {
        let mut processes = HashMap::new();
        for line in output.lines() {
            let (pid, rest) = line.trim_start().split_once(char::is_whitespace)?;
            let (parent, started) = rest.trim_start().split_once(char::is_whitespace)?;
            let Some(pid) = NonZeroU32::new(pid.parse().ok()?) else {
                continue;
            };
            let parent = NonZeroU32::new(parent.parse().ok()?);
            let started = ProcessStart(parse_ps_start(started)?);
            if processes
                .insert(pid, ProcessParent { parent, started })
                .is_some()
            {
                return None;
            }
        }
        Some(Self(processes))
    }

    fn proves(&self, worker: ProcessIncarnation, adapter: ProcessIncarnation) -> bool {
        let Some(observed_worker) = self.0.get(&worker.pid) else {
            return false;
        };
        if worker.pid == adapter.pid || observed_worker.started != worker.started {
            return false;
        }
        let mut child = observed_worker;
        // Bound cycles and unexpectedly deep trees without retaining any process table.
        for _ in 0..64 {
            let Some(parent_id) = child.parent else {
                return false;
            };
            let Some(parent) = self.0.get(&parent_id) else {
                return false;
            };
            if parent.started > child.started {
                return false;
            }
            if parent_id == adapter.pid {
                return parent.started == adapter.started;
            }
            child = parent;
        }
        false
    }
}

#[cfg(test)]
#[path = "process_ancestry_test.rs"]
mod tests;
