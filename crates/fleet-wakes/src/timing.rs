//! Wake creation admission on Central's request clock sample.
use central_engine_model::{on_five_minute_grid as on_grid, supported_time as supported};
use fleet_timer_policy::lifetime_end;
use workspace_ids::SessionRef;
use workspace_model::central_mail::{CentralMailRefusal, TimingRequirement};
use workspace_model::{PostText, Timestamp};

/// A wake that was in the future when Central admitted its creation.
#[derive(Debug)]
pub struct FreshWakeCommand<'request> {
    target: &'request SessionRef,
    due_at: Timestamp,
    body: &'request PostText,
    admitted_at: Timestamp,
}

impl<'request> FreshWakeCommand<'request> {
    pub fn new(
        target: &'request SessionRef,
        due_at: Timestamp,
        body: &'request PostText,
        admitted_at: Timestamp,
    ) -> Result<Self, CentralMailRefusal> {
        validate_wake_due_time(due_at, admitted_at)?;
        Ok(Self {
            target,
            due_at,
            body,
            admitted_at,
        })
    }

    pub fn target(&self) -> &SessionRef {
        self.target
    }

    pub(super) fn due_at(&self) -> Timestamp {
        self.due_at
    }

    pub(super) fn body(&self) -> &PostText {
        self.body
    }

    pub(super) fn admitted_at(&self) -> Timestamp {
        self.admitted_at
    }
}

fn violation(field: &str, requirement: TimingRequirement) -> CentralMailRefusal {
    CentralMailRefusal::TimingViolation {
        field: field.to_owned(),
        requirement,
    }
}
fn out_of_range(field: &str) -> CentralMailRefusal {
    CentralMailRefusal::TimeOutOfRange {
        field: field.to_owned(),
    }
}

fn validate_wake_due_time(
    due_at: Timestamp,
    admitted_at: Timestamp,
) -> Result<(), CentralMailRefusal> {
    if !supported(admitted_at) {
        return Err(out_of_range("anchorAt"));
    }
    if !supported(due_at) {
        return Err(out_of_range("dueAt"));
    }
    if !on_grid(due_at) {
        return Err(violation("dueAt", TimingRequirement::FiveMinuteGrid));
    }
    if due_at <= admitted_at {
        return Err(violation("dueAt", TimingRequirement::FutureTime));
    }
    let expiry = lifetime_end(admitted_at).ok_or_else(|| out_of_range("anchorAt"))?;
    if due_at > expiry {
        return Err(violation("dueAt", TimingRequirement::WithinSevenDays));
    }
    Ok(())
}

#[cfg(test)]
#[path = "timing_test.rs"]
mod tests;
