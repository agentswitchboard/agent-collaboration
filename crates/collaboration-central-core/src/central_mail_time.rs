//! Fresh mail request timing admission.
use central_engine_model::supported_time;
use fleet_wakes::FreshWakeCommand;
use workspace_model::Timestamp;
use workspace_model::central_mail::{CentralMailOperation, CentralMailRefusal};

/// Last integer millisecond accepted by the portable alarm clock.
pub const MAX_ALARM_UNIX_MILLIS: i64 = central_engine_model::MAX_ALARM_UNIX_MILLIS;

/// Admit a fresh request at Central's clock sample and decode its wake command, if any.
pub(crate) fn decode_fresh_request(
    operation: &CentralMailOperation,
    now: Timestamp,
) -> Result<Option<FreshWakeCommand<'_>>, CentralMailRefusal> {
    if let CentralMailOperation::SetWake {
        target,
        due_at,
        text,
    } = operation
    {
        return FreshWakeCommand::new(target, *due_at, text, now).map(Some);
    }
    match refusal(operation, now) {
        Some(reason) => Err(reason),
        None => Ok(None),
    }
}

pub(crate) fn refusal(
    operation: &CentralMailOperation,
    now: Timestamp,
) -> Option<CentralMailRefusal> {
    if !supported_time(now) {
        return Some(CentralMailRefusal::TimeOutOfRange {
            field: "anchorAt".to_owned(),
        });
    }
    match operation {
        CentralMailOperation::SetAutomation { timing, .. } => {
            fleet_schedules::set_automation_refusal(timing, now)
        }
        CentralMailOperation::SendMessage { sent_at, .. }
            if !supported_time(fleet_mail::message_deadline::direct_receipt_mark(
                *sent_at, now,
            )) =>
        {
            Some(CentralMailRefusal::TimeOutOfRange {
                field: "anchorAt".to_owned(),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "central_mail_time_test.rs"]
mod tests;
